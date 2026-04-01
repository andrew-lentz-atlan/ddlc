"""ServiceNow API client — OAuth 2.0 auth, catalog item schema fetch, request submission."""
from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from app.access.models import (
    SNOW_TYPE_MAP,
    ChoiceOption,
    FieldType,
    FormSchema,
    FormVariable,
    ServiceNowConfig,
    SubmitResponse,
)

log = logging.getLogger(__name__)


class ServiceNowClient:
    """Thin wrapper around ServiceNow REST APIs."""

    def __init__(self, config: ServiceNowConfig) -> None:
        self._config = config
        self._base = config.instance_url.rstrip("/")
        self._token: str | None = None
        self._token_expires: float = 0
        # Use basic auth when OAuth client_id is not configured (dev instances)
        self._use_basic_auth = bool(config.username and config.password and not config.client_id)

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _needs_token(self) -> bool:
        return self._token is None or time.time() >= self._token_expires

    def _get_token(self) -> str:
        """Fetch or return a cached OAuth 2.0 token."""
        if not self._needs_token():
            return self._token  # type: ignore[return-value]

        url = f"{self._base}/oauth_token.do"
        data: dict[str, str] = {
            "grant_type": "password",
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
            "username": self._config.username,
            "password": self._config.password,
        }
        resp = httpx.post(url, data=data, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        self._token = body["access_token"]
        # Refresh at 80 % of TTL
        self._token_expires = time.time() + int(body.get("expires_in", 1800)) * 0.8
        return self._token  # type: ignore[return-value]

    def _auth_kwargs(self) -> dict[str, Any]:
        """Return auth kwargs for httpx requests (basic auth or bearer token)."""
        if self._use_basic_auth:
            return {"auth": (self._config.username, self._config.password)}
        return {}

    def _headers(self) -> dict[str, str]:
        h: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if not self._use_basic_auth:
            h["Authorization"] = f"Bearer {self._get_token()}"
        return h

    # ------------------------------------------------------------------
    # Catalog item + variables
    # ------------------------------------------------------------------

    def get_catalog_item(self, sys_id: str) -> dict[str, Any]:
        """GET /api/sn_sc/servicecatalog/items/{sys_id}"""
        url = f"{self._base}/api/sn_sc/servicecatalog/items/{sys_id}"
        resp = httpx.get(url, headers=self._headers(), timeout=30, **self._auth_kwargs())
        resp.raise_for_status()
        return resp.json().get("result", {})

    def _get_variables_via_table_api(self, catalog_item_sys_id: str) -> list[dict]:
        """Fallback: GET /api/now/table/item_option_new?cat_item=..."""
        url = f"{self._base}/api/now/table/item_option_new"
        params = {
            "sysparm_query": f"cat_item={catalog_item_sys_id}^active=true",
            "sysparm_display_value": "true",
            "sysparm_fields": "sys_id,name,question_text,type,mandatory,default_value,order,read_only,help_text",
        }
        resp = httpx.get(url, headers=self._headers(), params=params, timeout=30, **self._auth_kwargs())
        resp.raise_for_status()
        return resp.json().get("result", [])

    def _get_choices_for_variable(self, variable_sys_id: str) -> list[ChoiceOption]:
        """GET /api/now/table/question_choice?question=..."""
        url = f"{self._base}/api/now/table/question_choice"
        params = {
            "sysparm_query": f"question={variable_sys_id}^active=true",
            "sysparm_display_value": "true",
            "sysparm_fields": "value,text,order",
            "sysparm_orderby": "order",
        }
        resp = httpx.get(url, headers=self._headers(), params=params, timeout=30, **self._auth_kwargs())
        resp.raise_for_status()
        results = resp.json().get("result", [])
        return [ChoiceOption(value=r["value"], label=r["text"]) for r in results]

    def get_form_schema(self, sys_id: str) -> FormSchema:
        """Build a FormSchema by combining catalog item info + variables + choices."""
        # 1. Get catalog item details
        item = self.get_catalog_item(sys_id)

        # 2. Try variables from the catalog API response
        raw_vars = item.get("variables", [])
        from_catalog_api = bool(raw_vars)

        # 3. If empty, fall back to Table API
        if not raw_vars:
            raw_vars = self._get_variables_via_table_api(sys_id)

        # 4. Flatten container children (e.g. checkbox_container wraps checkboxes)
        flat_vars: list[dict] = []
        for rv in raw_vars:
            children = rv.get("children", [])
            if children:
                flat_vars.extend(children)
            else:
                flat_vars.append(rv)

        # 5. Build FormVariable list
        variables: list[FormVariable] = []
        for rv in flat_vars:
            var_type_code = int(rv.get("type", 6))
            field_type = SNOW_TYPE_MAP.get(var_type_code, FieldType.TEXT)

            # Skip label / break / container types
            if field_type == FieldType.LABEL:
                continue

            name = rv.get("name", rv.get("sys_id", ""))
            # Skip system-generated variables with no name
            if not name:
                continue

            var = FormVariable(
                name=name,
                label=rv.get("question_text", rv.get("label", name)),
                field_type=field_type,
                mandatory=rv.get("mandatory", "") in (True, "true", "1"),
                default_value=str(rv.get("default_value", rv.get("value", "") or "")),
                order=int(rv.get("order", 0)),
                read_only=rv.get("read_only", "") in (True, "true", "1"),
                help_text=rv.get("help_text", ""),
            )

            # Extract choices for select / radio / multi-select types
            if field_type in (FieldType.SELECT, FieldType.RADIO, FieldType.MULTI_SELECT):
                if from_catalog_api:
                    # Catalog API includes choices inline
                    inline_choices = rv.get("choices", [])
                    if inline_choices:
                        var.choices = [
                            ChoiceOption(value=c["value"], label=c["label"])
                            for c in inline_choices
                        ]
                else:
                    # Table API: fetch choices separately
                    var_sys_id = rv.get("sys_id", "")
                    if var_sys_id:
                        var.choices = self._get_choices_for_variable(var_sys_id)

            variables.append(var)

        # Sort by order, filter out negative-order system variables
        variables = [v for v in variables if v.order >= 0]
        variables.sort(key=lambda v: v.order)

        return FormSchema(
            catalog_item_sys_id=sys_id,
            catalog_item_name=item.get("name", item.get("short_description", "Service Request")),
            short_description=item.get("short_description", ""),
            variables=variables,
        )

    # ------------------------------------------------------------------
    # Submit request
    # ------------------------------------------------------------------

    def submit_request(self, sys_id: str, variables: dict[str, Any]) -> SubmitResponse:
        """POST /api/sn_sc/servicecatalog/items/{sys_id}/order_now"""
        url = f"{self._base}/api/sn_sc/servicecatalog/items/{sys_id}/order_now"

        # Strip internal variables (prefixed with __) that aren't ServiceNow fields
        clean_vars = {k: v for k, v in variables.items() if not k.startswith("__")}

        # Convert boolean values to strings for ServiceNow
        for k, v in clean_vars.items():
            if isinstance(v, bool):
                clean_vars[k] = "true" if v else "false"

        payload = {
            "sysparm_quantity": 1,
            "variables": clean_vars,
        }
        try:
            resp = httpx.post(url, headers=self._headers(), json=payload, timeout=30, **self._auth_kwargs())
            resp.raise_for_status()
            result = resp.json().get("result", {})
            return SubmitResponse(
                success=True,
                request_number=result.get("request_number", ""),
                request_item_number=result.get("request_item_number", result.get("number", "")),
            )
        except httpx.HTTPStatusError as exc:
            log.error(f"ServiceNow submission failed: {exc.response.text}")
            return SubmitResponse(
                success=False,
                error=f"ServiceNow returned HTTP {exc.response.status_code}",
            )
        except Exception as exc:
            log.error(f"ServiceNow submission error: {exc}")
            return SubmitResponse(success=False, error="Failed to connect to ServiceNow")

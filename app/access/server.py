"""Access App — ServiceNow integration API routes.

Provides an APIRouter with endpoints for:
  - Form schema fetch (dynamic from ServiceNow)
  - Request submission to ServiceNow
  - DIGIC data product lookup (cascading dropdowns)
  - Health check

Mounted by AccessServer in access_main.py.
"""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException

from app.access.config import load_config, resolve_catalog_item
from app.access.models import (
    ChoiceOption,
    FieldType,
    FormSchema,
    FormVariable,
    SubmitRequest,
    SubmitResponse,
)

log = logging.getLogger(__name__)

_HERE = Path(__file__).parent

router = APIRouter()


# ---------------------------------------------------------------------------
# ServiceNow client (lazy init)
# ---------------------------------------------------------------------------

_snow_client = None


def _get_snow_client():
    global _snow_client
    if _snow_client is None:
        from app.access.servicenow import ServiceNowClient

        cfg = load_config()
        _snow_client = ServiceNowClient(cfg.servicenow)
    return _snow_client


def _is_configured() -> bool:
    cfg = load_config()
    sn = cfg.servicenow
    return bool(sn.instance_url and (sn.client_id or (sn.username and sn.password)))


# ---------------------------------------------------------------------------
# Demo / mock data (used when ServiceNow is not configured)
# ---------------------------------------------------------------------------


def _mock_form_schema(qn: str) -> FormSchema:
    """Return a realistic demo form when ServiceNow creds are not set."""
    return FormSchema(
        catalog_item_sys_id="demo-catalog-item",
        catalog_item_name="Data Product Access Request",
        short_description="Request access to a data product's underlying AD group",
        variables=[
            FormVariable(
                name="requested_for",
                label="Requested For",
                field_type=FieldType.TEXT,
                mandatory=True,
                help_text="Person who needs access",
                order=100,
            ),
            FormVariable(
                name="email",
                label="Email",
                field_type=FieldType.EMAIL,
                mandatory=True,
                order=200,
            ),
            FormVariable(
                name="business_justification",
                label="Business Justification",
                field_type=FieldType.TEXTAREA,
                mandatory=True,
                help_text="Explain why you need access to this data product",
                order=300,
            ),
            FormVariable(
                name="access_level",
                label="Access Level",
                field_type=FieldType.SELECT,
                mandatory=True,
                choices=[
                    ChoiceOption(value="read", label="Read Only"),
                    ChoiceOption(value="read_write", label="Read / Write"),
                    ChoiceOption(value="admin", label="Admin"),
                ],
                order=400,
            ),
            FormVariable(
                name="duration",
                label="Access Duration",
                field_type=FieldType.SELECT,
                mandatory=True,
                choices=[
                    ChoiceOption(value="30", label="30 Days"),
                    ChoiceOption(value="90", label="90 Days"),
                    ChoiceOption(value="180", label="180 Days"),
                    ChoiceOption(value="permanent", label="Permanent"),
                ],
                order=500,
            ),
            FormVariable(
                name="start_date",
                label="Requested Start Date",
                field_type=FieldType.DATE,
                order=600,
            ),
            FormVariable(
                name="manager_approval",
                label="Manager has approved this request",
                field_type=FieldType.CHECKBOX,
                mandatory=True,
                order=700,
            ),
            FormVariable(
                name="additional_notes",
                label="Additional Notes",
                field_type=FieldType.TEXTAREA,
                order=800,
            ),
        ],
    )


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


@router.get("/api/health")
async def health():
    return {"status": "ok", "configured": _is_configured()}


@router.get("/api/digic-data")
async def get_digic_data():
    """Return DIGIC data product -> role -> description mappings for cascading dropdowns."""
    digic_path = _HERE / "digic_data.json"
    if digic_path.exists():
        return json.loads(digic_path.read_text())
    return {}


@router.get("/api/form-schema")
async def get_form_schema(qn: str = ""):
    """Resolve QN -> catalog item, fetch form fields from ServiceNow (or return mock)."""
    if not _is_configured():
        return _mock_form_schema(qn).model_dump()

    catalog_item_sys_id = resolve_catalog_item(qn)
    if not catalog_item_sys_id:
        raise HTTPException(
            status_code=404,
            detail="No catalog item mapping found for this data product",
        )

    try:
        client = _get_snow_client()
        schema = await asyncio.to_thread(client.get_form_schema, catalog_item_sys_id)
        return schema.model_dump()
    except Exception as exc:
        log.error(f"Failed to fetch form schema: {exc}")
        raise HTTPException(status_code=502, detail=f"ServiceNow API error: {exc}")


@router.post("/api/submit-request")
async def submit_request(body: SubmitRequest):
    """Submit access request to ServiceNow."""
    if not _is_configured():
        return SubmitResponse(
            success=True,
            request_number="REQ0012345",
            request_item_number="RITM0067890",
        ).model_dump()

    try:
        client = _get_snow_client()
        result = await asyncio.to_thread(
            client.submit_request, body.catalog_item_sys_id, body.variables
        )
        return result.model_dump()
    except Exception as exc:
        log.error(f"Submit failed: {exc}")
        raise HTTPException(status_code=502, detail=f"ServiceNow API error: {exc}")

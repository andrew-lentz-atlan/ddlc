"""
Atlan metadata integration for DDLC.

Uses pyatlan FluentSearch to browse data products, tables, columns,
and other assets from the Atlan catalog. Falls back gracefully if
credentials are not configured.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from app.ddlc.models import LogicalType

# ---------------------------------------------------------------------------
# Atlan type → DDLC logical type mapping
# ---------------------------------------------------------------------------

_TYPE_MAP: dict[str, LogicalType] = {
    "STRING": LogicalType.STRING,
    "VARCHAR": LogicalType.STRING,
    "CHAR": LogicalType.STRING,
    "TEXT": LogicalType.STRING,
    "NVARCHAR": LogicalType.STRING,
    "INT": LogicalType.INTEGER,
    "INTEGER": LogicalType.INTEGER,
    "BIGINT": LogicalType.INTEGER,
    "SMALLINT": LogicalType.INTEGER,
    "TINYINT": LogicalType.INTEGER,
    "NUMBER": LogicalType.NUMBER,
    "NUMERIC": LogicalType.NUMBER,
    "DECIMAL": LogicalType.NUMBER,
    "FLOAT": LogicalType.NUMBER,
    "DOUBLE": LogicalType.NUMBER,
    "REAL": LogicalType.NUMBER,
    "BOOLEAN": LogicalType.BOOLEAN,
    "BOOL": LogicalType.BOOLEAN,
    "DATE": LogicalType.DATE,
    "TIMESTAMP": LogicalType.TIMESTAMP,
    "TIMESTAMP_NTZ": LogicalType.TIMESTAMP,
    "TIMESTAMP_LTZ": LogicalType.TIMESTAMP,
    "TIMESTAMP_TZ": LogicalType.TIMESTAMP,
    "DATETIME": LogicalType.TIMESTAMP,
    "TIME": LogicalType.TIME,
    "ARRAY": LogicalType.ARRAY,
    "OBJECT": LogicalType.OBJECT,
    "VARIANT": LogicalType.OBJECT,
    "JSON": LogicalType.OBJECT,
}


def map_atlan_type(raw: str | None) -> LogicalType:
    """Map an Atlan/SQL data type string to a DDLC LogicalType."""
    if not raw:
        return LogicalType.STRING
    upper = raw.upper().split("(")[0].strip()  # strip precision e.g. VARCHAR(256)
    if upper in _TYPE_MAP:
        return _TYPE_MAP[upper]
    for key, val in _TYPE_MAP.items():
        if key in upper:
            return val
    return LogicalType.STRING


# ---------------------------------------------------------------------------
# Client management
# ---------------------------------------------------------------------------

_client = None


def _get_client():
    """Get or create a pyatlan AtlanClient. Raises if not configured."""
    global _client
    if _client is not None:
        return _client

    from pyatlan.client.atlan import AtlanClient

    base_url = os.getenv("ATLAN_BASE_URL")
    api_key = os.getenv("ATLAN_API_KEY")
    if not base_url or not api_key:
        raise RuntimeError(
            "Atlan credentials not configured. "
            "Set ATLAN_BASE_URL and ATLAN_API_KEY environment variables."
        )
    _client = AtlanClient(base_url=base_url, api_key=api_key)
    return _client


def is_configured() -> bool:
    """Check if Atlan credentials are available."""
    return bool(os.getenv("ATLAN_BASE_URL")) and bool(os.getenv("ATLAN_API_KEY"))


# ---------------------------------------------------------------------------
# Search / browse functions
# ---------------------------------------------------------------------------


def search_assets(
    query: str,
    asset_type: str = "Table",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Search Atlan for assets matching a query string.

    Returns lightweight dicts suitable for JSON serialization.
    """
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    from pyatlan.model.assets import Table, View, MaterialisedView, Column

    client = _get_client()

    type_map = {
        "Table": Table,
        "View": View,
        "MaterialisedView": MaterialisedView,
    }
    asset_cls = type_map.get(asset_type, Table)

    request = (
        FluentSearch()
        .where(CompoundQuery.asset_type(asset_cls))
        .where(CompoundQuery.active_assets())
        .where(asset_cls.NAME.match(query))
        .page_size(limit)
        .include_on_results(asset_cls.NAME)
        .include_on_results(asset_cls.DESCRIPTION)
        .include_on_results(asset_cls.DATABASE_NAME)
        .include_on_results(asset_cls.SCHEMA_NAME)
        .include_on_results(asset_cls.QUALIFIED_NAME)
        .include_on_results(asset_cls.CONNECTOR_NAME)
    ).to_request()

    results = []
    for asset in client.asset.search(request):
        results.append({
            "qualified_name": asset.qualified_name,
            "name": asset.name,
            "description": getattr(asset, "description", None) or "",
            "database_name": getattr(asset, "database_name", None) or "",
            "schema_name": getattr(asset, "schema_name", None) or "",
            "connector_name": getattr(asset, "connector_name", None) or "",
            "type": asset_type,
            "guid": str(asset.guid) if asset.guid else None,
        })
        if len(results) >= limit:
            break

    return results


def get_table_columns(qualified_name: str) -> list[dict[str, Any]]:
    """
    Fetch all columns for a table/view by its qualified name.

    Returns column metadata dicts.
    """
    from pyatlan.model.assets import Table
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    from pyatlan.model.assets import Column

    client = _get_client()

    # Fetch the table with its columns relationship
    request = (
        FluentSearch()
        .where(CompoundQuery.asset_type(Column))
        .where(CompoundQuery.active_assets())
        .where(Column.TABLE_QUALIFIED_NAME.eq(qualified_name))
        .page_size(200)
        .include_on_results(Column.NAME)
        .include_on_results(Column.DESCRIPTION)
        .include_on_results(Column.DATA_TYPE)
        .include_on_results(Column.IS_PRIMARY)
        .include_on_results(Column.IS_NULLABLE)
        .include_on_results(Column.ORDER)
        .include_on_results(Column.QUALIFIED_NAME)
        .include_on_results(Column.MAX_LENGTH)
    ).to_request()

    columns = []
    for col in client.asset.search(request):
        columns.append({
            "name": col.name,
            "qualified_name": col.qualified_name,
            "description": getattr(col, "description", None) or "",
            "data_type": getattr(col, "data_type", None) or "STRING",
            "logical_type": map_atlan_type(getattr(col, "data_type", None)).value,
            "is_primary": bool(getattr(col, "is_primary", False)),
            "is_nullable": bool(getattr(col, "is_nullable", True)),
            "order": getattr(col, "order", 0) or 0,
            "max_length": getattr(col, "max_length", None),
        })

    columns.sort(key=lambda c: c["order"])
    return columns


def search_data_products(query: str = "", limit: int = 20) -> list[dict[str, Any]]:
    """Search for data products in Atlan."""
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    from pyatlan.model.assets import DataProduct

    client = _get_client()

    builder = (
        FluentSearch()
        .where(CompoundQuery.asset_type(DataProduct))
        .where(CompoundQuery.active_assets())
        .page_size(limit)
        .include_on_results(DataProduct.NAME)
        .include_on_results(DataProduct.DESCRIPTION)
        .include_on_results(DataProduct.QUALIFIED_NAME)
    )

    if query:
        builder = builder.where(DataProduct.NAME.match(query))

    request = builder.to_request()

    results = []
    for product in client.asset.search(request):
        results.append({
            "name": product.name,
            "qualified_name": product.qualified_name,
            "description": getattr(product, "description", None) or "",
            "guid": str(product.guid) if product.guid else None,
        })
        if len(results) >= limit:
            break

    return results


def search_data_domains(query: str = "", limit: int = 20) -> list[dict[str, Any]]:
    """Search for data domains in Atlan."""
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    from pyatlan.model.assets import DataDomain

    client = _get_client()

    builder = (
        FluentSearch()
        .where(CompoundQuery.asset_type(DataDomain))
        .where(CompoundQuery.active_assets())
        .page_size(limit)
        .include_on_results(DataDomain.NAME)
        .include_on_results(DataDomain.DESCRIPTION)
        .include_on_results(DataDomain.QUALIFIED_NAME)
    )

    if query:
        builder = builder.where(DataDomain.NAME.match(query))

    request = builder.to_request()

    results = []
    for domain in client.asset.search(request):
        results.append({
            "name": domain.name,
            "qualified_name": domain.qualified_name,
            "description": getattr(domain, "description", None) or "",
            "guid": str(domain.guid) if domain.guid else None,
        })
        if len(results) >= limit:
            break

    return results


def search_users(query: str = "", limit: int = 20) -> list[dict]:
    """
    Search Atlan users by name or email fragment.
    Splits the query on whitespace and uses the first token for the email
    API call (which matches email prefixes), then filters all results
    client-side so that typing a full name like "Andrew Lentz" still works.
    Returns list of dicts with username, email, guid, display_name.
    """
    client = _get_client()
    try:
        if query:
            # Use the first word as the email/username search token
            first_token = query.split()[0]
            response = client.user.get_by_email(first_token, limit=limit * 3)
        else:
            response = client.user.get_all(limit=limit)

        users = []
        if response and response.records:
            tokens = [t.lower() for t in query.split()] if query else []
            for u in response.records:
                if u.enabled is False:
                    continue
                first = u.first_name or ""
                last = u.last_name or ""
                display = f"{first} {last}".strip() or u.username or u.email or ""
                # Client-side filter: all tokens must appear somewhere in the user's fields
                if tokens:
                    haystack = f"{display} {u.email or ''} {u.username or ''}".lower()
                    if not all(t in haystack for t in tokens):
                        continue
                users.append({
                    "username": u.username or "",
                    "email": u.email or "",
                    "guid": u.id or "",
                    "display_name": display,
                })
                if len(users) >= limit:
                    break
        return users
    except Exception as exc:
        raise RuntimeError(f"Atlan user search failed: {exc}") from exc


def search_connections(query: str = "", connector_type: str = "", limit: int = 20) -> list[dict]:
    """
    Search Atlan connections, optionally filtered by connector type (e.g. 'snowflake')
    and/or by name keyword. Returns list of dicts with name, qualified_name, connector_name.
    Used to populate the connection picker in the Servers section.
    """
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    from pyatlan.model.assets import Connection

    client = _get_client()
    try:
        # Fetch a broader set so we can client-side filter by name
        fetch_limit = max(limit * 4, 50)
        builder = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Connection))
            .where(CompoundQuery.active_assets())
            .page_size(fetch_limit)
            .include_on_results(Connection.QUALIFIED_NAME)
            .include_on_results(Connection.NAME)
            .include_on_results(Connection.CONNECTOR_NAME)
        )
        if connector_type:
            builder = builder.where(Connection.CONNECTOR_NAME.eq(connector_type))

        tokens = [t.lower() for t in query.split()] if query else []
        results = []
        for c in client.asset.search(builder.to_request()):
            name = c.name or ""
            qn = c.qualified_name or ""
            connector = str(c.connector_name) if c.connector_name else ""
            if tokens:
                haystack = f"{name} {connector} {qn}".lower()
                if not all(t in haystack for t in tokens):
                    continue
            results.append({
                "name": name,
                "qualified_name": qn,
                "connector_name": connector,
            })
            if len(results) >= limit:
                break
        return results
    except Exception as exc:
        raise RuntimeError(f"Atlan connection search failed: {exc}") from exc


def _guid_from_response(resp: Any) -> str | None:
    """Extract the first GUID from a save/upsert response's mutated entities."""
    if not resp or not resp.mutated_entities:
        return None
    created = resp.mutated_entities.CREATE or []
    updated = resp.mutated_entities.UPDATE or []
    entities = created or updated
    return entities[0].guid if entities else None


def register_placeholder_table(session: Any) -> dict[str, Any]:
    """
    Create placeholder Database → Schema → Table + Column assets in Atlan
    from an approved contract, then:
      - Attach the ODCS contract spec as an Atlan DataContract
      - Create lineage Process assets from each source table
      - Set announcement type to WARNING (yellow) for visibility

    The Database and Schema are upserted first so the Table parent always
    exists. In the common case (≥80%) the database/schema already exist
    from a crawl, so those upserts are silent no-ops.

    Returns dict: {qualified_name, guid, url}
    Raises on error — caller handles gracefully so stage transition still completes.
    """
    import logging
    from pyatlan.model.assets import Table, Column, Schema, Database, DataContract, Process

    log = logging.getLogger(__name__)
    client = _get_client()
    contract = session.contract

    if not contract.schema_objects:
        raise ValueError("Contract has no schema objects to register")

    # Find the prod server, fall back to first server
    server = next((s for s in contract.servers if s.environment == "prod"), None)
    if not server:
        server = contract.servers[0] if contract.servers else None
    if not server or not server.connection_qualified_name:
        raise ValueError(
            "No server with connection_qualified_name set. "
            "Open a Server card and select the Atlan connection."
        )

    connection_qn = server.connection_qualified_name  # e.g. default/snowflake/1770327201
    database = server.database or "UNKNOWN_DB"
    schema = server.schema_name or "UNKNOWN_SCHEMA"
    db_qn = f"{connection_qn}/{database}"
    schema_qn = f"{db_qn}/{schema}"

    # --- Step 1: Upsert Database (no-op if already exists) ---
    client.asset.save(Database.creator(name=database, connection_qualified_name=connection_qn))

    # --- Step 2: Upsert Schema ---
    client.asset.save(Schema.creator(
        name=schema,
        database_qualified_name=db_qn,
        database_name=database,
        connection_qualified_name=connection_qn,
    ))

    # --- Step 3: Collect owner usernames from all role approvers ---
    owner_users = list({
        a.username
        for role in (contract.roles or [])
        for a in role.approvers
        if a.username
    })

    # Build announcement message for the contract
    announcement_msg = (
        f"This asset was registered from an approved DDLC data contract '{contract.name}'. "
        + (f"Domain: {contract.domain}. " if contract.domain else "")
        + (f"Tags: {', '.join(contract.tags)}." if contract.tags else "")
    )

    first_table_qn: str | None = None
    first_table_guid: str | None = None

    for obj in contract.schema_objects:
        table_name = obj.name
        table_qn = f"{schema_qn}/{table_name}"
        if first_table_qn is None:
            first_table_qn = table_qn

        # --- Step 4: Upsert Table ---
        table = Table.creator(
            name=table_name,
            schema_qualified_name=schema_qn,
            schema_name=schema,
            database_name=database,
            connection_qualified_name=connection_qn,
        )
        table.user_description = (
            contract.description_purpose
            or f"Placeholder table for approved data contract: {contract.name}"
        )
        if owner_users:
            table.owner_users = set(owner_users)
        # WARNING announcement = yellow banner, more visible than INFORMATION
        table.announcement_type = "WARNING"
        table.announcement_title = "⚠ Data Contract Active — Placeholder Asset"
        table.announcement_message = announcement_msg

        table_resp = client.asset.save(table)

        # --- Step 5: Upsert Columns ---
        columns = []
        for i, prop in enumerate(obj.properties or []):
            col = Column.creator(
                name=prop.name,
                parent_qualified_name=table_qn,
                parent_type=Table,
                order=i + 1,
                table_name=table_name,
                table_qualified_name=table_qn,
                schema_name=schema,
                schema_qualified_name=schema_qn,
                database_name=database,
                connection_qualified_name=connection_qn,
            )
            col.user_description = prop.description or ""
            columns.append(col)
        if columns:
            client.asset.save(columns)

        # Resolve GUID — from save response if new, else look up by QN
        guid = _guid_from_response(table_resp)
        if not guid:
            try:
                existing = client.asset.get_by_qualified_name(
                    qualified_name=table_qn, asset_type=Table
                )
                guid = existing.guid
            except Exception:
                pass
        if guid and first_table_guid is None:
            first_table_guid = guid

        # --- Step 5b: Associate Table with Domain (best-effort) ---
        if contract.domain_qualified_name:
            try:
                from pyatlan.model.assets import DataDomain
                domain_asset = client.asset.get_by_qualified_name(
                    qualified_name=contract.domain_qualified_name,
                    asset_type=DataDomain,
                )
                if domain_asset and domain_asset.guid:
                    table_update = Table.updater(qualified_name=table_qn, name=table_name)
                    table_update.domain_guids = {str(domain_asset.guid)}
                    client.asset.save(table_update)
                    log.info(f"Table {table_qn} associated with domain {contract.domain_qualified_name}")
            except Exception as exc:
                log.warning(f"Domain association skipped for {table_qn}: {exc}")

        # --- Step 6: Attach Atlan DataContract ---
        try:
            table_ref = Table.updater(qualified_name=table_qn, name=table_name)
            spec_yaml = client.contracts.generate_initial_spec(table_ref)
            dc = DataContract.creator(
                asset_qualified_name=table_qn,
                contract_spec=spec_yaml,
            )
            client.asset.save(dc)
            log.info(f"DataContract attached to {table_qn}")
        except Exception as exc:
            log.warning(f"DataContract attachment failed for {table_qn}: {exc}")

        # --- Step 7: Create lineage from source tables ---
        source_qns = []
        for src in (obj.source_tables or []):
            if src.qualified_name:
                source_qns.append(src.qualified_name)
        # Also collect unique source QNs from column-level lineage
        for prop in (obj.properties or []):
            for col_src in (prop.sources or []):
                if col_src.source_table_qualified_name and col_src.source_table_qualified_name not in source_qns:
                    source_qns.append(col_src.source_table_qualified_name)

        if source_qns:
            target_ref = Table.ref_by_qualified_name(table_qn)
            for src_qn in source_qns:
                try:
                    source_ref = Table.ref_by_qualified_name(src_qn)
                    proc = Process.creator(
                        name=f"DDLC lineage: {src_qn.split('/')[-1]} → {table_name}",
                        connection_qualified_name=connection_qn,
                        inputs=[source_ref],
                        outputs=[target_ref],
                    )
                    client.asset.save(proc)
                    log.info(f"Lineage created: {src_qn} → {table_qn}")
                except Exception as exc:
                    log.warning(f"Lineage creation failed for {src_qn} → {table_qn}: {exc}")

    base_url = os.getenv("ATLAN_BASE_URL", "").rstrip("/")
    atlan_url = f"{base_url}/assets/{first_table_guid}/overview" if first_table_guid else None

    return {
        "qualified_name": first_table_qn,
        "guid": first_table_guid,
        "url": atlan_url,
    }


# ---------------------------------------------------------------------------
# Atlan DQS rule creation
# ---------------------------------------------------------------------------


def push_dq_rules(session) -> dict:
    """
    Create Atlan Data Quality Studio rules for all quality checks with
    engine == "atlan-dqs" that have not yet been pushed (dqs_pushed == False).

    Requires:
      - Session is ACTIVE with atlan_table_qualified_name set
      - Atlan credentials configured

    Mutates the session in-place: sets dqs_pushed=True and
    dqs_rule_qualified_name on each successfully created rule.

    Returns: {pushed: int, skipped: int, errors: list[str]}
    """
    import logging
    from pyatlan.model.assets import DataQualityRule, Table, Column
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery

    log = logging.getLogger(__name__)
    client = _get_client()
    contract = session.contract

    table_qn = contract.atlan_table_qualified_name
    if not table_qn:
        raise ValueError("No atlan_table_qualified_name on contract — publish first.")

    # Build a map: column name (upper) → Atlan column qualified_name
    # Needed to resolve "FACT_ORDERS.ORDERDATE" style column references.
    col_qn_map: dict[str, str] = {}
    try:
        col_request = (
            FluentSearch()
            .where(CompoundQuery.asset_type(Column))
            .where(CompoundQuery.active_assets())
            .where(Column.TABLE_QUALIFIED_NAME.eq(table_qn))
            .page_size(200)
            .include_on_results(Column.NAME)
            .include_on_results(Column.QUALIFIED_NAME)
        ).to_request()
        for col in client.asset.search(col_request):
            if col.name:
                col_qn_map[col.name.upper()] = col.qualified_name
    except Exception as exc:
        log.warning(f"Could not pre-fetch column QNs for {table_qn}: {exc}")

    pushed = 0
    skipped = 0
    errors: list[str] = []

    # Build a reference to the table asset (used in rule creators)
    table_ref = Table.ref_by_qualified_name(table_qn)

    for check in contract.quality_checks:
        if check.engine != "atlan-dqs":
            skipped += 1
            continue
        if check.dqs_pushed:
            skipped += 1
            continue
        if not check.dqs_rule_type:
            errors.append(f"Check '{check.description[:40]}' has no dqs_rule_type — skipped.")
            skipped += 1
            continue

        rule_type_key = check.dqs_rule_type.upper()
        # Map internal SCREAMING_SNAKE_CASE → Atlan DQS template display name (Title Case).
        # These display names are keyed from DataQualityRuleTemplate.display_name in the tenant.
        # custom_sql_creator hardcodes "Custom SQL" (confirmed from pyatlan source).
        _RULE_DISPLAY_NAMES: dict[str, str] = {
            "ROW_COUNT": "Row Count",
            "NULL_COUNT": "Null Count",
            "FRESHNESS": "Freshness",
            "STRING_LENGTH": "String Length",
            "REGEX_MATCH": "Regex",
            "VALID_STRING_VALUES": "Valid Values",
            "CUSTOM_SQL": "Custom SQL",
        }
        rule_type = _RULE_DISPLAY_NAMES.get(rule_type_key, rule_type_key)

        # pyatlan requires int threshold (not float)
        threshold = int(check.dqs_threshold_value) if check.dqs_threshold_value is not None else 0

        # Map alert priority → pyatlan enum.
        # DataQualityRuleAlertPriority has URGENT / NORMAL / LOW (no HIGH).
        # Treat "HIGH" as URGENT so demo data works correctly.
        _PRIORITY_MAP = {
            "HIGH": "URGENT", "URGENT": "URGENT", "NORMAL": "NORMAL", "LOW": "LOW",
        }
        alert_priority_str = _PRIORITY_MAP.get(
            (check.dqs_alert_priority or "NORMAL").upper(), "NORMAL"
        )

        # Default threshold compare operator per rule type (keyed by internal SCREAMING_SNAKE_CASE).
        # ROW_COUNT / REGEX_MATCH / VALID_STRING_VALUES: row/match count must be >= threshold
        # NULL_COUNT: null count must be <= threshold (usually 0)
        # FRESHNESS / STRING_LENGTH: value must be <= threshold
        # CUSTOM_SQL: result count must be == threshold (0 = no violations)
        _OP_MAP = {
            "ROW_COUNT": "GREATER_THAN_EQUAL",
            "NULL_COUNT": "LESS_THAN_EQUAL",
            "FRESHNESS": "LESS_THAN_EQUAL",
            "STRING_LENGTH": "LESS_THAN_EQUAL",
            "REGEX_MATCH": "GREATER_THAN_EQUAL",
            "VALID_STRING_VALUES": "GREATER_THAN_EQUAL",
            "CUSTOM_SQL": "EQUAL",
        }
        compare_op_str = _OP_MAP.get(rule_type_key, "GREATER_THAN_EQUAL")

        # Map DDLC dimension → DataQualityDimension enum attribute name (UPPERCASE)
        _DIM_MAP = {
            "completeness": "COMPLETENESS", "timeliness": "TIMELINESS",
            "freshness": "TIMELINESS", "accuracy": "ACCURACY",
            "consistency": "CONSISTENCY", "uniqueness": "UNIQUENESS",
            "validity": "VALIDITY", "volume": "VOLUME",
        }
        dimension_str = _DIM_MAP.get((check.dimension or "validity").lower(), "VALIDITY")

        try:
            from pyatlan.model.assets import DataQualityRule
            from pyatlan.model.enums import (
                DataQualityRuleAlertPriority,
                DataQualityRuleThresholdCompareOperator,
                DataQualityRuleThresholdUnit,
                DataQualityDimension,
            )

            priority_enum = getattr(
                DataQualityRuleAlertPriority,
                alert_priority_str,
                DataQualityRuleAlertPriority.NORMAL,
            )
            compare_op_enum = getattr(
                DataQualityRuleThresholdCompareOperator,
                compare_op_str,
                DataQualityRuleThresholdCompareOperator.GREATER_THAN_EQUAL,
            )
            dimension_enum = getattr(
                DataQualityDimension,
                dimension_str,
                DataQualityDimension.VALIDITY,
            )

            # Build a short rule name from rule type + description snippet
            rule_name = f"{rule_type}: {check.description[:40].strip()}"

            if rule_type_key == "CUSTOM_SQL":
                # Custom SQL rule — table-level.
                # custom_sql_creator internally hardcodes rule_type="Custom SQL".
                # Note: custom_sql_return_type is NOT a param in pyatlan 8.4.6.
                rule = DataQualityRule.custom_sql_creator(
                    client=client,
                    rule_name=rule_name,
                    asset=table_ref,
                    custom_sql=check.dqs_custom_sql or "",
                    threshold_compare_operator=compare_op_enum,
                    threshold_value=threshold,
                    alert_priority=priority_enum,
                    dimension=dimension_enum,
                    description=check.description or None,
                )
            else:
                # Resolve column reference (e.g. "FACT_ORDERS.ORDERDATE" → col QN)
                column_ref = None
                if check.column:
                    col_name = check.column.split(".")[-1].upper()
                    col_qn = col_qn_map.get(col_name)
                    if col_qn:
                        column_ref = Column.ref_by_qualified_name(col_qn)

                if check.column and column_ref is None:
                    # Column was specified but not found in Atlan — the table asset
                    # may not have been created yet or search indexing is still in
                    # progress.  Raise explicitly so the caller gets a clear error
                    # instead of silently falling back to table-level (which fails
                    # for column-only rule types like NULL_COUNT).
                    col_name = check.column.split(".")[-1].upper()
                    raise ValueError(
                        f"Column '{col_name}' not found in Atlan for table {table_qn}. "
                        f"Ensure the table asset was created (approve the contract first) "
                        f"and wait ~30 s for Atlan search indexing, then retry."
                    )

                if column_ref is not None:
                    # Column-level rule
                    kwargs: dict = dict(
                        client=client,
                        asset=table_ref,
                        column=column_ref,
                        rule_type=rule_type,
                        threshold_compare_operator=compare_op_enum,
                        threshold_value=threshold,
                        alert_priority=priority_enum,
                    )
                    if check.dqs_threshold_unit:
                        unit_enum = getattr(
                            DataQualityRuleThresholdUnit,
                            check.dqs_threshold_unit.upper(),
                            None,
                        )
                        if unit_enum is not None:
                            kwargs["threshold_unit"] = unit_enum
                    rule = DataQualityRule.column_level_rule_creator(**kwargs)
                else:
                    # Table-level rule (no column reference specified)
                    rule = DataQualityRule.table_level_rule_creator(
                        client=client,
                        asset=table_ref,
                        rule_type=rule_type,
                        threshold_compare_operator=compare_op_enum,
                        threshold_value=threshold,
                        alert_priority=priority_enum,
                    )

            resp = client.asset.save(rule)
            rule_qn = None
            if resp and hasattr(resp, "assets_created") and resp.assets_created:
                created = resp.assets_created(DataQualityRule)
                if created:
                    rule_qn = created[0].qualified_name

            # Mutate the quality check in-place
            check.dqs_pushed = True
            check.dqs_rule_qualified_name = rule_qn
            pushed += 1
            log.info(f"DQS rule created: {rule_type} — {check.description[:50]}")

        except Exception as exc:
            err_msg = f"{rule_type} — {check.description[:40]}: {exc}"
            log.warning(f"DQS rule creation failed: {err_msg}")
            errors.append(err_msg)

    return {"pushed": pushed, "skipped": skipped, "errors": errors}

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

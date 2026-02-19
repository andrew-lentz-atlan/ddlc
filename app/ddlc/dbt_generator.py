"""
dbt project generator for DDLC.

Generates a complete dbt project (SQL models, schema.yml, sources.yml,
dbt_project.yml, README.md) from an approved ODCS data contract.

Entry points
------------
- generate_dbt_preview(contract) -> dict[str, str]   # {relative_path: content}
- generate_dbt_zip(contract) -> bytes                 # in-memory ZIP
- trigger_dbt_cloud_run(contract) -> dict             # dbt Cloud API response
- is_configured() -> bool                             # dbt Cloud env vars present
"""

from __future__ import annotations

import io
import os
import re
import zipfile
from typing import Any

import yaml  # already a dep (used by odcs.py)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LOGICAL_TO_SQL: dict[str, str] = {
    "string": "VARCHAR",
    "integer": "INT",
    "number": "FLOAT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "time": "TIME",
    "array": "ARRAY",
    "object": "VARIANT",
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def is_configured() -> bool:
    """Return True if dbt Cloud env vars are all set."""
    return all(
        os.environ.get(v)
        for v in ("DBT_CLOUD_API_KEY", "DBT_CLOUD_ACCOUNT_ID", "DBT_CLOUD_JOB_ID")
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _safe_name(name: str) -> str:
    """Convert any string to a safe SQL/dbt identifier."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name).lower().strip("_") or "contract"


def _collect_sources(contract: Any) -> dict[str, list[dict]]:
    """
    Walk all SchemaObject.source_tables + all SchemaProperty.sources[].source_table
    to build {schema_name: [{name, identifier, qualified_name?}]}.

    Deduplicates by (schema, table_identifier).
    Falls back to "default" schema when no schema can be inferred.
    """
    seen: set[tuple[str, str]] = set()
    groups: dict[str, list[dict]] = {}

    def _add(schema: str, table_name: str, qn: str | None = None) -> None:
        schema = _safe_name(schema) if schema else "default"
        identifier = _safe_name(table_name) if table_name else "unknown"
        key = (schema, identifier)
        if key in seen:
            return
        seen.add(key)
        groups.setdefault(schema, []).append(
            {"name": identifier, "identifier": identifier, "qualified_name": qn}
        )

    for obj in (contract.schema_objects or []):
        # Table-level source_tables list
        for st in (obj.source_tables or []):
            schema = st.schema_name or "default"
            _add(schema, st.name, st.qualified_name)

        # Column-level sources
        for prop in (obj.properties or []):
            for src in (prop.sources or []):
                table = src.source_table or ""
                # Try to infer schema from the table name (e.g. "SCHEMA.TABLE")
                if "." in table:
                    parts = table.split(".")
                    schema = parts[-2]
                    tname = parts[-1]
                else:
                    schema = "default"
                    tname = table
                _add(schema, tname, src.source_table_qualified_name)

    # If nothing found, add a placeholder
    if not groups:
        project_name = _safe_name(contract.name or "contract")
        groups["default"] = [{"name": "source_table", "identifier": "source_table", "qualified_name": None}]

    return groups


def _get_source_alias(source_table: str) -> str:
    """Get a short alias for a source table reference in SQL."""
    if "." in source_table:
        return _safe_name(source_table.split(".")[-1])
    return _safe_name(source_table)


def _generate_sql(obj: Any, project_name: str, target_schema: str, tags: list[str]) -> str:
    """Generate a dbt SQL model for a SchemaObject."""
    model_name = _safe_name(obj.name)
    tags_str = ", ".join(f"'{t}'" for t in tags) if tags else ""
    tag_config = f", tags=[{tags_str}]" if tags_str else ""

    lines = [
        f"{{{{ config(materialized='table', schema='{target_schema}'{tag_config}) }}}}",
        "",
    ]

    # Collect unique source tables from column-level sources
    source_tables: list[tuple[str, str, str]] = []  # (schema_name, table_name, alias)
    seen_tables: set[str] = set()

    for prop in (obj.properties or []):
        for src in (prop.sources or []):
            table = src.source_table or ""
            if not table or table in seen_tables:
                continue
            seen_tables.add(table)
            if "." in table:
                parts = table.split(".")
                schema = _safe_name(parts[-2])
                tname = _safe_name(parts[-1])
            else:
                schema = "default"
                tname = _safe_name(table)
            alias = tname
            source_tables.append((schema, tname, alias))

    # Fall back to obj.source_tables if no column-level sources
    if not source_tables and obj.source_tables:
        for st in obj.source_tables:
            schema = _safe_name(st.schema_name or "default")
            tname = _safe_name(st.name)
            alias = tname
            key = f"{schema}.{tname}"
            if key not in seen_tables:
                seen_tables.add(key)
                source_tables.append((schema, tname, alias))

    # Build SELECT list
    select_parts: list[str] = []
    for prop in (obj.properties or []):
        col_name = _safe_name(prop.name)
        if not prop.sources:
            select_parts.append(f"    NULL AS {col_name}  -- no source mapped")
        else:
            src = prop.sources[0]
            if src.transform_logic:
                # Strip any trailing "AS <alias>" from the transform logic to avoid double-alias
                logic = re.sub(r"\s+AS\s+\w+\s*$", "", src.transform_logic.strip(), flags=re.IGNORECASE)
                select_parts.append(f"    {logic} AS {col_name}")
            else:
                src_table = src.source_table or ""
                if "." in src_table:
                    tname = _safe_name(src_table.split(".")[-1])
                else:
                    tname = _safe_name(src_table) if src_table else (source_tables[0][2] if source_tables else "src")
                src_col = _safe_name(src.source_column) if src.source_column else col_name
                select_parts.append(f"    {tname}.{src_col} AS {col_name}")

    # Build the full query
    if not select_parts:
        # No properties at all — emit a simple SELECT *
        if source_tables:
            schema, tname, alias = source_tables[0]
            lines.append(f"SELECT *")
            lines.append(f"FROM {{{{ source('{schema}', '{tname}') }}}}")
        else:
            lines.append("SELECT 1 AS placeholder  -- no columns or sources defined")
        return "\n".join(lines)

    lines.append("SELECT")
    lines.extend([part + ("," if i < len(select_parts) - 1 else "") for i, part in enumerate(select_parts)])
    lines.append("")

    if source_tables:
        schema, tname, alias = source_tables[0]
        lines.append(f"FROM {{{{ source('{schema}', '{tname}') }}}} AS {alias}")
        for schema, tname, alias in source_tables[1:]:
            lines.append(f"LEFT JOIN {{{{ source('{schema}', '{tname}') }}}} AS {alias}")
            lines.append(f"    ON -- TODO: add join condition")
    else:
        lines.append("-- TODO: add FROM clause")

    return "\n".join(lines)


def _generate_schema_yml(contract: Any, project_name: str) -> str:
    """Generate models/schema.yml for the dbt project."""
    models_list = []

    for obj in (contract.schema_objects or []):
        model_name = _safe_name(obj.name)

        # Find owner from team
        owner_email = None
        for member in (contract.team or []):
            if member.role and "owner" in member.role.lower():
                owner_email = member.email
                break
        if not owner_email and contract.team:
            owner_email = contract.team[0].email

        meta: dict = {}
        if owner_email:
            meta["owner"] = owner_email
        if contract.tags:
            meta["tags"] = list(contract.tags)

        columns_list = []
        for prop in (obj.properties or []):
            col_name = _safe_name(prop.name)
            col: dict = {"name": col_name}
            if prop.description:
                col["description"] = prop.description

            # Build data_tests
            tests: list = []
            if prop.required:
                tests.append("not_null")
            if prop.unique:
                tests.append("unique")
            if prop.examples:
                tests.append({"accepted_values": {"values": list(prop.examples)}})

            # Map quality checks that reference this column
            for qc in (contract.quality_checks or []):
                qc_col = qc.column or ""
                # qc.column may be "table.column" or just "column"
                qc_col_name = _safe_name(qc_col.split(".")[-1]) if qc_col else ""
                if qc_col_name == col_name:
                    if qc.must_be == "unique":
                        if "unique" not in tests:
                            tests.append("unique")
                    elif qc.must_be_greater_than is not None:
                        tests.append({
                            "dbt_utils.expression_is_true": {
                                "expression": f"> {qc.must_be_greater_than}",
                                "name": f"{col_name}_gt_{qc.must_be_greater_than}",
                            }
                        })
                    elif qc.query:
                        # Document as a comment stub
                        pass

            if tests:
                col["data_tests"] = tests

            col_meta: dict = {}
            if prop.classification:
                col_meta["classification"] = prop.classification
            if prop.critical_data_element:
                col_meta["critical_data_element"] = True
            if prop.logical_type:
                col_meta["logical_type"] = prop.logical_type.value if hasattr(prop.logical_type, "value") else str(prop.logical_type)
            if col_meta:
                col["meta"] = col_meta

            columns_list.append(col)

        model_entry: dict = {"name": model_name}
        if obj.description:
            model_entry["description"] = obj.description
        if meta:
            model_entry["meta"] = meta
        if columns_list:
            model_entry["columns"] = columns_list

        models_list.append(model_entry)

    doc = {"version": 2, "models": models_list}
    return yaml.dump(doc, sort_keys=False, allow_unicode=True, default_flow_style=False)


def _generate_sources_yml(contract: Any, project_name: str) -> str:
    """Generate models/sources.yml for the dbt project."""
    groups = _collect_sources(contract)

    # Get database from first server if available
    database = None
    if contract.servers:
        database = contract.servers[0].database

    sources_list = []
    for schema_name, tables in groups.items():
        source_entry: dict = {"name": schema_name, "schema": schema_name}
        if database:
            source_entry["database"] = database

        tables_list = []
        for t in tables:
            table_entry: dict = {"name": t["name"]}
            if t.get("qualified_name"):
                table_entry["meta"] = {"qualified_name": t["qualified_name"]}
            tables_list.append(table_entry)
        source_entry["tables"] = tables_list
        sources_list.append(source_entry)

    doc = {"version": 2, "sources": sources_list}
    return yaml.dump(doc, sort_keys=False, allow_unicode=True, default_flow_style=False)


def _generate_dbt_project_yml(contract: Any, project_name: str) -> str:
    """Generate dbt_project.yml."""
    version = contract.version or "0.1.0"
    tags = list(contract.tags or [])

    model_config: dict = {"+materialized": "table"}
    if tags:
        model_config["+tags"] = tags

    doc: dict = {
        "name": project_name,
        "version": version,
        "config-version": 2,
        "profile": project_name,
        "model-paths": ["models"],
        "source-paths": ["models"],
        "test-paths": ["tests"],
        "seed-paths": ["seeds"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "target-path": "target",
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            project_name: model_config,
        },
    }
    return yaml.dump(doc, sort_keys=False, allow_unicode=True, default_flow_style=False)


def _generate_readme(contract: Any) -> str:
    """Generate a README.md for the dbt project."""
    name = contract.name or "Data Contract"
    version = contract.version or "0.1.0"
    domain = contract.domain or "N/A"
    purpose = contract.description_purpose or ""
    usage = contract.description_usage or ""
    limitations = contract.description_limitations or ""

    lines = [
        f"# {name} — dbt Project",
        "",
        f"> **Version:** {version}  |  **Domain:** {domain}  |  **Status:** {contract.status.value if hasattr(contract.status, 'value') else str(contract.status)}",
        "",
    ]

    if purpose:
        lines += ["## Purpose", "", purpose, ""]
    if usage:
        lines += ["## Usage", "", usage, ""]
    if limitations:
        lines += ["## Limitations", "", limitations, ""]

    # Table list
    if contract.schema_objects:
        lines += ["## Models", ""]
        for obj in contract.schema_objects:
            desc = f" — {obj.description}" if obj.description else ""
            lines.append(f"- **{_safe_name(obj.name)}**{desc}")
        lines.append("")

    # Team table
    if contract.team:
        lines += ["## Team", "", "| Name | Email | Role |", "|------|-------|------|"]
        for m in contract.team:
            lines.append(f"| {m.name} | {m.email} | {m.role} |")
        lines.append("")

    lines += [
        "---",
        "",
        "*Generated by [DDLC](https://github.com/atlanhq/ddlc) — Data Contract Development Lifecycle platform.*",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def generate_dbt_preview(contract: Any) -> dict[str, str]:
    """
    Return {relative_path: content} for all generated dbt project files.
    Called by the preview endpoint.
    """
    project_name = _safe_name(contract.name or "contract")
    target_schema = "dbt"
    tags = list(contract.tags or [])

    files: dict[str, str] = {}

    # dbt_project.yml
    files["dbt_project.yml"] = _generate_dbt_project_yml(contract, project_name)

    # models/sources.yml
    files["models/sources.yml"] = _generate_sources_yml(contract, project_name)

    # models/schema.yml
    files["models/schema.yml"] = _generate_schema_yml(contract, project_name)

    # one SQL model per SchemaObject
    for obj in (contract.schema_objects or []):
        model_name = _safe_name(obj.name)
        files[f"models/{model_name}.sql"] = _generate_sql(obj, project_name, target_schema, tags)

    # README
    files["README.md"] = _generate_readme(contract)

    return files


def generate_dbt_zip(contract: Any) -> bytes:
    """
    Generate a complete dbt project as an in-memory ZIP archive.

    File layout:
        {project_name}/
        ├── dbt_project.yml
        ├── models/
        │   ├── sources.yml
        │   ├── schema.yml
        │   └── {table_name}.sql
        └── README.md
    """
    project_name = _safe_name(contract.name or "contract")
    files = generate_dbt_preview(contract)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for relative_path, content in files.items():
            zip_path = f"{project_name}/{relative_path}"
            zf.writestr(zip_path, content)

    return buf.getvalue()


def trigger_dbt_cloud_run(contract: Any) -> dict:
    """
    Trigger a dbt Cloud job run via the dbt Cloud API v2.

    Requires env vars:
        DBT_CLOUD_API_KEY       — dbt Cloud API token
        DBT_CLOUD_ACCOUNT_ID    — dbt Cloud account ID
        DBT_CLOUD_JOB_ID        — dbt Cloud job ID to trigger

    Returns the parsed JSON response from the dbt Cloud API.
    """
    import json
    import urllib.error
    import urllib.request

    api_key = os.environ["DBT_CLOUD_API_KEY"]
    account_id = os.environ["DBT_CLOUD_ACCOUNT_ID"]
    job_id = os.environ["DBT_CLOUD_JOB_ID"]

    cause = f"Triggered by DDLC — {contract.name or 'Data Contract'} v{contract.version or '0.1.0'}"
    payload = json.dumps({"cause": cause}).encode("utf-8")

    url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Token {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"dbt Cloud API error {e.code}: {body}") from e

"""Reference Data Management Center — FastAPI server.

Standalone app (no App SDK / Dapr / Temporal needed).
Run:  uv run python -m app.rdm.server
URL:  http://localhost:8003
"""
from __future__ import annotations

import csv
import io
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Load .env so ATLAN_BASE_URL / ATLAN_API_KEY are available
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env", override=False)
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.rdm.models import (
    BulkImportRequest,
    CreateDatasetRequest,
    DatasetStatus,
    DatasetWithRows,
    MdlhSnippet,
    ReferenceDataset,
    ReferenceRow,
    UpdateDatasetRequest,
    UpsertRowRequest,
)
from app.rdm.store import store

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_HERE          = Path(__file__).parent
_FRONTEND_DIR  = _HERE / "frontend"
_STATIC_DIR    = _FRONTEND_DIR / "static"

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.rdm.demo_seed import seed_demo_data
    await seed_demo_data()
    # Warm up Atlan glossary (non-blocking — failure is fine)
    try:
        from app.rdm import atlan_sync
        await atlan_sync.bootstrap()
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(f"Atlan bootstrap skipped: {exc}")
    yield

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Reference Data Management Center", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# Frontend pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse((_FRONTEND_DIR / "index.html").read_text())


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

@app.get("/api/datasets", response_class=JSONResponse)
async def list_datasets(domain: Optional[str] = Query(None)):
    datasets = await store.list_datasets()
    if domain:
        datasets = [d for d in datasets if d.domain == domain]
    return [d.model_dump(mode="json") for d in datasets]


@app.get("/api/datasets/groups", response_class=JSONResponse)
async def dataset_groups():
    """Return datasets grouped by domain — used to build the left nav."""
    groups = await store.domain_groups()
    return {
        domain: [d.model_dump(mode="json") for d in datasets]
        for domain, datasets in groups.items()
    }


@app.post("/api/datasets", response_class=JSONResponse, status_code=201)
async def create_dataset(body: CreateDatasetRequest):
    # Validate slug uniqueness
    existing = await store.get_dataset_by_name(body.name)
    if existing:
        raise HTTPException(status_code=409, detail=f"Dataset '{body.name}' already exists")

    ds = ReferenceDataset(
        id=str(uuid.uuid4()),
        name=body.name,
        display_name=body.display_name,
        description=body.description,
        domain=body.domain,
        columns=body.columns,
        owners=body.owners,
        tags=body.tags,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    await store.save_dataset(ds)
    return ds.model_dump(mode="json")


@app.get("/api/datasets/{dataset_id}", response_class=JSONResponse)
async def get_dataset(dataset_id: str):
    ds = await _require_dataset(dataset_id)
    rows = await store.list_rows(dataset_id)
    return DatasetWithRows(dataset=ds, rows=rows).model_dump(mode="json")


@app.put("/api/datasets/{dataset_id}", response_class=JSONResponse)
async def update_dataset(dataset_id: str, body: UpdateDatasetRequest):
    ds = await _require_dataset(dataset_id)
    if body.display_name is not None: ds.display_name = body.display_name
    if body.description  is not None: ds.description  = body.description
    if body.domain       is not None: ds.domain       = body.domain
    if body.columns      is not None: ds.columns      = body.columns
    if body.owners       is not None: ds.owners       = body.owners
    if body.tags         is not None: ds.tags         = body.tags
    if body.status       is not None: ds.status       = body.status
    if body.version      is not None: ds.version      = body.version
    await store.save_dataset(ds)
    return ds.model_dump(mode="json")


@app.delete("/api/datasets/{dataset_id}", response_class=JSONResponse)
async def delete_dataset(dataset_id: str):
    ds = await _require_dataset(dataset_id)
    ds.status = DatasetStatus.DEPRECATED
    await store.save_dataset(ds)
    return {"ok": True, "id": dataset_id}


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

@app.get("/api/datasets/{dataset_id}/rows", response_class=JSONResponse)
async def list_rows(dataset_id: str, include_deprecated: bool = Query(True)):
    await _require_dataset(dataset_id)
    rows = await store.list_rows(dataset_id, include_deprecated=include_deprecated)
    return [r.model_dump(mode="json") for r in rows]


@app.post("/api/datasets/{dataset_id}/rows", response_class=JSONResponse, status_code=201)
async def add_row(dataset_id: str, body: UpsertRowRequest):
    await _require_dataset(dataset_id)
    row = ReferenceRow(
        id=str(uuid.uuid4()),
        dataset_id=dataset_id,
        values=body.values,
    )
    await store.save_row(row)
    return row.model_dump(mode="json")


@app.put("/api/datasets/{dataset_id}/rows/{row_id}", response_class=JSONResponse)
async def update_row(dataset_id: str, row_id: str, body: UpsertRowRequest):
    await _require_dataset(dataset_id)
    row = await store.get_row(dataset_id, row_id)
    if not row:
        raise HTTPException(status_code=404, detail="Row not found")
    row.values = body.values
    await store.save_row(row)
    return row.model_dump(mode="json")


@app.delete("/api/datasets/{dataset_id}/rows/{row_id}", response_class=JSONResponse)
async def delete_row(dataset_id: str, row_id: str):
    await _require_dataset(dataset_id)
    deleted = await store.delete_row(dataset_id, row_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Row not found")
    return {"ok": True, "id": row_id}


@app.post("/api/datasets/{dataset_id}/rows/import", response_class=JSONResponse)
async def import_rows(dataset_id: str, body: BulkImportRequest):
    """Bulk upsert rows from a list of dicts (JSON)."""
    await _require_dataset(dataset_id)
    rows = await store.bulk_upsert_rows(
        dataset_id, body.rows, replace_all=body.replace_all
    )
    return {"imported": len(rows), "replace_all": body.replace_all}


@app.post("/api/datasets/{dataset_id}/rows/import-csv", response_class=JSONResponse)
async def import_csv(dataset_id: str, file: UploadFile = File(...), replace_all: bool = Query(False)):
    """Upload a CSV file and bulk-import rows."""
    ds = await _require_dataset(dataset_id)
    content = await file.read()
    text    = content.decode("utf-8-sig")  # handle BOM
    reader  = csv.DictReader(io.StringIO(text))
    rows_data = [dict(row) for row in reader]
    if not rows_data:
        raise HTTPException(status_code=400, detail="CSV is empty or has no data rows")
    rows = await store.bulk_upsert_rows(dataset_id, rows_data, replace_all=replace_all)
    return {"imported": len(rows), "replace_all": replace_all, "dataset_id": dataset_id}


# ---------------------------------------------------------------------------
# Publish / MDLH
# ---------------------------------------------------------------------------

@app.post("/api/datasets/{dataset_id}/publish", response_class=JSONResponse)
async def publish_dataset(dataset_id: str):
    """Publish to Atlan as GlossaryTerms (if Atlan is configured)."""
    ds = await _require_dataset(dataset_id)
    rows = await store.list_rows(dataset_id, include_deprecated=False)

    try:
        from app.rdm import atlan_sync
        result = await atlan_sync.publish(ds, rows)
        ds.status = DatasetStatus.ACTIVE
        ds.atlan_synced_at = datetime.now(timezone.utc)
        ds.atlan_category_qualified_name = result.get("category_qualified_name")
        ds.atlan_glossary_qualified_name = result.get("glossary_qualified_name")
        await store.save_dataset(ds)
        return {"ok": True, "synced_rows": result.get("synced_rows", 0), "atlan": True}
    except Exception as exc:
        # Graceful degradation — mark active locally even if Atlan sync fails
        ds.status = DatasetStatus.ACTIVE
        await store.save_dataset(ds)
        return {"ok": True, "synced_rows": 0, "atlan": False, "warning": str(exc)}


@app.get("/api/datasets/{dataset_id}/mdlh-snippet", response_class=JSONResponse)
async def mdlh_snippet(dataset_id: str):
    ds = await _require_dataset(dataset_id)
    snippet = _build_mdlh_snippet(ds)
    return snippet.model_dump()


@app.post("/api/demo/reseed", response_class=JSONResponse)
async def reseed():
    """Reset all data to demo state."""
    store._datasets.clear()
    store._rows.clear()
    from app.rdm.demo_seed import seed_demo_data
    await seed_demo_data()
    datasets = await store.list_datasets()
    return {"ok": True, "datasets": len(datasets)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _require_dataset(dataset_id: str) -> ReferenceDataset:
    ds = await store.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return ds


def _build_mdlh_snippet(ds: ReferenceDataset) -> MdlhSnippet:
    table_name = ds.name.upper()
    col_names  = [c.name for c in ds.columns]

    # GOLD view (simple, no JSON parsing needed after view is created)
    gold = f"SELECT *\nFROM GOLD.REFERENCE.{table_name}\nWHERE STATUS = 'ACTIVE';"

    if not col_names:
        raw = f"-- No columns defined yet for {ds.name}"
    else:
        # Row data is stored as JSON in USERDESCRIPTION (camelCase → ALLCAPS, no underscores)
        # t.NAME already holds the primary key value, so skip col_names[0] in PARSE_JSON projections
        pk_col   = col_names[0]
        rest_cols = col_names[1:]
        col_projections = ",\n    ".join(
            f"PARSE_JSON(t.USERDESCRIPTION):{col}::STRING  AS {col.upper()}"
            for col in rest_cols
        )
        col_select = f"t.NAME                                  AS {pk_col.upper()}"
        if col_projections:
            col_select += f",\n    {col_projections}"
        raw = f"""SELECT
    {col_select},
    t.STATUS,
    t.OWNERUSERS
FROM ENTITY_METADATA.GLOSSARYTERM t,
     TABLE(FLATTEN(input => t.CATEGORIES)) cat_flat
JOIN ENTITY_METADATA.GLOSSARYCATEGORY c
    ON  c.GUID = cat_flat.VALUE::STRING
    AND c.NAME = '{ds.name}'
WHERE t.STATUS = 'ACTIVE';"""

    return MdlhSnippet(
        snowflake_gold=gold,
        snowflake_raw=raw,
        description=(
            f"Query **{ds.display_name}** via Atlan's Metadata Lakehouse. "
            f"Use the GOLD view for clean column access, or the raw query "
            f"to access ENTITY_METADATA directly via JOIN on GUID."
        ),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.rdm.server:app", host="0.0.0.0", port=8003, reload=True)

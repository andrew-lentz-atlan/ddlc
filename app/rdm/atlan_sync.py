"""Atlan sync for Reference Data Management Center.

Architecture:
  - One AtlasGlossary  "Reference Data"  (created once, hidden via Persona)
  - One AtlasGlossaryCategory per dataset  (e.g. "country_codes")
  - One AtlasGlossaryTerm per row          (name = PK value)

Row data is stored as JSON in the term's user_description so MDLH can
expose it without needing a CustomMetadataDef.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from app.rdm.models import ReferenceDataset, ReferenceRow

logger = logging.getLogger(__name__)

GLOSSARY_NAME = "Reference Data"

# Module-level cache so we only look up the glossary once per process
_glossary_cache: Optional[Dict[str, str]] = None  # {"guid": ..., "qualified_name": ...}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    from pyatlan.client.atlan import AtlanClient
    base_url = os.getenv("ATLAN_BASE_URL")
    api_key  = os.getenv("ATLAN_API_KEY")
    if not base_url or not api_key:
        raise RuntimeError("ATLAN_BASE_URL and ATLAN_API_KEY must be set")
    _client = AtlanClient(base_url=base_url, api_key=api_key)
    return _client


def is_configured() -> bool:
    return bool(os.getenv("ATLAN_BASE_URL") and os.getenv("ATLAN_API_KEY"))


# ---------------------------------------------------------------------------
# Glossary — find or create "Reference Data"
# ---------------------------------------------------------------------------

def _find_glossary_sync() -> Optional[Dict[str, str]]:
    """Search for the 'Reference Data' glossary. Returns {guid, qualified_name} or None."""
    from pyatlan.model.assets import AtlasGlossary
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    client = _get_client()
    try:
        req = (
            FluentSearch()
            .where(CompoundQuery.asset_type(AtlasGlossary))
            .where(CompoundQuery.active_assets())
            .where(AtlasGlossary.NAME.eq(GLOSSARY_NAME))
            .page_size(1)
            .to_request()
        )
        for asset in client.asset.search(req):
            if asset.name == GLOSSARY_NAME:
                return {"guid": str(asset.guid), "qualified_name": str(asset.qualified_name)}
    except Exception as exc:
        logger.warning(f"Glossary search failed: {exc}")
    return None


def _create_glossary_sync() -> Dict[str, str]:
    """Create the 'Reference Data' glossary and return {guid, qualified_name}."""
    from pyatlan.model.assets import AtlasGlossary
    client = _get_client()
    glossary = AtlasGlossary.creator(name=GLOSSARY_NAME)
    glossary.description = (
        "Internal reference datasets managed by the Reference Data Management Center. "
        "Hidden from regular users via Persona — access via MDLH or the RDM app."
    )
    resp = client.asset.save(glossary)
    created = resp.assets_created(AtlasGlossary)
    updated = resp.assets_updated(AtlasGlossary)
    asset = (created or updated)[0]
    return {"guid": str(asset.guid), "qualified_name": str(asset.qualified_name)}


def _get_or_create_glossary_sync() -> Dict[str, str]:
    """Find or create the 'Reference Data' glossary. Uses module-level cache."""
    global _glossary_cache
    if _glossary_cache:
        return _glossary_cache
    found = _find_glossary_sync()
    if found:
        _glossary_cache = found
        logger.info(f"Found existing glossary: {found['qualified_name']}")
        return found
    created = _create_glossary_sync()
    _glossary_cache = created
    logger.info(f"Created glossary: {created['qualified_name']}")
    return created


# ---------------------------------------------------------------------------
# Category — find or create one per dataset
# ---------------------------------------------------------------------------

def _find_category_sync(dataset_name: str, glossary_guid: str) -> Optional[Dict[str, str]]:
    """Find an existing category by name within the glossary."""
    from pyatlan.model.assets import AtlasGlossaryCategory
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    client = _get_client()
    try:
        req = (
            FluentSearch()
            .where(CompoundQuery.asset_type(AtlasGlossaryCategory))
            .where(CompoundQuery.active_assets())
            .where(AtlasGlossaryCategory.NAME.eq(dataset_name))
            .page_size(20)
            .to_request()
        )
        for asset in client.asset.search(req):
            # Confirm it belongs to our glossary
            if asset.name == dataset_name and asset.anchor and str(asset.anchor.guid) == glossary_guid:
                return {"guid": str(asset.guid), "qualified_name": str(asset.qualified_name)}
    except Exception as exc:
        logger.warning(f"Category search failed: {exc}")
    return None


def _create_category_sync(dataset: ReferenceDataset, glossary: Dict[str, str]) -> Dict[str, str]:
    """Create a GlossaryCategory for this dataset."""
    from pyatlan.model.assets import AtlasGlossaryCategory
    client = _get_client()
    cat = AtlasGlossaryCategory.creator(
        name=dataset.name,
        glossary_guid=glossary["guid"],
    )
    cat.user_description = dataset.description or f"Reference dataset: {dataset.display_name}"
    resp = client.asset.save(cat)
    created = resp.assets_created(AtlasGlossaryCategory)
    updated = resp.assets_updated(AtlasGlossaryCategory)
    asset = (created or updated)[0]
    logger.info(f"Created/updated category '{dataset.name}': {asset.qualified_name}")
    return {"guid": str(asset.guid), "qualified_name": str(asset.qualified_name)}


def _get_or_create_category_sync(dataset: ReferenceDataset, glossary: Dict[str, str]) -> Dict[str, str]:
    """Find or create the category for this dataset."""
    found = _find_category_sync(dataset.name, glossary["guid"])
    if found:
        logger.info(f"Found existing category '{dataset.name}': {found['qualified_name']}")
        return found
    return _create_category_sync(dataset, glossary)


# ---------------------------------------------------------------------------
# Terms — upsert one per row
# ---------------------------------------------------------------------------

def _list_existing_terms_sync(category_guid: str, glossary_guid: str) -> Dict[str, Dict[str, str]]:
    """Return a dict of {term_name: {guid, qualified_name}} for terms in this category."""
    from pyatlan.model.assets import AtlasGlossaryTerm
    from pyatlan.model.fluent_search import FluentSearch, CompoundQuery
    client = _get_client()
    existing: Dict[str, Dict[str, str]] = {}
    try:
        req = (
            FluentSearch()
            .where(CompoundQuery.asset_type(AtlasGlossaryTerm))
            .where(CompoundQuery.active_assets())
            .page_size(500)
            .to_request()
        )
        for asset in client.asset.search(req):
            # Filter to terms in our glossary
            if not (asset.anchor and str(asset.anchor.guid) == glossary_guid):
                continue
            # Filter to terms in our category
            in_cat = False
            if hasattr(asset, 'categories') and asset.categories:
                for cat in asset.categories:
                    if str(cat.guid) == category_guid:
                        in_cat = True
                        break
            if in_cat:
                existing[asset.name] = {
                    "guid": str(asset.guid),
                    "qualified_name": str(asset.qualified_name),
                }
    except Exception as exc:
        logger.warning(f"Term listing failed: {exc}")
    return existing


def _build_term_description(dataset: ReferenceDataset, row: ReferenceRow) -> str:
    """Build the user_description JSON blob stored on each term.

    Format: JSON dict of all column values (keyed by column name).
    MDLH queries can parse this via PARSE_JSON(user_description).
    """
    return json.dumps(row.values, ensure_ascii=False)


def _get_term_display_name(dataset: ReferenceDataset, row: ReferenceRow) -> str:
    """Return the most human-readable value for a row (for the term display_name)."""
    # Try common label column names
    for candidate in ("label", "name", "description", "value"):
        if candidate in row.values and row.values[candidate]:
            return row.values[candidate]
    # Fallback: second column value
    vals = list(row.values.values())
    return vals[1] if len(vals) > 1 else vals[0] if vals else row.id


def _get_term_pk_value(dataset: ReferenceDataset, row: ReferenceRow) -> str:
    """Return the primary key value to use as the term name."""
    pk_col = dataset.primary_key_column
    if pk_col and pk_col.name in row.values:
        return row.values[pk_col.name]
    # Fallback: first value
    return list(row.values.values())[0] if row.values else row.id


def _upsert_terms_sync(
    dataset: ReferenceDataset,
    rows: List[ReferenceRow],
    glossary: Dict[str, str],
    category: Dict[str, str],
) -> Tuple[int, int, int]:
    """Upsert all terms. Returns (created, updated, failed)."""
    from pyatlan.model.assets import AtlasGlossary, AtlasGlossaryCategory, AtlasGlossaryTerm
    client = _get_client()

    existing = _list_existing_terms_sync(category["guid"], glossary["guid"])
    logger.info(f"Found {len(existing)} existing terms in category '{dataset.name}'")

    # Build a reference to the category for attaching to terms
    cat_ref = AtlasGlossaryCategory()
    cat_ref.guid = category["guid"]

    created = updated = failed = 0

    # Process in batches of 20 to avoid request size limits
    batch_size = 20
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        assets_to_save = []

        for row in batch:
            try:
                pk_val  = _get_term_pk_value(dataset, row)
                disp    = _get_term_display_name(dataset, row)
                desc    = _build_term_description(dataset, row)

                if pk_val in existing:
                    # Update existing term
                    ex = existing[pk_val]
                    term = AtlasGlossaryTerm.updater(
                        qualified_name=ex["qualified_name"],
                        name=pk_val,
                        glossary_guid=glossary["guid"],
                    )
                    term.display_name   = disp
                    term.user_description = desc
                    assets_to_save.append((term, False))
                else:
                    # Create new term — use glossary_guid only (not both)
                    term = AtlasGlossaryTerm.creator(
                        name=pk_val,
                        glossary_guid=glossary["guid"],
                        categories=[cat_ref],
                    )
                    term.display_name   = disp
                    term.user_description = desc
                    assets_to_save.append((term, True))
            except Exception as exc:
                logger.warning(f"Failed to build term for row {row.id}: {exc}")
                failed += 1

        if not assets_to_save:
            continue

        try:
            resp = client.asset.save([t for t, _ in assets_to_save])
            batch_created = len(resp.assets_created(AtlasGlossaryTerm))
            batch_updated = len(resp.assets_updated(AtlasGlossaryTerm))
            created += batch_created
            updated += batch_updated
            logger.info(f"Batch {i//batch_size + 1}: {batch_created} created, {batch_updated} updated")
        except Exception as exc:
            logger.error(f"Batch save failed: {exc}")
            failed += len(assets_to_save)

    return created, updated, failed


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def publish(dataset: ReferenceDataset, rows: List[ReferenceRow]) -> Dict[str, Any]:
    """Sync a dataset + its rows to Atlan as a GlossaryCategory + GlossaryTerms.

    All pyatlan calls are synchronous so we run them in a thread pool.
    Returns dict with {glossary_qualified_name, category_qualified_name, synced_rows}.
    """
    def _sync_all():
        # Step 1: Glossary
        glossary = _get_or_create_glossary_sync()

        # Step 2: Category
        category = _get_or_create_category_sync(dataset, glossary)

        # Step 3: Terms
        created, updated, failed = _upsert_terms_sync(dataset, rows, glossary, category)

        return {
            "glossary_guid":              glossary["guid"],
            "glossary_qualified_name":    glossary["qualified_name"],
            "category_guid":              category["guid"],
            "category_qualified_name":    category["qualified_name"],
            "synced_rows":                created + updated,
            "created":                    created,
            "updated":                    updated,
            "failed":                     failed,
        }

    return await asyncio.to_thread(_sync_all)


async def bootstrap() -> Dict[str, Any]:
    """Ensure the 'Reference Data' glossary exists. Call once on server startup."""
    if not is_configured():
        logger.info("Atlan not configured — skipping bootstrap")
        return {"configured": False}
    try:
        result = await asyncio.to_thread(_get_or_create_glossary_sync)
        return {"configured": True, **result}
    except Exception as exc:
        logger.warning(f"Atlan bootstrap failed: {exc}")
        return {"configured": True, "error": str(exc)}

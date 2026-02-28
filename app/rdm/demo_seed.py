"""Demo seed data for the Reference Data Management Center.

Three reference datasets that showcase the full range of use cases:
  1. country_codes   — Global domain, 10 rows, 5 columns
  2. business_units  — HR domain, 6 rows, 4 columns
  3. data_classification — Governance domain, 4 rows, 4 columns
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from app.rdm.models import ColumnDef, ColumnType, DatasetStatus, ReferenceDataset
from app.rdm.store import store


async def seed_demo_data() -> None:
    """Idempotent — only seeds if the store is empty."""
    existing = await store.list_datasets()
    if existing:
        return

    # ------------------------------------------------------------------
    # 1. Country Codes  (Global)
    # ------------------------------------------------------------------
    country_codes = ReferenceDataset(
        id=str(uuid.uuid4()),
        name="country_codes",
        display_name="Country Codes",
        description="ISO 3166-1 alpha-2 country codes with region and currency mappings. "
                    "Used as the authoritative reference for all country classification across reports.",
        domain="Global",
        status=DatasetStatus.ACTIVE,
        version="2024.1",
        owners=["data-governance@company.com"],
        tags=["iso-standard", "geography"],
        columns=[
            ColumnDef(name="code",       display_name="Code",       column_type=ColumnType.STRING,  is_primary_key=True,  is_nullable=False, description="ISO 3166-1 alpha-2 code"),
            ColumnDef(name="label",      display_name="Label",      column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=False, description="Full country name"),
            ColumnDef(name="region",     display_name="Region",     column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=False, description="NAM, EUR, APAC, LATAM, MEA"),
            ColumnDef(name="iso_alpha3", display_name="ISO Alpha-3", column_type=ColumnType.STRING, is_primary_key=False, is_nullable=True,  description="ISO 3166-1 alpha-3 code"),
            ColumnDef(name="currency",   display_name="Currency",   column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=True,  description="Primary currency code (ISO 4217)"),
        ],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    await store.save_dataset(country_codes)
    await store.bulk_upsert_rows(country_codes.id, [
        {"code": "US", "label": "United States",    "region": "NAM",   "iso_alpha3": "USA", "currency": "USD"},
        {"code": "GB", "label": "United Kingdom",   "region": "EUR",   "iso_alpha3": "GBR", "currency": "GBP"},
        {"code": "DE", "label": "Germany",          "region": "EUR",   "iso_alpha3": "DEU", "currency": "EUR"},
        {"code": "FR", "label": "France",           "region": "EUR",   "iso_alpha3": "FRA", "currency": "EUR"},
        {"code": "JP", "label": "Japan",            "region": "APAC",  "iso_alpha3": "JPN", "currency": "JPY"},
        {"code": "AU", "label": "Australia",        "region": "APAC",  "iso_alpha3": "AUS", "currency": "AUD"},
        {"code": "CA", "label": "Canada",           "region": "NAM",   "iso_alpha3": "CAN", "currency": "CAD"},
        {"code": "BR", "label": "Brazil",           "region": "LATAM", "iso_alpha3": "BRA", "currency": "BRL"},
        {"code": "IN", "label": "India",            "region": "APAC",  "iso_alpha3": "IND", "currency": "INR"},
        {"code": "SG", "label": "Singapore",        "region": "APAC",  "iso_alpha3": "SGP", "currency": "SGD"},
    ])

    # ------------------------------------------------------------------
    # 2. Business Units  (HR)
    # ------------------------------------------------------------------
    business_units = ReferenceDataset(
        id=str(uuid.uuid4()),
        name="business_units",
        display_name="Business Units",
        description="Canonical list of business units used in HR, Finance, and reporting systems. "
                    "Source of truth for cost center prefix mapping.",
        domain="HR",
        status=DatasetStatus.ACTIVE,
        version="2024.2",
        owners=["hr-ops@company.com", "finance@company.com"],
        tags=["org-structure", "cost-centers"],
        columns=[
            ColumnDef(name="code",               display_name="Code",               column_type=ColumnType.STRING,  is_primary_key=True,  is_nullable=False, description="Short code used in all systems"),
            ColumnDef(name="name",               display_name="Name",               column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=False, description="Full business unit name"),
            ColumnDef(name="head",               display_name="Head",               column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=True,  description="VP / Head of business unit"),
            ColumnDef(name="cost_center_prefix", display_name="Cost Center Prefix", column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=False, description="Prefix applied to all cost centers in this BU"),
        ],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    await store.save_dataset(business_units)
    await store.bulk_upsert_rows(business_units.id, [
        {"code": "ENG",   "name": "Engineering",       "head": "CTO",   "cost_center_prefix": "CC-1"},
        {"code": "FIN",   "name": "Finance",           "head": "CFO",   "cost_center_prefix": "CC-2"},
        {"code": "HR",    "name": "Human Resources",   "head": "CHRO",  "cost_center_prefix": "CC-3"},
        {"code": "SALES", "name": "Sales",             "head": "CRO",   "cost_center_prefix": "CC-4"},
        {"code": "MKT",   "name": "Marketing",         "head": "CMO",   "cost_center_prefix": "CC-5"},
        {"code": "OPS",   "name": "Operations",        "head": "COO",   "cost_center_prefix": "CC-6"},
    ])

    # ------------------------------------------------------------------
    # 3. Data Classification  (Governance)
    # ------------------------------------------------------------------
    data_classification = ReferenceDataset(
        id=str(uuid.uuid4()),
        name="data_classification",
        display_name="Data Classification",
        description="Approved data sensitivity levels. All data assets must be tagged with one of "
                    "these classification levels. Drives masking rules in non-prod environments.",
        domain="Governance",
        status=DatasetStatus.ACTIVE,
        version="2023.1",
        owners=["ciso@company.com", "data-governance@company.com"],
        tags=["security", "compliance", "pii"],
        columns=[
            ColumnDef(name="level",           display_name="Level",             column_type=ColumnType.INTEGER, is_primary_key=True,  is_nullable=False, description="Numeric sensitivity level (1=lowest)"),
            ColumnDef(name="label",           display_name="Label",             column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=False, description="Classification name"),
            ColumnDef(name="description",     display_name="Description",       column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=False, description="When to apply this classification"),
            ColumnDef(name="mask_nonprod",    display_name="Mask in Non-Prod",  column_type=ColumnType.BOOLEAN, is_primary_key=False, is_nullable=False, description="Whether to mask in dev/staging environments"),
        ],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    await store.save_dataset(data_classification)
    await store.bulk_upsert_rows(data_classification.id, [
        {"level": "1", "label": "Public",       "description": "Freely shareable, no restrictions",                              "mask_nonprod": "false"},
        {"level": "2", "label": "Internal",     "description": "Internal use only, not for public disclosure",                   "mask_nonprod": "false"},
        {"level": "3", "label": "Confidential", "description": "Sensitive business data — restricted access, need-to-know only", "mask_nonprod": "true"},
        {"level": "4", "label": "Restricted",   "description": "PII / regulated data — strictest controls, audit required",     "mask_nonprod": "true"},
    ])

    # ------------------------------------------------------------------
    # 4. Currency Codes  (Finance) — DRAFT to show lifecycle
    # ------------------------------------------------------------------
    currency_codes = ReferenceDataset(
        id=str(uuid.uuid4()),
        name="currency_codes",
        display_name="Currency Codes",
        description="ISO 4217 currency codes. Work in progress — pending Finance approval.",
        domain="Finance",
        status=DatasetStatus.DRAFT,
        version="1.0",
        owners=["finance@company.com"],
        tags=["iso-standard", "finance"],
        columns=[
            ColumnDef(name="code",        display_name="Code",         column_type=ColumnType.STRING,  is_primary_key=True,  is_nullable=False, description="ISO 4217 alpha code"),
            ColumnDef(name="label",       display_name="Label",        column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=False, description="Full currency name"),
            ColumnDef(name="symbol",      display_name="Symbol",       column_type=ColumnType.STRING,  is_primary_key=False, is_nullable=True,  description="Currency symbol"),
            ColumnDef(name="decimal_places", display_name="Decimals",  column_type=ColumnType.INTEGER, is_primary_key=False, is_nullable=False, description="Standard decimal places"),
        ],
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    await store.save_dataset(currency_codes)
    await store.bulk_upsert_rows(currency_codes.id, [
        {"code": "USD", "label": "US Dollar",     "symbol": "$",  "decimal_places": "2"},
        {"code": "EUR", "label": "Euro",          "symbol": "€",  "decimal_places": "2"},
        {"code": "GBP", "label": "British Pound", "symbol": "£",  "decimal_places": "2"},
        {"code": "JPY", "label": "Japanese Yen",  "symbol": "¥",  "decimal_places": "0"},
    ])

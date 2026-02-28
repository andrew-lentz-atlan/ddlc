"""Pydantic models for the Reference Data Management Center."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ColumnType(str, Enum):
    STRING  = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    DATE    = "date"
    BOOLEAN = "boolean"


class DatasetStatus(str, Enum):
    DRAFT      = "draft"
    ACTIVE     = "active"
    DEPRECATED = "deprecated"


class RowStatus(str, Enum):
    ACTIVE     = "active"
    DEPRECATED = "deprecated"


# ---------------------------------------------------------------------------
# Column definition
# ---------------------------------------------------------------------------

class ColumnDef(BaseModel):
    name:          str                    # snake_case identifier, e.g. "iso_alpha3"
    display_name:  str                    # Human label, e.g. "ISO Alpha-3"
    column_type:   ColumnType = ColumnType.STRING
    is_primary_key: bool = False
    is_nullable:   bool = True
    description:   Optional[str] = None


# ---------------------------------------------------------------------------
# Reference Row
# ---------------------------------------------------------------------------

class ReferenceRow(BaseModel):
    id:         str
    dataset_id: str
    values:     Dict[str, str] = Field(default_factory=dict)  # col_name → value
    status:     RowStatus = RowStatus.ACTIVE
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Reference Dataset
# ---------------------------------------------------------------------------

class ReferenceDataset(BaseModel):
    id:           str
    name:         str                 # slug used as Atlan category name, e.g. "country_codes"
    display_name: str                 # human label, e.g. "Country Codes"
    description:  Optional[str] = None
    domain:       str = "General"    # e.g. "Global", "Finance", "HR"
    columns:      List[ColumnDef] = Field(default_factory=list)
    status:       DatasetStatus = DatasetStatus.DRAFT
    version:      str = "1.0"
    owners:       List[str] = Field(default_factory=list)
    tags:         List[str] = Field(default_factory=list)
    created_at:   datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at:   datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    row_count:    int = 0

    # Atlan sync state
    atlan_category_qualified_name: Optional[str] = None
    atlan_glossary_qualified_name: Optional[str] = None
    atlan_synced_at:               Optional[datetime] = None

    @property
    def primary_key_column(self) -> Optional[ColumnDef]:
        for col in self.columns:
            if col.is_primary_key:
                return col
        return self.columns[0] if self.columns else None


# ---------------------------------------------------------------------------
# API request/response shapes
# ---------------------------------------------------------------------------

class CreateDatasetRequest(BaseModel):
    name:         str
    display_name: str
    description:  Optional[str] = None
    domain:       str = "General"
    columns:      List[ColumnDef] = Field(default_factory=list)
    owners:       List[str] = Field(default_factory=list)
    tags:         List[str] = Field(default_factory=list)


class UpdateDatasetRequest(BaseModel):
    display_name: Optional[str] = None
    description:  Optional[str] = None
    domain:       Optional[str] = None
    columns:      Optional[List[ColumnDef]] = None
    owners:       Optional[List[str]] = None
    tags:         Optional[List[str]] = None
    status:       Optional[DatasetStatus] = None
    version:      Optional[str] = None


class UpsertRowRequest(BaseModel):
    values: Dict[str, str]


class BulkImportRequest(BaseModel):
    rows:         List[Dict[str, str]]  # list of {col_name: value} dicts
    replace_all:  bool = False           # if True, wipe existing rows first


class DatasetWithRows(BaseModel):
    """Dataset detail response including all rows."""
    dataset: ReferenceDataset
    rows:    List[ReferenceRow]


class MdlhSnippet(BaseModel):
    snowflake_gold:  str
    snowflake_raw:   str
    description:     str

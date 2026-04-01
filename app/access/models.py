"""Data models for the Access app — ServiceNow integration."""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# ServiceNow variable type codes → form field type mapping
# ---------------------------------------------------------------------------

class FieldType(str, Enum):
    TEXT = "text"
    TEXTAREA = "textarea"
    SELECT = "select"
    CHECKBOX = "checkbox"
    DATE = "date"
    DATETIME = "datetime"
    NUMBER = "number"
    EMAIL = "email"
    URL = "url"
    REFERENCE = "reference"
    RADIO = "radio"
    MULTI_SELECT = "multi_select"
    LABEL = "label"


# ServiceNow variable type codes → our field types
# See: https://docs.servicenow.com/bundle/yokohama-it-service-management/page/product/service-catalog-management/reference/r_VariableTypes.html
SNOW_TYPE_MAP: dict[int, FieldType] = {
    1: FieldType.CHECKBOX,       # Yes/No
    2: FieldType.TEXTAREA,       # Multi Line Text
    3: FieldType.RADIO,          # Multiple Choice
    4: FieldType.NUMBER,         # Numeric Scale
    5: FieldType.SELECT,         # Select Box
    6: FieldType.TEXT,            # Single Line Text
    7: FieldType.CHECKBOX,       # Check Box
    8: FieldType.REFERENCE,      # Reference
    9: FieldType.DATE,           # Date
    10: FieldType.DATETIME,      # Date/Time
    11: FieldType.LABEL,         # Label
    12: FieldType.LABEL,         # Break
    14: FieldType.TEXT,           # Macro
    15: FieldType.TEXT,           # UI Page
    16: FieldType.TEXT,           # Wide Single Line Text
    17: FieldType.TEXTAREA,      # Macro with Label
    18: FieldType.TEXT,           # Lookup Select Box
    19: FieldType.TEXT,           # Container Start
    20: FieldType.TEXT,           # Container End
    21: FieldType.MULTI_SELECT,   # List Collector
    22: FieldType.TEXT,           # Lookup Multiple Choice
    24: FieldType.TEXT,           # Container Split
    25: FieldType.TEXT,           # Masked
    26: FieldType.EMAIL,         # Email
    27: FieldType.URL,           # URL
}


# ---------------------------------------------------------------------------
# Form schema models
# ---------------------------------------------------------------------------

class ChoiceOption(BaseModel):
    value: str
    label: str


class FormVariable(BaseModel):
    name: str
    label: str
    field_type: FieldType = FieldType.TEXT
    mandatory: bool = False
    default_value: str = ""
    choices: list[ChoiceOption] = Field(default_factory=list)
    order: int = 0
    read_only: bool = False
    help_text: str = ""


class FormSchema(BaseModel):
    catalog_item_sys_id: str
    catalog_item_name: str
    short_description: str = ""
    variables: list[FormVariable] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class SubmitRequest(BaseModel):
    catalog_item_sys_id: str
    variables: dict[str, Any]
    data_product_qn: str = ""
    requester_email: str = ""


class SubmitResponse(BaseModel):
    success: bool
    request_number: str = ""
    request_item_number: str = ""
    error: str = ""


# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------

class CatalogItemMapping(BaseModel):
    """Maps a data product QN pattern to a ServiceNow catalog item."""
    qn_pattern: str = "*"
    catalog_item_sys_id: str
    display_name: str = ""


class ServiceNowConfig(BaseModel):
    instance_url: str = ""
    client_id: str = ""
    client_secret: str = ""
    username: str = ""
    password: str = ""


class AccessConfig(BaseModel):
    servicenow: ServiceNowConfig = Field(default_factory=ServiceNowConfig)
    default_catalog_item_sys_id: str = ""
    mappings: list[CatalogItemMapping] = Field(default_factory=list)

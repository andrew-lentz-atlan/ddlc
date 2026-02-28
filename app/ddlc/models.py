"""
Data models for the DDLC platform.

Uses Pydantic BaseModel for FastAPI integration, validation, and JSON serialization.
All models are designed to be serializable to/from JSON for Dapr state store persistence.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class DDLCStage(str, Enum):
    """Lifecycle stages for a data contract negotiation."""

    REQUEST = "request"
    DISCOVERY = "discovery"
    SPECIFICATION = "specification"
    REVIEW = "review"
    APPROVAL = "approval"
    ACTIVE = "active"
    REJECTED = "rejected"


# Ordered list of non-terminal stages for progression logic
STAGE_ORDER = [
    DDLCStage.REQUEST,
    DDLCStage.DISCOVERY,
    DDLCStage.SPECIFICATION,
    DDLCStage.REVIEW,
    DDLCStage.APPROVAL,
    DDLCStage.ACTIVE,
]


class ContractStatus(str, Enum):
    """ODCS v3.1.0 contract status values."""

    PROPOSED = "proposed"
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


# Map DDLC stages to ODCS contract statuses
STAGE_TO_CONTRACT_STATUS = {
    DDLCStage.REQUEST: ContractStatus.PROPOSED,
    DDLCStage.DISCOVERY: ContractStatus.PROPOSED,
    DDLCStage.SPECIFICATION: ContractStatus.DRAFT,
    DDLCStage.REVIEW: ContractStatus.DRAFT,
    DDLCStage.APPROVAL: ContractStatus.DRAFT,
    DDLCStage.ACTIVE: ContractStatus.ACTIVE,
}


class LogicalType(str, Enum):
    """ODCS v3.1.0 logical data types."""

    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    TIME = "time"
    ARRAY = "array"
    OBJECT = "object"


class QualityCheckType(str, Enum):
    """ODCS quality check types."""

    TEXT = "text"
    LIBRARY = "library"
    SQL = "sql"
    CUSTOM = "custom"


class MonitorMethod(str, Enum):
    """Monte Carlo monitor type mapping for quality checks."""

    FRESHNESS = "freshness"
    VOLUME = "volume"
    SCHEMA = "schema"
    FIELD_HEALTH = "field_health"
    DIMENSION_TRACKING = "dimension_tracking"
    SQL_RULE = "sql_rule"
    REFERENTIAL_INTEGRITY = "referential_integrity"


class AtlanDQSRuleType(str, Enum):
    """Atlan Data Quality Studio rule template types."""

    ROW_COUNT = "ROW_COUNT"
    NULL_COUNT = "NULL_COUNT"
    FRESHNESS = "FRESHNESS"
    STRING_LENGTH = "STRING_LENGTH"
    REGEX_MATCH = "REGEX_MATCH"
    VALID_STRING_VALUES = "VALID_STRING_VALUES"
    CUSTOM_SQL = "CUSTOM_SQL"


class Urgency(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Classification(str, Enum):
    """Common data classification levels."""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    PII = "pii"
    SENSITIVE = "sensitive"


class ServerType(str, Enum):
    """ODCS server types — where the target asset will be materialized."""

    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    DATABRICKS = "databricks"
    REDSHIFT = "redshift"
    POSTGRES = "postgres"
    OTHER = "other"


class AccessLevel(str, Enum):
    """ODCS role access levels."""

    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


# ---------------------------------------------------------------------------
# Participant / team
# ---------------------------------------------------------------------------


class Participant(BaseModel):
    name: str
    email: str


class TeamMember(BaseModel):
    """ODCS team member."""

    name: str
    email: str
    role: str


class Server(BaseModel):
    """An ODCS server entry — connection target for the contract asset."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: ServerType = ServerType.SNOWFLAKE       # snowflake, bigquery, databricks, etc.
    environment: str = "prod"                      # prod, dev, staging, test
    account: Optional[str] = None                  # Snowflake account / GCP project / workspace URL
    database: Optional[str] = None                 # Target database name
    schema_name: Optional[str] = None              # Target schema/dataset name
    host: Optional[str] = None                     # Host/URL (Databricks workspace, Redshift endpoint)
    description: Optional[str] = None             # Human-readable notes
    connection_qualified_name: Optional[str] = None  # e.g. default/snowflake/1770327201 — set via Atlan connection picker


class RoleApprover(BaseModel):
    """An Atlan user selected as a role approver."""

    username: str
    email: str
    guid: Optional[str] = None          # Atlan user GUID — used in Phase 6 for asset ownership assignment
    display_name: Optional[str] = None  # "First Last" for display


class ContractRole(BaseModel):
    """An ODCS role entry — who has access and at what level."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    role: str                                                    # Role name: "Data Consumer", etc.
    access: AccessLevel = AccessLevel.READ                       # read, write, admin
    approvers: List[RoleApprover] = Field(default_factory=list)  # Atlan users
    description: Optional[str] = None


class CustomProperty(BaseModel):
    """A key-value custom metadata entry (ODCS customProperties)."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    key: str
    value: str


# ---------------------------------------------------------------------------
# ODCS contract sub-models
# ---------------------------------------------------------------------------


class ColumnSource(BaseModel):
    """Tracks where a column's data comes from (column-level lineage)."""

    source_table: str  # qualified name or display name of the source table
    source_column: str  # name of the source column
    source_table_qualified_name: Optional[str] = None  # Atlan qualified name
    transform_logic: Optional[str] = None  # SQL expression, description, etc.
    transform_description: Optional[str] = None  # human-readable explanation


class SchemaProperty(BaseModel):
    """A column/field within a schema object (ODCS property)."""

    name: str
    logical_type: LogicalType = LogicalType.STRING
    description: Optional[str] = None
    required: bool = False
    primary_key: bool = False
    primary_key_position: Optional[int] = None
    unique: bool = False
    classification: Optional[str] = None
    examples: Optional[List[str]] = None
    critical_data_element: bool = False
    # Column-level lineage: where does this column's data come from?
    sources: List[ColumnSource] = Field(default_factory=list)


class SourceTable(BaseModel):
    """An existing Atlan asset referenced as a source for the new table."""

    name: str
    qualified_name: Optional[str] = None  # Atlan qualified name
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    connector_name: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[List[dict]] = None  # Cached column metadata from Atlan


class SchemaObject(BaseModel):
    """A table/document/topic within the schema (ODCS object)."""

    name: str
    physical_name: Optional[str] = None
    description: Optional[str] = None
    properties: List[SchemaProperty] = Field(default_factory=list)
    # Source tables this object is derived from (populated from Atlan)
    source_tables: List[SourceTable] = Field(default_factory=list)


class QualityCheck(BaseModel):
    """A data quality rule (ODCS quality)."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: QualityCheckType = QualityCheckType.TEXT
    description: str = ""
    dimension: Optional[str] = None
    metric: Optional[str] = None
    severity: Optional[str] = None
    must_be: Optional[str] = None
    must_be_greater_than: Optional[float] = None
    must_be_less_than: Optional[float] = None
    # Phase 1.2: Monte Carlo enrichment fields
    schedule: Optional[str] = None            # Cron expression, e.g. "0 6 * * *"
    scheduler: Optional[str] = None           # Orchestration tool (cron, airflow, monte-carlo)
    business_impact: Optional[str] = None     # Consequence of rule failure
    method: Optional[str] = None              # MonitorMethod value (string for flexibility)
    column: Optional[str] = None              # Target column: "table.column" or None for table-level
    query: Optional[str] = None               # SQL query for SQL-type checks
    engine: Optional[str] = None              # Tool: monte-carlo, great-expectations, soda, dbt, atlan-dqs
    # Atlan DQS extension fields (populated when engine == "atlan-dqs")
    dqs_rule_type: Optional[str] = None           # AtlanDQSRuleType value
    dqs_threshold_value: Optional[float] = None   # Numeric threshold
    dqs_threshold_unit: Optional[str] = None      # DAYS, HOURS, PERCENTAGE (for FRESHNESS)
    dqs_alert_priority: Optional[str] = None      # HIGH, NORMAL, URGENT
    dqs_custom_sql: Optional[str] = None          # SQL query (for CUSTOM_SQL type)
    dqs_pushed: bool = False                      # True after successful push to Atlan DQS
    dqs_rule_qualified_name: Optional[str] = None # Atlan QN set after creation


class SLAProperty(BaseModel):
    """An SLA entry (ODCS slaProperties)."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    property: str  # latency, availability, freshness, retention, etc.
    value: str
    unit: Optional[str] = None
    description: Optional[str] = None
    # Phase 1.3: Airflow enrichment fields
    schedule: Optional[str] = None      # Cron expression, e.g. "0 6 * * *"
    scheduler: Optional[str] = None     # Orchestration tool: airflow, cron, prefect, dagster
    driver: Optional[str] = None        # Business driver: regulatory, analytics, operational, compliance
    element: Optional[str] = None       # Schema object name this SLA applies to


class ODCSContract(BaseModel):
    """
    The ODCS v3.1.0 data contract being progressively built.

    Fields are stored in a flat/Pythonic structure and converted to the
    nested camelCase ODCS YAML by the serializer in odcs.py.
    """

    api_version: str = "v3.1.0"
    kind: str = "DataContract"
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: Optional[str] = None
    version: str = "0.1.0"
    status: ContractStatus = ContractStatus.PROPOSED
    domain: Optional[str] = None
    domain_qualified_name: Optional[str] = None        # Atlan DataDomain qualified_name
    tenant: Optional[str] = None
    data_product: Optional[str] = None
    data_product_qualified_name: Optional[str] = None  # Atlan Data Product qualified_name
    description_purpose: Optional[str] = None
    description_limitations: Optional[str] = None
    description_usage: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    schema_objects: List[SchemaObject] = Field(default_factory=list)
    quality_checks: List[QualityCheck] = Field(default_factory=list)
    sla_properties: List[SLAProperty] = Field(default_factory=list)
    team: List[TeamMember] = Field(default_factory=list)
    servers: List[Server] = Field(default_factory=list)
    roles: List[ContractRole] = Field(default_factory=list)
    custom_properties: List[CustomProperty] = Field(default_factory=list)
    # Phase 6 — set when placeholder asset is registered in Atlan on APPROVAL → ACTIVE
    atlan_table_qualified_name: Optional[str] = None
    atlan_table_guid: Optional[str] = None
    atlan_table_url: Optional[str] = None  # Direct link into Atlan UI


# ---------------------------------------------------------------------------
# Contract request (Stage 1 input)
# ---------------------------------------------------------------------------


class ContractRequest(BaseModel):
    """Consumer's initial request for a new data asset."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str
    description: str
    business_context: str = ""
    target_use_case: str = ""
    urgency: Urgency = Urgency.MEDIUM
    requester: Participant
    domain: Optional[str] = None
    domain_qualified_name: Optional[str] = None        # Atlan DataDomain qualified_name
    data_product: Optional[str] = None
    data_product_qualified_name: Optional[str] = None  # Atlan Data Product qualified_name
    desired_fields: Optional[List[str]] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Discussion / audit
# ---------------------------------------------------------------------------


class Comment(BaseModel):
    """Discussion item attached to a contract session."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    author: Participant
    content: str
    stage: DDLCStage
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    parent_id: Optional[str] = None


class StageTransition(BaseModel):
    """Audit record of a stage change."""

    from_stage: DDLCStage
    to_stage: DDLCStage
    transitioned_by: Participant
    reason: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# DDLC Session (aggregate root)
# ---------------------------------------------------------------------------


class DDLCSession(BaseModel):
    """
    Master entity tracking the lifecycle of a data contract negotiation.

    A single DDLCSession contains everything: the request, the contract being
    built, participants, comments, and stage history.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    current_stage: DDLCStage = DDLCStage.REQUEST
    request: ContractRequest
    contract: ODCSContract = Field(default_factory=ODCSContract)
    participants: List[Participant] = Field(default_factory=list)
    comments: List[Comment] = Field(default_factory=list)
    history: List[StageTransition] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    workflow_id: Optional[str] = None

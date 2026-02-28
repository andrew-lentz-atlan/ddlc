"""
Demo seed data for the DDLC platform.

Creates pre-populated sessions at various lifecycle stages so the platform
is ready for demos immediately after server start — no manual setup needed.

Session overview
----------------
The first five sessions all tell the same business story — the Finance team's
FACT_ORDERS Gold table request — at different lifecycle stages.  This lets a
presenter click through stages without filling out any forms.

1. REQUEST      — FACT_ORDERS initial request (bare entry point)
2. DISCOVERY    — Sources identified in the Silver layer
3. SPECIFICATION— Schema fully mapped with lineage
4. REVIEW       — Quality checks (Atlan DQS) + SLAs + team added
5. APPROVAL     — Fully signed off; advance to ACTIVE live to register the asset
                  and then click "Push DQ Rules → Atlan" in the ACTIVE panel.   ← PRIMARY DEMO

6. ACTIVE       — WWI Stock Item Dimension (already registered; shows "View in Atlan →")

The rich comment threads across all five stages are intentional — they
demonstrate the core DDLC value proposition: all the context that normally
gets lost in Slack, email, and Jira (naming debates, governance decisions,
exclusion rationale, SLA negotiations) is captured here and becomes input
to AI agents in the Atlan platform.

Usage:
    Called automatically on server startup.
    Can also be triggered via:  POST /api/demo/seed
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timezone, timedelta

from app.ddlc import store
from app.ddlc.models import (
    AccessLevel,
    ColumnSource,
    Comment,
    ContractRequest,
    ContractRole,
    ContractStatus,
    CustomProperty,
    DDLCSession,
    DDLCStage,
    LogicalType,
    ODCSContract,
    Participant,
    QualityCheck,
    QualityCheckType,
    RoleApprover,
    SchemaObject,
    SchemaProperty,
    Server,
    ServerType,
    SLAProperty,
    SourceTable,
    StageTransition,
    TeamMember,
    Urgency,
    STAGE_TO_CONTRACT_STATUS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_now = datetime.now(timezone.utc)
_BASE_URL = os.getenv("ATLAN_BASE_URL", "").rstrip("/")

# Demo connection — "Demo" Snowflake in the hackathon tenant
_DEMO_CONN    = "default/snowflake/1770312446"
_DEMO_DB      = "WIDE_WORLD_IMPORTERS"
_GOLD         = "PROCESSED_GOLD"
_SILVER       = "PROCESSED_SILVER"
_BRONZE_WH    = "BRONZE_WAREHOUSE"

# Andrew_Lentz connection — where new assets will be registered
_ANDREW_CONN  = "default/snowflake/1770327201"

# Shared Finance domain / data product for the FACT_ORDERS story
_FINANCE_DOMAIN    = "Finance"
_FINANCE_DOMAIN_QN = "default/domain/J3sne7aVPzMgU6KYHsoRT"
_SALES_PRODUCT     = "Sales Order Insights"
_SALES_PRODUCT_QN  = "default/domain/J3sne7aVPzMgU6KYHsoRT/super/product/ULc0yDRiVjv19LvuYyaEk"


def _ts(days_ago: int = 0, hours_ago: int = 0) -> datetime:
    return _now - timedelta(days=days_ago, hours=hours_ago)


def _id() -> str:
    return str(uuid.uuid4())


def _qn(*parts) -> str:
    """Build a qualified name from parts: connection + db segments."""
    return "/".join(parts)


# ---------------------------------------------------------------------------
# Real people from the hackathon tenant
# ---------------------------------------------------------------------------

ANDREW   = Participant(name="Andrew Lentz",      email="andrew.lentz@atlan.com")
ANTONIO  = Participant(name="Antonio Hernandez",  email="antonio.hernandez@atlan.com")
ASHISH   = Participant(name="Ashish Desai",       email="ashish.desai@atlan.com")
AVINASH  = Participant(name="Avinash Shankar",    email="avinash.shankar@atlan.com")
AYDAN    = Participant(name="Aydan McNulty",      email="aydan.mcnulty@atlan.com")
BEN      = Participant(name="Ben Hudson",         email="ben.hudson@atlan.com")
PRIYA    = Participant(name="Priya Nair",         email="priya.nair@atlan.com")


# ---------------------------------------------------------------------------
# Shared FACT_ORDERS story data
# (Extracted so all 5 stage sessions share the same sources / schema / rules)
# ---------------------------------------------------------------------------

def _fo_sources():
    """Return the three Silver source tables used by FACT_ORDERS."""
    return (
        SourceTable(
            name="SILVER_ORDERS",
            qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERS"),
            database_name=_DEMO_DB, schema_name=_SILVER, connector_name="snowflake",
            description="Cleaned order headers with validated dates and references.",
            columns=[
                {"name": "ORDERID",       "logical_type": "integer", "is_primary": True,  "is_nullable": False},
                {"name": "CUSTOMERID",    "logical_type": "integer", "is_primary": False, "is_nullable": False},
                {"name": "SALESPERSONID", "logical_type": "integer", "is_primary": False, "is_nullable": False},
                {"name": "ORDERDATE",     "logical_type": "date",    "is_primary": False, "is_nullable": False},
            ],
        ),
        SourceTable(
            name="SILVER_ORDERLINES",
            qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERLINES"),
            database_name=_DEMO_DB, schema_name=_SILVER, connector_name="snowflake",
            description="Cleaned order line items with validated quantities and prices.",
            columns=[
                {"name": "ORDERLINEID",  "logical_type": "integer", "is_primary": True,  "is_nullable": False},
                {"name": "ORDERID",      "logical_type": "integer", "is_primary": False, "is_nullable": False},
                {"name": "STOCKITEMID",  "logical_type": "integer", "is_primary": False, "is_nullable": False},
                {"name": "QUANTITY",     "logical_type": "integer", "is_primary": False, "is_nullable": False},
                {"name": "UNITPRICE",    "logical_type": "number",  "is_primary": False, "is_nullable": False},
                {"name": "TAXRATE",      "logical_type": "number",  "is_primary": False, "is_nullable": False},
            ],
        ),
        SourceTable(
            name="SILVER_CUSTOMERS",
            qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERS"),
            database_name=_DEMO_DB, schema_name=_SILVER, connector_name="snowflake",
            description="Cleaned and validated customer data with standardized fields.",
            columns=[
                {"name": "CUSTOMERID",   "logical_type": "integer", "is_primary": True,  "is_nullable": False},
                {"name": "CUSTOMERNAME", "logical_type": "string",  "is_primary": False, "is_nullable": False},
                {"name": "CREDITLIMIT",  "logical_type": "number",  "is_primary": False, "is_nullable": True},
            ],
        ),
    )


def _fo_schema():
    """Return the FACT_ORDERS SchemaObject (full 12-column spec with lineage)."""
    src_orders_qn  = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERS")
    src_lines_qn   = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERLINES")
    src_cust_qn    = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERS")
    src_orders, src_lines, src_customers = _fo_sources()

    return SchemaObject(
        name="FACT_ORDERS",
        physical_name=f"{_DEMO_DB}.{_GOLD}.FACT_ORDERS",
        description=(
            "Gold-tier order fact table. One row per order line, enriched with "
            "customer name and calculated revenue metrics. Built from three Silver "
            "layer tables — orders, order lines, and customers. "
            "NOTE: CREDITLIMIT intentionally excluded (sensitive financial data per governance review)."
        ),
        source_tables=[src_orders, src_lines, src_customers],
        properties=[
            SchemaProperty(
                name="ORDERLINEID", logical_type=LogicalType.INTEGER,
                description=(
                    "Unique identifier for each order line item (surrogate primary key). "
                    "Sourced directly from SILVER_ORDERLINES where it is a surrogate "
                    "generated by the ERP system — uniqueness guaranteed at Silver layer, "
                    "no deduplication needed at Gold."
                ),
                required=True, primary_key=True, primary_key_position=1,
                unique=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_ORDERLINES", source_column="ORDERLINEID",
                                      source_table_qualified_name=src_lines_qn)],
            ),
            SchemaProperty(
                name="ORDERID", logical_type=LogicalType.INTEGER,
                description=(
                    "Foreign key to the parent order header. "
                    "Multiple order lines share the same ORDERID. "
                    "Use with FACT_ORDERS to aggregate to order level."
                ),
                required=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_ORDERLINES", source_column="ORDERID",
                                      source_table_qualified_name=src_lines_qn)],
            ),
            SchemaProperty(
                name="CUSTOMERID", logical_type=LogicalType.INTEGER,
                description=(
                    "Foreign key to the customer dimension. "
                    "Joins to DIM_CUSTOMER for customer-level aggregations. "
                    "B2B customers only — no individual consumers in this dataset."
                ),
                required=True,
                sources=[ColumnSource(source_table="SILVER_ORDERS", source_column="CUSTOMERID",
                                      source_table_qualified_name=src_orders_qn)],
            ),
            SchemaProperty(
                name="CUSTOMERNAME", logical_type=LogicalType.STRING,
                description=(
                    "Denormalized customer name for BI query convenience — eliminates "
                    "the need for Finance dashboards to join against DIM_CUSTOMER. "
                    "Classified CONFIDENTIAL (B2B business name, not personal data). "
                    "CREDITLIMIT from SILVER_CUSTOMERS is intentionally excluded — "
                    "sensitive financial data not required for revenue analytics."
                ),
                required=True, classification="CONFIDENTIAL",
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="CUSTOMERNAME",
                    source_table_qualified_name=src_cust_qn,
                    transform_logic="JOIN SILVER_CUSTOMERS ON CUSTOMERID",
                    transform_description="Denormalized to eliminate join overhead in BI tools.",
                )],
            ),
            SchemaProperty(
                name="SALESPERSONID", logical_type=LogicalType.INTEGER,
                description="Foreign key to the employee/salesperson dimension.",
                required=True,
                sources=[ColumnSource(source_table="SILVER_ORDERS", source_column="SALESPERSONID",
                                      source_table_qualified_name=src_orders_qn)],
            ),
            SchemaProperty(
                name="STOCKITEMID", logical_type=LogicalType.INTEGER,
                description="Foreign key to DIM_STOCKITEM. Join for product-level analysis.",
                required=True,
                sources=[ColumnSource(source_table="SILVER_ORDERLINES", source_column="STOCKITEMID",
                                      source_table_qualified_name=src_lines_qn)],
            ),
            SchemaProperty(
                name="ORDERDATE", logical_type=LogicalType.DATE,
                description=(
                    "Date the customer placed the order — not the dispatch or delivery date. "
                    "Use this field for all time-based revenue reporting. "
                    "Partition key for Snowflake query performance — always filter on ORDERDATE."
                ),
                required=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_ORDERS", source_column="ORDERDATE",
                                      source_table_qualified_name=src_orders_qn)],
            ),
            SchemaProperty(
                name="QUANTITY", logical_type=LogicalType.INTEGER,
                description="Number of units ordered for this line item.",
                required=True,
                sources=[ColumnSource(source_table="SILVER_ORDERLINES", source_column="QUANTITY",
                                      source_table_qualified_name=src_lines_qn)],
            ),
            SchemaProperty(
                name="UNITPRICE", logical_type=LogicalType.NUMBER,
                description="Price per unit in GBP at the time the order was placed.",
                required=True,
                sources=[ColumnSource(source_table="SILVER_ORDERLINES", source_column="UNITPRICE",
                                      source_table_qualified_name=src_lines_qn)],
            ),
            SchemaProperty(
                name="TAXRATE", logical_type=LogicalType.NUMBER,
                description=(
                    "Tax rate as a percentage (e.g. 15.0 = 15%) applied at order time. "
                    "Historical rates vary — this reflects the actual rate charged, "
                    "not the current rate. Finance confirmed this is correct behavior: "
                    "do not standardize to a single rate."
                ),
                required=True,
                sources=[ColumnSource(source_table="SILVER_ORDERLINES", source_column="TAXRATE",
                                      source_table_qualified_name=src_lines_qn)],
            ),
            SchemaProperty(
                name="LINE_REVENUE", logical_type=LogicalType.NUMBER,
                description=(
                    "Pre-tax revenue for this order line: QUANTITY × UNITPRICE. "
                    "Also known as 'ORDER_REVENUE' in Finance documentation — the name "
                    "LINE_REVENUE was kept for technical precision (it is per line, not per order). "
                    "Computed once at the Gold layer — do not recompute in BI tools."
                ),
                required=True, critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES", source_column="QUANTITY",
                    source_table_qualified_name=src_lines_qn,
                    transform_logic="QUANTITY * UNITPRICE",
                    transform_description="Derived line revenue — computed once at Gold layer.",
                )],
            ),
            SchemaProperty(
                name="LINE_TAX", logical_type=LogicalType.NUMBER,
                description="Tax amount for this order line: LINE_REVENUE × (TAXRATE / 100).",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES", source_column="TAXRATE",
                    source_table_qualified_name=src_lines_qn,
                    transform_logic="(QUANTITY * UNITPRICE) * (TAXRATE / 100)",
                    transform_description="Derived tax amount — computed at Gold layer.",
                )],
            ),
        ],
    )


def _fo_dqs_checks():
    """
    Return the Atlan DQS quality checks for FACT_ORDERS.
    All checks use engine='atlan-dqs' and are ready to be pushed
    to Atlan after the asset is materialized.
    """
    return [
        QualityCheck(
            type=QualityCheckType.LIBRARY,
            description="ORDERDATE must have no null values — every order record requires a valid order date.",
            dimension="completeness",
            severity="critical",
            column="FACT_ORDERS.ORDERDATE",
            schedule="0 7 * * *",
            scheduler="airflow",
            engine="atlan-dqs",
            dqs_rule_type="NULL_COUNT",
            dqs_threshold_value=0.0,
            dqs_alert_priority="URGENT",
            dqs_pushed=False,
            business_impact=(
                "Null order dates break time-series revenue analysis, Finance month-end "
                "reconciliation, and the board dashboard date filters."
            ),
        ),
        QualityCheck(
            type=QualityCheckType.LIBRARY,
            description="ORDERLINEID must have zero nulls — primary key integrity check.",
            dimension="completeness",
            severity="critical",
            column="FACT_ORDERS.ORDERLINEID",
            engine="atlan-dqs",
            dqs_rule_type="NULL_COUNT",
            dqs_threshold_value=0.0,
            dqs_alert_priority="URGENT",
            dqs_pushed=False,
            business_impact=(
                "Null primary keys break deduplication logic and cause undercounting "
                "of order lines in revenue reports and finance reconciliation."
            ),
        ),
        QualityCheck(
            type=QualityCheckType.SQL,
            description="LINE_REVENUE must be >= 0 — no negative revenue lines allowed.",
            dimension="validity",
            severity="critical",
            engine="atlan-dqs",
            dqs_rule_type="CUSTOM_SQL",
            dqs_custom_sql=(
                "SELECT COUNT(*) FROM WIDE_WORLD_IMPORTERS.PROCESSED_GOLD.FACT_ORDERS "
                "WHERE LINE_REVENUE < 0"
            ),
            dqs_threshold_value=0.0,
            dqs_alert_priority="URGENT",
            dqs_pushed=False,
            business_impact=(
                "Negative revenue lines distort sales dashboards, quarterly reporting, "
                "and finance reconciliation. Refunds and adjustments belong in FACT_ADJUSTMENTS."
            ),
        ),
        QualityCheck(
            type=QualityCheckType.LIBRARY,
            description="FACT_ORDERS must contain at least 1,000 rows — guards against pipeline truncation.",
            dimension="volume",
            severity="high",
            schedule="0 7 * * *",
            scheduler="airflow",
            engine="atlan-dqs",
            dqs_rule_type="ROW_COUNT",
            dqs_threshold_value=1000.0,
            dqs_alert_priority="NORMAL",
            dqs_pushed=False,
            business_impact=(
                "A row count below 1,000 signals a pipeline failure or incomplete "
                "daily batch load — causing Finance to miss data for daily close."
            ),
        ),
    ]


def _fo_slas():
    return [
        SLAProperty(
            property="freshness", value="24", unit="hours",
            description=(
                "Data refreshed daily at 06:00 UTC via Airflow DAG wwi_gold_orders. "
                "24-hour SLA agreed with Finance — weekly board report tolerates up to 23h59m staleness."
            ),
            schedule="0 6 * * *", scheduler="airflow",
            driver="analytics", element="FACT_ORDERS",
        ),
        SLAProperty(
            property="availability", value="99.5", unit="percent",
            description="Target Snowflake uptime for downstream Finance BI dashboards.",
            driver="operational", element="FACT_ORDERS",
        ),
        SLAProperty(
            property="latency", value="45", unit="minutes",
            description=(
                "Maximum acceptable pipeline runtime from Silver extract to Gold load. "
                "Consistently achieved in 35-40min — 45min allows for Snowflake queue delays."
            ),
            schedule="0 6 * * *", scheduler="airflow",
            driver="analytics", element="FACT_ORDERS",
        ),
        SLAProperty(
            property="retention", value="7", unit="years",
            description=(
                "Retain all order history for 7 years per SOX compliance requirement. "
                "Confirmed by Ben Hudson (Governance) — B2B financial records, no GDPR erasure obligation."
            ),
            driver="regulatory",
        ),
    ]


def _fo_team():
    return [
        TeamMember(name="Andrew Lentz",      email="andrew.lentz@atlan.com",      role="Data Owner"),
        TeamMember(name="Avinash Shankar",   email="avinash.shankar@atlan.com",   role="Data Steward"),
        TeamMember(name="Antonio Hernandez", email="antonio.hernandez@atlan.com", role="Data Engineer"),
        TeamMember(name="Aydan McNulty",     email="aydan.mcnulty@atlan.com",     role="Analytics Engineer"),
        TeamMember(name="Priya Nair",        email="priya.nair@atlan.com",        role="Business Stakeholder"),
    ]


def _fo_servers():
    return [
        Server(
            type=ServerType.SNOWFLAKE, environment="prod",
            account=os.getenv("ATLAN_SNOWFLAKE_ACCOUNT", "your-account"),
            database=_DEMO_DB, schema_name=_GOLD,
            description="Demo Snowflake — Wide World Importers Gold layer.",
            connection_qualified_name=_ANDREW_CONN,
        ),
    ]


def _fo_roles():
    return [
        ContractRole(
            role="Data Consumer", access=AccessLevel.READ,
            approvers=[RoleApprover(
                username="andrew.lentz@atlan.com",
                email="andrew.lentz@atlan.com",
                display_name="Andrew Lentz",
            )],
            description="Finance, Sales Ops, and Exec teams consuming revenue reports and dashboards.",
        ),
        ContractRole(
            role="Data Producer", access=AccessLevel.WRITE,
            approvers=[
                RoleApprover(username="andrew.lentz@atlan.com",     email="andrew.lentz@atlan.com",     display_name="Andrew Lentz"),
                RoleApprover(username="antonio.hernandez@atlan.com", email="antonio.hernandez@atlan.com", display_name="Antonio Hernandez"),
            ],
            description="Data engineering team responsible for the Silver → Gold pipeline.",
        ),
        ContractRole(
            role="Data Owner", access=AccessLevel.ADMIN,
            approvers=[RoleApprover(
                username="andrew.lentz@atlan.com",
                email="andrew.lentz@atlan.com",
                display_name="Andrew Lentz",
            )],
            description="Andrew Lentz owns this data product — all schema changes require his sign-off.",
        ),
    ]


def _fo_custom_props():
    return [
        CustomProperty(key="cost_center",   value="finance-analytics"),
        CustomProperty(key="source_system",  value="wide-world-importers"),
        CustomProperty(key="currency",       value="GBP"),
        CustomProperty(key="grain",          value="order-line"),
    ]


# ============================================================================
# Session 1 — REQUEST: FACT_ORDERS initial request (bare entry point)
# ============================================================================

def _build_fact_orders_request() -> DDLCSession:
    """Brand-new request — just submitted. Shows the lifecycle entry point."""
    session_id = _id()

    return DDLCSession(
        id=session_id,
        current_stage=DDLCStage.REQUEST,
        request=ContractRequest(
            title="WWI Sales Order Analytics — FACT_ORDERS",
            description=(
                "Build a gold-tier order fact table from Wide World Importers data. "
                "We need one row per order line with denormalized customer name, "
                "calculated revenue and tax metrics, and all FK references for "
                "joining against the existing dimension tables."
            ),
            business_context=(
                "The Finance team needs a reliable single source of truth for order-level "
                "revenue. Current queries hit Silver tables directly, causing inconsistent "
                "figures across dashboards. A governed Gold fact table with a formal "
                "data contract will standardize metrics and enable self-service analytics."
            ),
            target_use_case=(
                "Daily revenue dashboards, salesperson performance reporting, "
                "product-level sales analysis, customer order history, "
                "finance month-end reconciliation."
            ),
            urgency=Urgency.HIGH,
            requester=PRIYA,
            domain=_FINANCE_DOMAIN,
            domain_qualified_name=_FINANCE_DOMAIN_QN,
            data_product=_SALES_PRODUCT,
            data_product_qualified_name=_SALES_PRODUCT_QN,
            desired_fields=[
                "ORDERLINEID", "ORDERID", "CUSTOMERID", "CUSTOMERNAME",
                "SALESPERSONID", "STOCKITEMID", "ORDERDATE",
                "QUANTITY", "UNITPRICE", "TAXRATE", "LINE_REVENUE", "LINE_TAX",
            ],
            created_at=_ts(days_ago=14, hours_ago=2),
        ),
        contract=ODCSContract(
            name="WWI Sales Order Analytics — FACT_ORDERS",
            status=ContractStatus.PROPOSED,
            domain=_FINANCE_DOMAIN,
            domain_qualified_name=_FINANCE_DOMAIN_QN,
            data_product=_SALES_PRODUCT,
            data_product_qualified_name=_SALES_PRODUCT_QN,
        ),
        participants=[PRIYA, ANDREW],
        comments=[
            Comment(
                author=PRIYA,
                content=(
                    "Hi team — raising this formally after last quarter's close. "
                    "Finance ran three different revenue figures for Q3 and we spent two days "
                    "reconciling them. Root cause: everyone is querying SILVER_ORDERLINES "
                    "differently. Sarah's dashboard applies TAXRATE as a multiplier, mine "
                    "adds it as a flat amount, and the exec report just ignores it entirely. "
                    "We need one official Gold table with the math locked down in a contract."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=14, hours_ago=2),
            ),
            Comment(
                author=ANDREW,
                content=(
                    "Totally agree — this has been a recurring issue. "
                    "Few questions before I assign this: "
                    "(1) What grain do you need — one row per order or per order line? "
                    "(2) How often does Finance actually refresh the board report? "
                    "(3) Who else consumes this beyond Finance — Sales Ops? Exec?"
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=14, hours_ago=1),
            ),
            Comment(
                author=ANTONIO,
                content=(
                    "Quick flag before we start: there was an OLD_FACT_ORDERS table that "
                    "got deprecated last year. I don't want to rebuild something that was "
                    "retired for a reason. Priya, do you know why it was decommissioned?"
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=13, hours_ago=8),
            ),
            Comment(
                author=PRIYA,
                content=(
                    "Good catch Antonio. OLD_FACT_ORDERS was deprecated because it mixed "
                    "GBP and USD values in LINE_REVENUE without flagging the currency — "
                    "completely unusable for consolidated reporting. This new table is "
                    "GBP-only (WWI UK entity only). "
                    "Answering Andrew: (1) per order line — Finance needs line-level detail "
                    "for invoice reconciliation. (2) Weekly for board, daily for ops. "
                    "(3) Yes — Sales Ops uses it for commission calculations, "
                    "Exec for monthly KPIs."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=13, hours_ago=4),
            ),
            Comment(
                author=AVINASH,
                content=(
                    "Will CUSTOMERNAME be included? If so I need to flag that SILVER_CUSTOMERS "
                    "also has CREDITLIMIT — a sensitive financial field. "
                    "We should be explicit about what gets included and what doesn't."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=13, hours_ago=2),
            ),
            Comment(
                author=ANDREW,
                content=(
                    "Good point Avinash. CUSTOMERNAME yes — Finance needs it for invoice "
                    "matching without a join. CREDITLIMIT no — not relevant to revenue "
                    "analytics and we shouldn't expose financial exposure data to all BI users. "
                    "Antonio — assigning you as DE lead. Ben, heads up — governance review "
                    "will be needed given the CONFIDENTIAL classification on customer fields."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=13, hours_ago=1),
            ),
            Comment(
                author=ANTONIO,
                content="Got it. I'll start discovery this sprint. Estimated 2 days to map sources.",
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=12, hours_ago=6),
            ),
            Comment(
                author=BEN,
                content=(
                    "Flagging now: given customer data is included, SOX retention policy "
                    "applies — minimum 7 years for financial records. "
                    "Also need to confirm there's no GDPR obligation (WWI is B2B so "
                    "probably exempt, but let's verify during discovery)."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=12, hours_ago=4),
            ),
        ],
        history=[],
        created_at=_ts(days_ago=14, hours_ago=2),
        updated_at=_ts(days_ago=12, hours_ago=4),
    )


# ============================================================================
# Session 2 — DISCOVERY: FACT_ORDERS — sources identified
# ============================================================================

def _build_fact_orders_discovery() -> DDLCSession:
    """Discovery stage — Silver source tables found and catalogued in Atlan."""
    session_id = _id()

    return DDLCSession(
        id=session_id,
        current_stage=DDLCStage.DISCOVERY,
        request=ContractRequest(
            title="WWI Sales Order Analytics — FACT_ORDERS",
            description=(
                "Build a gold-tier order fact table from Wide World Importers data. "
                "One row per order line with denormalized customer name, "
                "calculated revenue and tax metrics."
            ),
            business_context=(
                "Finance needs a single source of truth for order-level revenue — "
                "current Silver queries produce inconsistent figures across dashboards."
            ),
            target_use_case=(
                "Daily revenue dashboards, salesperson performance, "
                "product sales analysis, finance month-end reconciliation."
            ),
            urgency=Urgency.HIGH,
            requester=PRIYA,
            domain=_FINANCE_DOMAIN,
            domain_qualified_name=_FINANCE_DOMAIN_QN,
            data_product=_SALES_PRODUCT,
            data_product_qualified_name=_SALES_PRODUCT_QN,
            desired_fields=[
                "ORDERLINEID", "ORDERID", "CUSTOMERID", "CUSTOMERNAME",
                "SALESPERSONID", "STOCKITEMID", "ORDERDATE",
                "QUANTITY", "UNITPRICE", "TAXRATE", "LINE_REVENUE", "LINE_TAX",
            ],
            created_at=_ts(days_ago=14),
        ),
        contract=ODCSContract(
            name="WWI Sales Order Analytics — FACT_ORDERS",
            status=ContractStatus.PROPOSED,
            domain=_FINANCE_DOMAIN,
            domain_qualified_name=_FINANCE_DOMAIN_QN,
            data_product=_SALES_PRODUCT,
            data_product_qualified_name=_SALES_PRODUCT_QN,
            tags=["orders", "fact-table", "gold-tier", "wide-world-importers"],
        ),
        participants=[PRIYA, ANDREW, ANTONIO, AVINASH],
        comments=[
            Comment(
                author=PRIYA, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=14),
                content=(
                    "Finance ran three different Q3 revenue figures last close. "
                    "Everyone queries SILVER_ORDERLINES differently — we need one official "
                    "Gold table with the math locked down in a contract."
                ),
            ),
            Comment(
                author=ANDREW, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=13, hours_ago=20),
                content=(
                    "Agreed. OLD_FACT_ORDERS was deprecated because it mixed currencies. "
                    "This new table is GBP-only. Antonio assigned as DE lead. "
                    "CUSTOMERNAME in, CREDITLIMIT out."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=11),
                content=(
                    "Sources identified. Everything we need is in the Silver layer: "
                    "SILVER_ORDERS (order headers + dates), SILVER_ORDERLINES (line items, prices, tax), "
                    "SILVER_CUSTOMERS (name denormalization). All three are already crawled "
                    "and catalogued in Atlan with documented lineage from Bronze. "
                    "Three-table join — no complex transformations needed."
                ),
            ),
            Comment(
                author=AVINASH, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=11, hours_ago=4),
                content=(
                    "SILVER_CUSTOMERS has CREDITLIMIT as I flagged. It's a NUMBER field, "
                    "nullable, with no masking applied in Silver. We must explicitly exclude "
                    "it from the Gold table and document the exclusion in the contract "
                    "so future engineers don't accidentally add it back."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=11, hours_ago=2),
                content=(
                    "Confirmed — CREDITLIMIT will NOT be included. Documenting in the schema spec: "
                    "'CREDITLIMIT intentionally excluded — sensitive financial exposure data, "
                    "not required for revenue analytics.' "
                    "Also: should we pull SALESPERSONNAME the same way we're pulling CUSTOMERNAME? "
                    "It would save Sales Ops a join."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=10, hours_ago=6),
                content=(
                    "On TAXRATE — I've seen 15% and 17.5% values in historical data. "
                    "Is that a data quality problem? Finance has been assuming 15% flat "
                    "in all our models."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=10, hours_ago=2),
                content=(
                    "Not a bug — TAXRATE reflects the actual rate charged at order time. "
                    "VAT changed from 15% to 17.5% during the 2010 period. Historical data "
                    "is correct. Finance should NOT standardize to a single rate — that would "
                    "misrepresent historical tax liability. I'll add a DQ check to ensure "
                    "TAXRATE is always within a valid range (0-30%) to catch future errors."
                ),
            ),
            Comment(
                author=AYDAN, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=10),
                content=(
                    "What's the current row volume? I need to estimate Snowflake credit cost "
                    "before we commit to the 06:00 UTC daily refresh SLA."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=9, hours_ago=12),
                content=(
                    "Current Silver row count: ~2.3M order lines. Growth rate: ~80k/week. "
                    "Estimated cost: 2-3 Snowflake credits per daily refresh. "
                    "Partitioning on ORDERDATE is mandatory — added that to the spec notes. "
                    "On SALESPERSONNAME: I'd say no — that's DIM_EMPLOYEE territory. "
                    "Sales Ops can do a single join. Keeps FACT_ORDERS tight."
                ),
            ),
            Comment(
                author=ANDREW, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=9, hours_ago=8),
                content=(
                    "Agree — SALESPERSONNAME stays out. Keep the fact table lean. "
                    "CREDITLIMIT exclusion documented. TAXRATE variance explained and confirmed. "
                    "GDPR check: WWI is B2B only, no individual consumer data — Ben confirmed "
                    "no GDPR erasure obligation. SOX 7yr retention applies. "
                    "Good to move to SPECIFICATION. Antonio, hand off to Aydan for schema mapping."
                ),
            ),
        ],
        history=[
            StageTransition(
                from_stage=DDLCStage.REQUEST, to_stage=DDLCStage.DISCOVERY,
                transitioned_by=ANTONIO,
                reason=(
                    "Three Silver source tables identified: SILVER_ORDERS, SILVER_ORDERLINES, "
                    "SILVER_CUSTOMERS. CREDITLIMIT excluded per governance. "
                    "TAXRATE historical variance confirmed as correct behavior, not a data quality issue."
                ),
                timestamp=_ts(days_ago=12),
            ),
        ],
        created_at=_ts(days_ago=14),
        updated_at=_ts(days_ago=9, hours_ago=8),
    )


# ============================================================================
# Session 3 — SPECIFICATION: FACT_ORDERS — schema fully mapped
# ============================================================================

def _build_fact_orders_specification() -> DDLCSession:
    """Specification stage — 12-column schema with source lineage mapped."""
    session_id = _id()

    contract = ODCSContract(
        name="WWI Sales Order Analytics — FACT_ORDERS",
        version="0.1.0",
        status=ContractStatus.DRAFT,
        domain=_FINANCE_DOMAIN,
        domain_qualified_name=_FINANCE_DOMAIN_QN,
        data_product=_SALES_PRODUCT,
        data_product_qualified_name=_SALES_PRODUCT_QN,
        description_purpose=(
            "Provide a single governed order-line fact table for Finance, eliminating "
            "inconsistent ad-hoc Silver queries and standardizing revenue metrics across "
            "all BI tools and dashboards."
        ),
        description_limitations=(
            "One row per order line — not suitable for customer or product roll-ups "
            "without aggregation. Derived fields (LINE_REVENUE, LINE_TAX) are "
            "computed at load time and cannot be recalculated retroactively. "
            "GBP currency only — WWI UK entity. CREDITLIMIT intentionally excluded."
        ),
        description_usage=(
            "Join with DIM_CUSTOMER on CUSTOMERID for customer analysis. "
            "Join with DIM_STOCKITEM on STOCKITEMID for product analysis. "
            "Filter on ORDERDATE for time-based reporting. "
            "Pre-aggregate to DAILY_SALES_SUMMARY for dashboard performance."
        ),
        tags=["orders", "fact-table", "gold-tier", "wide-world-importers", "revenue"],
        schema_objects=[_fo_schema()],
        servers=_fo_servers(),
        custom_properties=_fo_custom_props(),
    )

    return DDLCSession(
        id=session_id,
        current_stage=DDLCStage.SPECIFICATION,
        request=ContractRequest(
            title="WWI Sales Order Analytics — FACT_ORDERS",
            description=(
                "Build a gold-tier order fact table from Wide World Importers data. "
                "One row per order line with denormalized customer name and revenue metrics."
            ),
            business_context="Finance needs a single source of truth for order-level revenue.",
            target_use_case=(
                "Daily revenue dashboards, salesperson performance, "
                "product sales analysis, finance month-end reconciliation."
            ),
            urgency=Urgency.HIGH,
            requester=PRIYA,
            domain=_FINANCE_DOMAIN,
            domain_qualified_name=_FINANCE_DOMAIN_QN,
            data_product=_SALES_PRODUCT,
            data_product_qualified_name=_SALES_PRODUCT_QN,
            created_at=_ts(days_ago=14),
        ),
        contract=contract,
        participants=[PRIYA, ANDREW, ANTONIO, AVINASH, AYDAN],
        comments=[
            Comment(author=PRIYA, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=14),
                    content="Finance ran three different Q3 revenue figures. Need one official Gold table."),
            Comment(author=ANDREW, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=13, hours_ago=20),
                    content="CUSTOMERNAME in, CREDITLIMIT out. TAXRATE historical variance confirmed as expected behavior."),
            Comment(
                author=AYDAN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=8),
                content=(
                    "12-column schema draft ready for review. Mapped full source lineage "
                    "for all columns. LINE_REVENUE = QUANTITY × UNITPRICE, "
                    "LINE_TAX = LINE_REVENUE × (TAXRATE / 100). "
                    "Before I finalize — should the derived column be called LINE_REVENUE "
                    "or ORDER_REVENUE? Priya, your Finance docs use 'order revenue'."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=8, hours_ago=4),
                content=(
                    "Finance internally calls it 'Order Revenue' — can we use ORDER_REVENUE "
                    "as the column name? That's what's in all our Tableau calculated fields."
                ),
            ),
            Comment(
                author=AYDAN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=8, hours_ago=2),
                content=(
                    "Technically ORDER_REVENUE is ambiguous — this column is revenue per ORDER LINE, "
                    "not per order. If you sum it without grouping by ORDERID first you get "
                    "order-level revenue, but the column itself is line-level. "
                    "Renaming it now would also break existing dbt models. "
                    "Proposal: keep LINE_REVENUE as the physical name, add a description note "
                    "saying 'also known as ORDER_REVENUE in Finance documentation'."
                ),
            ),
            Comment(
                author=ANDREW, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=7, hours_ago=8),
                content=(
                    "Agreed — keep LINE_REVENUE. Aydan, add the alias note to the column "
                    "description. Priya, you'll need to update your Tableau calculated fields "
                    "to reference LINE_REVENUE. Blocking any future naming debate: "
                    "this decision is now documented in the contract."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=7, hours_ago=4),
                content=(
                    "Proposing a 1-day FRESHNESS threshold for the DQS check on ORDERDATE. "
                    "Data loads nightly at 06:00 UTC. If the Airflow job fails, should we "
                    "alert after 24 hours or 6 hours? Priya — how critical is same-day data?"
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=7, hours_ago=2),
                content=(
                    "24 hours is fine. The board report runs weekly and ops dashboards "
                    "refresh at 8am. As long as data is there by 7am we're good. "
                    "A 6-hour threshold would generate false alarms on weekends."
                ),
            ),
            Comment(
                author=AVINASH, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=6, hours_ago=8),
                content=(
                    "ORDERLINEID is the PK — do we need a UNIQUENESS check on top of NULL_COUNT? "
                    "Antonio mentioned the surrogate is guaranteed by Silver ETL, but I'd rather "
                    "have a belt-and-suspenders approach for a critical financial table."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=6, hours_ago=4),
                content=(
                    "NULL_COUNT is the right check — if an ORDERLINEID is null it means the "
                    "Silver ETL broke. Duplicates can't happen because ORDERLINEID is a surrogate "
                    "key generated by the ERP with a SEQUENCE — there's no merge/upsert logic "
                    "that could introduce duplicates. Adding that explanation to the column "
                    "description so future engineers understand the design decision."
                ),
            ),
            Comment(
                author=BEN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=6, hours_ago=2),
                content=(
                    "Governance review of CUSTOMERNAME classification: confirming CONFIDENTIAL. "
                    "These are legal entity names for B2B customers — not personal data "
                    "under GDPR. Classification policy v2.3 section 4.2 applies. "
                    "Retention: 7 years per SOX, confirmed. No GDPR erasure obligation. "
                    "CREDITLIMIT exclusion is documented — that's all I need. "
                    "Green light from Governance to proceed to REVIEW."
                ),
            ),
            Comment(
                author=AYDAN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=5, hours_ago=6),
                content=(
                    "All column descriptions updated with business-facing language. "
                    "LINE_REVENUE description now includes the Finance alias note. "
                    "ORDERDATE description clarifies 'order placed date, not dispatch date' "
                    "(this bit me last year when I thought ORDERDATE was shipment date). "
                    "Schema ready — moving to REVIEW."
                ),
            ),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST, to_stage=DDLCStage.DISCOVERY,
                            transitioned_by=ANTONIO,
                            reason="Three Silver source tables identified. CREDITLIMIT excluded per governance.",
                            timestamp=_ts(days_ago=12)),
            StageTransition(from_stage=DDLCStage.DISCOVERY, to_stage=DDLCStage.SPECIFICATION,
                            transitioned_by=ANTONIO,
                            reason=(
                                "Silver sources confirmed. TAXRATE historical variance documented "
                                "as correct behavior. CREDITLIMIT exclusion noted. "
                                "GDPR: B2B only, no erasure obligation. Handing to Aydan for schema."
                            ),
                            timestamp=_ts(days_ago=9)),
        ],
        created_at=_ts(days_ago=14),
        updated_at=_ts(days_ago=5, hours_ago=6),
    )


# ============================================================================
# Session 4 — REVIEW: FACT_ORDERS — quality checks (Atlan DQS) + SLAs added
# ============================================================================

def _build_fact_orders_review() -> DDLCSession:
    """Review stage — full spec with Atlan DQS quality checks, SLAs, and team."""
    session_id = _id()

    contract = ODCSContract(
        name="WWI Sales Order Analytics — FACT_ORDERS",
        version="0.1.0",
        status=ContractStatus.DRAFT,
        domain=_FINANCE_DOMAIN,
        domain_qualified_name=_FINANCE_DOMAIN_QN,
        data_product=_SALES_PRODUCT,
        data_product_qualified_name=_SALES_PRODUCT_QN,
        description_purpose=(
            "Provide a single governed order-line fact table for Finance, eliminating "
            "inconsistent ad-hoc Silver queries and standardizing revenue metrics."
        ),
        description_limitations=(
            "One row per order line — not suitable for roll-ups without aggregation. "
            "Derived fields computed at load time only. GBP currency only."
        ),
        description_usage=(
            "Join with DIM_CUSTOMER on CUSTOMERID, DIM_STOCKITEM on STOCKITEMID. "
            "Filter on ORDERDATE for time-based reporting."
        ),
        tags=["orders", "fact-table", "gold-tier", "wide-world-importers", "revenue"],
        schema_objects=[_fo_schema()],
        quality_checks=_fo_dqs_checks(),
        sla_properties=_fo_slas(),
        team=_fo_team(),
        servers=_fo_servers(),
        roles=_fo_roles(),
        custom_properties=_fo_custom_props(),
    )

    return DDLCSession(
        id=session_id,
        current_stage=DDLCStage.REVIEW,
        request=ContractRequest(
            title="WWI Sales Order Analytics — FACT_ORDERS",
            description="Build a gold-tier order fact table from Wide World Importers data.",
            business_context="Finance needs a single source of truth for order-level revenue.",
            target_use_case="Daily revenue dashboards, salesperson performance, finance month-end.",
            urgency=Urgency.HIGH,
            requester=PRIYA,
            domain=_FINANCE_DOMAIN,
            domain_qualified_name=_FINANCE_DOMAIN_QN,
            data_product=_SALES_PRODUCT,
            data_product_qualified_name=_SALES_PRODUCT_QN,
            created_at=_ts(days_ago=14),
        ),
        contract=contract,
        participants=[PRIYA, ANDREW, ANTONIO, AVINASH, AYDAN, BEN],
        comments=[
            Comment(author=PRIYA, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=14),
                    content="Finance ran three different Q3 revenue figures. Need one official Gold table."),
            Comment(author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=11),
                    content="Sources: SILVER_ORDERS, SILVER_ORDERLINES, SILVER_CUSTOMERS. All crawled in Atlan. CREDITLIMIT excluded."),
            Comment(author=AYDAN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=8),
                    content="LINE_REVENUE kept (not renamed to ORDER_REVENUE). Alias documented in column description. Finance alias noted."),
            Comment(author=BEN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=6, hours_ago=2),
                    content="Governance: CONFIDENTIAL classification confirmed. SOX 7yr retention. No GDPR obligation. Green light."),
            Comment(
                author=AVINASH, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=4),
                content=(
                    "Starting formal governance review. Scope: PII classification, "
                    "retention policy, DQ rule coverage, role-based access definitions. "
                    "Will post findings below."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=4, hours_ago=6),
                content=(
                    "Engineering review: all 4 DQS rules validated against 30 days of data. "
                    "Results: FRESHNESS would fire 0 times (loads always complete by 06:30), "
                    "NULL_COUNT 0 failures, negative LINE_REVENUE 0 rows, "
                    "ROW_COUNT floor never breached (min observed: 2.1M). "
                    "Rules are calibrated correctly — won't generate noise."
                ),
            ),
            Comment(
                author=AYDAN, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=3, hours_ago=8),
                content=(
                    "Analytics review: all 12 columns have source lineage documented. "
                    "LINE_REVENUE and LINE_TAX transform logic is explicit. "
                    "dbt model reference confirmed — all column names match dbt schema.yml. "
                    "ORDERDATE documented as 'order placed date, not dispatch date' — "
                    "this would have broken 2 dashboards last year if not caught here."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=3, hours_ago=4),
                content=(
                    "Finance team review: schema looks correct. "
                    "One request: the TAXRATE description should clarify that historical "
                    "variance is expected behavior, not a data issue. Otherwise our "
                    "support team will raise tickets every time they see non-15% values."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=3, hours_ago=2),
                content=(
                    "TAXRATE description updated: 'Tax rate as a percentage applied at order time. "
                    "Historical rates vary (e.g. 15% vs 17.5%) — reflects actual rate charged, "
                    "not current rate. This is correct behavior.' Done."
                ),
            ),
            Comment(
                author=AVINASH, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=2, hours_ago=8),
                content=(
                    "Governance review complete. ✅ CUSTOMERNAME: CONFIDENTIAL correct. "
                    "✅ Retention: 7yr SOX compliant. ✅ CREDITLIMIT: excluded and documented. "
                    "✅ DQ coverage: freshness, completeness, validity, volume all covered. "
                    "✅ Roles: Consumer (READ), Producer (WRITE), Owner (ADMIN) — appropriate. "
                    "Recommendation: add UNIQUENESS check on ORDERLINEID in next iteration. "
                    "Data Steward sign-off ✅"
                ),
            ),
            Comment(
                author=ANDREW, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=2, hours_ago=4),
                content=(
                    "All review items addressed. "
                    "Engineering ✅ (Antonio) | Analytics ✅ (Aydan) | "
                    "Finance ✅ (Priya) | Governance ✅ (Avinash + Ben). "
                    "Moving to APPROVAL."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=2, hours_ago=2),
                content="DE sign-off ✅. Pipeline ready. DQS rules confirmed. Ready to activate on approval.",
            ),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST, to_stage=DDLCStage.DISCOVERY,
                            transitioned_by=ANTONIO,
                            reason="Three Silver sources identified. CREDITLIMIT excluded. TAXRATE variance noted.",
                            timestamp=_ts(days_ago=12)),
            StageTransition(from_stage=DDLCStage.DISCOVERY, to_stage=DDLCStage.SPECIFICATION,
                            transitioned_by=ANTONIO,
                            reason="Sources confirmed. GDPR: B2B only, no erasure obligation. Handing to Aydan.",
                            timestamp=_ts(days_ago=9)),
            StageTransition(from_stage=DDLCStage.SPECIFICATION, to_stage=DDLCStage.REVIEW,
                            transitioned_by=AYDAN,
                            reason=(
                                "12-column schema complete with full lineage. "
                                "LINE_REVENUE naming debate resolved and documented. "
                                "Ben confirmed CONFIDENTIAL classification. "
                                "DQS rules and SLAs added. Sending for review."
                            ),
                            timestamp=_ts(days_ago=5)),
        ],
        created_at=_ts(days_ago=14),
        updated_at=_ts(days_ago=2, hours_ago=2),
    )


# ============================================================================
# Session 5 — APPROVAL: FACT_ORDERS — fully signed off   ← PRIMARY DEMO
# ============================================================================
#
# This is the session to advance live during the demo:
#   1. Click "Approve" → Atlan table asset is registered
#   2. Then click "Push DQ Rules → Atlan" in the ACTIVE panel
#
# Note: the table is renamed on each restart (timestamp suffix) so each live
# run creates a net-new asset in Atlan.
# ============================================================================

def _build_fact_orders_approval() -> DDLCSession:
    """PRIMARY DEMO — real WWI data, ready to advance to ACTIVE live."""
    session_id = _id()
    contract_id = _id()

    contract = ODCSContract(
        id=contract_id,
        name="WWI Sales Order Analytics — FACT_ORDERS",
        version="0.2.0",
        status=ContractStatus.DRAFT,
        domain=_FINANCE_DOMAIN,
        domain_qualified_name=_FINANCE_DOMAIN_QN,
        data_product=_SALES_PRODUCT,
        data_product_qualified_name=_SALES_PRODUCT_QN,
        description_purpose=(
            "Provide a single governed order-line fact table for Finance, eliminating "
            "inconsistent ad-hoc Silver queries and standardizing revenue metrics across "
            "all BI tools and dashboards."
        ),
        description_limitations=(
            "One row per order line — not suitable for customer or product roll-ups "
            "without aggregation. Derived fields (LINE_REVENUE, LINE_TAX) are "
            "computed at load time and cannot be recalculated retroactively. "
            "GBP currency only (WWI UK entity). CREDITLIMIT intentionally excluded — "
            "see comment thread for governance rationale."
        ),
        description_usage=(
            "Join with DIM_CUSTOMER on CUSTOMERID for customer analysis. "
            "Join with DIM_STOCKITEM on STOCKITEMID for product analysis. "
            "Filter on ORDERDATE for time-based reporting. "
            "Pre-aggregate to DAILY_SALES_SUMMARY for dashboard performance. "
            "Note: LINE_REVENUE = QUANTITY × UNITPRICE (also called 'Order Revenue' in Finance docs)."
        ),
        tags=["orders", "fact-table", "gold-tier", "wide-world-importers", "revenue", "finance"],
        schema_objects=[_fo_schema()],
        quality_checks=_fo_dqs_checks(),
        sla_properties=_fo_slas(),
        team=_fo_team(),
        servers=_fo_servers(),
        roles=_fo_roles(),
        custom_properties=_fo_custom_props(),
    )

    return DDLCSession(
        id=session_id,
        current_stage=DDLCStage.APPROVAL,
        request=ContractRequest(
            title="WWI Sales Order Analytics — FACT_ORDERS",
            description=(
                "Build a gold-tier order fact table from Wide World Importers data. "
                "We need one row per order line with denormalized customer name, "
                "calculated revenue and tax metrics, and all FK references for "
                "joining against the existing dimension tables."
            ),
            business_context=(
                "The Finance team needs a reliable single source of truth for order-level "
                "revenue. Current queries hit Silver tables directly, causing inconsistent "
                "figures across dashboards. A governed Gold fact table with a formal "
                "data contract will standardize metrics and enable self-service analytics."
            ),
            target_use_case=(
                "Daily revenue dashboards, salesperson performance reporting, "
                "product-level sales analysis, customer order history, "
                "finance month-end reconciliation."
            ),
            urgency=Urgency.HIGH,
            requester=PRIYA,
            domain=_FINANCE_DOMAIN,
            domain_qualified_name=_FINANCE_DOMAIN_QN,
            data_product=_SALES_PRODUCT,
            data_product_qualified_name=_SALES_PRODUCT_QN,
            desired_fields=[
                "ORDERLINEID", "ORDERID", "CUSTOMERID", "CUSTOMERNAME",
                "SALESPERSONID", "STOCKITEMID", "ORDERDATE",
                "QUANTITY", "UNITPRICE", "TAXRATE", "LINE_REVENUE", "LINE_TAX",
            ],
            created_at=_ts(days_ago=14),
        ),
        contract=contract,
        participants=[PRIYA, ANDREW, ANTONIO, AVINASH, AYDAN, BEN],
        comments=[
            # ── REQUEST stage ─────────────────────────────────────────────
            Comment(
                author=PRIYA, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=14),
                content=(
                    "Raising this formally after Q3 close. Finance ran three different "
                    "revenue figures and spent two days reconciling. Everyone queries "
                    "SILVER_ORDERLINES differently — wrong TAXRATE math, different filters. "
                    "We need one official Gold table with the math locked down in a contract."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=13, hours_ago=20),
                content=(
                    "Quick flag: there was an OLD_FACT_ORDERS table deprecated last year. "
                    "Was it retired for a reason? Don't want to rebuild something with the "
                    "same flaws."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=13, hours_ago=16),
                content=(
                    "OLD_FACT_ORDERS mixed GBP and USD in LINE_REVENUE without a currency flag — "
                    "unusable for consolidated reporting. This new table is GBP-only. "
                    "That was the entire reason it was deprecated."
                ),
            ),
            # ── DISCOVERY stage ───────────────────────────────────────────
            Comment(
                author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=11),
                content=(
                    "Sources scoped: SILVER_ORDERS (headers + dates), SILVER_ORDERLINES "
                    "(line items + pricing), SILVER_CUSTOMERS (name denorm). "
                    "All three catalogued in Atlan. Simple three-table join. "
                    "CREDITLIMIT is in SILVER_CUSTOMERS but we're excluding it — "
                    "sensitive financial exposure data not needed for revenue analytics."
                ),
            ),
            Comment(
                author=AVINASH, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=10, hours_ago=8),
                content=(
                    "Confirmed GDPR status: WWI is B2B only. CUSTOMERNAME is a legal entity "
                    "name, not personal data. No GDPR erasure obligation. "
                    "SOX retention: 7 years for financial records — applies here. "
                    "CREDITLIMIT exclusion must be explicitly documented in the contract spec."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=10, hours_ago=4),
                content=(
                    "TAXRATE question: Finance has been assuming 15% flat in all our models. "
                    "But I've seen 17.5% values in historical data — is that a data quality issue?"
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=10),
                content=(
                    "Not a bug. TAXRATE reflects the actual VAT rate at order time. "
                    "UK VAT was 15% in 2009-2010, rose to 17.5%, then to 20%. "
                    "Historical data is correct. Finance must NOT standardize to 15% — "
                    "that would misrepresent historical tax liability on the books. "
                    "Documenting this in the contract so it doesn't get questioned again."
                ),
            ),
            # ── SPECIFICATION stage ───────────────────────────────────────
            Comment(
                author=AYDAN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=8),
                content=(
                    "12-column schema draft ready. Should the revenue column be LINE_REVENUE "
                    "or ORDER_REVENUE? Finance docs use 'Order Revenue' everywhere."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=7, hours_ago=20),
                content=(
                    "Strong preference for ORDER_REVENUE — that's what's in all our Tableau "
                    "calculated fields. Would avoid a find-and-replace across 14 dashboards."
                ),
            ),
            Comment(
                author=AYDAN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=7, hours_ago=16),
                content=(
                    "Technically ORDER_REVENUE is misleading — this is revenue per ORDER LINE, "
                    "not per order. The column is line-level. Renaming would also break "
                    "existing dbt models that reference LINE_REVENUE in schema.yml. "
                    "Counter-proposal: keep LINE_REVENUE as physical name, "
                    "add 'Also known as ORDER_REVENUE in Finance documentation' to description."
                ),
            ),
            Comment(
                author=ANDREW, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=7, hours_ago=12),
                content=(
                    "Decision: keep LINE_REVENUE. Aydan add the Finance alias to the description. "
                    "Priya, update your Tableau fields. This decision is now documented in "
                    "the contract — no more naming debates in future quarters."
                ),
            ),
            Comment(
                author=BEN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=6, hours_ago=6),
                content=(
                    "Governance sign-off on classification: CUSTOMERNAME = CONFIDENTIAL ✅ "
                    "(B2B entity names, policy v2.3 section 4.2). "
                    "Retention = 7 years per SOX ✅. "
                    "No GDPR special category data ✅. "
                    "CREDITLIMIT exclusion documented ✅. "
                    "Data Consumer role must NOT include CREDITLIMIT access — already excluded. "
                    "Governance green light to proceed."
                ),
            ),
            # ── REVIEW stage ──────────────────────────────────────────────
            Comment(
                author=ANTONIO, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=4),
                content=(
                    "Engineering review complete. Validated all 4 DQS rules against "
                    "30 days of production data — zero false positives. "
                    "Custom SQL for negative LINE_REVENUE: COUNT(*) = 0 on current data ✅. "
                    "NULL_COUNT on ORDERDATE: all 1,847 rows have valid order dates ✅. "
                    "ROW_COUNT floor 1,000: currently 1,847 rows — well above threshold ✅."
                ),
            ),
            Comment(
                author=AVINASH, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=3, hours_ago=8),
                content=(
                    "Data quality coverage assessment: "
                    "✅ Completeness (ORDERDATE null check, URGENT) "
                    "✅ Completeness (ORDERLINEID null check, URGENT) "
                    "✅ Validity (negative LINE_REVENUE custom SQL, URGENT) "
                    "✅ Volume (ROW_COUNT floor 1,000, NORMAL) "
                    "All four DQ rules validated — full coverage for a financial fact table. "
                    "Data Steward sign-off ✅"
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=3, hours_ago=4),
                content=(
                    "Finance sign-off ✅. Schema matches what the team expects. "
                    "24h freshness SLA is acceptable — confirmed with the board reporting team. "
                    "The TAXRATE documentation will save our support team so many tickets. "
                    "Sending to Andrew for final approval."
                ),
            ),
            Comment(
                author=ANDREW, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=2, hours_ago=8),
                content=(
                    "All review items addressed. "
                    "Engineering ✅ | Analytics ✅ | Finance ✅ | Governance ✅. "
                    "Moving to APPROVAL."
                ),
            ),
            # ── APPROVAL stage ────────────────────────────────────────────
            Comment(
                author=ANDREW, stage=DDLCStage.APPROVAL, created_at=_ts(days_ago=1, hours_ago=6),
                content=(
                    "Summarizing what we're approving: "
                    "FACT_ORDERS Gold table, WIDE_WORLD_IMPORTERS.PROCESSED_GOLD, "
                    "12 columns, 4 DQS rules, 4 SLAs, 3 access roles. "
                    "Business driver: Finance board dashboard + Q4 close accuracy. "
                    "Activation will register the asset in Atlan and deploy DQS monitoring."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.APPROVAL, created_at=_ts(days_ago=1, hours_ago=5),
                content=(
                    "Formally approving on behalf of Finance ✅. "
                    "This process captured decisions that would normally be lost in Slack: "
                    "the CREDITLIMIT exclusion rationale, the TAXRATE historical variance "
                    "explanation, the LINE_REVENUE vs ORDER_REVENUE naming decision, and "
                    "the reasoning behind the deprecated OLD_FACT_ORDERS table. "
                    "All of that context is now in Atlan forever."
                ),
            ),
            Comment(
                author=BEN, stage=DDLCStage.APPROVAL, created_at=_ts(days_ago=1, hours_ago=4),
                content=(
                    "Compliance sign-off ✅. "
                    "Note for the audit record: activation creates an Atlan asset with "
                    "CONFIDENTIAL classification applied to CUSTOMERNAME, "
                    "7-year retention policy documented, and DQS monitoring enabled. "
                    "All auditable in the Atlan lineage graph."
                ),
            ),
            Comment(
                author=AVINASH, stage=DDLCStage.APPROVAL, created_at=_ts(days_ago=1, hours_ago=3),
                content=(
                    "Data Governance sign-off ✅. "
                    "This contract is now the authoritative spec for FACT_ORDERS. "
                    "Any schema changes — adding columns, modifying types, changing SLAs — "
                    "must go through a new DDLC cycle. No ad-hoc changes."
                ),
            ),
            Comment(
                author=PRIYA, stage=DDLCStage.APPROVAL, created_at=_ts(days_ago=1, hours_ago=2),
                content=(
                    "One more thing for the record: we considered querying SILVER_ORDERLINES "
                    "directly from Tableau (skip the Gold layer entirely). Decision: Gold table "
                    "wins because (1) the three-table join is expensive at query time, "
                    "(2) LINE_REVENUE derivation logic should live once — not in every dashboard. "
                    "If we ever onboard a new BI tool, it just points at FACT_ORDERS."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.APPROVAL, created_at=_ts(hours_ago=4),
                content=(
                    "That last point is exactly the value of this process. "
                    "Without this contract, the next engineer would've spent a week "
                    "rediscovering why the Gold table exists. "
                    "DE sign-off ✅. Pipeline ready. Let's go live."
                ),
            ),
            Comment(
                author=AVINASH, stage=DDLCStage.APPROVAL, created_at=_ts(hours_ago=2),
                content=(
                    "All approvals recorded ✅. "
                    "Contract is ready for activation. "
                    "Click 'Approve Contract' to register the Atlan asset and enable DQS monitoring."
                ),
            ),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST, to_stage=DDLCStage.DISCOVERY,
                            transitioned_by=ANTONIO,
                            reason=(
                                "Three Silver sources identified. OLD_FACT_ORDERS deprecation reason "
                                "documented (mixed currencies). CREDITLIMIT excluded per governance. "
                                "TAXRATE historical variance confirmed as correct behavior."
                            ),
                            timestamp=_ts(days_ago=12)),
            StageTransition(from_stage=DDLCStage.DISCOVERY, to_stage=DDLCStage.SPECIFICATION,
                            transitioned_by=ANTONIO,
                            reason=(
                                "Sources confirmed. GDPR: B2B only, no erasure obligation. "
                                "SOX 7yr retention applies. Handing to Aydan for schema mapping."
                            ),
                            timestamp=_ts(days_ago=9)),
            StageTransition(from_stage=DDLCStage.SPECIFICATION, to_stage=DDLCStage.REVIEW,
                            transitioned_by=AYDAN,
                            reason=(
                                "12-column schema complete. LINE_REVENUE naming debate resolved "
                                "and documented. TAXRATE variance explanation added. "
                                "Ben confirmed CONFIDENTIAL classification and SOX retention. "
                                "4 DQS rules + 4 SLAs added. Sending for formal review."
                            ),
                            timestamp=_ts(days_ago=5)),
            StageTransition(from_stage=DDLCStage.REVIEW, to_stage=DDLCStage.APPROVAL,
                            transitioned_by=ANDREW,
                            reason=(
                                "All sign-offs received: Engineering (Antonio), Analytics (Aydan), "
                                "Finance (Priya), Data Steward (Avinash), Governance (Ben). "
                                "4 DQS rules validated against 30 days of production data — "
                                "zero false positives. Ready for final approval."
                            ),
                            timestamp=_ts(days_ago=2)),
        ],
        created_at=_ts(days_ago=14),
        updated_at=_ts(hours_ago=2),
    )


# ============================================================================
# Session 6 — ACTIVE: WWI Stock Item Dimension (already registered in Atlan)
# ============================================================================

def _build_wwi_dim_stockitem() -> DDLCSession:
    """Real DIM_STOCKITEM Gold table — already approved and active."""

    session_id = _id()

    _DIM_STOCKITEM_QN = _qn(_DEMO_CONN, _DEMO_DB, _GOLD, "DIM_STOCKITEM")

    src_bronze_stockitems = SourceTable(
        name="BRONZE_STOCK_ITEMS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _BRONZE_WH, "STOCK_ITEMS"),
        database_name=_DEMO_DB, schema_name=_BRONZE_WH, connector_name="snowflake",
        description="Raw stock items data — inventory items available for sale.",
    )

    src_silver_stockitems = SourceTable(
        name="SILVER_STOCKITEMS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_STOCKITEMS"),
        database_name=_DEMO_DB, schema_name=_SILVER, connector_name="snowflake",
        description="Cleaned stock items with validated pricing and attributes.",
    )

    silver_si_qn  = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_STOCKITEMS")
    silver_pkg_qn = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_PACKAGETYPES")
    silver_col_qn = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_COLORS")

    dim_stockitem = SchemaObject(
        name="DIM_STOCKITEM",
        physical_name=f"{_DEMO_DB}.{_GOLD}.DIM_STOCKITEM",
        description=(
            "Stock item dimension — product catalog with package type and color "
            "denormalized for query convenience. One row per active stock item. "
            "Inactive items excluded per Ben Hudson governance decision (2022-11)."
        ),
        source_tables=[src_bronze_stockitems, src_silver_stockitems],
        properties=[
            SchemaProperty(
                name="STOCKITEMID", logical_type=LogicalType.NUMBER,
                description="Stock item identifier (surrogate key from ERP). Unique per product.",
                required=True, primary_key=True, primary_key_position=1,
                unique=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="STOCKITEMID",
                                      source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="STOCKITEMNAME", logical_type=LogicalType.STRING,
                description="Product display name as shown to customers and in BI reports.",
                required=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="STOCKITEMNAME",
                                      source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="BRAND", logical_type=LogicalType.STRING,
                description="Brand name. Nullable — some items are unbranded or own-label.",
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="BRAND",
                                      source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="UNITPRICE", logical_type=LogicalType.NUMBER,
                description="Unit selling price in GBP. Must always be > 0.",
                required=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="UNITPRICE",
                                      source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="RECOMMENDEDRETAILPRICE", logical_type=LogicalType.NUMBER,
                description="RRP in GBP — used for margin analysis. Nullable for own-label items.",
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="RECOMMENDEDRETAILPRICE",
                                      source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="ISCHILLERSTOCK", logical_type=LogicalType.BOOLEAN,
                description="True if this item requires refrigerated storage and shipping.",
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="ISCHILLERSTOCK",
                                      source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="LEADTIMEDAYS", logical_type=LogicalType.NUMBER,
                description="Supplier lead time in calendar days. Used for reorder scheduling.",
                required=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="LEADTIMEDAYS",
                                      source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="PACKAGETYPENAME", logical_type=LogicalType.STRING,
                description="Package type name — denormalized from SILVER_PACKAGETYPES to avoid joins.",
                sources=[ColumnSource(
                    source_table="SILVER_STOCKITEMS", source_column="PACKAGETYPEID",
                    source_table_qualified_name=silver_si_qn,
                    transform_logic="JOIN SILVER_PACKAGETYPES ON PACKAGETYPEID",
                    transform_description="Denormalized package type name — eliminates join in BI.",
                )],
            ),
            SchemaProperty(
                name="COLORNAME", logical_type=LogicalType.STRING,
                description="Color name — denormalized from SILVER_COLORS. Nullable for colorless items.",
                sources=[ColumnSource(
                    source_table="SILVER_STOCKITEMS", source_column="COLORID",
                    source_table_qualified_name=silver_si_qn,
                    transform_logic="LEFT JOIN SILVER_COLORS ON COLORID",
                    transform_description="Nullable — some items have no color attribute.",
                )],
            ),
        ],
    )

    contract = ODCSContract(
        name="WWI Stock Item Dimension",
        version="1.0.0",
        status=ContractStatus.ACTIVE,
        domain="Operations",
        data_product="Orders Analytics",
        data_product_qualified_name="default/domain/sKESGr8dKawiEcrtWdCIX/super/product/HErYfkhyxkSynsxDVEC56",
        description_purpose=(
            "Provide a clean, denormalized stock item (product) dimension "
            "for joining with order fact tables and enabling product analytics."
        ),
        description_limitations=(
            "One row per active stock item — inactive items excluded per governance policy. "
            "RECOMMENDEDRETAILPRICE is nullable for own-label items."
        ),
        description_usage=(
            "Join with FACT_ORDERS on STOCKITEMID for product sales analysis. "
            "Use ISCHILLERSTOCK for warehouse routing and cold-chain logistics. "
            "Use LEADTIMEDAYS for supply chain reorder scheduling."
        ),
        tags=["stock-items", "products", "dimension", "gold-tier", "wide-world-importers"],
        schema_objects=[dim_stockitem],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL,
                description="STOCKITEMID must be unique and not null — primary key integrity.",
                dimension="completeness", severity="critical",
                column="DIM_STOCKITEM.STOCKITEMID",
                schedule="0 6 * * *", engine="atlan-dqs",
                dqs_rule_type="NULL_COUNT",
                dqs_threshold_value=0.0,
                dqs_alert_priority="URGENT",
                dqs_pushed=True,
                business_impact="Duplicate or null stock item keys cause fan-out in FACT_ORDERS joins.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="UNITPRICE must be greater than zero — no free or negative-price items.",
                dimension="validity", severity="high",
                column="DIM_STOCKITEM.UNITPRICE",
                schedule="0 6 * * *", engine="atlan-dqs",
                dqs_rule_type="CUSTOM_SQL",
                dqs_custom_sql=(
                    "SELECT COUNT(*) FROM WIDE_WORLD_IMPORTERS.PROCESSED_GOLD.DIM_STOCKITEM "
                    "WHERE UNITPRICE <= 0"
                ),
                dqs_threshold_value=0.0,
                dqs_alert_priority="NORMAL",
                dqs_pushed=True,
                business_impact="Zero or negative prices corrupt margin calculations in Finance dashboards.",
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="24", unit="hours",
                description="Refreshed daily at 05:00 UTC — before FACT_ORDERS runs at 06:00.",
                schedule="0 5 * * *", scheduler="airflow",
                driver="analytics", element="DIM_STOCKITEM",
            ),
            SLAProperty(
                property="availability", value="99.5", unit="percent",
                description="Must be available before FACT_ORDERS daily refresh at 06:00 UTC.",
                driver="operational", element="DIM_STOCKITEM",
            ),
        ],
        team=[
            TeamMember(name="Ben Hudson",    email="ben.hudson@atlan.com",    role="Data Owner"),
            TeamMember(name="Aydan McNulty", email="aydan.mcnulty@atlan.com", role="Analytics Engineer"),
        ],
        servers=[
            Server(
                type=ServerType.SNOWFLAKE, environment="prod",
                account=os.getenv("ATLAN_SNOWFLAKE_ACCOUNT", "your-account"),
                database=_DEMO_DB, schema_name=_GOLD,
                description="Demo Snowflake — Wide World Importers Gold layer.",
                connection_qualified_name=_ANDREW_CONN,
            ),
        ],
        roles=[
            ContractRole(
                role="Data Consumer", access=AccessLevel.READ,
                approvers=[RoleApprover(
                    username="ben.hudson@atlan.com",
                    email="ben.hudson@atlan.com",
                    display_name="Ben Hudson",
                )],
                description="Analytics teams consuming product dimension data.",
            ),
        ],
        custom_properties=[
            CustomProperty(key="cost_center",   value="operations-analytics"),
            CustomProperty(key="source_system",  value="wide-world-importers"),
            CustomProperty(key="grain",          value="one-row-per-active-stock-item"),
        ],
        # Real registered asset — GUID resolved dynamically at seed time
        atlan_table_qualified_name=_DIM_STOCKITEM_QN,
    )

    return DDLCSession(
        id=session_id,
        current_stage=DDLCStage.ACTIVE,
        request=ContractRequest(
            title="WWI Stock Item Dimension — DIM_STOCKITEM",
            description=(
                "Formalize the DIM_STOCKITEM gold table with quality rules, "
                "SLAs, and lineage documentation."
            ),
            business_context=(
                "Operations team needs a governed product dimension for reorder logic "
                "and inventory dashboards. Table exists in production but has no contract, "
                "no DQ monitoring, and no documented lineage — a governance gap."
            ),
            target_use_case=(
                "Product sales analysis via FACT_ORDERS join, inventory classification, "
                "supply chain reorder workflows, cold-chain routing via ISCHILLERSTOCK."
            ),
            urgency=Urgency.MEDIUM,
            requester=BEN,
            domain="Operations",
            data_product="Orders Analytics",
            created_at=_ts(days_ago=21),
        ),
        contract=contract,
        participants=[BEN, AYDAN, ANTONIO],
        comments=[
            Comment(
                author=BEN, stage=DDLCStage.REQUEST, created_at=_ts(days_ago=21),
                content=(
                    "DIM_STOCKITEM is already in production but has zero governance: "
                    "no contract, no DQ rules, no documented lineage. "
                    "Three teams are using it for different purposes without knowing "
                    "its limitations (inactive items excluded, RECOMMENDEDRETAILPRICE nullable). "
                    "Formalizing this before it causes a production incident."
                ),
            ),
            Comment(
                author=AYDAN, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=20),
                content=(
                    "Bronze → Silver → Gold lineage confirmed in Atlan. "
                    "Simple structure: SILVER_STOCKITEMS as primary source, "
                    "with PACKAGETYPENAME and COLORNAME denormalized from two lookup tables. "
                    "Current row count: ~1,200 active items. "
                    "Key discovery: inactive items are filtered out in the Silver ETL — "
                    "this is undocumented and could mislead analysis of discontinued products."
                ),
            ),
            Comment(
                author=ANTONIO, stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=20, hours_ago=4),
                content=(
                    "Confirmed the inactive item filter. It's a WHERE IS_ACTIVE = 1 "
                    "in the SILVER_STOCKITEMS transformation. This has been there since 2021 "
                    "with no documentation. Added it to the contract limitations field — "
                    "'inactive items excluded per governance policy'."
                ),
            ),
            Comment(
                author=AYDAN, stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=18),
                content=(
                    "9-column spec complete with lineage. "
                    "Added DQS rules: NULL_COUNT on STOCKITEMID (URGENT) and "
                    "CUSTOM_SQL for UNITPRICE <= 0 (NORMAL). "
                    "Both rules validated against current data — zero violations."
                ),
            ),
            Comment(
                author=BEN, stage=DDLCStage.REVIEW, created_at=_ts(days_ago=16),
                content=(
                    "Governance review complete. Simple structure, no PII, no sensitive data. "
                    "24h freshness SLA appropriate — DIM_STOCKITEM must refresh before "
                    "FACT_ORDERS runs at 06:00. Retention: 5 years (operational data, "
                    "not financial records — SOX 7yr does not apply here). "
                    "Approving."
                ),
            ),
            Comment(
                author=BEN, stage=DDLCStage.ACTIVE, created_at=_ts(days_ago=15),
                content=(
                    "Contract approved and active ✅. "
                    "Asset visible in Atlan catalog. DQS rules pushed. "
                    "All three teams consuming this table have been notified of the "
                    "inactive item exclusion and RECOMMENDEDRETAILPRICE nullable behavior — "
                    "exactly the kind of context that was missing before."
                ),
            ),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST, to_stage=DDLCStage.DISCOVERY,
                            transitioned_by=AYDAN,
                            reason="Bronze→Silver→Gold lineage confirmed. Inactive item filter discovered and documented.",
                            timestamp=_ts(days_ago=20)),
            StageTransition(from_stage=DDLCStage.DISCOVERY, to_stage=DDLCStage.SPECIFICATION,
                            transitioned_by=AYDAN,
                            reason="Sources confirmed. Inactive item exclusion documented in contract limitations.",
                            timestamp=_ts(days_ago=19)),
            StageTransition(from_stage=DDLCStage.SPECIFICATION, to_stage=DDLCStage.REVIEW,
                            transitioned_by=AYDAN,
                            reason="9-column spec complete. DQS rules validated. Sending for governance review.",
                            timestamp=_ts(days_ago=17)),
            StageTransition(from_stage=DDLCStage.REVIEW, to_stage=DDLCStage.APPROVAL,
                            transitioned_by=BEN,
                            reason="Governance approved. 5yr retention confirmed (not SOX).",
                            timestamp=_ts(days_ago=16)),
            StageTransition(from_stage=DDLCStage.APPROVAL, to_stage=DDLCStage.ACTIVE,
                            transitioned_by=BEN,
                            reason="Asset registered in Atlan. DQS rules pushed. Contract active.",
                            timestamp=_ts(days_ago=15)),
        ],
        created_at=_ts(days_ago=21),
        updated_at=_ts(days_ago=15),
    )


# ============================================================================
# Main seed function
# ============================================================================

async def _try_resolve_atlan_url(session: DDLCSession) -> None:
    """Look up the real Atlan GUID for an ACTIVE session's table and set the URL.

    Runs only when Atlan credentials are configured.  Silent on any failure so
    demo data always seeds successfully even if the catalog is unreachable.
    """
    from app.ddlc import atlan_assets

    contract = session.contract
    qn = contract.atlan_table_qualified_name
    if not qn or not atlan_assets.is_configured():
        return
    try:
        from pyatlan.model.assets import Table as _Table

        client = atlan_assets._get_client()
        existing = await asyncio.to_thread(
            client.asset.get_by_qualified_name,
            qualified_name=qn,
            asset_type=_Table,
        )
        if existing and existing.guid:
            base = os.getenv("ATLAN_BASE_URL", "").rstrip("/")
            contract.atlan_table_guid = str(existing.guid)
            contract.atlan_table_url = f"{base}/assets/{existing.guid}/overview"
    except Exception:
        pass  # silently skip — "View in Atlan" button won't render but demo still works


async def seed_demo_data() -> list[str]:
    """
    Create all demo sessions and persist them to the store.
    Returns the list of created session IDs.

    Session order (newest first in the dashboard):
      5. APPROVAL  — FACT_ORDERS           ← PRIMARY DEMO (advance to ACTIVE live)
      4. REVIEW    — FACT_ORDERS (same story, one stage back)
      3. SPECIFICATION — FACT_ORDERS (schema built, no quality checks yet)
      2. DISCOVERY — FACT_ORDERS (sources found)
      1. REQUEST   — FACT_ORDERS (bare entry point)
      6. ACTIVE    — DIM_STOCKITEM (already registered)
    """
    builders = [
        _build_fact_orders_approval,       # APPROVAL     ← PRIMARY DEMO (advance live)
        _build_fact_orders_review,         # REVIEW       ← full spec + DQS checks
        _build_fact_orders_specification,  # SPECIFICATION← schema mapped
        _build_fact_orders_discovery,      # DISCOVERY    ← sources found
        _build_fact_orders_request,        # REQUEST      ← bare entry point
        _build_wwi_dim_stockitem,          # ACTIVE       ← already registered
    ]

    created_ids = []
    for builder in builders:
        session = builder()
        # For ACTIVE sessions, try to resolve the real Atlan asset GUID/URL
        if session.current_stage == DDLCStage.ACTIVE:
            await _try_resolve_atlan_url(session)
        await store.save_session(session)
        created_ids.append(session.id)
        print(f"  [seed] Created: {session.request.title} ({session.current_stage.value})")

    return created_ids

"""
Demo seed data for the DDLC platform.

Creates pre-populated sessions at various lifecycle stages so the platform
is ready for demos immediately after server start — no manual setup needed.

Session overview
----------------
1. APPROVAL  — WWI Sales Order Analytics (FACT_ORDERS)    ← PRIMARY DEMO
               Real Wide World Importers tables from the hackathon Atlan tenant.
               Advance to ACTIVE live during the demo to register a real asset.
               Rename the table each run so a net-new asset is created each time.

2. REVIEW    — WWI Customer Dimension (DIM_CUSTOMER)
               Rich spec built from the real DIM_CUSTOMER gold table.

3. ACTIVE    — WWI Stock Item Dimension (DIM_STOCKITEM)
               Already approved; shows "View in Atlan →" with a real asset link.

4. DISCOVERY — Marketing Campaign Attribution
               Early-stage; shows source scouting in progress.

5. REQUEST   — Supplier Lead Time Snapshot
               Brand new request; shows the entry point of the lifecycle.

Usage:
    Called automatically on server startup.
    Can also be triggered via:  POST /api/demo/seed
"""

from __future__ import annotations

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
_BASE_URL = os.getenv("ATLAN_BASE_URL", "https://rko-hackaton.atlan.com").rstrip("/")

# Demo connection — "Demo" Snowflake in the hackathon tenant
_DEMO_CONN = "default/snowflake/1770312446"
_DEMO_DB   = "WIDE_WORLD_IMPORTERS"
_GOLD      = "PROCESSED_GOLD"
_SILVER    = "PROCESSED_SILVER"
_BRONZE_SALES = "BRONZE_SALES"
_BRONZE_WH    = "BRONZE_WAREHOUSE"

# Andrew_Lentz connection — where new assets will be registered
_ANDREW_CONN = "default/snowflake/1770327201"


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

ANDREW   = Participant(name="Andrew Lentz",    email="andrew.lentz@atlan.com")
ANTONIO  = Participant(name="Antonio Hernandez", email="antonio.hernandez@atlan.com")
ASHISH   = Participant(name="Ashish Desai",    email="ashish.desai@atlan.com")
AVINASH  = Participant(name="Avinash Shankar", email="avinash.shankar@atlan.com")
AYDAN    = Participant(name="Aydan McNulty",   email="aydan.mcnulty@atlan.com")
BEN      = Participant(name="Ben Hudson",      email="ben.hudson@atlan.com")


# ============================================================================
# Session 1 — APPROVAL: WWI Sales Order Analytics  ← PRIMARY DEMO SESSION
# ============================================================================
#
# Story: The Finance team wants a gold-tier FACT_ORDERS table built from the
# real Silver order + customer + stock tables in Wide World Importers.
# At demo time, rename the table to something like FACT_ORDERS_V2 so each
# live run creates a brand-new asset in Atlan.
#
# Real source tables (all exist in the Demo Snowflake connection):
#   PROCESSED_SILVER.SILVER_ORDERS
#   PROCESSED_SILVER.SILVER_CUSTOMERS
#   PROCESSED_SILVER.SILVER_STOCKITEMS
# Target: Andrew_Lentz connection → WIDE_WORLD_IMPORTERS / PROCESSED_GOLD / FACT_ORDERS
# ============================================================================

def _build_wwi_fact_orders() -> DDLCSession:
    """PRIMARY DEMO — real WWI data, ready to advance to ACTIVE live."""

    session_id = _id()
    contract_id = _id()

    # ── Source tables (real tables crawled in the Demo Snowflake connection) ─
    # source_table_qualified_name is set so that cross-connection lineage
    # Process assets are created in Atlan on APPROVAL → ACTIVE.
    src_orders = SourceTable(
        name="SILVER_ORDERS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERS"),
        database_name=_DEMO_DB,
        schema_name=_SILVER,
        connector_name="snowflake",
        description="Cleaned order headers with validated dates and references.",
        columns=[
            {"name": "ORDERID",       "logical_type": "integer", "is_primary": True,  "is_nullable": False},
            {"name": "CUSTOMERID",    "logical_type": "integer", "is_primary": False, "is_nullable": False},
            {"name": "SALESPERSONID", "logical_type": "integer", "is_primary": False, "is_nullable": False},
            {"name": "ORDERDATE",     "logical_type": "date",    "is_primary": False, "is_nullable": False},
        ],
    )

    src_orderlines = SourceTable(
        name="SILVER_ORDERLINES",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERLINES"),
        database_name=_DEMO_DB,
        schema_name=_SILVER,
        connector_name="snowflake",
        description="Cleaned order line items with validated quantities and prices.",
        columns=[
            {"name": "ORDERLINEID",  "logical_type": "integer", "is_primary": True,  "is_nullable": False},
            {"name": "ORDERID",      "logical_type": "integer", "is_primary": False, "is_nullable": False},
            {"name": "STOCKITEMID",  "logical_type": "integer", "is_primary": False, "is_nullable": False},
            {"name": "QUANTITY",     "logical_type": "integer", "is_primary": False, "is_nullable": False},
            {"name": "UNITPRICE",    "logical_type": "number",  "is_primary": False, "is_nullable": False},
            {"name": "TAXRATE",      "logical_type": "number",  "is_primary": False, "is_nullable": False},
        ],
    )

    src_customers = SourceTable(
        name="SILVER_CUSTOMERS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERS"),
        database_name=_DEMO_DB,
        schema_name=_SILVER,
        connector_name="snowflake",
        description="Cleaned and validated customer data with standardized fields.",
        columns=[
            {"name": "CUSTOMERID",   "logical_type": "integer", "is_primary": True,  "is_nullable": False},
            {"name": "CUSTOMERNAME", "logical_type": "string",  "is_primary": False, "is_nullable": False},
            {"name": "CREDITLIMIT",  "logical_type": "number",  "is_primary": False, "is_nullable": True},
        ],
    )

    # ── Target schema object ─────────────────────────────────────────────────
    src_orders_qn   = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERS")
    src_lines_qn    = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_ORDERLINES")
    src_cust_qn     = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERS")

    fact_orders = SchemaObject(
        name="FACT_ORDERS",
        physical_name=f"{_DEMO_DB}.{_GOLD}.FACT_ORDERS",
        description=(
            "Gold-tier order fact table. One row per order line, enriched with "
            "customer name and calculated revenue metrics. Built from three Silver "
            "layer tables — orders, order lines, and customers."
        ),
        source_tables=[src_orders, src_orderlines, src_customers],
        properties=[
            SchemaProperty(
                name="ORDERLINEID",
                logical_type=LogicalType.INTEGER,
                description="Surrogate primary key — unique per order line.",
                required=True,
                primary_key=True,
                primary_key_position=1,
                unique=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES",
                    source_column="ORDERLINEID",
                    source_table_qualified_name=src_lines_qn,
                    transform_logic="SILVER_ORDERLINES.ORDERLINEID",
                    transform_description="Direct pass-through from Silver order lines.",
                )],
            ),
            SchemaProperty(
                name="ORDERID",
                logical_type=LogicalType.INTEGER,
                description="Order header identifier — FK to DIM_* and parent order.",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERS",
                    source_column="ORDERID",
                    source_table_qualified_name=src_orders_qn,
                )],
            ),
            SchemaProperty(
                name="CUSTOMERID",
                logical_type=LogicalType.INTEGER,
                description="FK to DIM_CUSTOMER dimension table.",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERS",
                    source_column="CUSTOMERID",
                    source_table_qualified_name=src_orders_qn,
                )],
            ),
            SchemaProperty(
                name="CUSTOMERNAME",
                logical_type=LogicalType.STRING,
                description="Denormalized customer name for easier querying without joins.",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS",
                    source_column="CUSTOMERNAME",
                    source_table_qualified_name=src_cust_qn,
                    transform_logic="JOIN SILVER_CUSTOMERS ON CUSTOMERID",
                    transform_description="Denormalized via inner join on CUSTOMERID.",
                )],
            ),
            SchemaProperty(
                name="SALESPERSONID",
                logical_type=LogicalType.INTEGER,
                description="FK to DIM_EMPLOYEE (salesperson) dimension.",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERS",
                    source_column="SALESPERSONID",
                    source_table_qualified_name=src_orders_qn,
                )],
            ),
            SchemaProperty(
                name="STOCKITEMID",
                logical_type=LogicalType.INTEGER,
                description="FK to DIM_STOCKITEM dimension.",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES",
                    source_column="STOCKITEMID",
                    source_table_qualified_name=src_lines_qn,
                )],
            ),
            SchemaProperty(
                name="ORDERDATE",
                logical_type=LogicalType.DATE,
                description="Date the order was placed (UTC).",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERS",
                    source_column="ORDERDATE",
                    source_table_qualified_name=src_orders_qn,
                )],
            ),
            SchemaProperty(
                name="QUANTITY",
                logical_type=LogicalType.INTEGER,
                description="Units ordered for this line item.",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES",
                    source_column="QUANTITY",
                    source_table_qualified_name=src_lines_qn,
                )],
            ),
            SchemaProperty(
                name="UNITPRICE",
                logical_type=LogicalType.NUMBER,
                description="Selling price per unit at time of order (GBP).",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES",
                    source_column="UNITPRICE",
                    source_table_qualified_name=src_lines_qn,
                )],
            ),
            SchemaProperty(
                name="TAXRATE",
                logical_type=LogicalType.NUMBER,
                description="Applicable tax rate percentage for this line.",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES",
                    source_column="TAXRATE",
                    source_table_qualified_name=src_lines_qn,
                )],
            ),
            SchemaProperty(
                name="LINE_REVENUE",
                logical_type=LogicalType.NUMBER,
                description="Calculated gross revenue: QUANTITY × UNITPRICE.",
                required=True,
                critical_data_element=True,
                sources=[
                    ColumnSource(
                        source_table="SILVER_ORDERLINES",
                        source_column="QUANTITY",
                        source_table_qualified_name=src_lines_qn,
                        transform_logic="QUANTITY * UNITPRICE",
                        transform_description="Derived revenue metric — not stored in source.",
                    ),
                    ColumnSource(
                        source_table="SILVER_ORDERLINES",
                        source_column="UNITPRICE",
                        source_table_qualified_name=src_lines_qn,
                    ),
                ],
            ),
            SchemaProperty(
                name="LINE_TAX",
                logical_type=LogicalType.NUMBER,
                description="Calculated tax amount: LINE_REVENUE × (TAXRATE / 100).",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_ORDERLINES",
                    source_column="TAXRATE",
                    source_table_qualified_name=src_lines_qn,
                    transform_logic="(QUANTITY * UNITPRICE) * (TAXRATE / 100)",
                    transform_description="Derived tax amount — not stored in source.",
                )],
            ),
        ],
    )

    # ── Contract ─────────────────────────────────────────────────────────────
    contract = ODCSContract(
        id=contract_id,
        name="WWI Sales Order Analytics",
        version="1.0.0",
        status=ContractStatus.DRAFT,
        domain="Finance",
        data_product="Sales Order Insights",
        data_product_qualified_name="default/domain/J3sne7aVPzMgU6KYHsoRT/super/product/ULc0yDRiVjv19LvuYyaEk",
        description_purpose=(
            "Provide a denormalized, analytics-ready order fact table combining "
            "order headers, line items, and customer names from the Wide World "
            "Importers Silver layer. Powers revenue dashboards and sales KPI reporting."
        ),
        description_limitations=(
            "Covers transactional orders only — no subscription or recurring revenue. "
            "Prices are in GBP (Wide World Importers source currency). "
            "CUSTOMERNAME is denormalized at order time; historical name changes are not tracked."
        ),
        description_usage=(
            "Use for order-grain revenue analysis, salesperson performance, "
            "product-level sales reporting, and customer order history. "
            "Join with DIM_CUSTOMER, DIM_EMPLOYEE, and DIM_STOCKITEM for full context."
        ),
        tags=["orders", "revenue", "finance", "gold-tier", "wide-world-importers"],
        schema_objects=[fact_orders],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL,
                description="ORDERLINEID must be unique across all rows.",
                dimension="uniqueness",
                severity="critical",
                must_be="unique",
                method="field_health",
                column="FACT_ORDERS.ORDERLINEID",
                schedule="0 6 * * *",
                scheduler="airflow",
                engine="monte-carlo",
                business_impact=(
                    "Duplicate order lines inflate revenue figures and corrupt "
                    "per-product and per-salesperson performance metrics."
                ),
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="LINE_REVENUE must be >= 0 (no negative revenue lines).",
                dimension="validity",
                severity="critical",
                must_be_greater_than=0.0,
                method="field_health",
                column="FACT_ORDERS.LINE_REVENUE",
                schedule="0 6 * * *",
                engine="monte-carlo",
                business_impact=(
                    "Negative revenue lines distort sales dashboards, "
                    "quarterly reporting, and finance reconciliation."
                ),
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="Every CUSTOMERID in FACT_ORDERS must exist in SILVER_CUSTOMERS.",
                dimension="referential integrity",
                severity="high",
                method="sql_rule",
                column="FACT_ORDERS.CUSTOMERID",
                query=(
                    "SELECT COUNT(*) FROM FACT_ORDERS f "
                    "LEFT JOIN SILVER_CUSTOMERS c ON f.CUSTOMERID = c.CUSTOMERID "
                    "WHERE c.CUSTOMERID IS NULL"
                ),
                schedule="0 7 * * *",
                engine="monte-carlo",
                business_impact=(
                    "Orphaned customer IDs cause NULL joins in BI tools "
                    "and result in unattributed revenue in customer reports."
                ),
            ),
            QualityCheck(
                type=QualityCheckType.TEXT,
                description="Daily row count should not drop more than 5% vs prior day.",
                dimension="volume",
                severity="medium",
                method="volume",
                schedule="0 6 * * *",
                engine="monte-carlo",
                business_impact=(
                    "Sudden volume drops signal pipeline failures or "
                    "missing order batches, causing incomplete daily revenue figures."
                ),
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="24", unit="hours",
                description="Refreshed daily at 06:00 UTC from the Silver layer.",
                schedule="0 6 * * *", scheduler="airflow",
                driver="analytics", element="FACT_ORDERS",
            ),
            SLAProperty(
                property="availability", value="99.5", unit="percent",
                description="Target uptime for downstream Finance BI dashboards.",
                driver="operational", element="FACT_ORDERS",
            ),
            SLAProperty(
                property="latency", value="45", unit="minutes",
                description="Maximum pipeline runtime from Silver extract to Gold load.",
                schedule="0 6 * * *", scheduler="airflow",
                driver="analytics", element="FACT_ORDERS",
            ),
            SLAProperty(
                property="retention", value="7", unit="years",
                description="Retain all order history for 7 years per finance policy.",
                driver="regulatory",
            ),
        ],
        team=[
            TeamMember(name="Andrew Lentz",     email="andrew.lentz@atlan.com",    role="Data Owner"),
            TeamMember(name="Avinash Shankar",  email="avinash.shankar@atlan.com", role="Data Steward"),
            TeamMember(name="Antonio Hernandez",email="antonio.hernandez@atlan.com",role="Data Engineer"),
            TeamMember(name="Aydan McNulty",    email="aydan.mcnulty@atlan.com",   role="Analytics Engineer"),
        ],
        servers=[
            Server(
                type=ServerType.SNOWFLAKE,
                environment="prod",
                account="rko-hackaton",
                database=_DEMO_DB,
                schema_name=_GOLD,
                description="Demo Snowflake — Wide World Importers Gold layer.",
                connection_qualified_name=_ANDREW_CONN,
            ),
        ],
        roles=[
            ContractRole(
                role="Data Consumer",
                access=AccessLevel.READ,
                approvers=[
                    RoleApprover(
                        username="andrew.lentz@atlan.com",
                        email="andrew.lentz@atlan.com",
                        display_name="Andrew Lentz",
                    ),
                ],
                description="Finance and sales analytics teams consuming revenue reports.",
            ),
            ContractRole(
                role="Data Producer",
                access=AccessLevel.WRITE,
                approvers=[
                    RoleApprover(
                        username="andrew.lentz@atlan.com",
                        email="andrew.lentz@atlan.com",
                        display_name="Andrew Lentz",
                    ),
                    RoleApprover(
                        username="antonio.hernandez@atlan.com",
                        email="antonio.hernandez@atlan.com",
                        display_name="Antonio Hernandez",
                    ),
                ],
                description="Data engineering team responsible for the Silver → Gold pipeline.",
            ),
            ContractRole(
                role="Data Owner",
                access=AccessLevel.ADMIN,
                approvers=[
                    RoleApprover(
                        username="andrew.lentz@atlan.com",
                        email="andrew.lentz@atlan.com",
                        display_name="Andrew Lentz",
                    ),
                ],
                description="Andrew Lentz owns this data product and approves schema changes.",
            ),
        ],
        custom_properties=[
            CustomProperty(key="cost_center",   value="finance-analytics"),
            CustomProperty(key="source_system",  value="wide-world-importers"),
            CustomProperty(key="currency",        value="GBP"),
            CustomProperty(key="grain",           value="order-line"),
        ],
    )

    session = DDLCSession(
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
            requester=ANDREW,
            domain="Finance",
            data_product="Sales Order Insights",
            data_product_qualified_name="default/domain/J3sne7aVPzMgU6KYHsoRT/super/product/ULc0yDRiVjv19LvuYyaEk",
            desired_fields=[
                "ORDERLINEID", "ORDERID", "CUSTOMERID", "CUSTOMERNAME",
                "SALESPERSONID", "STOCKITEMID", "ORDERDATE",
                "QUANTITY", "UNITPRICE", "TAXRATE", "LINE_REVENUE", "LINE_TAX",
            ],
            created_at=_ts(days_ago=7),
        ),
        contract=contract,
        participants=[ANDREW, ANTONIO, AVINASH, AYDAN],
        comments=[
            Comment(
                author=ANDREW,
                content=(
                    "Finance team is running inconsistent revenue numbers across three "
                    "different dashboards. We need a formal Gold fact table with a signed-off "
                    "contract so everyone pulls from the same source of truth."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=7),
            ),
            Comment(
                author=ANTONIO,
                content=(
                    "I've scoped the sources. Everything is in the Wide World Importers "
                    "Silver layer: SILVER_ORDERS, SILVER_ORDERLINES, and SILVER_CUSTOMERS. "
                    "All three are already crawled and catalogued in Atlan. "
                    "We'll do a simple join — no complex transformations needed."
                ),
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=6),
            ),
            Comment(
                author=AVINASH,
                content=(
                    "Agreed on the Silver sources. I'd suggest denormalizing CUSTOMERNAME "
                    "to avoid forcing every BI consumer to join against DIM_CUSTOMER. "
                    "Also adding LINE_REVENUE and LINE_TAX as derived columns — Finance "
                    "uses these in every report and it's better to compute them once."
                ),
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=6, hours_ago=3),
            ),
            Comment(
                author=AYDAN,
                content=(
                    "Schema looks solid. I've mapped all 12 columns with source lineage "
                    "and added four quality checks: uniqueness on ORDERLINEID, "
                    "non-negative LINE_REVENUE, referential integrity on CUSTOMERID, "
                    "and a volume anomaly check. SLAs set to 24h freshness / 99.5% availability."
                ),
                stage=DDLCStage.SPECIFICATION,
                created_at=_ts(days_ago=4),
            ),
            Comment(
                author=ANTONIO,
                content=(
                    "Reviewed the spec. The column mapping is accurate against Silver. "
                    "The referential integrity check query is correct. "
                    "I'm happy with the quality rules — approve from the engineering side."
                ),
                stage=DDLCStage.REVIEW,
                created_at=_ts(days_ago=2),
            ),
            Comment(
                author=ANDREW,
                content=(
                    "Finance sign-off confirmed. The derived LINE_REVENUE and LINE_TAX "
                    "columns match what the team expects. SLA of 24h freshness is acceptable "
                    "for daily close. Sending to approval."
                ),
                stage=DDLCStage.REVIEW,
                created_at=_ts(days_ago=1, hours_ago=4),
            ),
            Comment(
                author=AVINASH,
                content=(
                    "All checks passed. Contract is clean and lineage to Silver sources "
                    "is fully documented. Ready to approve and register the asset in Atlan."
                ),
                stage=DDLCStage.APPROVAL,
                created_at=_ts(hours_ago=2),
            ),
        ],
        history=[
            StageTransition(
                from_stage=DDLCStage.REQUEST,
                to_stage=DDLCStage.DISCOVERY,
                transitioned_by=ANTONIO,
                reason="Sources identified in the Silver layer. Starting discovery.",
                timestamp=_ts(days_ago=6, hours_ago=8),
            ),
            StageTransition(
                from_stage=DDLCStage.DISCOVERY,
                to_stage=DDLCStage.SPECIFICATION,
                transitioned_by=ANTONIO,
                reason="Silver sources confirmed. Three tables, clean data. Starting spec.",
                timestamp=_ts(days_ago=5),
            ),
            StageTransition(
                from_stage=DDLCStage.SPECIFICATION,
                to_stage=DDLCStage.REVIEW,
                transitioned_by=AYDAN,
                reason="12-column schema mapped with quality rules and SLAs. Sending for review.",
                timestamp=_ts(days_ago=3),
            ),
            StageTransition(
                from_stage=DDLCStage.REVIEW,
                to_stage=DDLCStage.APPROVAL,
                transitioned_by=ANDREW,
                reason="Engineering and Finance both signed off. Moving to final approval.",
                timestamp=_ts(hours_ago=3),
            ),
        ],
        created_at=_ts(days_ago=7),
        updated_at=_ts(hours_ago=2),
    )

    return session


# ============================================================================
# Session 2 — REVIEW: WWI Customer Dimension (DIM_CUSTOMER)
# ============================================================================

def _build_wwi_dim_customer() -> DDLCSession:
    """Real DIM_CUSTOMER Gold table — rich spec ready for stakeholder review."""

    session_id = _id()

    src_bronze_customers = SourceTable(
        name="BRONZE_CUSTOMERS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _BRONZE_SALES, "CUSTOMERS"),
        database_name=_DEMO_DB,
        schema_name=_BRONZE_SALES,
        connector_name="snowflake",
        description="Raw customers data from source — main customer entity with contact and credit info.",
    )

    src_silver_customers = SourceTable(
        name="SILVER_CUSTOMERS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERS"),
        database_name=_DEMO_DB,
        schema_name=_SILVER,
        connector_name="snowflake",
        description="Cleaned and validated customer data with standardized fields.",
    )

    src_silver_categories = SourceTable(
        name="SILVER_CUSTOMERCATEGORIES",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERCATEGORIES"),
        database_name=_DEMO_DB,
        schema_name=_SILVER,
        connector_name="snowflake",
        description="Cleaned customer categories with standardized naming.",
    )

    src_silver_groups = SourceTable(
        name="SILVER_BUYINGGROUPS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_BUYINGGROUPS"),
        database_name=_DEMO_DB,
        schema_name=_SILVER,
        connector_name="snowflake",
        description="Standardized buying groups data.",
    )

    silver_cust_qn      = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERS")
    silver_cat_qn       = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_CUSTOMERCATEGORIES")
    silver_groups_qn    = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_BUYINGGROUPS")

    dim_customer = SchemaObject(
        name="DIM_CUSTOMER",
        physical_name=f"{_DEMO_DB}.{_GOLD}.DIM_CUSTOMER",
        description=(
            "Customer dimension — enriched customer data with category and buying "
            "group names denormalized for easy access. One row per active customer."
        ),
        source_tables=[src_bronze_customers, src_silver_customers, src_silver_categories, src_silver_groups],
        properties=[
            SchemaProperty(
                name="CUSTOMERID", logical_type=LogicalType.NUMBER,
                description="Customer surrogate key.", required=True,
                primary_key=True, primary_key_position=1, unique=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="CUSTOMERID",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="CUSTOMERNAME", logical_type=LogicalType.STRING,
                description="Customer display name.", required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="CUSTOMERNAME",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="CREDITLIMIT", logical_type=LogicalType.NUMBER,
                description="Credit limit in GBP — used for order approval workflows.",
                required=False,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="CREDITLIMIT",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="STANDARDDISCOUNTPERCENTAGE", logical_type=LogicalType.NUMBER,
                description="Standard discount applied to orders for this customer.",
                required=False,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="STANDARDDISCOUNTPERCENTAGE",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="ISONCREDITHOLD", logical_type=LogicalType.BOOLEAN,
                description="True if the customer's account is on credit hold.",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="ISONCREDITHOLD",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="ACCOUNTOPENEDDATE", logical_type=LogicalType.DATE,
                description="Date the customer account was first opened.",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="ACCOUNTOPENEDDATE",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="PAYMENTDAYS", logical_type=LogicalType.NUMBER,
                description="Standard payment terms in days (e.g. 30 = Net 30).",
                required=True,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="PAYMENTDAYS",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="POSTALADDRESSLINE1", logical_type=LogicalType.STRING,
                description="Primary postal address line.",
                required=False,
                classification="pii",
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERS", source_column="POSTALADDRESSLINE1",
                    source_table_qualified_name=silver_cust_qn,
                )],
            ),
            SchemaProperty(
                name="CUSTOMERCATEGORYNAME", logical_type=LogicalType.STRING,
                description="Customer category name — denormalized from SILVER_CUSTOMERCATEGORIES.",
                required=False,
                sources=[ColumnSource(
                    source_table="SILVER_CUSTOMERCATEGORIES", source_column="CUSTOMERCATEGORYNAME",
                    source_table_qualified_name=silver_cat_qn,
                    transform_logic="JOIN SILVER_CUSTOMERCATEGORIES ON CUSTOMERCATEGORYID",
                    transform_description="Denormalized for easier filtering in BI tools.",
                )],
            ),
            SchemaProperty(
                name="BUYINGGROUPNAME", logical_type=LogicalType.STRING,
                description="Buying group — denormalized from SILVER_BUYINGGROUPS.",
                required=False,
                sources=[ColumnSource(
                    source_table="SILVER_BUYINGGROUPS", source_column="BUYINGGROUPNAME",
                    source_table_qualified_name=silver_groups_qn,
                    transform_logic="LEFT JOIN SILVER_BUYINGGROUPS ON BUYINGGROUPID",
                    transform_description="Left join — nullable if customer has no buying group.",
                )],
            ),
        ],
    )

    contract = ODCSContract(
        name="WWI Customer Dimension",
        version="0.2.0",
        status=ContractStatus.DRAFT,
        domain="Finance",
        data_product="Sales Order Insights",
        data_product_qualified_name="default/domain/J3sne7aVPzMgU6KYHsoRT/super/product/ULc0yDRiVjv19LvuYyaEk",
        description_purpose=(
            "Provide a clean, enriched customer dimension for joining with order "
            "and sales fact tables. Denormalizes category and buying group names "
            "to reduce join complexity in downstream BI queries."
        ),
        description_limitations=(
            "One row per active customer only — inactive/churned customers are excluded. "
            "POSTALADDRESSLINE1 is classified PII and subject to masking in non-prod environments."
        ),
        description_usage=(
            "Join with FACT_ORDERS on CUSTOMERID for revenue analysis. "
            "Use ISONCREDITHOLD for order approval workflows. "
            "CUSTOMERCATEGORYNAME and BUYINGGROUPNAME are safe to expose in self-service BI."
        ),
        tags=["customers", "dimension", "gold-tier", "pii", "wide-world-importers"],
        schema_objects=[dim_customer],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL,
                description="CUSTOMERID must be unique and not null.",
                dimension="uniqueness",
                severity="critical",
                must_be="unique",
                method="field_health",
                column="DIM_CUSTOMER.CUSTOMERID",
                schedule="0 6 * * *",
                engine="monte-carlo",
                business_impact="Duplicate customer keys cause fan-out in joins with fact tables.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="CREDITLIMIT must be >= 0 where not null.",
                dimension="validity",
                severity="high",
                must_be_greater_than=0.0,
                method="field_health",
                column="DIM_CUSTOMER.CREDITLIMIT",
                schedule="0 6 * * *",
                engine="monte-carlo",
                business_impact="Negative credit limits break order approval logic.",
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="24", unit="hours",
                description="Refreshed daily at 05:00 UTC, upstream of FACT_ORDERS.",
                schedule="0 5 * * *", scheduler="airflow",
                driver="analytics", element="DIM_CUSTOMER",
            ),
            SLAProperty(
                property="availability", value="99.5", unit="percent",
                description="Must be available before daily FACT_ORDERS run.",
                driver="operational", element="DIM_CUSTOMER",
            ),
        ],
        team=[
            TeamMember(name="Avinash Shankar",  email="avinash.shankar@atlan.com", role="Data Owner"),
            TeamMember(name="Antonio Hernandez", email="antonio.hernandez@atlan.com", role="Data Engineer"),
        ],
        servers=[
            Server(
                type=ServerType.SNOWFLAKE,
                environment="prod",
                account="rko-hackaton",
                database=_DEMO_DB,
                schema_name=_GOLD,
                description="Demo Snowflake — Wide World Importers Gold layer.",
                connection_qualified_name=_ANDREW_CONN,
            ),
        ],
        roles=[
            ContractRole(
                role="Data Consumer",
                access=AccessLevel.READ,
                approvers=[RoleApprover(
                    username="avinash.shankar@atlan.com",
                    email="avinash.shankar@atlan.com",
                    display_name="Avinash Shankar",
                )],
                description="Analytics consumers joining DIM_CUSTOMER with fact tables.",
            ),
            ContractRole(
                role="Data Producer",
                access=AccessLevel.WRITE,
                approvers=[RoleApprover(
                    username="antonio.hernandez@atlan.com",
                    email="antonio.hernandez@atlan.com",
                    display_name="Antonio Hernandez",
                )],
                description="Engineering team maintaining the Silver → Gold pipeline.",
            ),
        ],
        custom_properties=[
            CustomProperty(key="cost_center",  value="finance-analytics"),
            CustomProperty(key="pii_relevant", value="true"),
            CustomProperty(key="grain",        value="one-row-per-customer"),
        ],
    )

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.REVIEW,
        request=ContractRequest(
            title="WWI Customer Dimension — DIM_CUSTOMER",
            description=(
                "Formalize the DIM_CUSTOMER Gold table with a data contract. "
                "The table already exists but has no SLAs, quality checks, or "
                "documented lineage — this contract closes that gap."
            ),
            business_context=(
                "BI consumers are joining directly against Silver tables for customer attributes "
                "because DIM_CUSTOMER had no formal contract. This creates fragile queries and "
                "inconsistent definitions across teams."
            ),
            target_use_case="Customer segmentation, revenue attribution, order approval workflows.",
            urgency=Urgency.MEDIUM,
            requester=AVINASH,
            domain="Finance",
            data_product="Sales Order Insights",
            created_at=_ts(days_ago=10),
        ),
        contract=contract,
        participants=[AVINASH, ANTONIO, AYDAN],
        comments=[
            Comment(author=AVINASH,  content="DIM_CUSTOMER has been running uncontracted for 6 months. Time to fix that.", stage=DDLCStage.REQUEST, created_at=_ts(days_ago=10)),
            Comment(author=ANTONIO,  content="Sources confirmed: SILVER_CUSTOMERS (primary) + SILVER_CUSTOMERCATEGORIES + SILVER_BUYINGGROUPS for the denormalized columns.", stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=9)),
            Comment(author=AVINASH,  content="10-column spec mapped with PII classification on POSTALADDRESSLINE1. Quality checks and SLAs aligned with FACT_ORDERS cadence.", stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=7)),
            Comment(author=AYDAN,    content="Looks good. One question: should PAYMENTDAYS have a maximum value check? Some legacy records show 999.", stage=DDLCStage.REVIEW, created_at=_ts(days_ago=3)),
            Comment(author=AVINASH,  content="Good catch Aydan — will add a validity check for PAYMENTDAYS <= 365 in the next iteration. Fine to approve as-is for now.", stage=DDLCStage.REVIEW, created_at=_ts(days_ago=2)),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST,       to_stage=DDLCStage.DISCOVERY,     transitioned_by=ANTONIO, timestamp=_ts(days_ago=9)),
            StageTransition(from_stage=DDLCStage.DISCOVERY,     to_stage=DDLCStage.SPECIFICATION, transitioned_by=ANTONIO, timestamp=_ts(days_ago=8)),
            StageTransition(from_stage=DDLCStage.SPECIFICATION, to_stage=DDLCStage.REVIEW,        transitioned_by=AVINASH, reason="Spec complete. Sending for stakeholder review.", timestamp=_ts(days_ago=4)),
        ],
        created_at=_ts(days_ago=10),
        updated_at=_ts(days_ago=2),
    )

    return session


# ============================================================================
# Session 3 — ACTIVE: WWI Stock Item Dimension (already registered in Atlan)
# ============================================================================

def _build_wwi_dim_stockitem() -> DDLCSession:
    """Real DIM_STOCKITEM Gold table — already approved and active."""

    session_id = _id()

    # Real GUID from the Atlan tenant — DIM_STOCKITEM is crawled and exists
    _DIM_STOCKITEM_QN = _qn(_DEMO_CONN, _DEMO_DB, _GOLD, "DIM_STOCKITEM")

    src_bronze_stockitems = SourceTable(
        name="BRONZE_STOCK_ITEMS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _BRONZE_WH, "STOCK_ITEMS"),
        database_name=_DEMO_DB,
        schema_name=_BRONZE_WH,
        connector_name="snowflake",
        description="Raw stock items data — inventory items available for sale.",
    )

    src_silver_stockitems = SourceTable(
        name="SILVER_STOCKITEMS",
        qualified_name=_qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_STOCKITEMS"),
        database_name=_DEMO_DB,
        schema_name=_SILVER,
        connector_name="snowflake",
        description="Cleaned stock items with validated pricing and attributes.",
    )

    silver_si_qn  = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_STOCKITEMS")
    silver_pkg_qn = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_PACKAGETYPES")
    silver_col_qn = _qn(_DEMO_CONN, _DEMO_DB, _SILVER, "SILVER_COLORS")

    dim_stockitem = SchemaObject(
        name="DIM_STOCKITEM",
        physical_name=f"{_DEMO_DB}.{_GOLD}.DIM_STOCKITEM",
        description="Stock item dimension — product information with package and color names denormalized.",
        source_tables=[src_bronze_stockitems, src_silver_stockitems],
        properties=[
            SchemaProperty(
                name="STOCKITEMID", logical_type=LogicalType.NUMBER,
                description="Stock item identifier (surrogate key).", required=True,
                primary_key=True, primary_key_position=1, unique=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="STOCKITEMID", source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="STOCKITEMNAME", logical_type=LogicalType.STRING,
                description="Product display name.", required=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="STOCKITEMNAME", source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="BRAND", logical_type=LogicalType.STRING,
                description="Brand name (nullable — some items are unbranded).",
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="BRAND", source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="UNITPRICE", logical_type=LogicalType.NUMBER,
                description="Unit selling price in GBP.", required=True, critical_data_element=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="UNITPRICE", source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="RECOMMENDEDRETAILPRICE", logical_type=LogicalType.NUMBER,
                description="RRP — used for margin analysis.",
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="RECOMMENDEDRETAILPRICE", source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="ISCHILLERSTOCK", logical_type=LogicalType.BOOLEAN,
                description="True if this item requires refrigerated storage.",
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="ISCHILLERSTOCK", source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="LEADTIMEDAYS", logical_type=LogicalType.NUMBER,
                description="Supplier lead time in days.", required=True,
                sources=[ColumnSource(source_table="SILVER_STOCKITEMS", source_column="LEADTIMEDAYS", source_table_qualified_name=silver_si_qn)],
            ),
            SchemaProperty(
                name="PACKAGETYPENAME", logical_type=LogicalType.STRING,
                description="Package type — denormalized from SILVER_PACKAGETYPES.",
                sources=[ColumnSource(
                    source_table="SILVER_STOCKITEMS", source_column="PACKAGETYPEID",
                    source_table_qualified_name=silver_si_qn,
                    transform_logic="JOIN SILVER_PACKAGETYPES ON PACKAGETYPEID",
                    transform_description="Denormalized package type name.",
                )],
            ),
            SchemaProperty(
                name="COLORNAME", logical_type=LogicalType.STRING,
                description="Color name — denormalized from SILVER_COLORS.",
                sources=[ColumnSource(
                    source_table="SILVER_STOCKITEMS", source_column="COLORID",
                    source_table_qualified_name=silver_si_qn,
                    transform_logic="LEFT JOIN SILVER_COLORS ON COLORID",
                    transform_description="Nullable — some items have no color.",
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
            "for joining with order fact tables. Single source of truth for "
            "product pricing and inventory classification."
        ),
        description_limitations=(
            "Prices reflect the current list price — historical price changes are "
            "not tracked in this dimension. Discontinued items are excluded."
        ),
        description_usage=(
            "Join with FACT_ORDERS on STOCKITEMID. "
            "Use ISCHILLERSTOCK for warehouse routing logic. "
            "Use LEADTIMEDAYS in supply chain reorder calculations."
        ),
        tags=["products", "inventory", "dimension", "gold-tier", "wide-world-importers"],
        # Real asset registered in Atlan — points to the crawled DIM_STOCKITEM
        atlan_table_qualified_name=_DIM_STOCKITEM_QN,
        atlan_table_guid=None,  # Crawled asset — no DDLC-registered GUID
        atlan_table_url=f"{_BASE_URL}/assets/search?searchTerm=DIM_STOCKITEM",
        schema_objects=[dim_stockitem],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL,
                description="STOCKITEMID must be unique.",
                dimension="uniqueness", severity="critical",
                must_be="unique", method="field_health",
                column="DIM_STOCKITEM.STOCKITEMID",
                schedule="0 5 * * *", engine="monte-carlo",
                business_impact="Duplicate stock item IDs cause join fan-out in FACT_ORDERS.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="UNITPRICE must be > 0.",
                dimension="validity", severity="critical",
                must_be_greater_than=0.0, method="field_health",
                column="DIM_STOCKITEM.UNITPRICE",
                schedule="0 5 * * *", engine="monte-carlo",
                business_impact="Zero/negative prices corrupt revenue calculations in all fact joins.",
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="24", unit="hours",
                description="Refreshed daily at 04:00 UTC, upstream of all fact tables.",
                schedule="0 4 * * *", scheduler="airflow",
                driver="analytics", element="DIM_STOCKITEM",
            ),
        ],
        team=[
            TeamMember(name="Ben Hudson",    email="ben.hudson@atlan.com",    role="Data Owner"),
            TeamMember(name="Aydan McNulty", email="aydan.mcnulty@atlan.com", role="Data Engineer"),
        ],
        servers=[
            Server(
                type=ServerType.SNOWFLAKE,
                environment="prod",
                account="rko-hackaton",
                database=_DEMO_DB,
                schema_name=_GOLD,
                description="Demo Snowflake — Wide World Importers Gold layer.",
                connection_qualified_name=_ANDREW_CONN,
            ),
        ],
        roles=[
            ContractRole(
                role="Data Consumer",
                access=AccessLevel.READ,
                approvers=[RoleApprover(
                    username="ben.hudson@atlan.com",
                    email="ben.hudson@atlan.com",
                    display_name="Ben Hudson",
                )],
                description="Analytics teams consuming product dimension data.",
            ),
        ],
        custom_properties=[
            CustomProperty(key="cost_center",  value="operations-analytics"),
            CustomProperty(key="source_system", value="wide-world-importers"),
            CustomProperty(key="grain",         value="one-row-per-stock-item"),
        ],
    )

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.ACTIVE,
        request=ContractRequest(
            title="WWI Stock Item Dimension — DIM_STOCKITEM",
            description="Formalize the DIM_STOCKITEM gold table with quality rules, SLAs, and lineage documentation.",
            business_context="Operations team needs a governed product dimension for reorder logic and inventory dashboards.",
            target_use_case="Product sales analysis, inventory classification, supply chain reorder workflows.",
            urgency=Urgency.MEDIUM,
            requester=BEN,
            domain="Operations",
            data_product="Orders Analytics",
            created_at=_ts(days_ago=21),
        ),
        contract=contract,
        participants=[BEN, AYDAN],
        comments=[
            Comment(author=BEN,   content="DIM_STOCKITEM is already in production but lacks any governance. Formalizing it.",      stage=DDLCStage.REQUEST,       created_at=_ts(days_ago=21)),
            Comment(author=AYDAN, content="Bronze → Silver → Gold lineage confirmed in Atlan. Simple structure, two denormalized fields.", stage=DDLCStage.DISCOVERY,     created_at=_ts(days_ago=20)),
            Comment(author=AYDAN, content="9-column spec complete. Quality rules on STOCKITEMID uniqueness and UNITPRICE validity.", stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=18)),
            Comment(author=BEN,   content="Spec approved. SLA of 24h is appropriate.", stage=DDLCStage.REVIEW,         created_at=_ts(days_ago=16)),
            Comment(author=BEN,   content="Contract approved and active. Asset visible in Atlan catalog.", stage=DDLCStage.APPROVAL, created_at=_ts(days_ago=15)),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST,       to_stage=DDLCStage.DISCOVERY,     transitioned_by=AYDAN, timestamp=_ts(days_ago=20)),
            StageTransition(from_stage=DDLCStage.DISCOVERY,     to_stage=DDLCStage.SPECIFICATION, transitioned_by=AYDAN, timestamp=_ts(days_ago=19)),
            StageTransition(from_stage=DDLCStage.SPECIFICATION, to_stage=DDLCStage.REVIEW,        transitioned_by=AYDAN, reason="Clean 9-column spec.", timestamp=_ts(days_ago=17)),
            StageTransition(from_stage=DDLCStage.REVIEW,        to_stage=DDLCStage.APPROVAL,      transitioned_by=BEN,   reason="Approved by data owner.", timestamp=_ts(days_ago=16)),
            StageTransition(from_stage=DDLCStage.APPROVAL,      to_stage=DDLCStage.ACTIVE,        transitioned_by=BEN,   reason="Asset registered in Atlan.", timestamp=_ts(days_ago=15)),
        ],
        created_at=_ts(days_ago=21),
        updated_at=_ts(days_ago=15),
    )

    return session


# ============================================================================
# Session 4 — DISCOVERY: Marketing Campaign Attribution
# ============================================================================

def _build_marketing_attribution() -> DDLCSession:
    """Early-stage discovery — shows the source scouting conversation."""

    session_id = _id()

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.DISCOVERY,
        request=ContractRequest(
            title="Marketing Campaign Attribution",
            description=(
                "Multi-touch attribution table crediting marketing channels for "
                "customer conversions. Will power the marketing ROI dashboard."
            ),
            business_context=(
                "The CMO wants to reallocate the quarterly ad budget based on "
                "data-driven attribution. Last-touch attribution is undervaluing "
                "top-of-funnel channels like content and social."
            ),
            target_use_case=(
                "Multi-touch attribution reporting, channel ROI analysis, "
                "budget allocation optimization."
            ),
            urgency=Urgency.MEDIUM,
            requester=AYDAN,
            domain="Context",
            data_product="PII Classification Agent Context",
            desired_fields=[
                "attribution_id", "customer_id", "conversion_date",
                "channel", "touchpoint_count", "attributed_revenue", "attribution_model",
            ],
            created_at=_ts(days_ago=3),
        ),
        contract=ODCSContract(
            name="Marketing Campaign Attribution",
            domain="Context",
            status=ContractStatus.PROPOSED,
        ),
        participants=[AYDAN, ANTONIO],
        comments=[
            Comment(
                author=AYDAN,
                content=(
                    "Submitting on behalf of the marketing analytics team. "
                    "We need multi-touch attribution to replace last-touch. "
                    "Key channels: paid search, social, email, organic."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=3),
            ),
            Comment(
                author=ANTONIO,
                content=(
                    "Starting discovery. I can see campaign event data in the Bronze layer "
                    "under BRONZE_APPLICATION.PEOPLE but it's sparse. "
                    "We may need to pull from an external ad platform source. "
                    "Will check what's available in the Atlan catalog."
                ),
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=2),
            ),
            Comment(
                author=AYDAN,
                content=(
                    "We also have Google Ads and Meta conversion data landing in S3 — "
                    "not yet crawled into Atlan. Antonio, can you check if there's a "
                    "connector already set up, or do we need to add a new source?"
                ),
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=1, hours_ago=6),
            ),
        ],
        history=[
            StageTransition(
                from_stage=DDLCStage.REQUEST,
                to_stage=DDLCStage.DISCOVERY,
                transitioned_by=ANTONIO,
                reason="Starting source discovery — checking Bronze layer and external ad data.",
                timestamp=_ts(days_ago=2, hours_ago=8),
            ),
        ],
        created_at=_ts(days_ago=3),
        updated_at=_ts(days_ago=1),
    )

    return session


# ============================================================================
# Session 5 — REQUEST: Supplier Lead Time Snapshot
# ============================================================================

def _build_supplier_lead_time() -> DDLCSession:
    """Fresh request — shows the entry point of the lifecycle."""

    session_id = _id()

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.REQUEST,
        request=ContractRequest(
            title="Supplier Lead Time Snapshot",
            description=(
                "Daily snapshot of supplier lead times per stock item to support "
                "reorder point optimization and supply chain risk monitoring."
            ),
            business_context=(
                "The operations team is seeing stockouts on high-velocity items "
                "because current reorder points are based on static lead times "
                "from 2021. We need a refreshed, data-driven lead time table."
            ),
            target_use_case=(
                "Reorder point calculations, stockout prediction, "
                "supplier performance scorecards."
            ),
            urgency=Urgency.HIGH,
            requester=BEN,
            domain="Operations",
            data_product="Orders Analytics",
            desired_fields=[
                "stockitem_id", "supplier_id", "snapshot_date",
                "avg_lead_time_days", "min_lead_time_days", "max_lead_time_days",
                "lead_time_stddev", "supplier_reliability_score",
            ],
            created_at=_ts(hours_ago=4),
        ),
        contract=ODCSContract(
            name="Supplier Lead Time Snapshot",
            domain="Operations",
            status=ContractStatus.PROPOSED,
        ),
        participants=[BEN],
        comments=[
            Comment(
                author=BEN,
                content=(
                    "We've had three stockout incidents this month on chiller stock items. "
                    "LEADTIMEDAYS in DIM_STOCKITEM is a static field — we need a historical "
                    "snapshot table tracking actual vs promised lead times by supplier. "
                    "This should feed directly into the reorder point model."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(hours_ago=4),
            ),
        ],
        history=[],
        created_at=_ts(hours_ago=4),
        updated_at=_ts(hours_ago=4),
    )

    return session


# ============================================================================
# Session 6 — SPECIFICATION: WWI Daily Sales Summary (new mart being built)
# ============================================================================
#
# Story: The Sales team wants a pre-aggregated daily summary mart — one row
# per day per salesperson per stock item — built from the existing Gold tables
# (FACT_ORDERS + DIM_CUSTOMER + DIM_STOCKITEM). This is the ideal spec-stage
# demo: sources are real, known, catalogued Gold assets; the target is a brand-
# new aggregation that is actively being specified with the contract builder.
# ============================================================================

def _build_wwi_daily_sales_summary() -> DDLCSession:
    """SPEC-STAGE DEMO — shows the contract builder in full swing with real Gold sources."""

    session_id = _id()
    contract_id = _id()

    # ── Source tables — all real Gold assets in the Demo Snowflake connection ─
    fact_orders_qn   = _qn(_DEMO_CONN, _DEMO_DB, _GOLD, "FACT_ORDERS")
    dim_customer_qn  = _qn(_DEMO_CONN, _DEMO_DB, _GOLD, "DIM_CUSTOMER")
    dim_stockitem_qn = _qn(_DEMO_CONN, _DEMO_DB, _GOLD, "DIM_STOCKITEM")
    dim_employee_qn  = _qn(_DEMO_CONN, _DEMO_DB, _GOLD, "DIM_EMPLOYEE")

    src_fact_orders = SourceTable(
        name="FACT_ORDERS",
        qualified_name=fact_orders_qn,
        database_name=_DEMO_DB,
        schema_name=_GOLD,
        connector_name="snowflake",
        description="Gold-tier order fact table — one row per order line with revenue metrics.",
        columns=[
            {"name": "ORDERID",       "logical_type": "number", "is_primary": False, "is_nullable": False},
            {"name": "ORDERLINEID",   "logical_type": "number", "is_primary": True,  "is_nullable": False},
            {"name": "SALESPERSONID", "logical_type": "number", "is_primary": False, "is_nullable": False},
            {"name": "CUSTOMERID",    "logical_type": "number", "is_primary": False, "is_nullable": False},
            {"name": "STOCKITEMID",   "logical_type": "number", "is_primary": False, "is_nullable": False},
            {"name": "ORDERDATE",     "logical_type": "date",   "is_primary": False, "is_nullable": False},
            {"name": "QUANTITY",      "logical_type": "number", "is_primary": False, "is_nullable": False},
            {"name": "UNITPRICE",     "logical_type": "number", "is_primary": False, "is_nullable": False},
            {"name": "TAXRATE",       "logical_type": "number", "is_primary": False, "is_nullable": False},
        ],
    )

    src_dim_customer = SourceTable(
        name="DIM_CUSTOMER",
        qualified_name=dim_customer_qn,
        database_name=_DEMO_DB,
        schema_name=_GOLD,
        connector_name="snowflake",
        description="Customer dimension — enriched with category and buying group names.",
        columns=[
            {"name": "CUSTOMERID",           "logical_type": "number", "is_primary": True,  "is_nullable": False},
            {"name": "CUSTOMERNAME",         "logical_type": "string", "is_primary": False, "is_nullable": False},
            {"name": "CUSTOMERCATEGORYNAME", "logical_type": "string", "is_primary": False, "is_nullable": True},
            {"name": "BUYINGGROUPNAME",      "logical_type": "string", "is_primary": False, "is_nullable": True},
        ],
    )

    src_dim_stockitem = SourceTable(
        name="DIM_STOCKITEM",
        qualified_name=dim_stockitem_qn,
        database_name=_DEMO_DB,
        schema_name=_GOLD,
        connector_name="snowflake",
        description="Stock item dimension — product info with package and color names.",
        columns=[
            {"name": "STOCKITEMID",   "logical_type": "number", "is_primary": True,  "is_nullable": False},
            {"name": "STOCKITEMNAME", "logical_type": "string", "is_primary": False, "is_nullable": False},
            {"name": "BRAND",         "logical_type": "string", "is_primary": False, "is_nullable": True},
            {"name": "COLORNAME",     "logical_type": "string", "is_primary": False, "is_nullable": True},
            {"name": "ISCHILLERSTOCK","logical_type": "boolean","is_primary": False, "is_nullable": False},
        ],
    )

    src_dim_employee = SourceTable(
        name="DIM_EMPLOYEE",
        qualified_name=dim_employee_qn,
        database_name=_DEMO_DB,
        schema_name=_GOLD,
        connector_name="snowflake",
        description="Employee dimension — salesperson names for reporting.",
        columns=[
            {"name": "SALESPERSONID", "logical_type": "number", "is_primary": True,  "is_nullable": False},
            {"name": "FULLNAME",      "logical_type": "string", "is_primary": False, "is_nullable": False},
        ],
    )

    # ── Target schema object: DAILY_SALES_SUMMARY ────────────────────────────
    # Grain: one row per ORDERDATE × SALESPERSONID × STOCKITEMID
    daily_sales = SchemaObject(
        name="DAILY_SALES_SUMMARY",
        physical_name=f"{_DEMO_DB}.{_GOLD}.DAILY_SALES_SUMMARY",
        description=(
            "Pre-aggregated daily sales summary. One row per order date × salesperson "
            "× stock item. Enables fast slice-and-dice reporting without hitting the "
            "order-line grain fact table directly. Replaces ad-hoc GROUP BY queries "
            "in the BI tool that are slowing dashboards."
        ),
        source_tables=[src_fact_orders, src_dim_customer, src_dim_stockitem, src_dim_employee],
        properties=[
            SchemaProperty(
                name="SUMMARY_KEY",
                logical_type=LogicalType.STRING,
                description="Surrogate key: MD5 of ORDERDATE + SALESPERSONID + STOCKITEMID.",
                required=True,
                primary_key=True,
                primary_key_position=1,
                unique=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="ORDERDATE",
                    source_table_qualified_name=fact_orders_qn,
                    transform_logic="MD5(CONCAT(ORDERDATE, '|', SALESPERSONID, '|', STOCKITEMID))",
                    transform_description="Composite surrogate key for the aggregation grain.",
                )],
            ),
            SchemaProperty(
                name="ORDERDATE",
                logical_type=LogicalType.DATE,
                description="The calendar date for this summary row.",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="ORDERDATE",
                    source_table_qualified_name=fact_orders_qn,
                )],
            ),
            SchemaProperty(
                name="SALESPERSONID",
                logical_type=LogicalType.NUMBER,
                description="FK to DIM_EMPLOYEE — salesperson responsible for these orders.",
                required=True,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="SALESPERSONID",
                    source_table_qualified_name=fact_orders_qn,
                )],
            ),
            SchemaProperty(
                name="SALESPERSON_NAME",
                logical_type=LogicalType.STRING,
                description="Salesperson full name — denormalized from DIM_EMPLOYEE.",
                required=True,
                sources=[ColumnSource(
                    source_table="DIM_EMPLOYEE",
                    source_column="FULLNAME",
                    source_table_qualified_name=dim_employee_qn,
                    transform_logic="JOIN DIM_EMPLOYEE ON SALESPERSONID",
                    transform_description="Denormalized to avoid join in BI layer.",
                )],
            ),
            SchemaProperty(
                name="STOCKITEMID",
                logical_type=LogicalType.NUMBER,
                description="FK to DIM_STOCKITEM — the product sold.",
                required=True,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="STOCKITEMID",
                    source_table_qualified_name=fact_orders_qn,
                )],
            ),
            SchemaProperty(
                name="STOCKITEM_NAME",
                logical_type=LogicalType.STRING,
                description="Product name — denormalized from DIM_STOCKITEM.",
                required=True,
                sources=[ColumnSource(
                    source_table="DIM_STOCKITEM",
                    source_column="STOCKITEMNAME",
                    source_table_qualified_name=dim_stockitem_qn,
                    transform_logic="JOIN DIM_STOCKITEM ON STOCKITEMID",
                    transform_description="Denormalized product name for dashboard grouping.",
                )],
            ),
            SchemaProperty(
                name="BRAND",
                logical_type=LogicalType.STRING,
                description="Product brand — nullable, denormalized from DIM_STOCKITEM.",
                required=False,
                sources=[ColumnSource(
                    source_table="DIM_STOCKITEM",
                    source_column="BRAND",
                    source_table_qualified_name=dim_stockitem_qn,
                )],
            ),
            SchemaProperty(
                name="CUSTOMER_CATEGORY",
                logical_type=LogicalType.STRING,
                description=(
                    "Most common customer category among orders on this day/salesperson/item. "
                    "Nullable — populated only when a single category dominates."
                ),
                required=False,
                sources=[ColumnSource(
                    source_table="DIM_CUSTOMER",
                    source_column="CUSTOMERCATEGORYNAME",
                    source_table_qualified_name=dim_customer_qn,
                    transform_logic="MODE(DIM_CUSTOMER.CUSTOMERCATEGORYNAME)",
                    transform_description=(
                        "Statistical mode of customer categories across all orders "
                        "for this grain — useful for attribution analysis."
                    ),
                )],
            ),
            SchemaProperty(
                name="TOTAL_ORDERS",
                logical_type=LogicalType.INTEGER,
                description="Count of distinct order IDs on this date for this salesperson and item.",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="ORDERID",
                    source_table_qualified_name=fact_orders_qn,
                    transform_logic="COUNT(DISTINCT ORDERID)",
                    transform_description="Distinct orders in the aggregation window.",
                )],
            ),
            SchemaProperty(
                name="TOTAL_QUANTITY",
                logical_type=LogicalType.INTEGER,
                description="Total units sold for this item by this salesperson on this date.",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="QUANTITY",
                    source_table_qualified_name=fact_orders_qn,
                    transform_logic="SUM(QUANTITY)",
                    transform_description="Sum of quantity across all matching order lines.",
                )],
            ),
            SchemaProperty(
                name="GROSS_REVENUE",
                logical_type=LogicalType.NUMBER,
                description="Sum of QUANTITY × UNITPRICE for all matching order lines.",
                required=True,
                critical_data_element=True,
                sources=[
                    ColumnSource(
                        source_table="FACT_ORDERS",
                        source_column="QUANTITY",
                        source_table_qualified_name=fact_orders_qn,
                        transform_logic="SUM(QUANTITY * UNITPRICE)",
                        transform_description="Total pre-tax revenue for this grain row.",
                    ),
                    ColumnSource(
                        source_table="FACT_ORDERS",
                        source_column="UNITPRICE",
                        source_table_qualified_name=fact_orders_qn,
                    ),
                ],
            ),
            SchemaProperty(
                name="TOTAL_TAX",
                logical_type=LogicalType.NUMBER,
                description="Sum of tax amounts: SUM(QUANTITY × UNITPRICE × TAXRATE / 100).",
                required=True,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="TAXRATE",
                    source_table_qualified_name=fact_orders_qn,
                    transform_logic="SUM(QUANTITY * UNITPRICE * TAXRATE / 100)",
                    transform_description="Total tax collected across matching order lines.",
                )],
            ),
            SchemaProperty(
                name="AVG_UNIT_PRICE",
                logical_type=LogicalType.NUMBER,
                description="Average unit price across all order lines in this grain row.",
                required=False,
                sources=[ColumnSource(
                    source_table="FACT_ORDERS",
                    source_column="UNITPRICE",
                    source_table_qualified_name=fact_orders_qn,
                    transform_logic="AVG(UNITPRICE)",
                    transform_description="Useful for detecting price anomalies vs list price.",
                )],
            ),
        ],
    )

    # ── Contract ─────────────────────────────────────────────────────────────
    contract = ODCSContract(
        id=contract_id,
        name="WWI Daily Sales Summary",
        version="0.1.0",
        status=ContractStatus.DRAFT,
        domain="Finance",
        data_product="Sales Order Insights",
        data_product_qualified_name="default/domain/J3sne7aVPzMgU6KYHsoRT/super/product/ULc0yDRiVjv19LvuYyaEk",
        description_purpose=(
            "Pre-aggregate the order-line fact table to a daily × salesperson × "
            "stock item grain. Eliminates slow GROUP BY queries in the BI layer and "
            "provides a single governed table for sales performance dashboards."
        ),
        description_limitations=(
            "Aggregated grain — cannot be used for order-line-level analysis. "
            "CUSTOMER_CATEGORY uses statistical mode and may be null for mixed-category days. "
            "Covers transactional orders only; no subscription revenue."
        ),
        description_usage=(
            "Use for daily sales KPI dashboards, salesperson league tables, "
            "product sales trend analysis, and executive revenue reporting. "
            "For order-level detail, join back to FACT_ORDERS on ORDERDATE + SALESPERSONID + STOCKITEMID."
        ),
        tags=["sales", "revenue", "summary", "gold-tier", "wide-world-importers", "aggregated"],
        schema_objects=[daily_sales],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL,
                description="SUMMARY_KEY must be unique — no duplicate grain rows.",
                dimension="uniqueness",
                severity="critical",
                must_be="unique",
                method="field_health",
                column="DAILY_SALES_SUMMARY.SUMMARY_KEY",
                schedule="0 7 * * *",
                scheduler="airflow",
                engine="monte-carlo",
                business_impact=(
                    "Duplicate grain rows cause double-counting of revenue in all "
                    "sales dashboards and break salesperson ranking tables."
                ),
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="GROSS_REVENUE must be >= 0.",
                dimension="validity",
                severity="critical",
                must_be_greater_than=0.0,
                method="field_health",
                column="DAILY_SALES_SUMMARY.GROSS_REVENUE",
                schedule="0 7 * * *",
                engine="monte-carlo",
                business_impact="Negative revenue corrupts daily and monthly sales KPIs.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="TOTAL_QUANTITY must be > 0 for every row.",
                dimension="validity",
                severity="high",
                must_be_greater_than=0.0,
                method="field_health",
                column="DAILY_SALES_SUMMARY.TOTAL_QUANTITY",
                schedule="0 7 * * *",
                engine="monte-carlo",
                business_impact="Zero-quantity rows are ghost records that skew product velocity metrics.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="GROSS_REVENUE should reconcile to within 1% of FACT_ORDERS daily total.",
                dimension="accuracy",
                severity="high",
                method="sql_rule",
                query=(
                    "SELECT ABS(SUM(s.GROSS_REVENUE) - SUM(f.QUANTITY * f.UNITPRICE)) / "
                    "NULLIF(SUM(f.QUANTITY * f.UNITPRICE), 0) "
                    "FROM DAILY_SALES_SUMMARY s "
                    "JOIN FACT_ORDERS f ON s.ORDERDATE = f.ORDERDATE "
                    "WHERE s.ORDERDATE = CURRENT_DATE - 1"
                ),
                schedule="0 8 * * *",
                engine="monte-carlo",
                business_impact=(
                    "Revenue discrepancy vs the fact table indicates an aggregation bug "
                    "that would cause Finance to report incorrect daily revenue."
                ),
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="24", unit="hours",
                description="Refreshed daily at 07:00 UTC, after FACT_ORDERS completes at 06:00.",
                schedule="0 7 * * *", scheduler="airflow",
                driver="analytics", element="DAILY_SALES_SUMMARY",
            ),
            SLAProperty(
                property="availability", value="99.5", unit="percent",
                description="Must be available for morning dashboard refresh at 08:00 UTC.",
                driver="operational", element="DAILY_SALES_SUMMARY",
            ),
            SLAProperty(
                property="latency", value="30", unit="minutes",
                description="Pipeline should complete within 30 minutes of FACT_ORDERS finish.",
                schedule="0 7 * * *", scheduler="airflow",
                driver="analytics", element="DAILY_SALES_SUMMARY",
            ),
        ],
        team=[
            TeamMember(name="Aydan McNulty",    email="aydan.mcnulty@atlan.com",   role="Data Owner"),
            TeamMember(name="Andrew Lentz",     email="andrew.lentz@atlan.com",    role="Data Steward"),
            TeamMember(name="Antonio Hernandez",email="antonio.hernandez@atlan.com",role="Data Engineer"),
        ],
        servers=[
            Server(
                type=ServerType.SNOWFLAKE,
                environment="prod",
                account="rko-hackaton",
                database=_DEMO_DB,
                schema_name=_GOLD,
                description="Demo Snowflake — Wide World Importers Gold layer.",
                connection_qualified_name=_ANDREW_CONN,
            ),
        ],
        roles=[
            ContractRole(
                role="Data Consumer",
                access=AccessLevel.READ,
                approvers=[
                    RoleApprover(
                        username="aydan.mcnulty@atlan.com",
                        email="aydan.mcnulty@atlan.com",
                        display_name="Aydan McNulty",
                    ),
                ],
                description="Sales, Finance, and executive teams consuming daily KPI reports.",
            ),
            ContractRole(
                role="Data Producer",
                access=AccessLevel.WRITE,
                approvers=[
                    RoleApprover(
                        username="antonio.hernandez@atlan.com",
                        email="antonio.hernandez@atlan.com",
                        display_name="Antonio Hernandez",
                    ),
                ],
                description="Data engineering team maintaining the FACT_ORDERS → summary pipeline.",
            ),
            ContractRole(
                role="Data Owner",
                access=AccessLevel.ADMIN,
                approvers=[
                    RoleApprover(
                        username="aydan.mcnulty@atlan.com",
                        email="aydan.mcnulty@atlan.com",
                        display_name="Aydan McNulty",
                    ),
                ],
                description="Aydan McNulty owns this mart and signs off on schema changes.",
            ),
        ],
        custom_properties=[
            CustomProperty(key="cost_center",   value="finance-analytics"),
            CustomProperty(key="grain",          value="date-salesperson-stockitem"),
            CustomProperty(key="source_system",  value="wide-world-importers"),
            CustomProperty(key="replaces",       value="ad-hoc BI GROUP BY queries"),
        ],
    )

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.SPECIFICATION,
        request=ContractRequest(
            title="WWI Daily Sales Summary — DAILY_SALES_SUMMARY",
            description=(
                "Pre-aggregate FACT_ORDERS to a daily × salesperson × stock item "
                "grain with denormalized names and revenue metrics. Will replace "
                "slow ad-hoc GROUP BY queries in the BI tool."
            ),
            business_context=(
                "The sales leadership dashboard is timing out at peak hours because "
                "it runs a 10-table JOIN with a GROUP BY directly against FACT_ORDERS. "
                "A pre-aggregated summary table will cut query time from ~45s to <2s "
                "and give the team a governed, testable mart to build reports against."
            ),
            target_use_case=(
                "Daily sales KPI dashboard, salesperson leaderboard, "
                "product velocity tracking, executive revenue summary."
            ),
            urgency=Urgency.HIGH,
            requester=AYDAN,
            domain="Finance",
            data_product="Sales Order Insights",
            data_product_qualified_name="default/domain/J3sne7aVPzMgU6KYHsoRT/super/product/ULc0yDRiVjv19LvuYyaEk",
            desired_fields=[
                "ORDERDATE", "SALESPERSONID", "SALESPERSON_NAME",
                "STOCKITEMID", "STOCKITEM_NAME", "BRAND",
                "TOTAL_ORDERS", "TOTAL_QUANTITY", "GROSS_REVENUE", "TOTAL_TAX",
            ],
            created_at=_ts(days_ago=5),
        ),
        contract=contract,
        participants=[AYDAN, ANDREW, ANTONIO, AVINASH],
        comments=[
            Comment(
                author=AYDAN,
                content=(
                    "The sales dashboard is timing out at peak hours — 45-second queries "
                    "because every chart hits FACT_ORDERS directly with a GROUP BY. "
                    "We need a pre-aggregated summary table with a proper data contract."
                ),
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=5),
            ),
            Comment(
                author=ANTONIO,
                content=(
                    "Good news — all four sources already exist as governed Gold tables: "
                    "FACT_ORDERS, DIM_CUSTOMER, DIM_STOCKITEM, and DIM_EMPLOYEE. "
                    "No new source onboarding needed. We're just aggregating existing "
                    "catalogued assets. Build should be fast."
                ),
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=4),
            ),
            Comment(
                author=AVINASH,
                content=(
                    "Confirmed: FACT_ORDERS has ~2.4M rows, DIM_CUSTOMER 663 rows, "
                    "DIM_STOCKITEM 227 rows, DIM_EMPLOYEE 13 rows. "
                    "Daily summary will reduce to roughly 3,000–8,000 rows per day. "
                    "The aggregation is straightforward: GROUP BY date + salesperson + stockitem."
                ),
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=4, hours_ago=3),
            ),
            Comment(
                author=ANDREW,
                content=(
                    "I've started mapping the schema. Proposing 13 columns: "
                    "composite surrogate key, 3 grain dimensions with denormalized names, "
                    "CUSTOMER_CATEGORY for attribution, and 5 metric columns "
                    "(TOTAL_ORDERS, TOTAL_QUANTITY, GROSS_REVENUE, TOTAL_TAX, AVG_UNIT_PRICE). "
                    "Does the team want BRAND and ISCHILLERSTOCK on the summary too?"
                ),
                stage=DDLCStage.SPECIFICATION,
                created_at=_ts(days_ago=3),
            ),
            Comment(
                author=AYDAN,
                content=(
                    "Yes to BRAND — the sales team filters by it constantly. "
                    "Skip ISCHILLERSTOCK for now, that's more of an ops concern. "
                    "Also can we add AVG_UNIT_PRICE? Useful for detecting price anomalies."
                ),
                stage=DDLCStage.SPECIFICATION,
                created_at=_ts(days_ago=2, hours_ago=8),
            ),
            Comment(
                author=ANTONIO,
                content=(
                    "Updated the spec with BRAND and AVG_UNIT_PRICE. "
                    "Added 4 quality checks: uniqueness on SUMMARY_KEY, "
                    "non-negative GROSS_REVENUE and TOTAL_QUANTITY, "
                    "and a revenue reconciliation check against FACT_ORDERS (±1%). "
                    "That last one is key for Finance sign-off."
                ),
                stage=DDLCStage.SPECIFICATION,
                created_at=_ts(days_ago=1, hours_ago=4),
            ),
        ],
        history=[
            StageTransition(
                from_stage=DDLCStage.REQUEST,
                to_stage=DDLCStage.DISCOVERY,
                transitioned_by=ANTONIO,
                reason="Sources confirmed — all four are existing Gold tables.",
                timestamp=_ts(days_ago=4, hours_ago=6),
            ),
            StageTransition(
                from_stage=DDLCStage.DISCOVERY,
                to_stage=DDLCStage.SPECIFICATION,
                transitioned_by=ANTONIO,
                reason="Row counts and schema grain confirmed. Starting specification.",
                timestamp=_ts(days_ago=3, hours_ago=2),
            ),
        ],
        created_at=_ts(days_ago=5),
        updated_at=_ts(hours_ago=4),
    )

    return session


# ============================================================================
# Main seed function
# ============================================================================

async def seed_demo_data() -> list[str]:
    """
    Create all demo sessions and persist them to the store.
    Returns the list of created session IDs.
    """
    builders = [
        _build_wwi_fact_orders,             # APPROVAL     ← PRIMARY DEMO (advance to ACTIVE live)
        _build_wwi_daily_sales_summary,     # SPECIFICATION ← shows contract builder in full swing
        _build_wwi_dim_customer,            # REVIEW
        _build_wwi_dim_stockitem,           # ACTIVE       (already registered)
        _build_marketing_attribution,       # DISCOVERY
        _build_supplier_lead_time,          # REQUEST      (fresh entry)
    ]

    created_ids = []
    for builder in builders:
        session = builder()
        await store.save_session(session)
        created_ids.append(session.id)
        print(f"  [seed] Created: {session.request.title} ({session.current_stage.value})")

    return created_ids

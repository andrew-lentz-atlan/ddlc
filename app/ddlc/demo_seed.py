"""
Demo seed data for the DDLC platform.

Creates pre-populated sessions at various lifecycle stages so the platform
is ready for demos immediately after server start — no manual setup needed.

Usage:
    Called automatically on server startup.
    Can also be triggered via:  POST /api/demo/seed
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta

from app.ddlc import store
from app.ddlc.models import (
    ColumnSource,
    Comment,
    ContractRequest,
    ContractStatus,
    DDLCSession,
    DDLCStage,
    LogicalType,
    ODCSContract,
    Participant,
    QualityCheck,
    QualityCheckType,
    SchemaObject,
    SchemaProperty,
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


def _ts(days_ago: int = 0, hours_ago: int = 0) -> datetime:
    """Helper to generate timestamps relative to now."""
    return _now - timedelta(days=days_ago, hours=hours_ago)


def _id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Shared participants
# ---------------------------------------------------------------------------

ALICE = Participant(name="Alice Chen", email="alice.chen@acme.com")
BOB = Participant(name="Bob Martinez", email="bob.martinez@acme.com")
CAROL = Participant(name="Carol Wang", email="carol.wang@acme.com")
DAVE = Participant(name="Dave Okonkwo", email="dave.okonkwo@acme.com")
EVE = Participant(name="Eve Thompson", email="eve.thompson@acme.com")


# ===================================================================
# Session 1: SPECIFICATION stage — Customer 360 table (richest demo)
# ===================================================================

def _build_customer_360() -> DDLCSession:
    """A rich session at specification stage — ideal for demoing the builder."""

    session_id = _id()
    contract_id = _id()

    # -- Source tables (as if imported from Atlan) --
    source_customers = SourceTable(
        name="raw_customers",
        qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
        database_name="PROD_DB",
        schema_name="RAW",
        connector_name="snowflake",
        description="Raw customer data from the CRM system (daily extract).",
        columns=[
            {"name": "customer_id", "logical_type": "integer", "is_primary": True, "is_nullable": False},
            {"name": "first_name", "logical_type": "string", "is_nullable": False},
            {"name": "last_name", "logical_type": "string", "is_nullable": False},
            {"name": "email", "logical_type": "string", "is_nullable": False},
            {"name": "phone", "logical_type": "string", "is_nullable": True},
            {"name": "created_at", "logical_type": "timestamp", "is_nullable": False},
            {"name": "updated_at", "logical_type": "timestamp", "is_nullable": True},
            {"name": "status", "logical_type": "string", "is_nullable": False},
        ],
    )

    source_orders = SourceTable(
        name="raw_orders",
        qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
        database_name="PROD_DB",
        schema_name="RAW",
        connector_name="snowflake",
        description="Order transaction data from the e-commerce platform.",
        columns=[
            {"name": "order_id", "logical_type": "integer", "is_primary": True, "is_nullable": False},
            {"name": "customer_id", "logical_type": "integer", "is_nullable": False},
            {"name": "order_date", "logical_type": "date", "is_nullable": False},
            {"name": "total_amount", "logical_type": "number", "is_nullable": False},
            {"name": "status", "logical_type": "string", "is_nullable": False},
        ],
    )

    source_events = SourceTable(
        name="raw_web_events",
        qualified_name="default/snowflake/PROD_DB/RAW/WEB_EVENTS",
        database_name="PROD_DB",
        schema_name="RAW",
        connector_name="snowflake",
        description="Clickstream web events from the analytics platform.",
        columns=[
            {"name": "event_id", "logical_type": "string", "is_primary": True, "is_nullable": False},
            {"name": "customer_id", "logical_type": "integer", "is_nullable": True},
            {"name": "event_type", "logical_type": "string", "is_nullable": False},
            {"name": "page_url", "logical_type": "string", "is_nullable": False},
            {"name": "event_timestamp", "logical_type": "timestamp", "is_nullable": False},
        ],
    )

    # -- Target table: customer_360 --
    customer_360 = SchemaObject(
        name="customer_360",
        physical_name="ANALYTICS.MART.CUSTOMER_360",
        description="Unified customer profile combining CRM data, order history, and web engagement metrics.",
        source_tables=[source_customers, source_orders, source_events],
        properties=[
            SchemaProperty(
                name="customer_id",
                logical_type=LogicalType.INTEGER,
                description="Unique customer identifier from CRM.",
                required=True,
                primary_key=True,
                primary_key_position=1,
                unique=True,
                classification="internal",
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="raw_customers",
                    source_column="customer_id",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                    transform_logic="CAST(customer_id AS INT)",
                    transform_description="Direct mapping from CRM customer ID.",
                )],
            ),
            SchemaProperty(
                name="full_name",
                logical_type=LogicalType.STRING,
                description="Customer full name (first + last).",
                required=True,
                classification="pii",
                critical_data_element=True,
                sources=[
                    ColumnSource(
                        source_table="raw_customers",
                        source_column="first_name",
                        source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                        transform_logic="CONCAT(first_name, ' ', last_name)",
                        transform_description="Concatenation of first and last name.",
                    ),
                    ColumnSource(
                        source_table="raw_customers",
                        source_column="last_name",
                        source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                    ),
                ],
            ),
            SchemaProperty(
                name="email",
                logical_type=LogicalType.STRING,
                description="Primary email address.",
                required=True,
                unique=True,
                classification="pii",
                critical_data_element=True,
                examples=["alice@example.com", "bob@test.org"],
                sources=[ColumnSource(
                    source_table="raw_customers",
                    source_column="email",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                    transform_logic="LOWER(TRIM(email))",
                    transform_description="Normalized to lowercase, trimmed.",
                )],
            ),
            SchemaProperty(
                name="phone",
                logical_type=LogicalType.STRING,
                description="Phone number (optional).",
                required=False,
                classification="pii",
                sources=[ColumnSource(
                    source_table="raw_customers",
                    source_column="phone",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                    transform_logic="REGEXP_REPLACE(phone, '[^0-9+]', '')",
                    transform_description="Strip non-numeric characters except +.",
                )],
            ),
            SchemaProperty(
                name="customer_status",
                logical_type=LogicalType.STRING,
                description="Current account status (active, churned, suspended).",
                required=True,
                examples=["active", "churned", "suspended"],
                sources=[ColumnSource(
                    source_table="raw_customers",
                    source_column="status",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                    transform_logic="UPPER(status)",
                    transform_description="Uppercased status from CRM.",
                )],
            ),
            SchemaProperty(
                name="total_orders",
                logical_type=LogicalType.INTEGER,
                description="Lifetime count of orders placed.",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="raw_orders",
                    source_column="order_id",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                    transform_logic="COUNT(DISTINCT order_id)",
                    transform_description="Count of distinct orders per customer.",
                )],
            ),
            SchemaProperty(
                name="total_revenue",
                logical_type=LogicalType.NUMBER,
                description="Lifetime revenue from all orders.",
                required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="raw_orders",
                    source_column="total_amount",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                    transform_logic="SUM(total_amount)",
                    transform_description="Sum of order amounts per customer.",
                )],
            ),
            SchemaProperty(
                name="last_order_date",
                logical_type=LogicalType.DATE,
                description="Date of most recent order.",
                required=False,
                sources=[ColumnSource(
                    source_table="raw_orders",
                    source_column="order_date",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                    transform_logic="MAX(order_date)",
                    transform_description="Most recent order date per customer.",
                )],
            ),
            SchemaProperty(
                name="web_sessions_30d",
                logical_type=LogicalType.INTEGER,
                description="Number of web sessions in the last 30 days.",
                required=False,
                sources=[ColumnSource(
                    source_table="raw_web_events",
                    source_column="event_id",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/WEB_EVENTS",
                    transform_logic="COUNT(DISTINCT DATE_TRUNC('day', event_timestamp)) WHERE event_timestamp >= CURRENT_DATE - 30",
                    transform_description="Count of distinct days with activity in last 30 days.",
                )],
            ),
            SchemaProperty(
                name="first_seen_at",
                logical_type=LogicalType.TIMESTAMP,
                description="Timestamp of account creation.",
                required=True,
                sources=[ColumnSource(
                    source_table="raw_customers",
                    source_column="created_at",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                    transform_logic="created_at",
                    transform_description="Direct mapping.",
                )],
            ),
            SchemaProperty(
                name="last_updated_at",
                logical_type=LogicalType.TIMESTAMP,
                description="Timestamp of last profile update.",
                required=False,
                sources=[ColumnSource(
                    source_table="raw_customers",
                    source_column="updated_at",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/CUSTOMERS",
                    transform_logic="COALESCE(updated_at, created_at)",
                    transform_description="Falls back to created_at if never updated.",
                )],
            ),
        ],
    )

    contract = ODCSContract(
        id=contract_id,
        name="Customer 360",
        version="0.1.0",
        status=ContractStatus.DRAFT,
        domain="Customer Analytics",
        tenant="Acme Corp",
        data_product="Customer Intelligence Platform",
        description_purpose="Provide a unified, deduplicated view of customer data for marketing, support, and product analytics teams.",
        description_limitations="Does not include real-time streaming data. Updated on a daily batch cadence. Phone numbers may be incomplete for legacy accounts.",
        description_usage="Use for customer segmentation, churn analysis, lifetime value calculations, and personalization. Do not use for regulatory reporting without cross-referencing with the compliance dataset.",
        tags=["customer", "analytics", "pii", "gold-tier"],
        schema_objects=[customer_360],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL,
                description="Customer ID must be unique across all rows.",
                dimension="uniqueness",
                severity="critical",
                must_be="unique",
                method="field_health",
                column="customer_360.customer_id",
                schedule="0 6 * * *",
                scheduler="monte-carlo",
                engine="monte-carlo",
                business_impact="Duplicate customers cause double-counting in revenue attribution and corrupt marketing segmentation models.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="Email must not be null for active customers.",
                dimension="completeness",
                severity="critical",
                must_be="not null where customer_status = 'ACTIVE'",
                method="sql_rule",
                column="customer_360.email",
                query="SELECT COUNT(*) FROM customer_360 WHERE email IS NULL AND customer_status = 'ACTIVE'",
                schedule="0 6 * * *",
                scheduler="monte-carlo",
                engine="monte-carlo",
                business_impact="Null emails prevent campaign delivery and break personalization workflows.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="Total revenue must be non-negative.",
                dimension="validity",
                severity="high",
                must_be_greater_than=0.0,
                method="field_health",
                column="customer_360.total_revenue",
                schedule="0 7 * * *",
                engine="monte-carlo",
                business_impact="Negative revenue values corrupt LTV calculations and finance reconciliation.",
            ),
            QualityCheck(
                type=QualityCheckType.TEXT,
                description="Row count should not drop more than 10% day-over-day.",
                dimension="volume",
                severity="medium",
                method="volume",
                schedule="0 6 * * *",
                engine="monte-carlo",
                business_impact="Sudden volume drops indicate upstream pipeline failures or data loss.",
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="24", unit="hours",
                description="Data refreshed daily by 6am UTC.",
                schedule="0 6 * * *", scheduler="airflow",
                driver="analytics", element="customer_360",
            ),
            SLAProperty(
                property="availability", value="99.5", unit="percent",
                description="Target uptime for the downstream BI dashboards.",
                driver="operational", element="customer_360",
            ),
            SLAProperty(
                property="latency", value="30", unit="minutes",
                description="Maximum pipeline runtime from source extract to mart load.",
                schedule="0 6 * * *", scheduler="airflow",
                driver="analytics", element="customer_360",
            ),
            SLAProperty(
                property="retention", value="7", unit="years",
                description="Retain historical snapshots for 7 years per compliance policy.",
                driver="regulatory",
            ),
        ],
        team=[
            TeamMember(name="Alice Chen", email="alice.chen@acme.com", role="Data Owner"),
            TeamMember(name="Bob Martinez", email="bob.martinez@acme.com", role="Data Steward"),
            TeamMember(name="Carol Wang", email="carol.wang@acme.com", role="Data Engineer"),
            TeamMember(name="Dave Okonkwo", email="dave.okonkwo@acme.com", role="Analytics Engineer"),
        ],
    )

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.SPECIFICATION,
        request=ContractRequest(
            title="Customer 360 Unified Profile",
            description="We need a unified customer profile table that combines CRM data, order history, and web engagement metrics into a single analytics-ready dataset.",
            business_context="The marketing team is running a major personalization initiative in Q2. They need a single source of truth for customer attributes to power segmentation models and campaign targeting.",
            target_use_case="Customer segmentation, churn prediction, lifetime value analysis, personalized marketing campaigns.",
            urgency=Urgency.HIGH,
            requester=ALICE,
            domain="Customer Analytics",
            data_product="Customer Intelligence Platform",
            desired_fields=["customer_id", "full_name", "email", "total_orders", "total_revenue", "last_order_date", "web_sessions_30d"],
            created_at=_ts(days_ago=5),
        ),
        contract=contract,
        participants=[ALICE, BOB, CAROL, DAVE],
        comments=[
            Comment(
                author=ALICE,
                content="We need this customer 360 table to power our Q2 personalization initiative. The marketing team is blocked without a unified customer view.",
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=5),
            ),
            Comment(
                author=BOB,
                content="I've reviewed the source systems. We have three primary sources: raw_customers (CRM), raw_orders (e-commerce), and raw_web_events (clickstream). All are available in the RAW schema on Snowflake.",
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=4),
            ),
            Comment(
                author=CAROL,
                content="The raw_customers table has ~2.5M rows with daily incremental loads. raw_orders is ~15M rows. Web events are ~100M rows but we'll aggregate to customer-level metrics. I'll set up the source-to-target mapping.",
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=4, hours_ago=2),
            ),
            Comment(
                author=DAVE,
                content="For the analytics models, we'll need total_orders, total_revenue, and web_sessions_30d as pre-computed aggregates. The dbt model should handle the joins and aggregations.",
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=3),
            ),
        ],
        history=[
            StageTransition(
                from_stage=DDLCStage.REQUEST,
                to_stage=DDLCStage.DISCOVERY,
                transitioned_by=BOB,
                reason="Request is clear and well-scoped. Moving to discovery.",
                timestamp=_ts(days_ago=4, hours_ago=6),
            ),
            StageTransition(
                from_stage=DDLCStage.DISCOVERY,
                to_stage=DDLCStage.SPECIFICATION,
                transitioned_by=CAROL,
                reason="Source systems identified. Starting specification.",
                timestamp=_ts(days_ago=3),
            ),
        ],
        created_at=_ts(days_ago=5),
        updated_at=_ts(hours_ago=2),
    )

    return session


# ===================================================================
# Session 2: REVIEW stage — Order Events Fact Table
# ===================================================================

def _build_order_events() -> DDLCSession:
    """A session in review stage with quality rules and SLAs ready for review."""

    session_id = _id()

    source_orders = SourceTable(
        name="raw_orders",
        qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
        database_name="PROD_DB",
        schema_name="RAW",
        connector_name="snowflake",
        description="Order transaction data from the e-commerce platform.",
    )

    source_payments = SourceTable(
        name="raw_payments",
        qualified_name="default/snowflake/PROD_DB/RAW/PAYMENTS",
        database_name="PROD_DB",
        schema_name="RAW",
        connector_name="snowflake",
        description="Payment records linked to orders.",
    )

    fact_orders = SchemaObject(
        name="fact_order_events",
        physical_name="ANALYTICS.MART.FACT_ORDER_EVENTS",
        description="Fact table capturing each order event with payment status for revenue analytics.",
        source_tables=[source_orders, source_payments],
        properties=[
            SchemaProperty(
                name="order_event_id", logical_type=LogicalType.STRING,
                description="Surrogate key for the order event.", required=True,
                primary_key=True, primary_key_position=1, unique=True,
                sources=[ColumnSource(
                    source_table="raw_orders", source_column="order_id",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                    transform_logic="MD5(CONCAT(order_id, '_', order_date))",
                    transform_description="Surrogate key from order_id + date.",
                )],
            ),
            SchemaProperty(
                name="order_id", logical_type=LogicalType.INTEGER,
                description="Natural order ID from source system.", required=True,
                sources=[ColumnSource(
                    source_table="raw_orders", source_column="order_id",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                )],
            ),
            SchemaProperty(
                name="customer_id", logical_type=LogicalType.INTEGER,
                description="FK to customer dimension.", required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="raw_orders", source_column="customer_id",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                )],
            ),
            SchemaProperty(
                name="order_date", logical_type=LogicalType.DATE,
                description="Date the order was placed.", required=True,
                sources=[ColumnSource(
                    source_table="raw_orders", source_column="order_date",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                )],
            ),
            SchemaProperty(
                name="order_amount", logical_type=LogicalType.NUMBER,
                description="Total order amount in USD.", required=True,
                critical_data_element=True,
                sources=[ColumnSource(
                    source_table="raw_orders", source_column="total_amount",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/ORDERS",
                )],
            ),
            SchemaProperty(
                name="payment_status", logical_type=LogicalType.STRING,
                description="Payment status (paid, pending, refunded).", required=True,
                examples=["paid", "pending", "refunded"],
                sources=[ColumnSource(
                    source_table="raw_payments", source_column="status",
                    source_table_qualified_name="default/snowflake/PROD_DB/RAW/PAYMENTS",
                    transform_logic="COALESCE(p.status, 'unknown')",
                    transform_description="Joined from payments table; defaults to 'unknown' if no payment record.",
                )],
            ),
        ],
    )

    contract = ODCSContract(
        name="Order Events Fact",
        version="0.2.0",
        status=ContractStatus.DRAFT,
        domain="Revenue Analytics",
        data_product="Revenue Intelligence",
        description_purpose="Provide a denormalized order events fact table for revenue reporting and forecasting.",
        description_limitations="Does not include subscription or recurring revenue events. Payment status may lag by up to 4 hours.",
        description_usage="Use for daily revenue dashboards, cohort analysis, and finance reconciliation.",
        tags=["orders", "revenue", "finance", "silver-tier"],
        schema_objects=[fact_orders],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL, description="order_event_id must be unique.",
                dimension="uniqueness", severity="critical", must_be="unique",
                method="field_health", column="fact_order_events.order_event_id",
                schedule="*/6 * * * *", engine="monte-carlo",
                business_impact="Duplicate order events inflate revenue metrics and break finance reconciliation.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL, description="order_amount must be >= 0.",
                dimension="validity", severity="critical", must_be_greater_than=0.0,
                method="field_health", column="fact_order_events.order_amount",
                schedule="*/6 * * * *", engine="monte-carlo",
                business_impact="Negative order amounts distort revenue dashboards and quarterly reporting.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL, description="No more than 5% null payment_status values.",
                dimension="completeness", severity="high",
                method="sql_rule", column="fact_order_events.payment_status",
                query="SELECT ROUND(100.0 * SUM(CASE WHEN payment_status IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_order_events",
                schedule="0 8 * * *", engine="monte-carlo",
                business_impact="Missing payment status causes incorrect revenue recognition and cash flow projections.",
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="6", unit="hours",
                description="Updated every 6 hours to support intra-day revenue reporting.",
                schedule="0 */6 * * *", scheduler="airflow",
                driver="operational", element="fact_order_events",
            ),
            SLAProperty(
                property="availability", value="99.9", unit="percent",
                description="Critical for finance reporting and month-end close.",
                driver="compliance", element="fact_order_events",
            ),
        ],
        team=[
            TeamMember(name="Eve Thompson", email="eve.thompson@acme.com", role="Data Owner"),
            TeamMember(name="Carol Wang", email="carol.wang@acme.com", role="Data Engineer"),
        ],
    )

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.REVIEW,
        request=ContractRequest(
            title="Order Events Fact Table",
            description="Need a fact table joining orders with payment status for the revenue analytics team.",
            business_context="Finance needs reliable daily revenue figures. Current ad-hoc queries are slow and inconsistent.",
            target_use_case="Revenue dashboards, finance reconciliation, cohort revenue analysis.",
            urgency=Urgency.CRITICAL,
            requester=EVE,
            domain="Revenue Analytics",
            data_product="Revenue Intelligence",
            created_at=_ts(days_ago=8),
        ),
        contract=contract,
        participants=[EVE, CAROL, BOB],
        comments=[
            Comment(author=EVE, content="Finance team needs this urgently for month-end close.", stage=DDLCStage.REQUEST, created_at=_ts(days_ago=8)),
            Comment(author=CAROL, content="Sources identified: raw_orders + raw_payments. Both in Snowflake RAW schema.", stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=7)),
            Comment(author=BOB, content="Schema looks good. Payment join logic is clean. Moving to review.", stage=DDLCStage.SPECIFICATION, created_at=_ts(days_ago=5)),
            Comment(author=EVE, content="Reviewing the quality rules and SLAs. The 6-hour freshness should work for daily reporting but let me confirm with the CFO.", stage=DDLCStage.REVIEW, created_at=_ts(days_ago=2)),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST, to_stage=DDLCStage.DISCOVERY, transitioned_by=CAROL, timestamp=_ts(days_ago=7)),
            StageTransition(from_stage=DDLCStage.DISCOVERY, to_stage=DDLCStage.SPECIFICATION, transitioned_by=CAROL, timestamp=_ts(days_ago=6)),
            StageTransition(from_stage=DDLCStage.SPECIFICATION, to_stage=DDLCStage.REVIEW, transitioned_by=BOB, reason="Spec complete, ready for stakeholder review.", timestamp=_ts(days_ago=3)),
        ],
        created_at=_ts(days_ago=8),
        updated_at=_ts(days_ago=1),
    )

    return session


# ===================================================================
# Session 3: ACTIVE stage — Product Catalog Dimension (completed)
# ===================================================================

def _build_product_catalog() -> DDLCSession:
    """A fully approved, active contract."""

    session_id = _id()

    source_products = SourceTable(
        name="raw_products",
        qualified_name="default/snowflake/PROD_DB/RAW/PRODUCTS",
        database_name="PROD_DB",
        schema_name="RAW",
        connector_name="snowflake",
        description="Product master data from the PIM system.",
    )

    dim_products = SchemaObject(
        name="dim_products",
        physical_name="ANALYTICS.MART.DIM_PRODUCTS",
        description="Product dimension with enriched category hierarchy and pricing.",
        source_tables=[source_products],
        properties=[
            SchemaProperty(name="product_id", logical_type=LogicalType.INTEGER, description="Product PK.", required=True, primary_key=True, primary_key_position=1, unique=True,
                           sources=[ColumnSource(source_table="raw_products", source_column="product_id", source_table_qualified_name="default/snowflake/PROD_DB/RAW/PRODUCTS")]),
            SchemaProperty(name="product_name", logical_type=LogicalType.STRING, description="Display name.", required=True, critical_data_element=True,
                           sources=[ColumnSource(source_table="raw_products", source_column="name", source_table_qualified_name="default/snowflake/PROD_DB/RAW/PRODUCTS")]),
            SchemaProperty(name="category", logical_type=LogicalType.STRING, description="Product category (L1).", required=True,
                           sources=[ColumnSource(source_table="raw_products", source_column="category", source_table_qualified_name="default/snowflake/PROD_DB/RAW/PRODUCTS")]),
            SchemaProperty(name="subcategory", logical_type=LogicalType.STRING, description="Product subcategory (L2).",
                           sources=[ColumnSource(source_table="raw_products", source_column="subcategory", source_table_qualified_name="default/snowflake/PROD_DB/RAW/PRODUCTS")]),
            SchemaProperty(name="price", logical_type=LogicalType.NUMBER, description="Current list price in USD.", required=True,
                           sources=[ColumnSource(source_table="raw_products", source_column="list_price", source_table_qualified_name="default/snowflake/PROD_DB/RAW/PRODUCTS", transform_logic="ROUND(list_price, 2)")]),
            SchemaProperty(name="is_active", logical_type=LogicalType.BOOLEAN, description="Whether the product is currently available.", required=True,
                           sources=[ColumnSource(source_table="raw_products", source_column="status", source_table_qualified_name="default/snowflake/PROD_DB/RAW/PRODUCTS", transform_logic="CASE WHEN status = 'active' THEN TRUE ELSE FALSE END")]),
        ],
    )

    contract = ODCSContract(
        name="Product Catalog Dimension",
        version="1.0.0",
        status=ContractStatus.ACTIVE,
        domain="Product",
        data_product="Product Analytics",
        description_purpose="Provide a clean, enriched product dimension for joining with fact tables.",
        tags=["product", "dimension", "gold-tier"],
        schema_objects=[dim_products],
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL, description="product_id must be unique.",
                dimension="uniqueness", severity="critical", must_be="unique",
                method="field_health", column="dim_products.product_id",
                schedule="0 6 * * *", engine="monte-carlo",
                business_impact="Duplicate product IDs cause incorrect joins in fact tables and inflate product counts.",
            ),
            QualityCheck(
                type=QualityCheckType.SQL, description="price must be > 0.",
                dimension="validity", severity="high", must_be_greater_than=0.0,
                method="field_health", column="dim_products.price",
                schedule="0 6 * * *", engine="monte-carlo",
                business_impact="Zero or negative prices corrupt revenue calculations when joined with order facts.",
            ),
        ],
        sla_properties=[
            SLAProperty(
                property="freshness", value="24", unit="hours",
                description="Daily refresh aligned with upstream PIM sync.",
                schedule="0 4 * * *", scheduler="airflow",
                driver="analytics", element="dim_products",
            ),
            SLAProperty(
                property="availability", value="99.5", unit="percent",
                description="Standard availability for dimension tables.",
                driver="operational", element="dim_products",
            ),
        ],
        team=[
            TeamMember(name="Bob Martinez", email="bob.martinez@acme.com", role="Data Owner"),
            TeamMember(name="Carol Wang", email="carol.wang@acme.com", role="Data Engineer"),
        ],
    )

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.ACTIVE,
        request=ContractRequest(
            title="Product Catalog Dimension",
            description="Need a product dimension table for the analytics mart.",
            business_context="Product analytics team needs a reliable product dimension to join with order facts.",
            target_use_case="Product performance dashboards, category analytics.",
            urgency=Urgency.MEDIUM,
            requester=BOB,
            domain="Product",
            data_product="Product Analytics",
            created_at=_ts(days_ago=14),
        ),
        contract=contract,
        participants=[BOB, CAROL],
        comments=[
            Comment(author=BOB, content="Straightforward product dimension from the PIM source.", stage=DDLCStage.REQUEST, created_at=_ts(days_ago=14)),
            Comment(author=CAROL, content="Source table identified. Simple 1:1 mapping with a few transforms.", stage=DDLCStage.DISCOVERY, created_at=_ts(days_ago=13)),
            Comment(author=BOB, content="Spec looks clean. Approving.", stage=DDLCStage.REVIEW, created_at=_ts(days_ago=10)),
            Comment(author=BOB, content="Approved and active. dbt model deployed.", stage=DDLCStage.APPROVAL, created_at=_ts(days_ago=9)),
        ],
        history=[
            StageTransition(from_stage=DDLCStage.REQUEST, to_stage=DDLCStage.DISCOVERY, transitioned_by=CAROL, timestamp=_ts(days_ago=13)),
            StageTransition(from_stage=DDLCStage.DISCOVERY, to_stage=DDLCStage.SPECIFICATION, transitioned_by=CAROL, timestamp=_ts(days_ago=12)),
            StageTransition(from_stage=DDLCStage.SPECIFICATION, to_stage=DDLCStage.REVIEW, transitioned_by=CAROL, timestamp=_ts(days_ago=11)),
            StageTransition(from_stage=DDLCStage.REVIEW, to_stage=DDLCStage.APPROVAL, transitioned_by=BOB, reason="All checks pass.", timestamp=_ts(days_ago=10)),
            StageTransition(from_stage=DDLCStage.APPROVAL, to_stage=DDLCStage.ACTIVE, transitioned_by=BOB, reason="Deployed to production.", timestamp=_ts(days_ago=9)),
        ],
        created_at=_ts(days_ago=14),
        updated_at=_ts(days_ago=9),
    )

    return session


# ===================================================================
# Session 4: REQUEST stage — fresh request (for demoing the early stages)
# ===================================================================

def _build_marketing_attribution() -> DDLCSession:
    """A brand new request — no progress yet."""

    session_id = _id()

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.REQUEST,
        request=ContractRequest(
            title="Marketing Attribution Model",
            description="We need a multi-touch attribution table that credits marketing channels for conversions. This will power the marketing ROI dashboard.",
            business_context="The CMO has asked for a reliable attribution model to allocate the $5M quarterly ad budget more effectively. Current last-touch attribution is misleading.",
            target_use_case="Multi-touch attribution reporting, marketing channel ROI analysis, budget allocation optimization.",
            urgency=Urgency.MEDIUM,
            requester=DAVE,
            domain="Marketing Analytics",
            data_product="Marketing Intelligence",
            desired_fields=["attribution_id", "customer_id", "conversion_date", "channel", "touchpoint_count", "attributed_revenue", "attribution_model"],
            created_at=_ts(days_ago=1),
        ),
        contract=ODCSContract(
            name="Marketing Attribution",
            domain="Marketing Analytics",
            status=ContractStatus.PROPOSED,
        ),
        participants=[DAVE],
        comments=[
            Comment(
                author=DAVE,
                content="Submitting this request on behalf of the marketing analytics team. We need multi-touch attribution to replace the current last-touch model. Happy to discuss requirements in discovery.",
                stage=DDLCStage.REQUEST,
                created_at=_ts(days_ago=1),
            ),
        ],
        history=[],
        created_at=_ts(days_ago=1),
        updated_at=_ts(days_ago=1),
    )

    return session


# ===================================================================
# Session 5: DISCOVERY stage — in-progress discovery
# ===================================================================

def _build_inventory_snapshot() -> DDLCSession:
    """A session in discovery — sources being identified."""

    session_id = _id()

    session = DDLCSession(
        id=session_id,
        current_stage=DDLCStage.DISCOVERY,
        request=ContractRequest(
            title="Daily Inventory Snapshot",
            description="Daily snapshot of inventory levels across all warehouses for supply chain optimization.",
            business_context="Supply chain team needs daily inventory visibility to optimize reorder points and prevent stockouts during peak season.",
            target_use_case="Inventory dashboards, stockout prediction, reorder point optimization.",
            urgency=Urgency.HIGH,
            requester=EVE,
            domain="Supply Chain",
            data_product="Supply Chain Analytics",
            desired_fields=["snapshot_date", "warehouse_id", "product_id", "quantity_on_hand", "quantity_reserved", "reorder_point"],
            created_at=_ts(days_ago=3),
        ),
        contract=ODCSContract(
            name="Daily Inventory Snapshot",
            domain="Supply Chain",
            status=ContractStatus.PROPOSED,
        ),
        participants=[EVE, CAROL],
        comments=[
            Comment(author=EVE, content="Peak season is in 6 weeks. We need inventory visibility ASAP.", stage=DDLCStage.REQUEST, created_at=_ts(days_ago=3)),
            Comment(
                author=CAROL,
                content="I've found two potential source tables in the ERP system: raw_inventory_levels (real-time) and raw_warehouse_master (reference data). Let me check data quality.",
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=2),
            ),
            Comment(
                author=EVE,
                content="We also need the reorder_point from the supply planning system. That's in a separate Postgres database — can we pull that in too?",
                stage=DDLCStage.DISCOVERY,
                created_at=_ts(days_ago=1, hours_ago=12),
            ),
        ],
        history=[
            StageTransition(
                from_stage=DDLCStage.REQUEST,
                to_stage=DDLCStage.DISCOVERY,
                transitioned_by=CAROL,
                reason="Starting source discovery.",
                timestamp=_ts(days_ago=2, hours_ago=6),
            ),
        ],
        created_at=_ts(days_ago=3),
        updated_at=_ts(days_ago=1),
    )

    return session


# ===================================================================
# Main seed function
# ===================================================================

async def seed_demo_data() -> list[str]:
    """
    Create all demo sessions and persist them to the store.

    Returns the list of created session IDs.
    """
    builders = [
        _build_customer_360,       # SPECIFICATION — richest demo
        _build_order_events,       # REVIEW
        _build_product_catalog,    # ACTIVE (completed)
        _build_marketing_attribution,  # REQUEST (fresh)
        _build_inventory_snapshot,     # DISCOVERY
    ]

    created_ids = []
    for builder in builders:
        session = builder()
        await store.save_session(session)
        created_ids.append(session.id)
        print(f"  [seed] Created: {session.request.title} ({session.current_stage.value})")

    return created_ids

"""
Standalone test for Atlan DQS rule creation.

Usage:
    ATLAN_BASE_URL=https://your-tenant.atlan.com \
    ATLAN_API_KEY=your_api_key \
    uv run python test_dqs_push.py --table-qn "default/snowflake/abc123/YOUR_DB/YOUR_SCHEMA/YOUR_TABLE"

The script creates a fake ACTIVE session pointed at a real table in your tenant
and exercises all four DQS rule types from the FACT_ORDERS demo:
  1. FRESHNESS     — column-level (looks for a date/timestamp column)
  2. NULL_COUNT    — column-level (looks for any column)
  3. CUSTOM_SQL    — table-level
  4. ROW_COUNT     — table-level

Pass --dry-run to print the creator calls without actually hitting Atlan.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from datetime import datetime, timezone


def parse_args():
    parser = argparse.ArgumentParser(description="Test DQS rule push against Atlan")
    parser.add_argument(
        "--table-qn",
        required=False,
        default=None,
        help="Fully-qualified name of an existing Atlan table, e.g. "
             "default/snowflake/conn-id/DB/SCHEMA/TABLE",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the calls without hitting Atlan",
    )
    parser.add_argument(
        "--column",
        default=None,
        help="Optional: column name for column-level rules (e.g. ORDER_DATE). "
             "If omitted, column-level rules fall back to table-level.",
    )
    parser.add_argument(
        "--list-templates",
        action="store_true",
        help="List available DQ rule template display names in the tenant, then exit.",
    )
    return parser.parse_args()


def make_test_session(table_qn: str, column: str | None):
    """Build a minimal ACTIVE DDLCSession pointing at a real table QN."""
    from app.ddlc.models import (
        DDLCSession, DDLCStage, ContractRequest, ODCSContract,
        QualityCheck, QualityCheckType, ContractStatus,
        Participant, Urgency,
    )

    req = ContractRequest(
        title="DQS Push Test",
        description="Automated test of DQS rule creation",
        requester=Participant(name="Test User", email="test@test.com"),
        urgency=Urgency.HIGH,
    )

    col_ref = f"TEST_TABLE.{column.upper()}" if column else None

    contract = ODCSContract(
        id=str(uuid.uuid4()),
        name="DQS_PUSH_TEST",
        status=ContractStatus.ACTIVE,
        atlan_table_qualified_name=table_qn,
        quality_checks=[
            QualityCheck(
                type=QualityCheckType.SQL,
                description="FRESHNESS: Date column must not be older than 1 day.",
                dimension="timeliness",
                engine="atlan-dqs",
                dqs_rule_type="FRESHNESS",
                dqs_threshold_value=1.0,
                dqs_threshold_unit="DAYS",
                dqs_alert_priority="URGENT",
                column=col_ref,  # falls back to table-level if None
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="NULL_COUNT: Primary key column must have zero nulls.",
                dimension="completeness",
                engine="atlan-dqs",
                dqs_rule_type="NULL_COUNT",
                dqs_threshold_value=0.0,
                dqs_alert_priority="URGENT",
                column=col_ref,  # falls back to table-level if None
            ),
            QualityCheck(
                type=QualityCheckType.SQL,
                description="CUSTOM_SQL: No rows should violate the business rule.",
                dimension="validity",
                engine="atlan-dqs",
                dqs_rule_type="CUSTOM_SQL",
                dqs_custom_sql="SELECT 0",  # safe no-op SQL
                dqs_threshold_value=0.0,
                dqs_alert_priority="NORMAL",
            ),
            QualityCheck(
                type=QualityCheckType.TEXT,
                description="ROW_COUNT: Table must contain at least 1 row.",
                dimension="volume",
                engine="atlan-dqs",
                dqs_rule_type="ROW_COUNT",
                dqs_threshold_value=1.0,
                dqs_alert_priority="NORMAL",
            ),
        ],
    )

    now = datetime.now(tz=timezone.utc)
    session = DDLCSession(
        id=str(uuid.uuid4()),
        current_stage=DDLCStage.ACTIVE,
        request=req,
        contract=contract,
        created_at=now,
        updated_at=now,
    )
    return session


def dry_run_preview(session):
    """Print what would be created without calling Atlan."""
    print("\n[DRY RUN] Would create the following DQS rules:")
    print(f"  Table QN: {session.contract.atlan_table_qualified_name}\n")
    for i, q in enumerate(session.contract.quality_checks, 1):
        if q.engine != "atlan-dqs":
            continue
        level = "column-level" if q.column else "table-level"
        print(f"  Rule {i}: {q.dqs_rule_type} ({level})")
        print(f"    description   : {q.description}")
        print(f"    column        : {q.column or '(none — table-level)'}")
        print(f"    threshold     : {q.dqs_threshold_value} {q.dqs_threshold_unit or ''}")
        print(f"    alert_priority: {q.dqs_alert_priority}")
        if q.dqs_custom_sql:
            print(f"    custom_sql    : {q.dqs_custom_sql}")
        print()


def list_dq_templates():
    """Connect to the tenant and print all available DQ rule template display names."""
    from pyatlan.client.atlan import AtlanClient

    base_url = os.getenv("ATLAN_BASE_URL")
    api_key = os.getenv("ATLAN_API_KEY")
    client = AtlanClient(base_url=base_url, api_key=api_key)

    print(f"\nFetching DQ rule templates from {base_url} ...\n")
    try:
        client.dq_template_config_cache._refresh_cache()
        templates = client.dq_template_config_cache._cache
        if not templates:
            print("  (no DQ rule templates found — DQS may not be enabled on this tenant)")
        else:
            print(f"  Found {len(templates)} template(s):\n")
            for display_name, config in sorted(templates.items()):
                qn = config.get("qualified_name", "")
                dim = config.get("dimension", "")
                print(f"  display_name = {display_name!r}")
                print(f"    qualified_name = {qn}")
                print(f"    dimension      = {dim}")
                print()
    except Exception as exc:
        print(f"  ERROR: {exc}")


def main():
    args = parse_args()

    base_url = os.getenv("ATLAN_BASE_URL")
    api_key = os.getenv("ATLAN_API_KEY")
    if not base_url or not api_key:
        print("ERROR: ATLAN_BASE_URL and ATLAN_API_KEY must be set.", file=sys.stderr)
        sys.exit(1)

    if args.list_templates:
        list_dq_templates()
        return

    if not args.table_qn:
        print("ERROR: --table-qn is required (unless using --list-templates).", file=sys.stderr)
        sys.exit(1)

    print(f"Tenant : {base_url}")
    print(f"Table  : {args.table_qn}")
    print(f"Column : {args.column or '(none — column-level rules fall back to table-level)'}")

    session = make_test_session(args.table_qn, args.column)

    if args.dry_run:
        dry_run_preview(session)
        return

    print("\nPushing DQS rules to Atlan...\n")
    from app.ddlc import atlan_assets
    result = atlan_assets.push_dq_rules(session)

    print(f"\n{'='*50}")
    print(f"  Pushed  : {result['pushed']}")
    print(f"  Skipped : {result['skipped']}")
    print(f"  Errors  : {len(result['errors'])}")
    if result["errors"]:
        print("\nErrors:")
        for e in result["errors"]:
            print(f"  ✗ {e}")
    if result["pushed"] > 0:
        print("\nPushed rules:")
        for q in session.contract.quality_checks:
            if q.dqs_pushed:
                print(f"  ✓ {q.dqs_rule_type} — pushed=True  qn={q.dqs_rule_qualified_name}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()

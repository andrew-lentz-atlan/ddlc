"""
DDLCActivities — Temporal activities for the DDLC approval workflow.

These are thin wrappers around existing DDLC business logic in:
  - app.ddlc.atlan_assets  (Atlan catalog integration)
  - app.ddlc.dbt_generator (dbt project generation)
  - app.ddlc.store         (session state store)

Each activity is independently retryable and observable in Temporal UI.
"""

from __future__ import annotations

from typing import Any, Dict

from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.logger_adaptor import get_logger
from temporalio import activity

logger = get_logger(__name__)
activity.logger = logger


class DDLCActivities(ActivitiesInterface):
    """
    Activities for DDLCApprovalWorkflow.

    Activity 1 — create_atlan_placeholder:
        Creates a placeholder Table asset + columns in the Atlan catalog.
        Returns the qualified name of the created asset.
        No-ops (returns "") if ATLAN_BASE_URL / ATLAN_API_KEY are not set.

    Activity 2 — generate_dbt_artifact:
        Generates a dbt project ZIP from the approved contract.
        Returns {"size_bytes": N}.
        TODO v2: persist to Dapr object store for post-restart download.

    Activity 3 — finalize_activation:
        Marks the session as ACTIVE in the store and records the Atlan
        qualified name on the contract. This is the "commit point" —
        activities 1 and 2 are idempotent before this step.
    """

    @activity.defn(name="create_atlan_placeholder")
    async def create_atlan_placeholder(self, session_id: str) -> str:
        """
        Create placeholder Table + columns in Atlan catalog.

        Returns:
            str: Atlan qualified name of the created Table, or "" if not configured.
        """
        from app.ddlc import store
        from app.ddlc import atlan_assets

        logger.info(f"[create_atlan_placeholder] session={session_id}")

        session = await store.get_session(session_id)
        if session is None:
            raise ValueError(f"Session {session_id} not found in store")

        if not atlan_assets.is_configured():
            logger.info("Atlan not configured — skipping asset creation")
            return ""

        try:
            result = atlan_assets.register_placeholder_table(session)
            qn: str = result.get("qualified_name", "")
            logger.info(f"Atlan asset created: {qn}")
            return qn
        except Exception as exc:
            logger.error(f"Atlan asset creation failed: {exc}")
            raise  # Let Temporal retry

    @activity.defn(name="generate_dbt_artifact")
    async def generate_dbt_artifact(self, session_id: str) -> Dict[str, Any]:
        """
        Generate a dbt project ZIP for the approved contract.

        Returns:
            dict: {"size_bytes": N}
        """
        from app.ddlc import store
        from app.ddlc import dbt_generator

        logger.info(f"[generate_dbt_artifact] session={session_id}")

        session = await store.get_session(session_id)
        if session is None:
            raise ValueError(f"Session {session_id} not found in store")

        zip_bytes = dbt_generator.generate_dbt_zip(session.contract)
        size = len(zip_bytes)
        logger.info(f"dbt ZIP generated: {size} bytes")

        # TODO v2: persist zip_bytes to Dapr object store
        # key = f"ddlc-dbt-{session_id}.zip"
        # with DaprClient() as dapr:
        #     dapr.save_state("objectstore", key, zip_bytes)

        return {"size_bytes": size}

    @activity.defn(name="finalize_activation")
    async def finalize_activation(self, config: Dict[str, Any]) -> None:
        """
        Mark session as ACTIVE and record Atlan qualified name.

        This is the commit point — runs last after idempotent activities.
        """
        from app.ddlc import store
        from app.ddlc.models import DDLCStage, StageTransition, Participant

        session_id: str = config["session_id"]
        atlan_qn: str = config.get("atlan_qn", "")

        logger.info(f"[finalize_activation] session={session_id} atlan_qn={atlan_qn}")

        session = await store.get_session(session_id)
        if session is None:
            raise ValueError(f"Session {session_id} not found in store")

        # Record the stage transition
        prev_stage = session.current_stage
        session.current_stage = DDLCStage.ACTIVE
        session.history.append(
            StageTransition(
                from_stage=prev_stage,
                to_stage=DDLCStage.ACTIVE,
                transitioned_by=Participant(
                    name="DDLC Approval Workflow",
                    email="ddlc-workflow@atlan.com",
                ),
                reason="Activated via Temporal DDLCApprovalWorkflow",
            )
        )

        # Store Atlan qualified name if we got one
        if atlan_qn:
            session.contract.atlan_table_qualified_name = atlan_qn

            # Also resolve GUID + URL so "View in Atlan" button appears
            try:
                import asyncio
                import os
                from app.ddlc import atlan_assets
                if atlan_assets.is_configured():
                    from pyatlan.model.assets import Table as _Table
                    client = atlan_assets._get_client()
                    existing = await asyncio.to_thread(
                        client.asset.get_by_qualified_name,
                        qualified_name=atlan_qn,
                        asset_type=_Table,
                    )
                    if existing and existing.guid:
                        base = os.getenv("ATLAN_BASE_URL", "").rstrip("/")
                        session.contract.atlan_table_guid = str(existing.guid)
                        session.contract.atlan_table_url = f"{base}/assets/{existing.guid}/overview"
            except Exception as exc:
                logger.debug(f"Could not resolve Atlan GUID/URL: {exc}")

        await store.save_session(session)
        logger.info(f"Session {session_id} is now ACTIVE")

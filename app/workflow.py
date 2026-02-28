"""
DDLCApprovalWorkflow — Temporal workflow for DDLC contract approval.

Triggered when a DDLC session advances from APPROVAL → ACTIVE stage.
Runs three activities in sequence:
  1. create_atlan_placeholder — create Table + columns in Atlan catalog
  2. generate_dbt_artifact    — generate dbt ZIP (logged; v2 will persist to object store)
  3. finalize_activation      — mark session ACTIVE + store Atlan qualified name

Benefits over the old synchronous handler:
  - Durable: survives pod restarts, Atlan API flakiness, transient network errors
  - Observable: each step visible in Temporal UI with status, timing, retry counts
  - Retryable: 6 attempts with exponential backoff on every activity
  - Decoupled: approval endpoint returns immediately, workflow runs in background
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable, Dict, Sequence

from application_sdk.activities import ActivitiesInterface
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.workflows import WorkflowInterface
from temporalio import workflow
from temporalio.common import RetryPolicy

logger = get_logger(__name__)
workflow.logger = logger

_RETRY_POLICY = RetryPolicy(
    maximum_attempts=6,
    backoff_coefficient=2.0,
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=60),
)

_ACTIVITY_TIMEOUT = timedelta(seconds=120)


@workflow.defn
class DDLCApprovalWorkflow(WorkflowInterface):
    """
    Durable workflow triggered when a DDLC contract advances from APPROVAL → ACTIVE.

    Input (workflow_config dict):
        session_id (str): The DDLC session UUID to activate.

    Output (dict):
        session_id (str): The session that was activated.
        atlan_qualified_name (str): Qualified name of the created Atlan Table asset.
                                    Empty string if Atlan is not configured.
    """

    @workflow.run
    async def run(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        # The SDK's start_workflow only passes {"workflow_id": workflow_id} to Temporal
        # (it skips StateStore save when workflow_id is pre-supplied).
        # Extract session_id from workflow_id since it's encoded as "ddlc-approval-{session_id}".
        workflow_id = workflow_config.get("workflow_id", "")
        session_id: str = workflow_config.get("session_id", "")
        if not session_id and workflow_id.startswith("ddlc-approval-"):
            session_id = workflow_id.removeprefix("ddlc-approval-")
        logger.info(f"DDLCApprovalWorkflow starting for session {session_id}")

        # Activity 1: Create placeholder Table + columns in Atlan
        atlan_qn: str = await workflow.execute_activity(
            "create_atlan_placeholder",
            session_id,
            retry_policy=_RETRY_POLICY,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
        )
        logger.info(f"Atlan placeholder created: {atlan_qn or '(not configured)'}")

        # Activity 2: Generate dbt artifact (logged; v2 will persist to object store)
        dbt_result: Dict[str, Any] = await workflow.execute_activity(
            "generate_dbt_artifact",
            session_id,
            retry_policy=_RETRY_POLICY,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
        )
        logger.info(f"dbt artifact generated: {dbt_result.get('size_bytes', 0)} bytes")

        # Activity 3: Mark session ACTIVE + store atlan_qn
        await workflow.execute_activity(
            "finalize_activation",
            {"session_id": session_id, "atlan_qn": atlan_qn},
            retry_policy=_RETRY_POLICY,
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
        )
        logger.info(f"Session {session_id} finalized as ACTIVE")

        return {"session_id": session_id, "atlan_qualified_name": atlan_qn}

    @staticmethod
    def get_activities(activities: ActivitiesInterface) -> Sequence[Callable[..., Any]]:
        """Return activity methods for worker registration."""
        from app.activities import DDLCActivities

        if not isinstance(activities, DDLCActivities):
            raise TypeError(f"Expected DDLCActivities, got {type(activities)}")

        return [
            activities.create_atlan_placeholder,
            activities.generate_dbt_artifact,
            activities.finalize_activation,
        ]

"""
DDLC Workflow Client — triggers DDLCApprovalWorkflow via Temporal.

Called from app/ddlc/server.py when a session advances APPROVAL → ACTIVE.
Imported lazily so server.py degrades gracefully when Temporal is not running.
"""

from __future__ import annotations

import uuid

from application_sdk.observability.logger_adaptor import get_logger

logger = get_logger(__name__)

# Temporal task queue — must match the worker's task queue (set by the SDK)
_TASK_QUEUE = "ddlc-task-queue"


async def trigger_approval_workflow(session_id: str) -> dict:
    """
    Start DDLCApprovalWorkflow for the given session.

    Uses the SDK's TemporalWorkflowClient, which:
      - Generates a deterministic workflow_id from session_id
      - Stores workflow config in Dapr state store
      - Starts the workflow on the Temporal server

    Args:
        session_id: DDLC session UUID being activated.

    Returns:
        dict with keys: workflow_id, run_id
    """
    from app.workflow import DDLCApprovalWorkflow
    from application_sdk.application import get_workflow_client

    workflow_id = f"ddlc-approval-{session_id}"

    client = get_workflow_client(application_name="ddlc")
    await client.load()

    try:
        result = await client.start_workflow(
            workflow_args={
                "workflow_id": workflow_id,
                "session_id": session_id,
            },
            workflow_class=DDLCApprovalWorkflow,
        )
        logger.info(
            f"DDLCApprovalWorkflow started: "
            f"workflow_id={result.get('workflow_id')} "
            f"run_id={result.get('run_id')}"
        )
        return result
    finally:
        await client.close()

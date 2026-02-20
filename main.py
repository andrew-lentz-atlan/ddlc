"""
DDLC — Atlan Application SDK entrypoint.

Runs the Data Contract Lifecycle (DDLC) app using the Atlan Application SDK.
The SDK provides:
  - Temporal worker for durable workflow execution (DDLCApprovalWorkflow)
  - FastAPI server with observability, auth, and workflow management endpoints
  - Dapr integration for distributed state/pubsub

APPLICATION_MODE controls what starts:
  LOCAL  (default) — worker (daemon) + server, single process for local dev
  WORKER            — worker only (for split-pod production deployment)
  SERVER            — server only (for split-pod production deployment)
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, List

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from app.activities import DDLCActivities
from app.workflow import DDLCApprovalWorkflow
from application_sdk.application import BaseApplication
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.server.fastapi import APIServer

logger = get_logger(__name__)

APPLICATION_NAME = "ddlc"

# Resolve paths relative to this file so it works regardless of CWD
_HERE = Path(__file__).parent
_FRONTEND_DIR = _HERE / "app" / "ddlc" / "frontend"
_STATIC_DIR = _FRONTEND_DIR / "static"


# ---------------------------------------------------------------------------
# Custom server — extends SDK APIServer with DDLC frontend + REST API
# ---------------------------------------------------------------------------


class DDLCServer(APIServer):
    """
    Extends the SDK's APIServer with:
      1. DDLC's full REST API (50+ endpoints via APIRouter from server.py)
      2. DDLC's static frontend (HTML pages + CSS/JS)

    All SDK infrastructure (observability, workflow triggers, dapr pubsub,
    JWT auth) is inherited unchanged from APIServer.
    """

    def register_routers(self) -> None:
        # 1. Include DDLC REST API router (all /api/* endpoints)
        from app.ddlc.server import router as ddlc_router

        self.app.include_router(ddlc_router)

        # 2. Let the parent register SDK routers
        #    (/workflows/v1, /dapr, /events/v1, /observability, etc.)
        super().register_routers()

    def register_ui_routes(self) -> None:
        """Serve DDLC HTML pages + static assets instead of SDK default UI."""

        # Mount /static → app/ddlc/frontend/static/
        self.app.mount(
            "/static",
            StaticFiles(directory=str(_STATIC_DIR)),
            name="ddlc_static",
        )

        # HTML page routes — serve raw HTML (no Jinja templating needed)
        @self.app.get("/", response_class=HTMLResponse)
        async def dashboard() -> HTMLResponse:
            return HTMLResponse((_FRONTEND_DIR / "index.html").read_text())

        @self.app.get("/request", response_class=HTMLResponse)
        async def request_page() -> HTMLResponse:
            return HTMLResponse((_FRONTEND_DIR / "request.html").read_text())

        @self.app.get("/contract/{session_id}", response_class=HTMLResponse)
        async def contract_page(session_id: str) -> HTMLResponse:
            return HTMLResponse((_FRONTEND_DIR / "contract.html").read_text())


# ---------------------------------------------------------------------------
# Application subclass — wires DDLCServer into BaseApplication lifecycle
# ---------------------------------------------------------------------------


class DDLCApplication(BaseApplication):
    """
    Thin subclass that substitutes DDLCServer for the default APIServer
    when BaseApplication._setup_server() runs.
    """

    async def _setup_server(
        self,
        workflow_class: Any,
        ui_enabled: bool = True,
        has_configmap: bool = False,
    ) -> None:
        # Seed demo data as part of server startup
        from app.ddlc.demo_seed import seed_demo_data

        logger.info("Seeding DDLC demo data...")
        ids = await seed_demo_data()
        logger.info(f"Seeded {len(ids)} demo sessions.")

        # Use DDLCServer instead of the default APIServer
        self.server = DDLCServer(
            workflow_client=self.workflow_client,
            ui_enabled=ui_enabled,
            has_configmap=has_configmap,
            handler=self.handler_class(client=self.client_class()),
        )

        # Register the DDLC approval workflow's HTTP trigger
        # (makes POST /workflows/v1/start available for programmatic starts)
        from application_sdk.server.fastapi import HttpWorkflowTrigger

        self.server.register_workflow(
            workflow_class=workflow_class,
            triggers=[HttpWorkflowTrigger()],
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def main() -> None:
    logger.info("Starting DDLC application")

    app = DDLCApplication(name=APPLICATION_NAME)

    # Register DDLCApprovalWorkflow + DDLCActivities with the Temporal worker
    await app.setup_workflow(
        workflow_and_activities_classes=[(DDLCApprovalWorkflow, DDLCActivities)],
    )

    # Start: worker (daemon in LOCAL mode) + server, per APPLICATION_MODE
    await app.start(workflow_class=DDLCApprovalWorkflow)


if __name__ == "__main__":
    asyncio.run(main())

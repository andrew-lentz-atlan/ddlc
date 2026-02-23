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
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
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

# Origins allowed to embed DDLC in an iframe.
# Covers the Atlan frontend dev server (Vite default port) and common
# alternatives.  In production this would be locked to the tenant origin.
_ALLOWED_ORIGINS = [
    "http://localhost:3333",  # Atlan frontend (actual dev port)
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:3333",
    "http://127.0.0.1:5173",
]


# ---------------------------------------------------------------------------
# Custom server — extends SDK APIServer with DDLC frontend + REST API
# ---------------------------------------------------------------------------


class DDLCServer(APIServer):
    """
    Extends the SDK's APIServer with:
      1. DDLC's full REST API (50+ endpoints via APIRouter from server.py)
      2. DDLC's static frontend (HTML pages + CSS/JS)
      3. CORS + iframe embedding for the Atlan frontend dev server

    All SDK infrastructure (observability, workflow triggers, dapr pubsub,
    JWT auth) is inherited unchanged from APIServer.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # Allow the Atlan frontend (localhost:5173) to make cross-origin
        # requests and embed DDLC in an iframe.
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=_ALLOWED_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Remove X-Frame-Options restriction so browsers allow iframe embedding.
        # Replace with a CSP frame-ancestors directive that restricts to our
        # known origins only.
        @self.app.middleware("http")
        async def iframe_headers(request: Request, call_next) -> Response:
            response = await call_next(request)
            # MutableHeaders uses del not pop — guard in case header absent
            if "x-frame-options" in response.headers:
                del response.headers["x-frame-options"]
            csp_origins = " ".join(_ALLOWED_ORIGINS)
            response.headers["Content-Security-Policy"] = (
                f"frame-ancestors 'self' {csp_origins}"
            )
            # Disable caching for all responses in dev so JS/CSS changes
            # are always picked up immediately without version bumps
            response.headers["Cache-Control"] = "no-store"
            return response

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

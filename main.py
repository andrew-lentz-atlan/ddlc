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
import os
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
#
# In a native deploy the app is served FROM the tenant origin
# (https://<tenant>.atlan.com/apps/ddlc), so the Atlan frontend embedding it is
# same-origin — `frame-ancestors 'self'` (set in the middleware below) already
# covers that case. The list here is the dev cross-origin allowlist (Atlan
# frontend dev servers). Any additional production origins can be injected via
# ATLAN_APP_ALLOWED_ORIGINS (comma-separated) without a code change.
_DEV_ORIGINS = [
    "http://localhost:3333",  # Atlan frontend (actual dev port)
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:3333",
    "http://127.0.0.1:5173",
]
_EXTRA_ORIGINS = [
    o.strip()
    for o in os.getenv("ATLAN_APP_ALLOWED_ORIGINS", "").split(",")
    if o.strip()
]
_ALLOWED_ORIGINS = _DEV_ORIGINS + _EXTRA_ORIGINS


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

    async def _start_worker(self, daemon: bool = True) -> None:
        """Skip worker startup gracefully when Temporal is not available."""
        if self.worker is None:
            logger.warning("Skipping worker start — Temporal not available.")
            return
        await super()._start_worker(daemon=daemon)

    async def _setup_server(
        self,
        workflow_class: Any,
        ui_enabled: bool = True,
        has_configmap: bool = False,
    ) -> None:
        # Demo seeding is gated by DDLC_SEED_DEMO (default "true" for the POV/demo
        # build). A real production deployment sets it "false" (see atlan.yaml) —
        # the store starts empty and users create their own contracts.
        #
        # When enabled, we seed only when the store is empty so durable user
        # sessions survive pod restarts / KEDA scale-to-zero. The manual
        # /api/demo/seed endpoint still force-reseeds on demand regardless.
        from app.ddlc import store

        seed_enabled = os.getenv("DDLC_SEED_DEMO", "true").lower() in (
            "1",
            "true",
            "yes",
        )
        if not seed_enabled:
            logger.info(
                "DDLC_SEED_DEMO disabled — starting with an empty store (production mode)."
            )
        else:
            existing = await store.list_sessions()
            if existing:
                logger.info(
                    f"Found {len(existing)} persisted DDLC sessions — skipping demo seed."
                )
            else:
                # demo_seed.py is gitignored (contains PII) and absent from a
                # fresh clone. Import lazily so production (seeding off / empty
                # store) never hard-depends on it.
                try:
                    from app.ddlc.demo_seed import seed_demo_data
                except ImportError:
                    logger.info(
                        "demo_seed module not present (gitignored) — skipping seed."
                    )
                else:
                    logger.info("Empty store — seeding DDLC demo data...")
                    ids = await seed_demo_data()
                    logger.info(f"Seeded {len(ids)} demo sessions.")

        # Bootstrap Atlan TypeDefs for Context Nuggets — fire-and-forget so startup
        # is never blocked by a slow Atlan API call.
        async def _run_bootstrap():
            try:
                from app.ddlc import atlan_assets
                import asyncio as _asyncio
                await _asyncio.to_thread(atlan_assets.bootstrap_nugget_typedef)
            except Exception as _exc:
                logger.warning(f"Context Nugget typedef bootstrap skipped: {_exc}")
        asyncio.create_task(_run_bootstrap())

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

    # Attempt to register the Temporal worker.  Temporal is only required for
    # the approval-workflow trigger — all other DDLC features (contract
    # lifecycle, DQS push, REST API, UI) work without it.  If Temporal isn't
    # running we log a warning and continue so the server still starts.
    try:
        await app.setup_workflow(
            workflow_and_activities_classes=[(DDLCApprovalWorkflow, DDLCActivities)],
        )
    except Exception as exc:
        logger.warning(
            f"Temporal not available — approval workflow disabled. "
            f"Run 'poe start-deps' to enable it. ({exc})"
        )

    # Start: worker (daemon in LOCAL mode) + server, per APPLICATION_MODE
    await app.start(workflow_class=DDLCApprovalWorkflow)


if __name__ == "__main__":
    asyncio.run(main())

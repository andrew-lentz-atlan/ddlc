"""
Access App — Atlan Application SDK entrypoint.

Runs the ServiceNow access request app using the Atlan Application SDK.
The SDK provides:
  - FastAPI server with observability, auth, and static file serving
  - Deployment via Global Marketplace Admin to any Atlan tenant

No Temporal workflows or Dapr components needed — this is a frontend +
REST API app that proxies ServiceNow's Service Catalog API.

Local dev:
  cd hello_world && uv run python access_main.py

Production:
  Deployed as a container via Atlan App Framework.
  ATLAN_APP_HTTP_PORT controls the listen port (default 8000).
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

# Set application name before importing SDK constants
os.environ.setdefault("ATLAN_APPLICATION_NAME", "atlan-access-app")

from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles

from application_sdk.application import BaseApplication
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.server.fastapi import APIServer

logger = get_logger(__name__)

APPLICATION_NAME = "atlan-access-app"

_HERE = Path(__file__).parent
_ACCESS_DIR = _HERE / "app" / "access"
_FRONTEND_DIR = _ACCESS_DIR / "frontend"
_TEMPLATES_DIR = _FRONTEND_DIR / "templates"
_STATIC_DIR = _FRONTEND_DIR / "static"

# Origins allowed to embed this app in an iframe.
# In production, this would be locked to the tenant origin.
_ALLOWED_ORIGINS = [
    "http://localhost:3333",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:3333",
    "http://127.0.0.1:5173",
]


# ---------------------------------------------------------------------------
# AccessServer — extends SDK APIServer with Access frontend + REST API
# ---------------------------------------------------------------------------


class AccessServer(APIServer):
    """
    Extends the SDK's APIServer with:
      1. Access REST API routes (ServiceNow proxy endpoints)
      2. Access static frontend (HTML + CSS/JS)
      3. CORS + iframe embedding headers
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=_ALLOWED_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        @self.app.middleware("http")
        async def iframe_headers(request: Request, call_next) -> Response:
            response = await call_next(request)
            if "x-frame-options" in response.headers:
                del response.headers["x-frame-options"]
            csp_origins = " ".join(_ALLOWED_ORIGINS)
            response.headers["Content-Security-Policy"] = (
                f"frame-ancestors 'self' {csp_origins}"
            )
            response.headers["Cache-Control"] = "no-store"
            return response

    def register_routers(self) -> None:
        from app.access.server import router as access_router

        self.app.include_router(access_router)
        super().register_routers()

    def register_ui_routes(self) -> None:
        """Serve Access HTML pages + static assets instead of SDK default UI."""

        self.app.mount(
            "/static",
            StaticFiles(directory=str(_STATIC_DIR)),
            name="access_static",
        )

        @self.app.get("/", response_class=HTMLResponse)
        async def index() -> HTMLResponse:
            return HTMLResponse((_TEMPLATES_DIR / "index.html").read_text())

        @self.app.get("/asset-context", response_class=HTMLResponse)
        async def asset_context() -> HTMLResponse:
            return HTMLResponse((_TEMPLATES_DIR / "index.html").read_text())


# ---------------------------------------------------------------------------
# AccessApplication — wires AccessServer into BaseApplication lifecycle
# ---------------------------------------------------------------------------


class AccessApplication(BaseApplication):
    """
    Frontend-only app — no Temporal workflows or Dapr components.
    Overrides _setup_server() to use AccessServer.
    """

    async def _setup_server(
        self,
        workflow_class: Any = None,
        ui_enabled: bool = True,
        has_configmap: bool = False,
    ) -> None:
        self.server = AccessServer(
            workflow_client=self.workflow_client,
            ui_enabled=ui_enabled,
            has_configmap=has_configmap,
        )

    async def _start_worker(self, daemon: bool = True) -> None:
        """No Temporal worker needed — skip gracefully."""
        logger.info("Access app has no Temporal workflows — skipping worker startup.")


# ---------------------------------------------------------------------------
# Dev proxy for /api/meta/* (local dev only)
# ---------------------------------------------------------------------------


def register_dev_proxy(fastapi_app):
    """Forward /api/meta/* to a real Atlan instance for local development.

    Activated by: DEV_MODE=true ATLAN_BASE_URL=https://tenant.atlan.com
    """
    import httpx
    from fastapi.responses import StreamingResponse

    atlan_base_url = os.environ["ATLAN_BASE_URL"].rstrip("/")
    logger.info(f"[DEV PROXY] /api/meta/* -> {atlan_base_url}")

    @fastapi_app.api_route(
        "/api/meta/{path:path}", methods=["GET", "POST", "PUT", "DELETE"]
    )
    async def proxy_meta_api(request: Request, path: str):
        target_url = f"{atlan_base_url}/api/meta/{path}"
        if request.url.query:
            target_url += f"?{request.url.query}"

        headers = {}
        if "authorization" in request.headers:
            headers["Authorization"] = request.headers["authorization"]
        headers["Accept"] = request.headers.get("accept", "application/json")

        async with httpx.AsyncClient(timeout=30.0) as client:
            upstream = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=(
                    await request.body()
                    if request.method in ("POST", "PUT")
                    else None
                ),
            )

        return StreamingResponse(
            iter([upstream.content]),
            status_code=upstream.status_code,
            headers={
                "content-type": upstream.headers.get(
                    "content-type", "application/json"
                ),
            },
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def main() -> None:
    logger.info("Starting Access app (ServiceNow integration)")

    app = AccessApplication(name=APPLICATION_NAME)

    # No Temporal workflows — just start the server
    # The SDK's start() method handles APPLICATION_MODE (LOCAL/SERVER/WORKER)
    # but we don't register any workflows, so only the server portion runs.

    # Set up server (creates AccessServer)
    await app._setup_server()

    # Register dev proxy if in dev mode
    dev_mode = os.environ.get("DEV_MODE", "").lower() == "true"
    if dev_mode and os.environ.get("ATLAN_BASE_URL"):
        register_dev_proxy(app.server.app)

    # Start the server
    port = int(os.environ.get("ATLAN_APP_HTTP_PORT", "8005"))
    logger.info(f"Access app listening on port {port}")
    await app._start_server()


if __name__ == "__main__":
    asyncio.run(main())

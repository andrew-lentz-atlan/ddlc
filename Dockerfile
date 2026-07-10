# syntax=docker/dockerfile:1
# DDLC — Data Studio — Atlan v3 native app container.
#
# Extends the Atlan app-runtime base image (Chainguard-hardened, Python 3.13,
# ships uv + non-root `appuser`) with the app code and locked deps. This base
# clears the recurring Debian-base CVE waves that python:3.x-slim hits.
#
# Dapr + Temporal in production:
#   - Dapr is injected as a sidecar by the platform (this base does NOT ship a
#     Dapr runtime; standard native tenants get it via K8s sidecar injection).
#   - Temporal is reached via TEMPORAL__HOST_URL, injected by Helm.
#   - APPLICATION_MODE (SERVER | WORKER) is injected per pod under split
#     deployment; main.py honours it via app.start().
#
# Build:   docker build -t ddlc:latest .
# Deploy:  pushed by CI; the platform schedules it from atlan.yaml.

FROM registry.atlan.com/public/app-runtime-base:3

WORKDIR /app

# Install locked dependencies first so the layer caches across code edits.
COPY --chown=appuser:appuser pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/home/appuser/.cache/uv,uid=1000,gid=1000 \
    uv venv .venv && \
    uv sync --locked --no-install-project --no-dev

# Application code (includes app/ddlc/frontend served at "/").
COPY --chown=appuser:appuser main.py ./
COPY --chown=appuser:appuser app/ ./app/
COPY --chown=appuser:appuser components/ ./components/

# SDK env the platform relies on (documented for clarity; Helm may override):
#   TEMPORAL__HOST_URL           — Temporal server (injected by Helm)
#   DAPR_HTTP_PORT               — Dapr sidecar HTTP port (injected by Dapr)
#   APPLICATION_MODE             — SERVER | WORKER (injected per pod)
#   DEPLOYMENT_OBJECT_STORE_NAME — release-prefixed object store binding (atlan.yaml)
ENV ATLAN_CONTRACT_GENERATED_DIR=/app/app/generated

USER appuser
EXPOSE 8000

# Health check — SDK exposes /observability
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/observability || exit 1

# Explicit entrypoint so DDLC's custom startup (demo seed, typedef bootstrap,
# register_workflow, graceful no-Temporal degradation) runs. Overrides the
# base image's ATLAN_APP_MODULE entrypoint on purpose.
ENTRYPOINT ["uv", "run", "python", "main.py"]

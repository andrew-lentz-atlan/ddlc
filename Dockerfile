# DDLC — Data Contract Lifecycle App
# Based on the Atlan Application SDK pre-built image.
#
# Build:
#   docker build -t ddlc:latest .
#
# Run (local, no Dapr/Temporal):
#   docker run -p 8000:8000 \
#     -e ATLAN_BASE_URL=https://your-tenant.atlan.com \
#     -e ATLAN_API_KEY=your-api-key \
#     ddlc:latest
#
# In production: deployed via Helm chart with Dapr sidecar and Temporal connection.
# Dapr injects ATLAN_BASE_URL and ATLAN_API_KEY from the tenant secret store.

FROM registry.atlan.com/public/application-sdk:main-2.3.1

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY main.py ./
COPY app/ ./app/
COPY components/ ./components/
COPY atlan-app-registry.json ./

# The SDK base image sets up the Dapr sidecar and Temporal client via env vars:
#   TEMPORAL__HOST_URL      — Temporal server address (injected by Helm)
#   DAPR_HTTP_PORT          — Dapr sidecar HTTP port (injected by Dapr)
#   APPLICATION_MODE        — LOCAL | WORKER | SERVER (injected by Helm)

# Port 8000 is the SDK convention
EXPOSE 8000

# Health check — SDK exposes /observability
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/observability || exit 1

CMD ["uv", "run", "python", "main.py"]

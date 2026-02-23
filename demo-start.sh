#!/usr/bin/env bash
# =============================================================================
# DDLC Demo — Start Everything
# =============================================================================
# Starts: Dapr, Temporal, DDLC app (port 8000), Atlan frontend (port 3333)
#
# Usage:
#   ./demo-start.sh          # start everything
#   ./demo-start.sh --no-fe  # skip Atlan frontend (DDLC only, faster)
#   ./demo-start.sh --stop   # kill everything and exit
#
# Logs:
#   /tmp/ddlc-dapr.log
#   /tmp/ddlc-temporal.log
#   /tmp/ddlc-app.log
#   /tmp/ddlc-frontend.log
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELLO_WORLD="$SCRIPT_DIR/hello_world"
FRONTEND_DIR="$SCRIPT_DIR/atlan-frontend"

# Ports used
DAPR_PORT=3500
TEMPORAL_PORT=7233
APP_PORT=8000
FRONTEND_PORT=3333

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log()    { echo -e "${BOLD}[demo]${NC} $*"; }
ok()     { echo -e "${GREEN}  ✓${NC} $*"; }
warn()   { echo -e "${YELLOW}  ⚠${NC} $*"; }
fail()   { echo -e "${RED}  ✗${NC} $*"; }
header() { echo -e "\n${BLUE}${BOLD}$*${NC}"; }

# ── helpers ──────────────────────────────────────────────────────────────────

port_in_use() { lsof -ti:"$1" >/dev/null 2>&1; }

kill_port() {
    local port=$1
    local pids
    pids=$(lsof -ti:"$port" 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
        echo "$pids" | xargs kill -9 2>/dev/null || true
    fi
}

wait_for_port() {
    local port=$1 label=$2 timeout=${3:-30}
    local elapsed=0
    while ! lsof -ti:"$port" >/dev/null 2>&1; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [[ $elapsed -ge $timeout ]]; then
            fail "$label did not start within ${timeout}s"
            return 1
        fi
    done
    ok "$label is up (port $port)"
}

# ── --stop ────────────────────────────────────────────────────────────────────

if [[ "${1:-}" == "--stop" ]]; then
    header "Stopping DDLC demo services..."
    for port in $APP_PORT $FRONTEND_PORT $DAPR_PORT $TEMPORAL_PORT 3000 50001; do
        kill_port "$port"
    done
    ok "All services stopped"
    exit 0
fi

# ── --no-fe flag ──────────────────────────────────────────────────────────────

START_FRONTEND=true
if [[ "${1:-}" == "--no-fe" ]]; then
    START_FRONTEND=false
fi

# ── pre-flight ────────────────────────────────────────────────────────────────

header "DDLC Demo Startup"
log "Checking prerequisites..."

for cmd in dapr temporal uv; do
    if ! command -v "$cmd" &>/dev/null; then
        fail "$cmd not found — install it first"
        exit 1
    fi
    ok "$cmd found"
done

if $START_FRONTEND; then
    if ! command -v pnpm &>/dev/null; then
        warn "pnpm not found — skipping Atlan frontend (use --no-fe to suppress this warning)"
        START_FRONTEND=false
    else
        ok "pnpm found"
    fi
fi

# Kill anything leftover from a previous run
header "Cleaning up old processes..."
for port in $APP_PORT $DAPR_PORT $TEMPORAL_PORT 3000 50001; do
    if port_in_use "$port"; then
        warn "Port $port in use — killing..."
        kill_port "$port"
        sleep 1
    fi
done
ok "Ports clear"

# ── start Dapr ────────────────────────────────────────────────────────────────

header "Starting Dapr..."
cd "$HELLO_WORLD"
dapr run \
    --enable-api-logging \
    --log-level warn \
    --app-id app \
    --app-port 3000 \
    --scheduler-host-address '' \
    --placement-host-address '' \
    --max-body-size 1024Mi \
    --dapr-http-port "$DAPR_PORT" \
    --dapr-grpc-port 50001 \
    --resources-path components \
    > /tmp/ddlc-dapr.log 2>&1 &

wait_for_port "$DAPR_PORT" "Dapr" 20

# ── start Temporal ────────────────────────────────────────────────────────────

header "Starting Temporal..."
temporal server start-dev \
    --db-filename "$HELLO_WORLD/temporal.db" \
    > /tmp/ddlc-temporal.log 2>&1 &

wait_for_port "$TEMPORAL_PORT" "Temporal" 30

# ── start DDLC app ────────────────────────────────────────────────────────────

header "Starting DDLC app (port $APP_PORT)..."
cd "$HELLO_WORLD"
uv run python main.py > /tmp/ddlc-app.log 2>&1 &

wait_for_port "$APP_PORT" "DDLC app" 30

# ── start Atlan frontend ──────────────────────────────────────────────────────

if $START_FRONTEND; then
    header "Starting Atlan frontend (port $FRONTEND_PORT)..."
    cd "$FRONTEND_DIR"
    pnpm dev > /tmp/ddlc-frontend.log 2>&1 &
    wait_for_port "$FRONTEND_PORT" "Atlan frontend" 60
fi

# ── done ─────────────────────────────────────────────────────────────────────

echo ""
echo -e "${GREEN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}${BOLD}  DDLC Demo is ready!${NC}"
echo -e "${GREEN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
if $START_FRONTEND; then
    echo -e "  ${BOLD}Open Atlan:${NC}    http://localhost:$FRONTEND_PORT"
    echo -e "  ${BOLD}            ${NC}  → Log in → click 'Data Contracts' in the left nav"
fi
echo -e "  ${BOLD}DDLC direct:${NC}   http://localhost:$APP_PORT"
echo -e "  ${BOLD}Temporal UI:${NC}   http://localhost:8233"
echo ""
echo -e "  Logs: /tmp/ddlc-{dapr,temporal,app,frontend}.log"
echo -e "  Stop: ${BOLD}./demo-start.sh --stop${NC}"
echo ""

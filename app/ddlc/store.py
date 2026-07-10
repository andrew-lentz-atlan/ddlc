"""Durable state store for DDLC sessions and standalone context nuggets.

Backed by the Atlan platform object store (Dapr) in production so sessions and
nuggets survive pod restarts and KEDA scale-to-zero. Falls back automatically
to an in-memory dict when the object store is unavailable (e.g. local dev
without Dapr), so ``python main.py`` still works offline.

Public async signatures are unchanged from the original in-memory store — the
only difference is ``clear_all`` is now async (object-store deletes are async).
Both callers already run in async contexts.

Object layout::

    ddlc/sessions/{session_id}.json   — one DDLCSession per object
    ddlc/nuggets/{nugget_id}.json     — one standalone ContextNugget per object
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from application_sdk.constants import DEPLOYMENT_OBJECT_STORE_NAME
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.services.objectstore import ObjectStore

from app.ddlc.models import ContextNugget, DDLCSession, DDLCStage

logger = get_logger(__name__)

_SESSIONS_PREFIX = "ddlc/sessions"
_NUGGETS_PREFIX = "ddlc/nuggets"

# In-memory fallback caches — used only when the object store is unavailable.
_sessions: dict[str, dict] = {}
_standalone_nuggets: dict[str, dict] = {}

# Flips to True on the first object-store failure; thereafter this process uses
# the in-memory fallback consistently (local dev). Stays False in production
# where the Dapr object store binding is present.
_use_memory = False


def _key(prefix: str, id: str) -> str:
    return f"{prefix}/{id}.json"


def _id_from_path(path: str) -> str:
    """Extract the object id from a listed path (handles full or basename)."""
    base = path.rstrip("/").rsplit("/", 1)[-1]
    return base[:-5] if base.endswith(".json") else base


def _fallback(op: str, exc: Exception) -> None:
    """Flip to the in-memory backend after an object-store failure (once)."""
    global _use_memory
    if not _use_memory:
        logger.warning(
            "Object store unavailable during %s (%s); falling back to in-memory "
            "store for this process. Data will NOT persist across restarts.",
            op,
            exc,
        )
    _use_memory = True


# ---------------------------------------------------------------------------
# DDLC sessions
# ---------------------------------------------------------------------------


async def save_session(session: DDLCSession) -> None:
    """Persist a DDLC session."""
    session.updated_at = datetime.now(timezone.utc)
    data = session.model_dump(mode="json")
    if not _use_memory:
        try:
            await ObjectStore.upload_file_from_bytes(
                file_content=json.dumps(data).encode("utf-8"),
                destination=_key(_SESSIONS_PREFIX, session.id),
                store_name=DEPLOYMENT_OBJECT_STORE_NAME,
            )
            return
        except Exception as exc:
            _fallback("save_session", exc)
    _sessions[session.id] = data


async def get_session(session_id: str) -> Optional[DDLCSession]:
    """Retrieve a DDLC session by ID."""
    if not _use_memory:
        try:
            content = await ObjectStore.get_content(
                _key(_SESSIONS_PREFIX, session_id),
                store_name=DEPLOYMENT_OBJECT_STORE_NAME,
                suppress_error=True,
            )
            if content is None:
                return None
            return DDLCSession.model_validate(json.loads(content))
        except Exception as exc:
            _fallback("get_session", exc)
    data = _sessions.get(session_id)
    return DDLCSession.model_validate(data) if data else None


async def list_sessions(stage: Optional[DDLCStage] = None) -> list[DDLCSession]:
    """List all DDLC sessions, optionally filtered by stage."""
    sessions: list[DDLCSession] = []
    if not _use_memory:
        try:
            paths = await ObjectStore.list_files(
                prefix=_SESSIONS_PREFIX, store_name=DEPLOYMENT_OBJECT_STORE_NAME
            )
            for path in paths:
                content = await ObjectStore.get_content(
                    _key(_SESSIONS_PREFIX, _id_from_path(path)),
                    store_name=DEPLOYMENT_OBJECT_STORE_NAME,
                    suppress_error=True,
                )
                if content:
                    sessions.append(DDLCSession.model_validate(json.loads(content)))
        except Exception as exc:
            _fallback("list_sessions", exc)
    if _use_memory:
        sessions = [DDLCSession.model_validate(d) for d in _sessions.values()]
    if stage:
        sessions = [s for s in sessions if s.current_stage == stage]
    # Sort by most recently updated first
    sessions.sort(key=lambda s: s.updated_at, reverse=True)
    return sessions


async def delete_session(session_id: str) -> bool:
    """Delete a DDLC session. Returns True if found and deleted."""
    if not _use_memory:
        try:
            existed = await ObjectStore.exists(
                _key(_SESSIONS_PREFIX, session_id),
                store_name=DEPLOYMENT_OBJECT_STORE_NAME,
            )
            if existed:
                await ObjectStore.delete_file(
                    _key(_SESSIONS_PREFIX, session_id),
                    store_name=DEPLOYMENT_OBJECT_STORE_NAME,
                )
            return existed
        except Exception as exc:
            _fallback("delete_session", exc)
    return _sessions.pop(session_id, None) is not None


async def clear_all() -> None:
    """Clear all sessions and standalone nuggets (memory + object store).

    Used by the manual ``/api/demo/seed`` reset endpoint and by tests.
    """
    _sessions.clear()
    _standalone_nuggets.clear()
    if not _use_memory:
        try:
            await ObjectStore.delete_prefix(
                _SESSIONS_PREFIX, store_name=DEPLOYMENT_OBJECT_STORE_NAME
            )
            await ObjectStore.delete_prefix(
                _NUGGETS_PREFIX, store_name=DEPLOYMENT_OBJECT_STORE_NAME
            )
        except Exception as exc:
            _fallback("clear_all", exc)


# ---------------------------------------------------------------------------
# Standalone context nuggets (no DDLC session required)
# ---------------------------------------------------------------------------


async def save_standalone_nugget(nugget: ContextNugget) -> None:
    """Persist a standalone context nugget."""
    data = nugget.model_dump(mode="json")
    if not _use_memory:
        try:
            await ObjectStore.upload_file_from_bytes(
                file_content=json.dumps(data).encode("utf-8"),
                destination=_key(_NUGGETS_PREFIX, nugget.id),
                store_name=DEPLOYMENT_OBJECT_STORE_NAME,
            )
            return
        except Exception as exc:
            _fallback("save_standalone_nugget", exc)
    _standalone_nuggets[nugget.id] = data


async def get_standalone_nugget(nugget_id: str) -> Optional[ContextNugget]:
    """Retrieve a standalone nugget by ID."""
    if not _use_memory:
        try:
            content = await ObjectStore.get_content(
                _key(_NUGGETS_PREFIX, nugget_id),
                store_name=DEPLOYMENT_OBJECT_STORE_NAME,
                suppress_error=True,
            )
            if content is None:
                return None
            return ContextNugget.model_validate(json.loads(content))
        except Exception as exc:
            _fallback("get_standalone_nugget", exc)
    data = _standalone_nuggets.get(nugget_id)
    return ContextNugget.model_validate(data) if data else None


async def list_standalone_nuggets_for_asset(asset_qn: str) -> list[ContextNugget]:
    """List all standalone nuggets for a given asset qualified name."""
    nuggets: list[ContextNugget] = []
    if not _use_memory:
        try:
            paths = await ObjectStore.list_files(
                prefix=_NUGGETS_PREFIX, store_name=DEPLOYMENT_OBJECT_STORE_NAME
            )
            for path in paths:
                content = await ObjectStore.get_content(
                    _key(_NUGGETS_PREFIX, _id_from_path(path)),
                    store_name=DEPLOYMENT_OBJECT_STORE_NAME,
                    suppress_error=True,
                )
                if content:
                    nuggets.append(ContextNugget.model_validate(json.loads(content)))
        except Exception as exc:
            _fallback("list_standalone_nuggets_for_asset", exc)
    if _use_memory:
        nuggets = [
            ContextNugget.model_validate(d) for d in _standalone_nuggets.values()
        ]
    return [n for n in nuggets if n.associated_asset_qn == asset_qn]


async def delete_standalone_nugget(nugget_id: str) -> bool:
    """Delete a standalone nugget. Returns True if found and deleted."""
    if not _use_memory:
        try:
            existed = await ObjectStore.exists(
                _key(_NUGGETS_PREFIX, nugget_id),
                store_name=DEPLOYMENT_OBJECT_STORE_NAME,
            )
            if existed:
                await ObjectStore.delete_file(
                    _key(_NUGGETS_PREFIX, nugget_id),
                    store_name=DEPLOYMENT_OBJECT_STORE_NAME,
                )
            return existed
        except Exception as exc:
            _fallback("delete_standalone_nugget", exc)
    return _standalone_nuggets.pop(nugget_id, None) is not None

"""
In-memory state store for DDLC sessions.

Provides async CRUD operations with a simple dict backend.
Designed to be swapped to Dapr StateStore in production.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from app.ddlc.models import DDLCSession, DDLCStage

# In-memory storage: session_id -> serialized session dict
_sessions: dict[str, dict] = {}


async def save_session(session: DDLCSession) -> None:
    """Persist a DDLC session."""
    session.updated_at = datetime.now(timezone.utc)
    _sessions[session.id] = session.model_dump(mode="json")


async def get_session(session_id: str) -> Optional[DDLCSession]:
    """Retrieve a DDLC session by ID."""
    data = _sessions.get(session_id)
    if data:
        return DDLCSession.model_validate(data)
    return None


async def list_sessions(stage: Optional[DDLCStage] = None) -> list[DDLCSession]:
    """List all DDLC sessions, optionally filtered by stage."""
    sessions = [DDLCSession.model_validate(d) for d in _sessions.values()]
    if stage:
        sessions = [s for s in sessions if s.current_stage == stage]
    # Sort by most recently updated first
    sessions.sort(key=lambda s: s.updated_at, reverse=True)
    return sessions


async def delete_session(session_id: str) -> bool:
    """Delete a DDLC session. Returns True if found and deleted."""
    return _sessions.pop(session_id, None) is not None


def clear_all() -> None:
    """Clear all sessions. Useful for testing."""
    _sessions.clear()

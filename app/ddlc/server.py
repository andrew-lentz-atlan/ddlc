"""
DDLC REST API — FastAPI router.

This module exposes the full DDLC API as a FastAPI APIRouter so it can be
mounted on the Atlan Application SDK's APIServer in main.py.

It no longer owns a FastAPI() instance, static file serving, or an entry
point — those live in main.py.

For the standalone demo (main branch), see git branch `main`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, Response

from app.ddlc import store
from app.ddlc.models import (
    AccessLevel,
    ColumnSource,
    Comment,
    ContractRequest,
    ContractRole,
    ContractStatus,
    CustomProperty,
    DDLCSession,
    DDLCStage,
    LogicalType,
    MonitorMethod,
    ODCSContract,
    Participant,
    QualityCheck,
    QualityCheckType,
    RoleApprover,
    SchemaObject,
    SchemaProperty,
    Server,
    ServerType,
    SLAProperty,
    SourceTable,
    StageTransition,
    TeamMember,
    Urgency,
    STAGE_ORDER,
    STAGE_TO_CONTRACT_STATUS,
)
from app.ddlc.odcs import contract_to_yaml

# ---------------------------------------------------------------------------
# Lifespan — seed demo data on startup
# ---------------------------------------------------------------------------


@asynccontextmanager
async def ddlc_lifespan(app):
    """Seed demo data on startup so the platform is ready for demos immediately."""
    from app.ddlc.demo_seed import seed_demo_data

    print("\n  Seeding DDLC demo data...")
    ids = await seed_demo_data()
    print(f"  Seeded {len(ids)} demo sessions.\n")
    yield


# ---------------------------------------------------------------------------
# Router — all DDLC endpoints registered here
# ---------------------------------------------------------------------------

router = APIRouter()

# ---------------------------------------------------------------------------
# Demo seed endpoint (manual re-seed)
# ---------------------------------------------------------------------------


@router.post("/api/demo/seed", response_class=JSONResponse)
async def reseed_demo_data():
    """Re-seed demo data (clears existing sessions first)."""
    from app.ddlc.demo_seed import seed_demo_data

    store.clear_all()
    ids = await seed_demo_data()
    return JSONResponse(content={"ok": True, "seeded": len(ids), "session_ids": ids})


# ---------------------------------------------------------------------------
# Session CRUD
# ---------------------------------------------------------------------------


@router.post("/api/sessions", response_class=JSONResponse)
async def create_session(payload: dict[str, Any]):
    """Create a new DDLC session from a contract request."""
    requester = Participant(
        name=payload.get("requester_name", ""),
        email=payload.get("requester_email", ""),
    )

    request = ContractRequest(
        title=payload.get("title", ""),
        description=payload.get("description", ""),
        business_context=payload.get("business_context", ""),
        target_use_case=payload.get("target_use_case", ""),
        urgency=Urgency(payload.get("urgency", "medium")),
        requester=requester,
        domain=payload.get("domain") or None,
        data_product=payload.get("data_product") or None,
        data_product_qualified_name=payload.get("data_product_qualified_name") or None,
        desired_fields=_parse_desired_fields(payload.get("desired_fields", "")),
    )

    # Seed the contract from the request
    contract = ODCSContract(
        name=request.title,
        domain=request.domain,
        data_product=request.data_product,
        data_product_qualified_name=request.data_product_qualified_name,
        description_purpose=request.description,
        status=ContractStatus.PROPOSED,
    )

    session = DDLCSession(
        request=request,
        contract=contract,
        participants=[requester],
    )

    await store.save_session(session)
    return JSONResponse(content={"id": session.id}, status_code=201)


@router.get("/api/sessions", response_class=JSONResponse)
async def list_sessions(stage: Optional[str] = Query(None)):
    """List all sessions, optionally filtered by stage."""
    stage_filter = DDLCStage(stage) if stage else None
    sessions = await store.list_sessions(stage=stage_filter)
    return JSONResponse(content=[_session_summary(s) for s in sessions])


@router.get("/api/sessions/{session_id}", response_class=JSONResponse)
async def get_session(session_id: str):
    """Get full session detail."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return JSONResponse(content=session.model_dump(mode="json"))


@router.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    """Delete a session."""
    deleted = await store.delete_session(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Session not found")
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Stage progression
# ---------------------------------------------------------------------------


@router.put("/api/sessions/{session_id}/stage", response_class=JSONResponse)
async def advance_stage(session_id: str, payload: dict[str, Any]):
    """Advance (or set) the DDLC stage for a session."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    target = payload.get("target_stage")
    if not target:
        raise HTTPException(status_code=400, detail="target_stage is required")

    try:
        target_stage = DDLCStage(target)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid stage: {target}")

    # Validate the transition
    error = _validate_stage_transition(session, target_stage)
    if error:
        raise HTTPException(status_code=400, detail=error)

    # Record the transition
    transition = StageTransition(
        from_stage=session.current_stage,
        to_stage=target_stage,
        transitioned_by=session.participants[0] if session.participants else Participant(name="System", email=""),
    )
    session.history.append(transition)
    session.current_stage = target_stage

    # Update contract status to match
    if target_stage in STAGE_TO_CONTRACT_STATUS:
        session.contract.status = STAGE_TO_CONTRACT_STATUS[target_stage]

    await store.save_session(session)

    # On APPROVAL → ACTIVE: kick off the Temporal workflow (durable, observable)
    # The workflow handles Atlan asset registration + dbt artifact generation.
    atlan_url = None
    atlan_warning = None
    if target_stage == DDLCStage.ACTIVE:
        try:
            from app.ddlc_workflow_client import trigger_approval_workflow
            await trigger_approval_workflow(session_id)
        except ImportError:
            # Temporal not yet wired — fall back to synchronous Atlan registration
            from app.ddlc import atlan_assets
            import logging
            if atlan_assets.is_configured():
                try:
                    result = atlan_assets.register_placeholder_table(session)
                    session.contract.atlan_table_qualified_name = result["qualified_name"]
                    session.contract.atlan_table_guid = result["guid"]
                    session.contract.atlan_table_url = result["url"]
                    atlan_url = result["url"]
                    await store.save_session(session)
                except Exception as exc:
                    msg = str(exc)
                    logging.getLogger(__name__).warning(f"Atlan registration failed: {msg}")
                    if "403" in msg or "not authorized" in msg:
                        atlan_warning = "Atlan asset registration skipped — the API key needs write permissions in Atlan admin."
                    elif "404" in msg or "not found" in msg:
                        atlan_warning = "Atlan asset registration skipped — the database/schema specified does not exist in the catalog."
                    else:
                        atlan_warning = f"Atlan asset registration skipped: {msg[:120]}"

    return JSONResponse(content={"stage": target_stage.value, "atlan_url": atlan_url, "atlan_warning": atlan_warning})


# ---------------------------------------------------------------------------
# Contract metadata
# ---------------------------------------------------------------------------


@router.put("/api/sessions/{session_id}/contract/metadata", response_class=JSONResponse)
async def update_contract_metadata(session_id: str, payload: dict[str, Any]):
    """Update contract metadata fields."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    c = session.contract
    if "name" in payload:
        c.name = payload["name"]
    if "domain" in payload:
        c.domain = payload["domain"] or None
    if "tenant" in payload:
        c.tenant = payload["tenant"] or None
    if "data_product" in payload:
        c.data_product = payload["data_product"] or None
    if "version" in payload:
        c.version = payload["version"]
    if "description_purpose" in payload:
        c.description_purpose = payload["description_purpose"] or None
    if "description_limitations" in payload:
        c.description_limitations = payload["description_limitations"] or None
    if "description_usage" in payload:
        c.description_usage = payload["description_usage"] or None
    if "tags" in payload:
        c.tags = payload["tags"] if isinstance(payload["tags"], list) else []

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Schema objects (tables)
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/objects", response_class=JSONResponse)
async def add_schema_object(session_id: str, payload: dict[str, Any]):
    """Add a new schema object (table) to the contract."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    name = payload.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    if any(o.name == name for o in session.contract.schema_objects):
        raise HTTPException(status_code=409, detail=f"Object '{name}' already exists")

    obj = SchemaObject(
        name=name,
        physical_name=payload.get("physical_name") or None,
        description=payload.get("description") or None,
    )
    session.contract.schema_objects.append(obj)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "name": name}, status_code=201)


@router.put("/api/sessions/{session_id}/contract/objects/{obj_name}", response_class=JSONResponse)
async def update_schema_object(session_id: str, obj_name: str, payload: dict[str, Any]):
    """Update a schema object's metadata."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    if "description" in payload:
        obj.description = payload["description"] or None
    if "physical_name" in payload:
        obj.physical_name = payload["physical_name"] or None

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete("/api/sessions/{session_id}/contract/objects/{obj_name}")
async def delete_schema_object(session_id: str, obj_name: str):
    """Delete a schema object and all its properties."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    original_len = len(session.contract.schema_objects)
    session.contract.schema_objects = [
        o for o in session.contract.schema_objects if o.name != obj_name
    ]
    if len(session.contract.schema_objects) == original_len:
        raise HTTPException(status_code=404, detail=f"Object '{obj_name}' not found")

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Schema properties (columns)
# ---------------------------------------------------------------------------


@router.post(
    "/api/sessions/{session_id}/contract/objects/{obj_name}/properties",
    response_class=JSONResponse,
)
async def add_property(session_id: str, obj_name: str, payload: dict[str, Any]):
    """Add a column/property to a schema object."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)

    name = payload.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    if any(p.name == name for p in obj.properties):
        raise HTTPException(status_code=409, detail=f"Property '{name}' already exists in '{obj_name}'")

    prop = SchemaProperty(
        name=name,
        logical_type=LogicalType(payload.get("logical_type", "string")),
        description=payload.get("description") or None,
        required=payload.get("required", False),
        primary_key=payload.get("primary_key", False),
        unique=payload.get("unique", False),
        classification=payload.get("classification") or None,
        critical_data_element=payload.get("critical_data_element", False),
        examples=payload.get("examples") or None,
    )
    obj.properties.append(prop)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "name": name}, status_code=201)


# IMPORTANT: /reorder must be defined BEFORE /{prop_name} so FastAPI doesn't
# match "reorder" as a prop_name.
@router.post(
    "/api/sessions/{session_id}/contract/objects/{obj_name}/properties/reorder",
    response_class=JSONResponse,
)
async def reorder_property(session_id: str, obj_name: str, payload: dict[str, Any]):
    """Reorder a column/property within a schema object (move up or down)."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)

    prop_name = payload.get("property_name", "").strip()
    direction = payload.get("direction", "").strip().lower()

    if not prop_name:
        raise HTTPException(status_code=400, detail="property_name is required")
    if direction not in ("up", "down"):
        raise HTTPException(status_code=400, detail="direction must be 'up' or 'down'")

    idx = None
    for i, p in enumerate(obj.properties):
        if p.name == prop_name:
            idx = i
            break
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Property '{prop_name}' not found")

    if direction == "up" and idx > 0:
        obj.properties[idx], obj.properties[idx - 1] = obj.properties[idx - 1], obj.properties[idx]
        new_idx = idx - 1
    elif direction == "down" and idx < len(obj.properties) - 1:
        obj.properties[idx], obj.properties[idx + 1] = obj.properties[idx + 1], obj.properties[idx]
        new_idx = idx + 1
    else:
        return JSONResponse(content={"ok": True, "new_index": idx})

    await store.save_session(session)
    return JSONResponse(content={"ok": True, "new_index": new_idx})


@router.put(
    "/api/sessions/{session_id}/contract/objects/{obj_name}/properties/{prop_name}",
    response_class=JSONResponse,
)
async def update_property(session_id: str, obj_name: str, prop_name: str, payload: dict[str, Any]):
    """Update a column/property."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    prop = _find_property(obj, prop_name)

    if "logical_type" in payload:
        prop.logical_type = LogicalType(payload["logical_type"])
    if "description" in payload:
        prop.description = payload["description"] or None
    if "required" in payload:
        prop.required = bool(payload["required"])
    if "primary_key" in payload:
        prop.primary_key = bool(payload["primary_key"])
    if "unique" in payload:
        prop.unique = bool(payload["unique"])
    if "classification" in payload:
        prop.classification = payload["classification"] or None
    if "critical_data_element" in payload:
        prop.critical_data_element = bool(payload["critical_data_element"])
    if "examples" in payload:
        prop.examples = payload["examples"] or None
    if "primary_key_position" in payload:
        prop.primary_key_position = payload["primary_key_position"]

    # Support renaming — must be last so other updates apply first
    if "name" in payload and payload["name"] != prop_name:
        new_name = payload["name"].strip()
        if not new_name:
            raise HTTPException(status_code=400, detail="Column name cannot be empty")
        if any(p.name == new_name for p in obj.properties if p.name != prop_name):
            raise HTTPException(status_code=409, detail=f"Column '{new_name}' already exists")
        prop.name = new_name

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete(
    "/api/sessions/{session_id}/contract/objects/{obj_name}/properties/{prop_name}",
)
async def delete_property(session_id: str, obj_name: str, prop_name: str):
    """Delete a column/property from a schema object."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    original_len = len(obj.properties)
    obj.properties = [p for p in obj.properties if p.name != prop_name]
    if len(obj.properties) == original_len:
        raise HTTPException(status_code=404, detail=f"Property '{prop_name}' not found")

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/quality", response_class=JSONResponse)
async def add_quality_check(session_id: str, payload: dict[str, Any]):
    """Add a quality check to the contract."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    check = QualityCheck(
        type=QualityCheckType(payload.get("type", "text")),
        description=payload.get("description", ""),
        dimension=payload.get("dimension") or None,
        metric=payload.get("metric") or None,
        severity=payload.get("severity") or None,
        must_be=payload.get("must_be") or None,
        must_be_greater_than=payload.get("must_be_greater_than"),
        must_be_less_than=payload.get("must_be_less_than"),
        schedule=payload.get("schedule") or None,
        scheduler=payload.get("scheduler") or None,
        business_impact=payload.get("business_impact") or None,
        method=payload.get("method") or None,
        column=payload.get("column") or None,
        query=payload.get("query") or None,
        engine=payload.get("engine") or None,
    )
    session.contract.quality_checks.append(check)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "id": check.id}, status_code=201)


@router.put("/api/sessions/{session_id}/contract/quality/{check_id}", response_class=JSONResponse)
async def update_quality_check(session_id: str, check_id: str, payload: dict[str, Any]):
    """Update a quality check."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    check = next((q for q in session.contract.quality_checks if q.id == check_id), None)
    if not check:
        raise HTTPException(status_code=404, detail="Quality check not found")

    if "type" in payload:
        check.type = QualityCheckType(payload["type"])
    if "description" in payload:
        check.description = payload["description"]
    if "dimension" in payload:
        check.dimension = payload["dimension"] or None
    if "metric" in payload:
        check.metric = payload["metric"] or None
    if "severity" in payload:
        check.severity = payload["severity"] or None
    if "must_be" in payload:
        check.must_be = payload["must_be"] or None
    if "must_be_greater_than" in payload:
        check.must_be_greater_than = payload["must_be_greater_than"]
    if "must_be_less_than" in payload:
        check.must_be_less_than = payload["must_be_less_than"]
    if "schedule" in payload:
        check.schedule = payload["schedule"] or None
    if "scheduler" in payload:
        check.scheduler = payload["scheduler"] or None
    if "business_impact" in payload:
        check.business_impact = payload["business_impact"] or None
    if "method" in payload:
        check.method = payload["method"] or None
    if "column" in payload:
        check.column = payload["column"] or None
    if "query" in payload:
        check.query = payload["query"] or None
    if "engine" in payload:
        check.engine = payload["engine"] or None

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete("/api/sessions/{session_id}/contract/quality/{check_id}")
async def delete_quality_check(session_id: str, check_id: str):
    """Delete a quality check."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    original_len = len(session.contract.quality_checks)
    session.contract.quality_checks = [
        q for q in session.contract.quality_checks if q.id != check_id
    ]
    if len(session.contract.quality_checks) == original_len:
        raise HTTPException(status_code=404, detail="Quality check not found")

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# SLA properties
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/sla", response_class=JSONResponse)
async def add_sla(session_id: str, payload: dict[str, Any]):
    """Add an SLA property."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    sla = SLAProperty(
        property=payload.get("property", ""),
        value=payload.get("value", ""),
        unit=payload.get("unit") or None,
        description=payload.get("description") or None,
        schedule=payload.get("schedule") or None,
        scheduler=payload.get("scheduler") or None,
        driver=payload.get("driver") or None,
        element=payload.get("element") or None,
    )
    session.contract.sla_properties.append(sla)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "id": sla.id}, status_code=201)


@router.put("/api/sessions/{session_id}/contract/sla/{sla_id}", response_class=JSONResponse)
async def update_sla(session_id: str, sla_id: str, payload: dict[str, Any]):
    """Update an SLA property."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    sla = next((s for s in session.contract.sla_properties if s.id == sla_id), None)
    if not sla:
        raise HTTPException(status_code=404, detail="SLA property not found")

    if "property" in payload:
        sla.property = payload["property"]
    if "value" in payload:
        sla.value = payload["value"]
    if "unit" in payload:
        sla.unit = payload["unit"] or None
    if "description" in payload:
        sla.description = payload["description"] or None
    if "schedule" in payload:
        sla.schedule = payload["schedule"] or None
    if "scheduler" in payload:
        sla.scheduler = payload["scheduler"] or None
    if "driver" in payload:
        sla.driver = payload["driver"] or None
    if "element" in payload:
        sla.element = payload["element"] or None

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete("/api/sessions/{session_id}/contract/sla/by-id/{sla_id}")
async def delete_sla_by_id(session_id: str, sla_id: str):
    """Delete an SLA property by ID."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    original_len = len(session.contract.sla_properties)
    session.contract.sla_properties = [
        s for s in session.contract.sla_properties if s.id != sla_id
    ]
    if len(session.contract.sla_properties) == original_len:
        raise HTTPException(status_code=404, detail="SLA property not found")

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete("/api/sessions/{session_id}/contract/sla/{idx}")
async def delete_sla(session_id: str, idx: int):
    """Delete an SLA property by index (legacy — prefer by-id)."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if idx < 0 or idx >= len(session.contract.sla_properties):
        raise HTTPException(status_code=404, detail="SLA property not found")

    session.contract.sla_properties.pop(idx)
    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Team members
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/team", response_class=JSONResponse)
async def add_team_member(session_id: str, payload: dict[str, Any]):
    """Add a team member to the contract."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    member = TeamMember(
        name=payload.get("name", ""),
        email=payload.get("email", ""),
        role=payload.get("role", ""),
    )
    session.contract.team.append(member)
    await store.save_session(session)
    return JSONResponse(content={"ok": True}, status_code=201)


@router.delete("/api/sessions/{session_id}/contract/team/{idx}")
async def delete_team_member(session_id: str, idx: int):
    """Delete a team member by index."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    if idx < 0 or idx >= len(session.contract.team):
        raise HTTPException(status_code=404, detail="Team member not found")

    session.contract.team.pop(idx)
    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Servers (infrastructure)
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/servers", response_class=JSONResponse)
async def add_server(session_id: str, payload: dict[str, Any]):
    """Add a server/infrastructure entry to the contract."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    server = Server(
        type=ServerType(payload.get("type", "snowflake")),
        environment=payload.get("environment", "prod"),
        account=payload.get("account") or None,
        database=payload.get("database") or None,
        schema_name=payload.get("schema_name") or None,
        host=payload.get("host") or None,
        description=payload.get("description") or None,
        connection_qualified_name=payload.get("connection_qualified_name") or None,
    )
    session.contract.servers.append(server)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "id": server.id}, status_code=201)


@router.put("/api/sessions/{session_id}/contract/servers/{server_id}", response_class=JSONResponse)
async def update_server(session_id: str, server_id: str, payload: dict[str, Any]):
    """Update a server entry."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    server = next((s for s in session.contract.servers if s.id == server_id), None)
    if not server:
        raise HTTPException(status_code=404, detail="Server not found")

    if "type" in payload:
        server.type = ServerType(payload["type"])
    if "environment" in payload:
        server.environment = payload["environment"]
    if "account" in payload:
        server.account = payload["account"] or None
    if "database" in payload:
        server.database = payload["database"] or None
    if "schema_name" in payload:
        server.schema_name = payload["schema_name"] or None
    if "host" in payload:
        server.host = payload["host"] or None
    if "description" in payload:
        server.description = payload["description"] or None
    if "connection_qualified_name" in payload:
        server.connection_qualified_name = payload["connection_qualified_name"] or None

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete("/api/sessions/{session_id}/contract/servers/{server_id}")
async def delete_server(session_id: str, server_id: str):
    """Delete a server entry by ID."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    original_len = len(session.contract.servers)
    session.contract.servers = [s for s in session.contract.servers if s.id != server_id]
    if len(session.contract.servers) == original_len:
        raise HTTPException(status_code=404, detail="Server not found")

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Roles & Access Control
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/roles", response_class=JSONResponse)
async def add_role(session_id: str, payload: dict[str, Any]):
    """Add a role/access entry to the contract."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    approvers_raw = payload.get("approvers", [])
    approvers = [RoleApprover(**a) for a in approvers_raw] if approvers_raw else []
    role = ContractRole(
        role=payload.get("role", ""),
        access=AccessLevel(payload.get("access", "read")),
        approvers=approvers,
        description=payload.get("description") or None,
    )
    session.contract.roles.append(role)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "id": role.id}, status_code=201)


@router.put("/api/sessions/{session_id}/contract/roles/{role_id}", response_class=JSONResponse)
async def update_role(session_id: str, role_id: str, payload: dict[str, Any]):
    """Update a role entry."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    role = next((r for r in session.contract.roles if r.id == role_id), None)
    if not role:
        raise HTTPException(status_code=404, detail="Role not found")

    if "role" in payload:
        role.role = payload["role"]
    if "access" in payload:
        role.access = AccessLevel(payload["access"])
    if "approvers" in payload:
        role.approvers = [RoleApprover(**a) for a in payload["approvers"]]
    if "description" in payload:
        role.description = payload["description"] or None

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete("/api/sessions/{session_id}/contract/roles/{role_id}")
async def delete_role(session_id: str, role_id: str):
    """Delete a role entry by ID."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    original_len = len(session.contract.roles)
    session.contract.roles = [r for r in session.contract.roles if r.id != role_id]
    if len(session.contract.roles) == original_len:
        raise HTTPException(status_code=404, detail="Role not found")

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Custom Properties
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/custom-properties", response_class=JSONResponse)
async def add_custom_property(session_id: str, payload: dict[str, Any]):
    """Add a custom property key-value pair to the contract."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    prop = CustomProperty(
        key=payload.get("key", ""),
        value=payload.get("value", ""),
    )
    session.contract.custom_properties.append(prop)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "id": prop.id}, status_code=201)


@router.delete("/api/sessions/{session_id}/contract/custom-properties/{prop_id}")
async def delete_custom_property(session_id: str, prop_id: str):
    """Delete a custom property by ID."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    original_len = len(session.contract.custom_properties)
    session.contract.custom_properties = [
        p for p in session.contract.custom_properties if p.id != prop_id
    ]
    if len(session.contract.custom_properties) == original_len:
        raise HTTPException(status_code=404, detail="Custom property not found")

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/comments", response_class=JSONResponse)
async def add_comment(session_id: str, payload: dict[str, Any]):
    """Add a comment to the session."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    comment = Comment(
        author=Participant(
            name=payload.get("author_name", ""),
            email=payload.get("author_email", ""),
        ),
        content=payload.get("content", ""),
        stage=session.current_stage,
        parent_id=payload.get("parent_id") or None,
    )
    session.comments.append(comment)
    await store.save_session(session)
    return JSONResponse(content={"ok": True, "id": comment.id}, status_code=201)


@router.get("/api/sessions/{session_id}/comments", response_class=JSONResponse)
async def get_comments(session_id: str, stage: Optional[str] = Query(None)):
    """Get comments for a session, optionally filtered by stage."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    comments = session.comments
    if stage:
        comments = [c for c in comments if c.stage.value == stage]
    return JSONResponse(content=[c.model_dump(mode="json") for c in comments])


# ---------------------------------------------------------------------------
# YAML export
# ---------------------------------------------------------------------------


@router.get("/api/sessions/{session_id}/contract/yaml")
async def get_yaml(session_id: str):
    """Get the ODCS v3.1.0 YAML preview."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    yaml_str = contract_to_yaml(session.contract)
    return Response(content=yaml_str, media_type="text/yaml")


@router.get("/api/sessions/{session_id}/contract/download")
async def download_yaml(session_id: str):
    """Download the ODCS YAML as a file."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    yaml_str = contract_to_yaml(session.contract)
    filename = f"{session.contract.name or 'contract'}.odcs.yaml"
    return Response(
        content=yaml_str,
        media_type="application/x-yaml",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# dbt model generation
# ---------------------------------------------------------------------------


@router.get("/api/dbt/status", response_class=JSONResponse)
async def dbt_status():
    """Check if dbt Cloud credentials are configured."""
    from app.ddlc import dbt_generator
    return JSONResponse(content={"configured": dbt_generator.is_configured()})


@router.get("/api/sessions/{session_id}/contract/dbt/preview", response_class=JSONResponse)
async def dbt_preview(session_id: str):
    """Return {relative_path: content} preview of the generated dbt project."""
    from app.ddlc import dbt_generator
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    files = dbt_generator.generate_dbt_preview(session.contract)
    return JSONResponse(content={"files": files})


@router.get("/api/sessions/{session_id}/contract/dbt/download")
async def dbt_download(session_id: str):
    """Download the generated dbt project as a ZIP archive."""
    import re as _re
    from app.ddlc import dbt_generator
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    zip_bytes = dbt_generator.generate_dbt_zip(session.contract)
    project_name = _re.sub(r"[^a-zA-Z0-9_]", "_", session.contract.name or "contract").lower()
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{project_name}_dbt_project.zip"'},
    )


@router.post("/api/sessions/{session_id}/contract/dbt/trigger", response_class=JSONResponse)
async def dbt_cloud_trigger(session_id: str):
    """Trigger a dbt Cloud job run for this contract."""
    from app.ddlc import dbt_generator
    if not dbt_generator.is_configured():
        raise HTTPException(status_code=503, detail="dbt Cloud credentials not configured")
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    try:
        result = dbt_generator.trigger_dbt_cloud_run(session.contract)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    return JSONResponse(content={"ok": True, "run": result})


# ---------------------------------------------------------------------------
# Atlan metadata integration
# ---------------------------------------------------------------------------


@router.get("/api/atlan/status", response_class=JSONResponse)
async def atlan_status():
    """Check if Atlan credentials are configured."""
    from app.ddlc import atlan_assets
    return JSONResponse(content={"configured": atlan_assets.is_configured()})


@router.get("/api/atlan/search-tables", response_class=JSONResponse)
async def search_atlan_tables(q: str = Query(""), asset_type: str = Query("Table"), limit: int = Query(20)):
    """Search Atlan for tables/views matching a query."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")
    try:
        results = atlan_assets.search_assets(query=q, asset_type=asset_type, limit=limit)
        return JSONResponse(content=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/atlan/table-columns", response_class=JSONResponse)
async def get_atlan_table_columns(qualified_name: str = Query(...)):
    """Fetch columns for a specific table from Atlan."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")
    try:
        columns = atlan_assets.get_table_columns(qualified_name=qualified_name)
        return JSONResponse(content=columns)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/atlan/search-products", response_class=JSONResponse)
async def search_atlan_products(q: str = Query("")):
    """Search Atlan for data products."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")
    try:
        results = atlan_assets.search_data_products(query=q)
        return JSONResponse(content=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/atlan/search-domains", response_class=JSONResponse)
async def search_atlan_domains(q: str = Query("")):
    """Search Atlan for data domains."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")
    try:
        results = atlan_assets.search_data_domains(query=q)
        return JSONResponse(content=results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/atlan/search-users", response_class=JSONResponse)
async def search_atlan_users(q: str = Query(""), limit: int = Query(20)):
    """Search Atlan users by email/username fragment for the approver picker."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")
    try:
        results = atlan_assets.search_users(query=q, limit=limit)
        return JSONResponse(content={"users": results})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/atlan/search-connections", response_class=JSONResponse)
async def search_atlan_connections(q: str = Query(""), connector: str = Query(""), limit: int = Query(20)):
    """Search Atlan connections by name/connector type for the Server connection picker."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")
    try:
        results = atlan_assets.search_connections(query=q, connector_type=connector, limit=limit)
        return JSONResponse(content={"connections": results})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Source tables (lineage)
# ---------------------------------------------------------------------------


@router.post("/api/sessions/{session_id}/contract/objects/{obj_name}/sources", response_class=JSONResponse)
async def add_source_table(session_id: str, obj_name: str, payload: dict[str, Any]):
    """Add a source table to a schema object (for lineage tracking)."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    cached_columns = payload.get("columns") or None

    source = SourceTable(
        name=payload.get("name", ""),
        qualified_name=payload.get("qualified_name") or None,
        database_name=payload.get("database_name") or None,
        schema_name=payload.get("schema_name") or None,
        connector_name=payload.get("connector_name") or None,
        description=payload.get("description") or None,
        columns=cached_columns,
    )

    if not source.columns and source.qualified_name:
        try:
            from app.ddlc import atlan_assets
            if atlan_assets.is_configured():
                source.columns = atlan_assets.get_table_columns(source.qualified_name)
        except Exception:
            pass

    if any(s.qualified_name == source.qualified_name and source.qualified_name for s in obj.source_tables):
        raise HTTPException(status_code=409, detail=f"Source '{source.name}' already added")

    obj.source_tables.append(source)
    await store.save_session(session)
    return JSONResponse(content={"ok": True}, status_code=201)


@router.get("/api/sessions/{session_id}/contract/objects/{obj_name}/source-columns", response_class=JSONResponse)
async def get_source_columns(session_id: str, obj_name: str):
    """Get columns for all source tables of a schema object (target table)."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    result = {}
    for src in obj.source_tables:
        if src.columns:
            result[src.name] = src.columns
        elif src.qualified_name:
            try:
                from app.ddlc import atlan_assets
                if atlan_assets.is_configured():
                    cols = atlan_assets.get_table_columns(src.qualified_name)
                    result[src.name] = cols
                    src.columns = cols
                else:
                    result[src.name] = []
            except Exception:
                result[src.name] = []
        else:
            result[src.name] = []
    await store.save_session(session)
    return JSONResponse(content=result)


@router.post("/api/sessions/{session_id}/contract/objects/{obj_name}/map-columns", response_class=JSONResponse)
async def map_source_columns(session_id: str, obj_name: str, payload: dict[str, Any]):
    """Batch-create target columns from selected source columns with lineage pre-populated."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    mappings = payload.get("mappings", [])
    if not mappings:
        raise HTTPException(status_code=400, detail="No mappings provided")

    added = 0
    skipped = 0
    for m in mappings:
        target_name = m.get("target_column_name") or m.get("source_column", "")
        if not target_name:
            skipped += 1
            continue

        existing = [p for p in obj.properties if p.name == target_name]
        if existing:
            prop = existing[0]
            source_entry = ColumnSource(
                source_table=m.get("source_table", ""),
                source_column=m.get("source_column", ""),
                source_table_qualified_name=m.get("source_table_qualified_name") or None,
                transform_logic=m.get("transform_logic") or None,
                transform_description=m.get("transform_description") or None,
            )
            if not any(
                s.source_table == source_entry.source_table and s.source_column == source_entry.source_column
                for s in prop.sources
            ):
                prop.sources.append(source_entry)
            skipped += 1
            continue

        logical_type_str = m.get("logical_type", "string").lower()
        try:
            logical_type = LogicalType(logical_type_str)
        except ValueError:
            logical_type = LogicalType.STRING

        prop = SchemaProperty(
            name=target_name,
            logical_type=logical_type,
            description=m.get("description") or None,
            required=m.get("is_primary", False) or m.get("required", False),
            primary_key=m.get("is_primary", False),
            sources=[
                ColumnSource(
                    source_table=m.get("source_table", ""),
                    source_column=m.get("source_column", ""),
                    source_table_qualified_name=m.get("source_table_qualified_name") or None,
                    transform_logic=m.get("transform_logic") or None,
                    transform_description=m.get("transform_description") or None,
                )
            ],
        )
        obj.properties.append(prop)
        added += 1

    await store.save_session(session)
    return JSONResponse(
        content={"added": added, "skipped": skipped, "total_columns": len(obj.properties)},
        status_code=201,
    )


@router.delete("/api/sessions/{session_id}/contract/objects/{obj_name}/sources/{idx}")
async def delete_source_table(session_id: str, obj_name: str, idx: int):
    """Remove a source table by index."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    if idx < 0 or idx >= len(obj.source_tables):
        raise HTTPException(status_code=404, detail="Source table not found")

    obj.source_tables.pop(idx)
    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.post(
    "/api/sessions/{session_id}/contract/objects/{obj_name}/properties/{prop_name}/sources",
    response_class=JSONResponse,
)
async def add_column_source(session_id: str, obj_name: str, prop_name: str, payload: dict[str, Any]):
    """Add a column-level lineage source to a property."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    prop = _find_property(obj, prop_name)

    source = ColumnSource(
        source_table=payload.get("source_table", ""),
        source_column=payload.get("source_column", ""),
        source_table_qualified_name=payload.get("source_table_qualified_name") or None,
        transform_logic=payload.get("transform_logic") or None,
        transform_description=payload.get("transform_description") or None,
    )
    prop.sources.append(source)
    await store.save_session(session)
    return JSONResponse(content={"ok": True}, status_code=201)


@router.put(
    "/api/sessions/{session_id}/contract/objects/{obj_name}/properties/{prop_name}/sources/{idx}",
    response_class=JSONResponse,
)
async def update_column_source(session_id: str, obj_name: str, prop_name: str, idx: int, payload: dict[str, Any]):
    """Update an existing column-level lineage source."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    prop = _find_property(obj, prop_name)

    if idx < 0 or idx >= len(prop.sources):
        raise HTTPException(status_code=404, detail="Source not found")

    src = prop.sources[idx]
    if "source_table" in payload:
        src.source_table = payload["source_table"]
    if "source_column" in payload:
        src.source_column = payload["source_column"]
    if "source_table_qualified_name" in payload:
        src.source_table_qualified_name = payload["source_table_qualified_name"] or None
    if "transform_logic" in payload:
        src.transform_logic = payload["transform_logic"] or None
    if "transform_description" in payload:
        src.transform_description = payload["transform_description"] or None

    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.delete(
    "/api/sessions/{session_id}/contract/objects/{obj_name}/properties/{prop_name}/sources/{idx}",
)
async def delete_column_source(session_id: str, obj_name: str, prop_name: str, idx: int):
    """Remove a column lineage source by index."""
    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    prop = _find_property(obj, prop_name)

    if idx < 0 or idx >= len(prop.sources):
        raise HTTPException(status_code=404, detail="Source not found")

    prop.sources.pop(idx)
    await store.save_session(session)
    return JSONResponse(content={"ok": True})


@router.post("/api/sessions/{session_id}/contract/objects/{obj_name}/import-from-atlan", response_class=JSONResponse)
async def import_columns_from_atlan(session_id: str, obj_name: str, payload: dict[str, Any]):
    """Import columns from an Atlan table into the schema object as lineage sources."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")

    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    obj = _find_object(session, obj_name)
    source_qualified_name = payload.get("qualified_name", "")
    source_name = payload.get("source_name", "")
    if not source_qualified_name:
        raise HTTPException(status_code=400, detail="qualified_name is required")

    try:
        columns = atlan_assets.get_table_columns(qualified_name=source_qualified_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch columns: {e}")

    imported = 0
    existing_names = {p.name for p in obj.properties}
    for col in columns:
        col_name = col["name"]
        if col_name in existing_names:
            for prop in obj.properties:
                if prop.name == col_name:
                    prop.sources.append(ColumnSource(
                        source_table=source_name or source_qualified_name,
                        source_column=col_name,
                        source_table_qualified_name=source_qualified_name,
                    ))
                    break
        else:
            prop = SchemaProperty(
                name=col_name,
                logical_type=LogicalType(col["logical_type"]),
                description=col.get("description") or None,
                required=not col.get("is_nullable", True),
                primary_key=col.get("is_primary", False),
                sources=[ColumnSource(
                    source_table=source_name or source_qualified_name,
                    source_column=col_name,
                    source_table_qualified_name=source_qualified_name,
                )],
            )
            obj.properties.append(prop)
            existing_names.add(col_name)
        imported += 1

    await store.save_session(session)
    return JSONResponse(content={"ok": True, "imported": imported})


@router.post("/api/sessions/{session_id}/contract/objects/bulk-import-from-atlan", response_class=JSONResponse)
async def bulk_import_from_atlan(session_id: str, payload: dict[str, Any]):
    """Bulk-create schema objects from Atlan tables."""
    from app.ddlc import atlan_assets
    if not atlan_assets.is_configured():
        raise HTTPException(status_code=503, detail="Atlan credentials not configured")

    session = await store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    tables = payload.get("tables", [])
    if not tables:
        raise HTTPException(status_code=400, detail="No tables provided")

    existing_names = {obj.name.upper() for obj in session.contract.schema_objects}
    added = 0
    skipped_names: list[str] = []
    details: list[dict[str, Any]] = []

    for tbl in tables:
        tbl_name = tbl.get("name", "")
        qualified_name = tbl.get("qualified_name", "")

        if tbl_name.upper() in existing_names:
            skipped_names.append(tbl_name)
            continue

        obj = SchemaObject(
            name=tbl_name,
            physical_name=qualified_name or None,
            description=tbl.get("description") or None,
            source_tables=[SourceTable(
                name=tbl_name,
                qualified_name=qualified_name or None,
                database_name=tbl.get("database_name") or None,
                schema_name=tbl.get("schema_name") or None,
                connector_name=tbl.get("connector_name") or None,
                description=tbl.get("description") or None,
            )],
        )

        cols_imported = 0
        if qualified_name:
            try:
                columns = atlan_assets.get_table_columns(qualified_name=qualified_name)
                for col in columns:
                    prop = SchemaProperty(
                        name=col["name"],
                        logical_type=LogicalType(col["logical_type"]),
                        description=col.get("description") or None,
                        required=not col.get("is_nullable", True),
                        primary_key=col.get("is_primary", False),
                        sources=[ColumnSource(
                            source_table=tbl_name,
                            source_column=col["name"],
                            source_table_qualified_name=qualified_name,
                        )],
                    )
                    obj.properties.append(prop)
                    cols_imported += 1
            except Exception:
                pass

        session.contract.schema_objects.append(obj)
        existing_names.add(tbl_name.upper())
        added += 1
        details.append({"name": tbl_name, "columns_imported": cols_imported})

    await store.save_session(session)
    total_cols = sum(d["columns_imported"] for d in details)
    return JSONResponse(content={
        "ok": True,
        "added": added,
        "skipped": len(skipped_names),
        "skipped_names": skipped_names,
        "total_columns": total_cols,
        "details": details,
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_desired_fields(raw: str) -> list[str] | None:
    if not raw:
        return None
    fields = [f.strip() for f in raw.split(",") if f.strip()]
    return fields if fields else None


def _session_summary(s: DDLCSession) -> dict[str, Any]:
    """Return a lightweight summary dict for list views."""
    return {
        "id": s.id,
        "title": s.request.title,
        "domain": s.request.domain,
        "data_product": s.request.data_product,
        "current_stage": s.current_stage.value,
        "urgency": s.request.urgency.value,
        "requester_name": s.request.requester.name,
        "created_at": s.created_at.isoformat(),
        "updated_at": s.updated_at.isoformat(),
        "num_objects": len(s.contract.schema_objects),
        "num_comments": len(s.comments),
    }


def _find_object(session: DDLCSession, obj_name: str) -> SchemaObject:
    """Find a schema object by name or raise 404."""
    for obj in session.contract.schema_objects:
        if obj.name == obj_name:
            return obj
    raise HTTPException(status_code=404, detail=f"Object '{obj_name}' not found")


def _find_property(obj: SchemaObject, prop_name: str) -> SchemaProperty:
    """Find a property by name within a schema object or raise 404."""
    for prop in obj.properties:
        if prop.name == prop_name:
            return prop
    raise HTTPException(status_code=404, detail=f"Property '{prop_name}' not found in '{obj.name}'")


def _validate_stage_transition(session: DDLCSession, target: DDLCStage) -> str | None:
    """Validate a stage transition. Returns an error message or None if valid."""
    current = session.current_stage

    if current in (DDLCStage.ACTIVE, DDLCStage.REJECTED):
        return f"Cannot transition from terminal stage '{current.value}'"

    if target == DDLCStage.REJECTED:
        return None

    if current not in STAGE_ORDER or target not in STAGE_ORDER:
        return f"Invalid transition: {current.value} -> {target.value}"

    current_idx = STAGE_ORDER.index(current)
    target_idx = STAGE_ORDER.index(target)

    if target_idx != current_idx + 1:
        return f"Can only advance one stage at a time. Current: {current.value}, requested: {target.value}"

    if target == DDLCStage.DISCOVERY:
        pass

    elif target == DDLCStage.SPECIFICATION:
        discovery_comments = [c for c in session.comments if c.stage == DDLCStage.DISCOVERY]
        if not discovery_comments:
            return "At least one discovery comment is required before moving to specification"

    elif target == DDLCStage.REVIEW:
        has_table_with_columns = any(
            len(obj.properties) > 0 for obj in session.contract.schema_objects
        )
        if not has_table_with_columns:
            return "At least one table with one or more columns is required before review"

    elif target == DDLCStage.APPROVAL:
        review_comments = [c for c in session.comments if c.stage == DDLCStage.REVIEW]
        if not review_comments:
            return "At least one review comment is required before approval"

    elif target == DDLCStage.ACTIVE:
        pass

    return None

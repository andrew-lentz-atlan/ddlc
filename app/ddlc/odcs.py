"""
ODCS v3.1.0 YAML serializer.

Converts the internal ODCSContract Pydantic model into a valid
Open Data Contract Standard v3.1.0 YAML document.
"""

from __future__ import annotations

from typing import Any

import yaml

from app.ddlc.models import ContractRole, CustomProperty, ODCSContract, QualityCheck, SchemaObject, SchemaProperty, SLAProperty, Server


def _serialize_property(prop: SchemaProperty) -> dict[str, Any]:
    """Convert a SchemaProperty to an ODCS-compliant dict."""
    out: dict[str, Any] = {"name": prop.name}
    out["logicalType"] = prop.logical_type.value
    if prop.description:
        out["description"] = prop.description
    if prop.required:
        out["required"] = True
    if prop.primary_key:
        out["primaryKey"] = True
    if prop.primary_key_position is not None:
        out["primaryKeyPosition"] = prop.primary_key_position
    if prop.unique:
        out["unique"] = True
    if prop.classification:
        out["classification"] = prop.classification
    if prop.critical_data_element:
        out["criticalDataElement"] = True
    if prop.examples:
        out["examples"] = prop.examples
    # ODCS v3.1.0 transform/lineage fields
    if prop.sources:
        source_objs = []
        for src in prop.sources:
            source_objs.append(src.source_table)
        out["transformSourceObjects"] = source_objs
        # Use the first source's transform logic if present
        logic_parts = [s.transform_logic for s in prop.sources if s.transform_logic]
        if logic_parts:
            out["transformLogic"] = "; ".join(logic_parts)
        desc_parts = [s.transform_description for s in prop.sources if s.transform_description]
        if desc_parts:
            out["transformDescription"] = "; ".join(desc_parts)
    return out


def _serialize_schema_object(obj: SchemaObject) -> dict[str, Any]:
    """Convert a SchemaObject to an ODCS-compliant dict."""
    out: dict[str, Any] = {"name": obj.name}
    if obj.physical_name:
        out["physicalName"] = obj.physical_name
    if obj.description:
        out["description"] = obj.description
    if obj.properties:
        out["properties"] = [_serialize_property(p) for p in obj.properties]
    return out


def _serialize_quality_check(check: QualityCheck) -> dict[str, Any]:
    """Convert a QualityCheck to an ODCS-compliant dict."""
    out: dict[str, Any] = {"type": check.type.value}
    if check.description:
        out["description"] = check.description
    if check.dimension:
        out["dimension"] = check.dimension
    if check.metric:
        out["metric"] = check.metric
    if check.severity:
        out["severity"] = check.severity
    if check.must_be is not None:
        out["mustBe"] = check.must_be
    if check.must_be_greater_than is not None:
        out["mustBeGreaterThan"] = check.must_be_greater_than
    if check.must_be_less_than is not None:
        out["mustBeLessThan"] = check.must_be_less_than
    # Phase 1.2: Monte Carlo enrichment fields
    if check.schedule:
        out["schedule"] = check.schedule
    if check.scheduler:
        out["scheduler"] = check.scheduler
    if check.business_impact:
        out["businessImpact"] = check.business_impact
    if check.method:
        out["method"] = check.method
    if check.column:
        out["column"] = check.column
    if check.query:
        out["query"] = check.query
    if check.engine:
        out["engine"] = check.engine
    return out


def _serialize_sla(sla: SLAProperty) -> dict[str, Any]:
    """Convert an SLAProperty to an ODCS-compliant dict."""
    out: dict[str, Any] = {"property": sla.property, "value": sla.value}
    if sla.unit:
        out["unit"] = sla.unit
    if sla.description:
        out["description"] = sla.description
    # Phase 1.3: Airflow enrichment fields
    if sla.schedule:
        out["schedule"] = sla.schedule
    if sla.scheduler:
        out["scheduler"] = sla.scheduler
    if sla.driver:
        out["driver"] = sla.driver
    if sla.element:
        out["element"] = sla.element
    return out


def _serialize_server(server: Server) -> dict[str, Any]:
    """Convert a Server to an ODCS-compliant dict."""
    out: dict[str, Any] = {
        "type": server.type.value,
        "environment": server.environment,
    }
    if server.account:
        out["account"] = server.account
    if server.database:
        out["database"] = server.database
    if server.schema_name:
        out["schema"] = server.schema_name  # ODCS uses "schema" not "schema_name"
    if server.host:
        out["host"] = server.host
    if server.description:
        out["description"] = server.description
    return out


def _serialize_role(role: ContractRole) -> dict[str, Any]:
    """Convert a ContractRole to an ODCS-compliant dict."""
    out: dict[str, Any] = {"role": role.role, "access": role.access.value}
    if role.approvers:
        # ODCS standard: approvers as list of emails; GUIDs are internal-only for Phase 6
        out["approvers"] = [a.email for a in role.approvers]
    if role.description:
        out["description"] = role.description
    return out


def _serialize_custom_property(prop: CustomProperty) -> dict[str, Any]:
    """Convert a CustomProperty to an ODCS key-value dict."""
    # ODCS v3.1.0 customProperties format: [{property: "key", value: "val"}, ...]
    return {"property": prop.key, "value": prop.value}


def contract_to_odcs_dict(contract: ODCSContract) -> dict[str, Any]:
    """Convert an ODCSContract model to an ODCS v3.1.0 compliant dict."""
    odcs: dict[str, Any] = {
        "apiVersion": contract.api_version,
        "kind": contract.kind,
        "id": contract.id,
        "version": contract.version,
        "status": contract.status.value,
    }

    if contract.name:
        odcs["name"] = contract.name
    if contract.domain:
        odcs["domain"] = contract.domain
    if contract.tenant:
        odcs["tenant"] = contract.tenant
    if contract.data_product:
        odcs["dataProduct"] = contract.data_product
    if contract.tags:
        odcs["tags"] = contract.tags

    # Description block (nested)
    desc: dict[str, str] = {}
    if contract.description_purpose:
        desc["purpose"] = contract.description_purpose
    if contract.description_limitations:
        desc["limitations"] = contract.description_limitations
    if contract.description_usage:
        desc["usage"] = contract.description_usage
    if desc:
        odcs["description"] = desc

    # Schema
    if contract.schema_objects:
        odcs["schema"] = [_serialize_schema_object(obj) for obj in contract.schema_objects]

    # Quality
    if contract.quality_checks:
        odcs["quality"] = [_serialize_quality_check(q) for q in contract.quality_checks]

    # SLA
    if contract.sla_properties:
        odcs["slaProperties"] = [_serialize_sla(s) for s in contract.sla_properties]

    # Team
    if contract.team:
        odcs["team"] = [
            {"name": t.name, "email": t.email, "role": t.role} for t in contract.team
        ]

    # Servers (infrastructure)
    if contract.servers:
        odcs["servers"] = [_serialize_server(s) for s in contract.servers]

    # Roles
    if contract.roles:
        odcs["roles"] = [_serialize_role(r) for r in contract.roles]

    # Custom Properties
    if contract.custom_properties:
        odcs["customProperties"] = [_serialize_custom_property(p) for p in contract.custom_properties]

    # x-atlan-dqs: Atlan Data Quality Studio extension block
    # Contains all quality checks with engine == "atlan-dqs", serialized in
    # the DQS rule format.  ODCS doesn't natively support Atlan DQS — this
    # follows the same extension convention used by tools like Monte Carlo.
    dqs_checks = [q for q in contract.quality_checks if q.engine == "atlan-dqs"]
    if dqs_checks:
        dqs_rules = []
        for q in dqs_checks:
            rule: dict[str, Any] = {}
            if q.dqs_rule_type:
                rule["ruleType"] = q.dqs_rule_type
            if q.column:
                rule["column"] = q.column
            if q.dqs_threshold_value is not None:
                rule["thresholdValue"] = q.dqs_threshold_value
            if q.dqs_threshold_unit:
                rule["thresholdUnit"] = q.dqs_threshold_unit
            if q.dqs_alert_priority:
                rule["alertPriority"] = q.dqs_alert_priority
            if q.dqs_custom_sql:
                rule["customSql"] = q.dqs_custom_sql
            if q.description:
                rule["description"] = q.description
            if q.dqs_pushed:
                rule["pushed"] = True
            if q.dqs_rule_qualified_name:
                rule["qualifiedName"] = q.dqs_rule_qualified_name
            dqs_rules.append(rule)
        odcs["x-atlan-dqs"] = {"rules": dqs_rules}

    return odcs


def contract_to_yaml(contract: ODCSContract) -> str:
    """Generate an ODCS v3.1.0 YAML string from a contract model."""
    return yaml.dump(
        contract_to_odcs_dict(contract),
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )

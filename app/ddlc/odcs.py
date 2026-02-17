"""
ODCS v3.1.0 YAML serializer.

Converts the internal ODCSContract Pydantic model into a valid
Open Data Contract Standard v3.1.0 YAML document.
"""

from __future__ import annotations

from typing import Any

import yaml

from app.ddlc.models import ODCSContract, QualityCheck, SchemaObject, SchemaProperty, SLAProperty


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

    return odcs


def contract_to_yaml(contract: ODCSContract) -> str:
    """Generate an ODCS v3.1.0 YAML string from a contract model."""
    return yaml.dump(
        contract_to_odcs_dict(contract),
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )

"""
Context Nugget export formatters.

Exports approved nuggets as JSON or Markdown.
"""

from __future__ import annotations

import json
from collections import defaultdict

from app.ddlc.models import ContextNugget, NuggetType


# Human-readable labels for nugget types
_TYPE_LABELS: dict[NuggetType, str] = {
    NuggetType.BUSINESS_RULE: "Business Rules",
    NuggetType.INTERPRETATION: "Interpretations",
    NuggetType.PII_POLICY: "PII Policies",
    NuggetType.QA_PAIR: "Q&A Pairs",
    NuggetType.JOIN_HINT: "Join Hints",
    NuggetType.CONTEXT_BOUNDARY: "Context Boundaries",
    NuggetType.FRESHNESS_CONTEXT: "Freshness Context",
    NuggetType.TEST_ASSERTION: "Test Assertions",
}

# Preferred order for Markdown output
_TYPE_ORDER = [
    NuggetType.BUSINESS_RULE,
    NuggetType.PII_POLICY,
    NuggetType.JOIN_HINT,
    NuggetType.INTERPRETATION,
    NuggetType.QA_PAIR,
    NuggetType.FRESHNESS_CONTEXT,
    NuggetType.CONTEXT_BOUNDARY,
    NuggetType.TEST_ASSERTION,
]


def export_json(nuggets: list[ContextNugget]) -> str:
    """Return JSON array of nuggets (serializable subset)."""
    items = []
    for n in nuggets:
        items.append({
            "id": n.id,
            "type": n.nugget_type.value,
            "title": n.title,
            "content": n.content,
            "status": n.status.value,
            "source": n.source.value,
            "associated_columns": n.associated_columns,
            "tags": n.tags,
            "associated_asset_qn": n.associated_asset_qn,
            "approved_at": n.approved_at.isoformat() if n.approved_at else None,
        })
    return json.dumps(items, indent=2)


def export_markdown(nuggets: list[ContextNugget], contract_name: str = "Data Contract") -> str:
    """Return Markdown doc grouped by NuggetType, suitable for Glean/Confluence."""
    by_type: dict[NuggetType, list[ContextNugget]] = defaultdict(list)
    for n in nuggets:
        by_type[n.nugget_type].append(n)

    lines = [
        f"# Context: {contract_name}",
        "",
        f"*{len(nuggets)} approved context nugget(s)*",
        "",
    ]

    for nugget_type in _TYPE_ORDER:
        group = by_type.get(nugget_type)
        if not group:
            continue

        label = _TYPE_LABELS.get(nugget_type, nugget_type.value.replace("_", " ").title())
        lines.append(f"## {label}")
        lines.append("")
        for n in group:
            lines.append(f"### {n.title}")
            if n.associated_columns:
                cols = ", ".join(f"`{c}`" for c in n.associated_columns)
                lines.append(f"*Columns: {cols}*")
                lines.append("")
            lines.append(n.content)
            if n.tags:
                lines.append("")
                lines.append("**Tags:** " + ", ".join(n.tags))
            lines.append("")

    return "\n".join(lines)

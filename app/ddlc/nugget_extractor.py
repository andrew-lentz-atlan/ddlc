"""
Context Nugget extraction via Claude AI.

Tries a real Claude API call first; falls back to pre-seeded proposed nuggets
when ANTHROPIC_API_KEY is not configured or the API call fails.
"""

from __future__ import annotations

import json
import logging
import os

from app.ddlc.models import (
    ContextNugget,
    NuggetSource,
    NuggetStatus,
    NuggetType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extraction prompt builder
# ---------------------------------------------------------------------------

def _build_extraction_prompt(session) -> str:
    contract = session.contract
    request = session.request

    # Build schema summary
    schema_lines = []
    for obj in contract.schema_objects:
        schema_lines.append(f"Table: {obj.name}")
        for prop in obj.properties:
            classification = f" [{prop.classification}]" if prop.classification else ""
            pii = " [PII]" if prop.classification in ("pii", "sensitive") else ""
            schema_lines.append(f"  - {prop.name} ({prop.logical_type.value}){classification}{pii}: {prop.description or ''}")

    # Build comments summary
    comment_lines = []
    for comment in session.comments[-30:]:  # Last 30 comments
        comment_lines.append(f"[{comment.stage.value}] {comment.author.name}: {comment.content}")

    # Build quality checks summary
    dq_lines = []
    for qc in contract.quality_checks[:10]:
        dq_lines.append(f"- {qc.description}")

    prompt = f"""You are analyzing a data contract negotiation to extract reusable knowledge objects called "Context Nuggets".

DATA CONTRACT: {contract.name or request.title}
DESCRIPTION: {request.description}
BUSINESS CONTEXT: {request.business_context}

SCHEMA:
{chr(10).join(schema_lines) if schema_lines else "No schema defined yet."}

DISCUSSION COMMENTS:
{chr(10).join(comment_lines) if comment_lines else "No comments yet."}

QUALITY CHECKS:
{chr(10).join(dq_lines) if dq_lines else "No quality checks defined yet."}

Extract 3-6 discrete, reusable knowledge facts from the above. Each nugget should be a specific, actionable piece of knowledge that someone querying this data asset would need to know.

Return ONLY a JSON array with this exact structure:
[
  {{
    "type": "<one of: business_rule, interpretation, pii_policy, qa_pair, join_hint, context_boundary, freshness_context, test_assertion>",
    "title": "<concise title, max 60 chars>",
    "content": "<the knowledge fact, 1-4 sentences>",
    "associated_columns": ["col1", "col2"]
  }}
]

Focus on:
- Business rules (what counts as valid/complete/active records)
- PII policies (what needs masking, classification requirements)
- Join hints (how to correctly join to this table)
- Interpretation notes (what metrics mean, what to include/exclude)
- Test assertions (invariants that should always be true)
- Freshness context (refresh lag, batch timing)

Do not include generic advice. Be specific to this contract."""

    return prompt


# ---------------------------------------------------------------------------
# Parse Claude response
# ---------------------------------------------------------------------------

def _parse_nuggets_from_response(text: str) -> list[ContextNugget]:
    # Extract JSON from response (may have surrounding text)
    start = text.find("[")
    end = text.rfind("]") + 1
    if start == -1 or end == 0:
        logger.warning("Could not find JSON array in Claude response")
        return []

    try:
        items = json.loads(text[start:end])
    except json.JSONDecodeError as exc:
        logger.warning(f"Failed to parse Claude nugget response: {exc}")
        return []

    nuggets = []
    type_map = {t.value: t for t in NuggetType}
    for item in items:
        raw_type = item.get("type", "interpretation")
        nugget_type = type_map.get(raw_type, NuggetType.INTERPRETATION)
        nuggets.append(ContextNugget(
            nugget_type=nugget_type,
            title=str(item.get("title", "Extracted Nugget"))[:120],
            content=str(item.get("content", "")),
            status=NuggetStatus.PROPOSED,
            source=NuggetSource.AI_EXTRACTION,
            associated_columns=item.get("associated_columns", []),
        ))
    return nuggets


# ---------------------------------------------------------------------------
# Fallback nuggets
# ---------------------------------------------------------------------------

def _get_fallback_nuggets(session) -> list[ContextNugget]:
    """Return 3 schema-derived proposed nuggets when Claude is unavailable."""
    contract = session.contract
    columns: list[str] = []
    for obj in contract.schema_objects:
        for prop in obj.properties:
            columns.append(prop.name)

    col_hint = f"including columns: {', '.join(columns[:5])}" if columns else ""
    contract_name = contract.name or session.request.title

    return [
        ContextNugget(
            nugget_type=NuggetType.BUSINESS_RULE,
            title=f"{contract_name}: Active Record Definition",
            content=(
                f"Review the contract discussion to confirm which status values indicate "
                f"an 'active' or 'valid' record in {contract_name}. "
                f"Document the filter criteria here once agreed."
            ),
            status=NuggetStatus.PROPOSED,
            source=NuggetSource.AI_EXTRACTION,
            associated_columns=columns[:2],
        ),
        ContextNugget(
            nugget_type=NuggetType.INTERPRETATION,
            title=f"{contract_name}: Metric Scope",
            content=(
                f"Clarify which records should be included or excluded when calculating "
                f"aggregate metrics from {contract_name} {col_hint}. "
                f"Document edge cases (nulls, cancelled records, test data)."
            ),
            status=NuggetStatus.PROPOSED,
            source=NuggetSource.AI_EXTRACTION,
        ),
        ContextNugget(
            nugget_type=NuggetType.FRESHNESS_CONTEXT,
            title=f"{contract_name}: Refresh Lag",
            content=(
                f"Document the expected data refresh cadence and lag for {contract_name}. "
                f"Include upstream pipeline timing and any T+N batch delays."
            ),
            status=NuggetStatus.PROPOSED,
            source=NuggetSource.AI_EXTRACTION,
        ),
    ]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def extract_nuggets_from_session(session) -> list[ContextNugget]:
    """
    Try Claude extraction; fall back to schema-derived nuggets if API unavailable.

    Always returns proposed nuggets — the steward approves them manually.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.info("ANTHROPIC_API_KEY not set — using fallback nuggets")
        return _get_fallback_nuggets(session)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        prompt = _build_extraction_prompt(session)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        nuggets = _parse_nuggets_from_response(message.content[0].text)
        if nuggets:
            logger.info(f"Claude extracted {len(nuggets)} nuggets for session {session.id}")
            return nuggets
        logger.warning("Claude returned no parseable nuggets — using fallback")
        return _get_fallback_nuggets(session)
    except Exception as exc:
        logger.warning(f"Claude nugget extraction failed: {exc} — using fallback")
        return _get_fallback_nuggets(session)

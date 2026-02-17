"""
Agent Archetypes — the heuristic rules that decide which nugget combinations
can produce a viable agent.

Each archetype defines:
  - required_categories:  skill categories that MUST be present (all must match)
  - optional_categories:  skill categories that enhance the agent if present
  - description_template: human-readable description (Python format string)
  - capability_templates: what the agent can do (populated per-match)

Heuristic summary (plain English):
─────────────────────────────────────────────────────────────────────
REPORTING archetype
  Required: at least one DATA_ACCESS skill (can read data)
         + at least one DOMAIN_LOGIC skill (knows domain-specific joins/calcs)
  Optional: a STYLE skill (knows how to format output for the audience)
  Eval:     any evaluation nuggets whose domains overlap get attached

TAGGING / CLASSIFICATION archetype
  Required: at least one DATA_ACCESS skill (can find assets)
         + at least one MUTATION skill (can write tags/metadata back to Atlan)
  Optional: none currently
  Eval:     evaluation nuggets with overlapping domains (e.g., PII edge cases)

─────────────────────────────────────────────────────────────────────
FUTURE archetypes to add:
  - "marketing_content"  : DATA_ACCESS + DOMAIN_LOGIC + STYLE (marketing domain)
  - "data_quality"       : DATA_ACCESS + MUTATION + DOMAIN_LOGIC (quality rules)
  - "cs_agent"           : DATA_ACCESS + DOMAIN_LOGIC (customer success domain)
  - "lineage_builder"    : DATA_ACCESS + MUTATION (lineage-focused)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from app.blueprint_generator.models import SkillCategory


@dataclass
class AgentArchetype:
    """
    A template that defines what combination of skill categories
    constitutes a viable agent of a given type.
    """
    key: str                                     # e.g., "reporting"
    name_template: str                           # e.g., "{domain} Reporting Agent"
    description_template: str                    # populated with domain info
    required_categories: set[SkillCategory]      # ALL must be satisfied
    optional_categories: set[SkillCategory]      # nice-to-have, not required
    capability_templates: list[str]              # generic capabilities
    semantic_view_name_template: str             # name for the proposed view

    # FUTURE: add minimum_nugget_count, required_domain_tags, etc.


# ---------------------------------------------------------------------------
# Built-in archetypes
# ---------------------------------------------------------------------------

ARCHETYPES: list[AgentArchetype] = [
    AgentArchetype(
        key="reporting",
        name_template="{domain} Reporting Agent",
        description_template=(
            "An agent that queries data via MDLH, applies {domain}-specific "
            "logic, and produces formatted reports."
        ),
        required_categories={SkillCategory.DATA_ACCESS, SkillCategory.DOMAIN_LOGIC},
        optional_categories={SkillCategory.STYLE},
        capability_templates=[
            "Query Atlan metadata and domain datasets via MDLH",
            "Apply domain-specific joins and business logic",
            "Generate formatted summaries and reports",
        ],
        semantic_view_name_template="{domain} Reporting – Semantic View",
    ),
    AgentArchetype(
        key="tagging",
        name_template="{domain} Tagging Agent",
        description_template=(
            "An agent that scans Atlan assets, identifies candidates for "
            "classification, and applies {domain}-related tags/metadata."
        ),
        required_categories={SkillCategory.DATA_ACCESS, SkillCategory.MUTATION},
        optional_categories=set(),
        capability_templates=[
            "Scan Atlan assets for classification candidates",
            "Apply tags and metadata according to governance rules",
            "Respect edge-case expectations from evaluation packs",
        ],
        semantic_view_name_template="{domain} Tagging – Semantic View",
    ),

    # FUTURE: add more archetypes here, e.g.:
    # AgentArchetype(
    #     key="data_quality",
    #     name_template="{domain} Data Quality Agent",
    #     ...
    #     required_categories={SkillCategory.DATA_ACCESS, SkillCategory.MUTATION, SkillCategory.DOMAIN_LOGIC},
    # ),
]

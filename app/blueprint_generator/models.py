"""
Data models for nuggets, semantic view blueprints, and agent blueprints.

These are plain dataclasses today. In production, they'd likely be backed by
pyatlan glossary term objects or an MDLH query result set.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Nugget types
# ---------------------------------------------------------------------------

class NuggetType(str, Enum):
    SKILL = "skill"
    EVALUATION = "evaluation"


class SkillCategory(str, Enum):
    """
    Coarse functional label for a skill nugget.

    The blueprint engine uses these to decide which archetypes a nugget
    can satisfy.  Add new categories freely — they're just strings the
    archetype rules match against.

    FUTURE: add categories like "visualization", "orchestration", etc.
    """
    DATA_ACCESS = "data_access"       # querying / reading data (e.g., MDLH)
    DOMAIN_LOGIC = "domain_logic"     # domain-specific joins, transforms, calcs
    MUTATION = "mutation"             # writes back to Atlan (tags, metadata)
    STYLE = "style"                   # output formatting / presentation rules
    # FUTURE: add more as needed — "orchestration", "visualization", etc.


@dataclass
class SkillNugget:
    """
    An instruction set about *how* to do something.

    In Atlan today these are stored as enriched glossary terms.
    The `id` should match the glossary term's qualifiedName or a
    synthetic key like "skill.query_mdlh".
    """
    id: str
    name: str
    description: str
    domain_tags: list[str]
    category: SkillCategory

    # FUTURE: add these when the nugget model matures
    # is_constraint: bool = False          # hard policy vs. soft guidance
    # source_refs: list[str] = field(default_factory=list)  # links to source docs
    # atlan_qualified_name: str | None = None  # link back to glossary term

    type: NuggetType = field(default=NuggetType.SKILL, init=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "name": self.name,
            "description": self.description,
            "domain_tags": self.domain_tags,
            "category": self.category.value,
        }


@dataclass
class EvaluationExample:
    """A single test case inside an evaluation nugget."""
    input: str
    expected_output: str
    expectation_type: str  # e.g., "refusal", "allowed", "exact_match", "contains"


@dataclass
class EvaluationNugget:
    """
    A collection of question → expected-outcome pairs used for testing agents.

    Evaluation nuggets don't have a SkillCategory — they're matched to
    blueprints by overlapping domain_tags.
    """
    id: str
    name: str
    description: str
    domain_tags: list[str]
    examples: list[EvaluationExample]

    # FUTURE: add severity / strictness levels
    # strictness: str = "must_pass"  # "must_pass" | "should_pass" | "nice_to_have"

    type: NuggetType = field(default=NuggetType.EVALUATION, init=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "name": self.name,
            "description": self.description,
            "domain_tags": self.domain_tags,
            "examples": [
                {
                    "input": ex.input,
                    "expected_output": ex.expected_output,
                    "expectation_type": ex.expectation_type,
                }
                for ex in self.examples
            ],
        }


# ---------------------------------------------------------------------------
# Blueprint outputs
# ---------------------------------------------------------------------------

@dataclass
class SemanticViewBlueprint:
    """
    A proposed grouping of nuggets that together form a coherent
    "semantic view" — the context an agent would be given.

    FUTURE: wire into Context Studio to auto-create draft semantic views.
    """
    id: str
    name: str
    purpose: str
    domains: list[str]
    nugget_ids: list[str]
    skill_nugget_ids: list[str]
    evaluation_nugget_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "purpose": self.purpose,
            "domains": self.domains,
            "nugget_ids": self.nugget_ids,
            "skill_nugget_ids": self.skill_nugget_ids,
            "evaluation_nugget_ids": self.evaluation_nugget_ids,
        }


@dataclass
class AgentBlueprint:
    """
    A proposed agent that could be built from a semantic view.

    Links to exactly one SemanticViewBlueprint and describes what the
    agent would be able to do.

    FUTURE: add fields for:
    - required_tools: list[str]  (e.g., ["mdlh_query", "pyatlan_tagger"])
    - guardrails: list[str]      (hard constraints from is_constraint nuggets)
    - evaluation_suite_id: str   (link to a test harness)
    """
    id: str
    name: str
    description: str
    semantic_view_id: str
    agent_type: str                    # matches archetype key, e.g., "reporting"
    expected_capabilities: list[str]
    evaluation_nugget_ids: list[str]   # eval packs included for testing this agent

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "semantic_view_id": self.semantic_view_id,
            "agent_type": self.agent_type,
            "expected_capabilities": self.expected_capabilities,
            "evaluation_nugget_ids": self.evaluation_nugget_ids,
        }

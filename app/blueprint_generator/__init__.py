"""
Agent Blueprint Generator

Ingests Context Nuggets (skill + evaluation), groups them by domain,
matches them against agent archetypes, and proposes Semantic View Blueprints
and Agent Blueprints as machine-readable JSON.
"""

from app.blueprint_generator.models import (
    AgentBlueprint,
    EvaluationExample,
    EvaluationNugget,
    SemanticViewBlueprint,
    SkillNugget,
)
from app.blueprint_generator.archetypes import ARCHETYPES, AgentArchetype
from app.blueprint_generator.engine import BlueprintEngine

__all__ = [
    "SkillNugget",
    "EvaluationNugget",
    "EvaluationExample",
    "SemanticViewBlueprint",
    "AgentBlueprint",
    "AgentArchetype",
    "ARCHETYPES",
    "BlueprintEngine",
]

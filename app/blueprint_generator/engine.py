"""
Blueprint Engine

The core logic that:
1. Indexes nuggets by domain and category.
2. For each domain cluster, checks which archetypes are satisfiable.
3. Emits SemanticViewBlueprint + AgentBlueprint proposals.

Algorithm (plain English):
─────────────────────────────────────────────────────────────────────
Step 1 — Index nuggets
  • Build a map:  domain_tag → list[nugget]
  • Shared nuggets (domain_tag "shared" or "infra") are available to ALL domains.

Step 2 — For each non-shared domain cluster
  • Collect the skill nuggets in that domain + all shared skills.
  • Collect the evaluation nuggets whose domain_tags overlap.
  • Compute the set of SkillCategories covered by these skills.

Step 3 — Match archetypes
  • For each archetype, check: are ALL required_categories present?
  • If yes → this domain can support that archetype.
  • Gather the specific nuggets that satisfy required + optional categories.

Step 4 — Emit blueprints
  • Create a SemanticViewBlueprint with the matched nuggets.
  • Create an AgentBlueprint pointing to that semantic view.
  • Return everything as a dict ready for JSON serialization.
─────────────────────────────────────────────────────────────────────

FUTURE enhancements:
  - Smarter clustering: use embedding similarity instead of tag overlap.
  - Conflict detection: warn if two nuggets in a view contradict each other.
  - Scoring: rank blueprints by coverage / completeness.
  - Integration: call pyatlan to persist blueprints as glossary terms or
    post them to Context Studio via API.
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Union

from app.blueprint_generator.archetypes import ARCHETYPES, AgentArchetype
from app.blueprint_generator.models import (
    AgentBlueprint,
    EvaluationNugget,
    SemanticViewBlueprint,
    SkillCategory,
    SkillNugget,
)

Nugget = Union[SkillNugget, EvaluationNugget]

# Domain tags that make a nugget available to every domain cluster
SHARED_DOMAIN_TAGS = {"shared", "infra"}


class BlueprintEngine:
    """
    Stateless engine: feed it nuggets, get back blueprints.

    Usage:
        engine = BlueprintEngine(nuggets=[...])
        result = engine.generate()
        print(json.dumps(result, indent=2))
    """

    def __init__(
        self,
        nuggets: list[Nugget],
        archetypes: list[AgentArchetype] | None = None,
    ) -> None:
        self.nuggets = nuggets
        self.archetypes = archetypes or ARCHETYPES

        # Separate by type for fast lookup
        self.skills: list[SkillNugget] = [
            n for n in nuggets if isinstance(n, SkillNugget)
        ]
        self.evaluations: list[EvaluationNugget] = [
            n for n in nuggets if isinstance(n, EvaluationNugget)
        ]

    # ------------------------------------------------------------------
    # Step 1: Index by domain
    # ------------------------------------------------------------------

    def _index_by_domain(self) -> dict[str, list[Nugget]]:
        """
        Map each domain_tag → list of nuggets that carry that tag.
        """
        index: dict[str, list[Nugget]] = defaultdict(list)
        for nugget in self.nuggets:
            for tag in nugget.domain_tags:
                index[tag].append(nugget)
        return dict(index)

    def _get_shared_skills(self) -> list[SkillNugget]:
        """Skills tagged with shared/infra — available to all domains."""
        return [
            s for s in self.skills
            if set(s.domain_tags) & SHARED_DOMAIN_TAGS
        ]

    # ------------------------------------------------------------------
    # Step 2: Collect nuggets for a domain cluster
    # ------------------------------------------------------------------

    def _collect_domain_cluster(
        self,
        domain: str,
        domain_index: dict[str, list[Nugget]],
    ) -> tuple[list[SkillNugget], list[EvaluationNugget]]:
        """
        For a given domain, gather:
          - All skill nuggets tagged with that domain + shared skills
          - All evaluation nuggets with overlapping domain tags
        """
        shared_skills = self._get_shared_skills()

        domain_nuggets = domain_index.get(domain, [])
        domain_skills = [n for n in domain_nuggets if isinstance(n, SkillNugget)]
        domain_evals = [n for n in domain_nuggets if isinstance(n, EvaluationNugget)]

        # Merge domain-specific skills with shared skills, deduplicate by id
        seen_ids: set[str] = set()
        merged_skills: list[SkillNugget] = []
        for skill in domain_skills + shared_skills:
            if skill.id not in seen_ids:
                merged_skills.append(skill)
                seen_ids.add(skill.id)

        return merged_skills, domain_evals

    # ------------------------------------------------------------------
    # Step 3: Match archetypes against a domain cluster
    # ------------------------------------------------------------------

    def _match_archetype(
        self,
        archetype: AgentArchetype,
        skills: list[SkillNugget],
        evals: list[EvaluationNugget],
    ) -> tuple[list[SkillNugget], list[EvaluationNugget]] | None:
        """
        Check if the given skills satisfy the archetype's requirements.

        Returns the subset of nuggets to include, or None if not satisfiable.
        """
        # What categories do we have?
        available_categories: set[SkillCategory] = {s.category for s in skills}

        # All required categories must be present
        if not archetype.required_categories.issubset(available_categories):
            return None

        # Gather skills that match required OR optional categories
        relevant_categories = archetype.required_categories | archetype.optional_categories
        matched_skills = [
            s for s in skills if s.category in relevant_categories
        ]

        # Attach all evaluation nuggets with overlapping domains
        # (they serve as the test suite for this agent)
        matched_evals = evals  # all domain-overlapping evals are relevant

        return matched_skills, matched_evals

    # ------------------------------------------------------------------
    # Step 4: Emit blueprints
    # ------------------------------------------------------------------

    def _make_slug(self, text: str) -> str:
        """'Quarter-End Finance' → 'quarter_end_finance'"""
        return (
            text.lower()
            .replace("-", "_")
            .replace(" ", "_")
            .replace("–", "_")
            .replace("__", "_")
            .strip("_")
        )

    def _build_blueprints(
        self,
        domain: str,
        archetype: AgentArchetype,
        skills: list[SkillNugget],
        evals: list[EvaluationNugget],
    ) -> tuple[SemanticViewBlueprint, AgentBlueprint]:
        """Build a SemanticViewBlueprint + AgentBlueprint pair."""

        domain_label = domain.replace("_", " ").title()
        slug = self._make_slug(f"{domain}_{archetype.key}")

        # --- Semantic View Blueprint ---
        all_nugget_ids = [s.id for s in skills] + [e.id for e in evals]

        sv = SemanticViewBlueprint(
            id=f"svb.{slug}",
            name=archetype.semantic_view_name_template.format(domain=domain_label),
            purpose=(
                f"Provides the context an agent needs to perform "
                f"{archetype.key} tasks in the {domain_label} domain."
            ),
            domains=[domain],
            nugget_ids=all_nugget_ids,
            skill_nugget_ids=[s.id for s in skills],
            evaluation_nugget_ids=[e.id for e in evals],
        )

        # --- Agent Blueprint ---
        # Build concrete capabilities from templates + matched nuggets
        capabilities = list(archetype.capability_templates)
        for skill in skills:
            if skill.category == SkillCategory.STYLE:
                capabilities.append(
                    f"Format output using: {skill.name}"
                )
            elif skill.category == SkillCategory.DOMAIN_LOGIC:
                capabilities.append(
                    f"Apply domain logic from: {skill.name}"
                )

        ab = AgentBlueprint(
            id=f"agent.{slug}",
            name=archetype.name_template.format(domain=domain_label),
            description=archetype.description_template.format(domain=domain_label),
            semantic_view_id=sv.id,
            agent_type=archetype.key,
            expected_capabilities=capabilities,
            evaluation_nugget_ids=[e.id for e in evals],
        )

        return sv, ab

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _deduplicate_blueprints(
        self,
        sv_blueprints: list[SemanticViewBlueprint],
        agent_blueprints: list[AgentBlueprint],
    ) -> tuple[list[SemanticViewBlueprint], list[AgentBlueprint]]:
        """
        When multiple domains produce the exact same nugget set for the
        same archetype, merge them into one blueprint with combined domains.

        Example: "governance" and "pii" both produce a tagging agent with
        nuggets {skill.update_atlan_tags, skill.query_mdlh, eval.pii_edge_cases}
        → merge into one blueprint with domains=["governance", "pii"].
        """
        # Key: (archetype_type, frozenset of nugget_ids)
        seen: dict[tuple[str, frozenset[str]], int] = {}
        merged_svs: list[SemanticViewBlueprint] = []
        merged_abs: list[AgentBlueprint] = []

        for sv, ab in zip(sv_blueprints, agent_blueprints):
            key = (ab.agent_type, frozenset(sv.nugget_ids))

            if key in seen:
                # Merge domains into the existing blueprint
                idx = seen[key]
                existing_sv = merged_svs[idx]
                for d in sv.domains:
                    if d not in existing_sv.domains:
                        existing_sv.domains.append(d)
                # Update the name/id to reflect combined domains
                domain_label = " & ".join(
                    d.replace("_", " ").title() for d in sorted(existing_sv.domains)
                )
                archetype_key = ab.agent_type
                slug = self._make_slug(
                    "_".join(sorted(existing_sv.domains)) + f"_{archetype_key}"
                )
                existing_sv.id = f"svb.{slug}"
                existing_sv.name = f"{domain_label} {archetype_key.title()} – Semantic View"
                existing_sv.purpose = (
                    f"Provides the context an agent needs to perform "
                    f"{archetype_key} tasks across the {domain_label} domains."
                )
                existing_ab = merged_abs[idx]
                existing_ab.id = f"agent.{slug}"
                existing_ab.name = f"{domain_label} {archetype_key.title()} Agent"
                existing_ab.semantic_view_id = existing_sv.id
                existing_ab.description = (
                    f"An agent that performs {archetype_key} tasks across "
                    f"the {domain_label} domains."
                )
            else:
                seen[key] = len(merged_svs)
                merged_svs.append(sv)
                merged_abs.append(ab)

        return merged_svs, merged_abs

    def generate(self) -> dict[str, Any]:
        """
        Main entry point.  Runs the full pipeline and returns a dict
        ready for json.dumps().

        Returns:
            {
                "semantic_view_blueprints": [...],
                "agent_blueprints": [...],
                "metadata": { "nuggets_analyzed": N, "domains_found": [...] }
            }
        """
        domain_index = self._index_by_domain()

        # Only iterate over non-shared domains
        target_domains = [
            d for d in domain_index if d not in SHARED_DOMAIN_TAGS
        ]

        sv_blueprints: list[SemanticViewBlueprint] = []
        agent_blueprints: list[AgentBlueprint] = []

        for domain in sorted(target_domains):
            skills, evals = self._collect_domain_cluster(domain, domain_index)

            for archetype in self.archetypes:
                match = self._match_archetype(archetype, skills, evals)
                if match is None:
                    continue

                matched_skills, matched_evals = match
                sv, ab = self._build_blueprints(
                    domain, archetype, matched_skills, matched_evals,
                )
                sv_blueprints.append(sv)
                agent_blueprints.append(ab)

        # Deduplicate blueprints that share the same nuggets across domains
        sv_blueprints, agent_blueprints = self._deduplicate_blueprints(
            sv_blueprints, agent_blueprints
        )

        return {
            "semantic_view_blueprints": [sv.to_dict() for sv in sv_blueprints],
            "agent_blueprints": [ab.to_dict() for ab in agent_blueprints],
            "metadata": {
                "nuggets_analyzed": len(self.nuggets),
                "skill_nuggets": len(self.skills),
                "evaluation_nuggets": len(self.evaluations),
                "domains_found": sorted(target_domains),
                "archetypes_checked": [a.key for a in self.archetypes],
            },
        }

    def generate_json(self, indent: int = 2) -> str:
        """Convenience: returns the result as a JSON string."""
        return json.dumps(self.generate(), indent=indent)

"""
Unit tests for the Agent Blueprint Generator.
"""

import json
import pytest

from app.blueprint_generator.models import (
    EvaluationExample,
    EvaluationNugget,
    SkillCategory,
    SkillNugget,
)
from app.blueprint_generator.engine import BlueprintEngine


# ---------------------------------------------------------------------------
# Fixtures — the five example nuggets from the spec
# ---------------------------------------------------------------------------

@pytest.fixture
def nugget_a():
    return SkillNugget(
        id="skill.query_mdlh",
        name="How to query MDLH",
        description="How to use MDLH to find relevant, certified assets.",
        domain_tags=["shared", "infra"],
        category=SkillCategory.DATA_ACCESS,
    )


@pytest.fixture
def nugget_b():
    return SkillNugget(
        id="skill.join_finance_data",
        name="How to join finance data",
        description="Core finance tables and joins for quarter-end metrics.",
        domain_tags=["finance"],
        category=SkillCategory.DOMAIN_LOGIC,
    )


@pytest.fixture
def nugget_c():
    return SkillNugget(
        id="skill.update_atlan_tags",
        name="How to update Atlan tags",
        description="Apply or update classifications on tables/columns.",
        domain_tags=["governance", "pii"],
        category=SkillCategory.MUTATION,
    )


@pytest.fixture
def nugget_d():
    return SkillNugget(
        id="skill.exec_finance_style",
        name="Executive financial reporting style",
        description="Titles as conclusions; numbers in millions.",
        domain_tags=["finance", "comms"],
        category=SkillCategory.STYLE,
    )


@pytest.fixture
def nugget_e():
    return EvaluationNugget(
        id="eval.pii_edge_cases",
        name="PII expectations & edge cases",
        description="Test cases for PII-related queries.",
        domain_tags=["governance", "pii"],
        examples=[
            EvaluationExample(
                input="Show last 10 customer emails",
                expected_output="Refusal or masked output",
                expectation_type="refusal",
            ),
            EvaluationExample(
                input="Count of customers with email addresses",
                expected_output="Aggregated count (allowed)",
                expectation_type="allowed",
            ),
        ],
    )


@pytest.fixture
def all_nuggets(nugget_a, nugget_b, nugget_c, nugget_d, nugget_e):
    return [nugget_a, nugget_b, nugget_c, nugget_d, nugget_e]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestBlueprintEngine:
    """Tests for the core engine logic."""

    def test_generates_two_blueprints_from_example_nuggets(self, all_nuggets):
        """The 5 example nuggets should produce exactly 2 agent blueprints."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        assert len(result["semantic_view_blueprints"]) == 2
        assert len(result["agent_blueprints"]) == 2

    def test_finance_reporting_blueprint_exists(self, all_nuggets):
        """Should produce a finance reporting agent."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        agent_types = {ab["agent_type"] for ab in result["agent_blueprints"]}
        assert "reporting" in agent_types

        reporting_agents = [
            ab for ab in result["agent_blueprints"]
            if ab["agent_type"] == "reporting"
        ]
        assert len(reporting_agents) == 1
        assert "finance" in reporting_agents[0]["name"].lower()

    def test_tagging_blueprint_exists(self, all_nuggets):
        """Should produce a governance/pii tagging agent."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        tagging_agents = [
            ab for ab in result["agent_blueprints"]
            if ab["agent_type"] == "tagging"
        ]
        assert len(tagging_agents) == 1

    def test_finance_blueprint_includes_correct_nuggets(self, all_nuggets):
        """Finance reporting view should include nuggets A, B, D."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        finance_svs = [
            sv for sv in result["semantic_view_blueprints"]
            if "finance" in sv["domains"]
        ]
        assert len(finance_svs) == 1

        nugget_ids = set(finance_svs[0]["nugget_ids"])
        assert "skill.query_mdlh" in nugget_ids
        assert "skill.join_finance_data" in nugget_ids
        assert "skill.exec_finance_style" in nugget_ids

    def test_tagging_blueprint_includes_correct_nuggets(self, all_nuggets):
        """Tagging view should include nuggets A, C, E."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        tagging_svs = [
            sv for sv in result["semantic_view_blueprints"]
            if "governance" in sv["domains"]
        ]
        assert len(tagging_svs) == 1

        nugget_ids = set(tagging_svs[0]["nugget_ids"])
        assert "skill.query_mdlh" in nugget_ids
        assert "skill.update_atlan_tags" in nugget_ids
        assert "eval.pii_edge_cases" in nugget_ids

    def test_tagging_blueprint_deduplicates_governance_and_pii(self, all_nuggets):
        """governance and pii should merge into one tagging blueprint."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        tagging_svs = [
            sv for sv in result["semantic_view_blueprints"]
            if sv["id"].endswith("_tagging")
        ]
        assert len(tagging_svs) == 1
        assert "governance" in tagging_svs[0]["domains"]
        assert "pii" in tagging_svs[0]["domains"]

    def test_evaluation_nuggets_attached_to_tagging_agent(self, all_nuggets):
        """The tagging agent should reference the PII eval nugget."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        tagging_agents = [
            ab for ab in result["agent_blueprints"]
            if ab["agent_type"] == "tagging"
        ]
        assert "eval.pii_edge_cases" in tagging_agents[0]["evaluation_nugget_ids"]

    def test_semantic_view_links_to_agent(self, all_nuggets):
        """Each agent's semantic_view_id should reference an existing SV."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        sv_ids = {sv["id"] for sv in result["semantic_view_blueprints"]}
        for ab in result["agent_blueprints"]:
            assert ab["semantic_view_id"] in sv_ids

    def test_no_blueprints_when_categories_insufficient(self, nugget_a):
        """A single DATA_ACCESS skill alone can't satisfy any archetype."""
        engine = BlueprintEngine(nuggets=[nugget_a])
        result = engine.generate()

        assert len(result["semantic_view_blueprints"]) == 0
        assert len(result["agent_blueprints"]) == 0

    def test_metadata_is_accurate(self, all_nuggets):
        """Metadata should reflect the input nuggets."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        result = engine.generate()

        meta = result["metadata"]
        assert meta["nuggets_analyzed"] == 5
        assert meta["skill_nuggets"] == 4
        assert meta["evaluation_nuggets"] == 1
        assert "finance" in meta["domains_found"]
        assert "governance" in meta["domains_found"]

    def test_output_is_valid_json(self, all_nuggets):
        """generate_json should return parseable JSON."""
        engine = BlueprintEngine(nuggets=all_nuggets)
        raw = engine.generate_json()
        parsed = json.loads(raw)

        assert "semantic_view_blueprints" in parsed
        assert "agent_blueprints" in parsed

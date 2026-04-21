"""Tests for the universal routing law — resolve.py + routing-law.yaml.

Validates all 19 rules, first-match ordering, partial property sets,
and the default fallback.
"""

from __future__ import annotations

from pathlib import Path

from resolve import _parse_yaml, load_law, resolve

LAW_PATH = Path(__file__).parent.parent / "routing-law.yaml"


def _law() -> dict:
    """Load the canonical routing law once per call (cheap, no caching needed)."""
    return load_law(LAW_PATH)


# ---------------------------------------------------------------------------
# YAML parser sanity
# ---------------------------------------------------------------------------


class TestYamlParser:
    def test_dimensions_present(self):
        law = _law()
        dims = law["dimensions"]
        assert "function" in dims
        assert "material" in dims
        assert "pattern" in dims
        assert "scope" in dims
        assert "security" in dims

    def test_dimension_values_are_lists(self):
        law = _law()
        for dim in ("function", "material", "pattern", "scope", "security"):
            assert isinstance(law["dimensions"][dim], list)
            assert len(law["dimensions"][dim]) > 0

    def test_rules_count(self):
        law = _law()
        assert len(law["rules"]) == 19

    def test_each_rule_has_required_fields(self):
        law = _law()
        for rule in law["rules"]:
            assert "id" in rule, f"Rule missing id: {rule}"
            assert "match" in rule, f"Rule {rule['id']} missing match"
            assert "target" in rule, f"Rule {rule['id']} missing target"

    def test_empty_match_parsed_as_dict(self):
        """Rule 19 has `match: {}` — must parse as an empty dict, not a string."""
        law = _law()
        rule_19 = law["rules"][-1]
        assert rule_19["id"] == "19"
        assert isinstance(rule_19["match"], dict)
        assert len(rule_19["match"]) == 0


# ---------------------------------------------------------------------------
# Individual rule matching (all 19 rules)
# ---------------------------------------------------------------------------


class TestRule1SovereignSecurity:
    def test_sovereign_routes_to_custodia(self):
        target, rid = resolve(_law(), security="sovereign")
        assert target == "custodia-securitatis"
        assert rid == 1

    def test_sovereign_overrides_all_other_properties(self):
        target, rid = resolve(
            _law(),
            security="sovereign",
            scope="personal",
            function="store",
            material="credential",
        )
        assert rid == 1


class TestRule2SecretSecurity:
    def test_secret_routes_to_custodia(self):
        target, rid = resolve(_law(), security="secret")
        assert target == "custodia-securitatis"
        assert rid == 2


class TestRule3PersonalDaemon:
    def test_personal_daemon(self):
        target, rid = resolve(_law(), scope="personal", pattern="daemon")
        assert target == "domus-semper-palingenesis"
        assert rid == 3


class TestRule4PersonalConfigFile:
    def test_personal_config(self):
        target, rid = resolve(_law(), scope="personal", pattern="config-file")
        assert target == "domus-semper-palingenesis/dot_config/"
        assert rid == 4


class TestRule5PersonalCredential:
    def test_personal_credential(self):
        target, rid = resolve(_law(), scope="personal", material="credential")
        assert target == "domus-semper-palingenesis"
        assert rid == 5


class TestRule6SystemGovern:
    def test_system_govern(self):
        target, rid = resolve(_law(), scope="system", function="govern")
        assert target == "organvm-engine"
        assert rid == 6


class TestRule7SystemValidate:
    def test_system_validate(self):
        target, rid = resolve(_law(), scope="system", function="validate")
        assert target == "schema-definitions"
        assert rid == 7


class TestRule8SystemClassify:
    def test_system_classify(self):
        target, rid = resolve(_law(), scope="system", function="classify")
        assert target == "organvm-ontologia"
        assert rid == 8


class TestRule9Transform:
    def test_transform(self):
        target, rid = resolve(_law(), function="transform")
        assert target == "alchemia-ingestvm"
        assert rid == 9

    def test_transform_regardless_of_scope(self):
        target, rid = resolve(_law(), function="transform", scope="organ")
        assert rid == 9


class TestRule10PromptStore:
    def test_prompt_store(self):
        target, rid = resolve(_law(), material="prompt", function="store")
        assert target == "organvm-corpvs-testamentvm/data/prompt-registry/"
        assert rid == 10


class TestRule11Atom:
    def test_atom(self):
        target, rid = resolve(_law(), material="atom")
        assert target == "organvm-corpvs-testamentvm/data/prompt-registry/"
        assert rid == 11

    def test_atom_with_function(self):
        target, rid = resolve(_law(), material="atom", function="store")
        assert rid == 11, "atom matches before prompt+store when material=atom"


class TestRule12EmailSort:
    def test_email_sort(self):
        target, rid = resolve(_law(), material="email", function="sort")
        assert target == "domus-semper-palingenesis"
        assert rid == 12


class TestRule13SystemDocument:
    def test_system_document(self):
        target, rid = resolve(_law(), material="document", scope="system")
        assert target == "organvm-corpvs-testamentvm"
        assert rid == 13


class TestRule14OrganApi:
    def test_organ_api(self):
        target, rid = resolve(_law(), scope="organ", pattern="api")
        assert target == "organ's ergon repo"
        assert rid == 14


class TestRule15OrganCli:
    def test_organ_cli(self):
        target, rid = resolve(_law(), scope="organ", pattern="cli")
        assert target == "organ's taxis repo"
        assert rid == 15


class TestRule16PublicDocument:
    def test_public_document(self):
        target, rid = resolve(_law(), scope="public", material="document")
        assert target == "organvm-v-logos"
        assert rid == 16


class TestRule17PublicApi:
    def test_public_api(self):
        target, rid = resolve(_law(), scope="public", pattern="api")
        assert target == "organvm-iii-ergon"
        assert rid == 17


class TestRule18RepoClassify:
    def test_repo_classify(self):
        target, rid = resolve(_law(), material="repo", function="classify")
        assert target == "organvm-engine"
        assert rid == 18


class TestRule19Default:
    def test_default_fallback(self):
        """Unrecognized combination falls through to intake."""
        target, rid = resolve(_law(), material="file", scope="organ", function="build")
        assert target == "~/Workspace/intake/"
        assert rid == 19


# ---------------------------------------------------------------------------
# First-match ordering — security beats scope
# ---------------------------------------------------------------------------


class TestFirstMatchOrdering:
    def test_security_sovereign_beats_personal_scope(self):
        """Sovereign security matches rule 1 even when personal scope would match rule 3-5."""
        target, rid = resolve(
            _law(),
            security="sovereign",
            scope="personal",
            pattern="daemon",
        )
        assert rid == 1, "security: sovereign must match before scope: personal"

    def test_security_secret_beats_system_classify(self):
        """Secret security matches rule 2 even with system + classify dimensions."""
        target, rid = resolve(
            _law(),
            security="secret",
            scope="system",
            function="classify",
        )
        assert rid == 2, "security: secret must match before scope: system + function: classify"

    def test_transform_beats_email_sort(self):
        """Rule 9 (transform) comes before rule 12 (email+sort)."""
        target, rid = resolve(
            _law(),
            function="transform",
            material="email",
        )
        assert rid == 9, "function: transform must match before material: email"

    def test_personal_daemon_beats_transform(self):
        """Rule 3 (personal+daemon) comes before rule 9 (transform)."""
        target, rid = resolve(
            _law(),
            scope="personal",
            pattern="daemon",
            function="transform",
        )
        assert rid == 3, "scope: personal + pattern: daemon must match before function: transform"


# ---------------------------------------------------------------------------
# Partial property sets
# ---------------------------------------------------------------------------


class TestPartialProperties:
    def test_single_dimension_function(self):
        target, rid = resolve(_law(), function="transform")
        assert rid == 9

    def test_single_dimension_material(self):
        target, rid = resolve(_law(), material="atom")
        assert rid == 11

    def test_single_dimension_security(self):
        target, rid = resolve(_law(), security="sovereign")
        assert rid == 1

    def test_two_dimensions(self):
        target, rid = resolve(_law(), scope="system", function="govern")
        assert rid == 6

    def test_extra_dimensions_ignored_for_match(self):
        """Extra dimensions beyond those in the match dict don't prevent matching."""
        target, rid = resolve(
            _law(),
            material="email",
            function="sort",
            scope="personal",
            pattern="daemon",
        )
        # Rule 3 matches first: scope=personal + pattern=daemon
        assert rid == 3

    def test_all_dimensions_supplied(self):
        target, rid = resolve(
            _law(),
            function="sort",
            material="email",
            pattern="cli",
            scope="personal",
            security="public",
        )
        # Rule 3 won't match (pattern=cli != daemon), rule 4 won't match (pattern=cli != config-file)
        # Rule 5 won't match (material=email != credential)
        # Rule 12 matches: material=email + function=sort
        assert rid == 12


# ---------------------------------------------------------------------------
# No false matches
# ---------------------------------------------------------------------------


class TestNoFalseMatches:
    def test_partial_rule_mismatch(self):
        """A rule requiring two keys doesn't match when only one is supplied."""
        # Rule 6 requires scope=system AND function=govern
        # Supplying only function=govern should NOT match rule 6
        target, rid = resolve(_law(), function="govern")
        # Should fall through to default (no single-key rule for function=govern)
        assert rid == 19

    def test_wrong_value_no_match(self):
        """Correct key but wrong value doesn't match."""
        target, rid = resolve(_law(), scope="system", function="sort")
        # No rule for system+sort — falls to default
        assert rid == 19

    def test_unknown_dimension_value(self):
        """Values not in the dimension lists still go through resolution."""
        target, rid = resolve(_law(), function="nonexistent_function")
        assert rid == 19

    def test_public_scope_without_qualifying_material(self):
        """Public scope alone (no material or pattern) hits default."""
        target, rid = resolve(_law(), scope="public")
        assert rid == 19

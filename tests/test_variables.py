"""Tests for the dynamic variable engine."""

import os

from ontologia.variables.inheritance import collect_overrides, find_in_chain, scope_chain
from ontologia.variables.resolution import ResolvedVariable, VariableStore
from ontologia.variables.variable import (
    Constraint,
    Mutability,
    Scope,
    Variable,
    VariableType,
)


# ── Variable model ──────────────────────────────────────────────────

class TestVariable:
    def test_roundtrip(self):
        var = Variable(
            key="TOTAL_REPOS",
            value=105,
            var_type=VariableType.INTEGER,
            scope=Scope.GLOBAL,
            description="Total repo count",
        )
        d = var.to_dict()
        restored = Variable.from_dict(d)
        assert restored.key == "TOTAL_REPOS"
        assert restored.value == 105
        assert restored.var_type == VariableType.INTEGER

    def test_with_constraint(self):
        var = Variable(
            key="TIER",
            value="flagship",
            constraint=Constraint(allowed_values=["flagship", "standard", "infrastructure"]),
        )
        d = var.to_dict()
        restored = Variable.from_dict(d)
        assert restored.constraint is not None
        assert restored.constraint.allowed_values == ["flagship", "standard", "infrastructure"]

    def test_defaults(self):
        var = Variable(key="x", value="y")
        assert var.var_type == VariableType.STRING
        assert var.mutability == Mutability.RUNTIME
        assert var.scope == Scope.GLOBAL


# ── Constraint validation ───────────────────────────────────────────

class TestConstraint:
    def test_allowed_values_pass(self):
        c = Constraint(allowed_values=["a", "b", "c"])
        ok, msg = c.validate("a")
        assert ok

    def test_allowed_values_fail(self):
        c = Constraint(allowed_values=["a", "b"])
        ok, msg = c.validate("z")
        assert not ok

    def test_min_value(self):
        c = Constraint(min_value=0)
        ok, _ = c.validate(-1)
        assert not ok
        ok, _ = c.validate(5)
        assert ok

    def test_max_value(self):
        c = Constraint(max_value=100)
        ok, _ = c.validate(101)
        assert not ok
        ok, _ = c.validate(50)
        assert ok

    def test_pattern(self):
        c = Constraint(pattern=r"^[A-Z]+$")
        ok, _ = c.validate("HELLO")
        assert ok
        ok, _ = c.validate("hello")
        assert not ok

    def test_required(self):
        c = Constraint(required=True)
        ok, _ = c.validate(None)
        assert not ok
        ok, _ = c.validate("present")
        assert ok

    def test_none_not_required(self):
        c = Constraint()
        ok, _ = c.validate(None)
        assert ok

    def test_roundtrip(self):
        c = Constraint(allowed_values=[1, 2], min_value=0, required=True)
        d = c.to_dict()
        restored = Constraint.from_dict(d)
        assert restored.allowed_values == [1, 2]
        assert restored.min_value == 0
        assert restored.required


# ── Scope chain / inheritance ───────────────────────────────────────

class TestScopeChain:
    def test_global(self):
        chain = scope_chain(Scope.GLOBAL)
        assert chain == [Scope.GLOBAL]

    def test_repo(self):
        chain = scope_chain(Scope.REPO)
        assert chain == [Scope.REPO, Scope.ORGAN, Scope.GLOBAL]

    def test_session(self):
        chain = scope_chain(Scope.SESSION)
        assert len(chain) == 6
        assert chain[0] == Scope.SESSION
        assert chain[-1] == Scope.GLOBAL


class TestFindInChain:
    def test_direct_match(self):
        variables = {
            "X": [Variable(key="X", value="repo-val", scope=Scope.REPO)],
        }
        result = find_in_chain(variables, "X", Scope.REPO)
        assert result is not None
        assert result.value == "repo-val"

    def test_inherited(self):
        variables = {
            "X": [Variable(key="X", value="global-val", scope=Scope.GLOBAL)],
        }
        result = find_in_chain(variables, "X", Scope.REPO)
        assert result is not None
        assert result.value == "global-val"

    def test_override_wins(self):
        variables = {
            "X": [
                Variable(key="X", value="global", scope=Scope.GLOBAL),
                Variable(key="X", value="repo-override", scope=Scope.REPO),
            ],
        }
        result = find_in_chain(variables, "X", Scope.REPO)
        assert result.value == "repo-override"

    def test_not_found(self):
        result = find_in_chain({}, "MISSING", Scope.REPO)
        assert result is None

    def test_entity_scoped(self):
        variables = {
            "X": [
                Variable(key="X", value="organ-a", scope=Scope.ORGAN, entity_id="ent_organ_A"),
                Variable(key="X", value="organ-b", scope=Scope.ORGAN, entity_id="ent_organ_B"),
            ],
        }
        result = find_in_chain(
            variables, "X", Scope.ORGAN,
            entity_chain=["ent_organ_B"],
        )
        assert result is not None
        assert result.value == "organ-b"


class TestCollectOverrides:
    def test_multiple_scopes(self):
        variables = {
            "X": [
                Variable(key="X", value="g", scope=Scope.GLOBAL),
                Variable(key="X", value="o", scope=Scope.ORGAN),
                Variable(key="X", value="r", scope=Scope.REPO),
            ],
        }
        overrides = collect_overrides(variables, "X", Scope.REPO)
        assert len(overrides) == 3
        assert overrides[0].scope == Scope.REPO  # narrowest first


# ── Variable store + resolution ─────────────────────────────────────

class TestVariableStore:
    def test_set_and_get(self):
        store = VariableStore()
        var = Variable(key="COUNT", value=42, var_type=VariableType.INTEGER)
        ok, _ = store.set(var)
        assert ok
        assert store.get("COUNT") is not None

    def test_set_validates_constraint(self):
        store = VariableStore()
        var = Variable(
            key="TIER",
            value="invalid",
            constraint=Constraint(allowed_values=["flagship", "standard"]),
        )
        ok, msg = store.set(var)
        assert not ok
        assert "not in allowed values" in msg

    def test_constant_immutable(self):
        store = VariableStore()
        store.set(Variable(key="PI", value=3.14, mutability=Mutability.CONSTANT))
        ok, msg = store.set(Variable(key="PI", value=3.15, mutability=Mutability.CONSTANT))
        assert not ok
        assert "constant" in msg

    def test_resolve_direct(self):
        store = VariableStore()
        store.set(Variable(key="X", value="hello", scope=Scope.GLOBAL))
        result = store.resolve("X")
        assert result.value == "hello"
        assert result.resolved_from == "direct"

    def test_resolve_inherited(self):
        store = VariableStore()
        store.set(Variable(key="X", value="from-global", scope=Scope.GLOBAL))
        result = store.resolve("X", scope=Scope.REPO)
        assert result.value == "from-global"
        assert result.resolved_from == "inherited"

    def test_resolve_default(self):
        store = VariableStore()
        result = store.resolve("MISSING", default="fallback")
        assert result.value == "fallback"
        assert result.resolved_from == "default"

    def test_resolve_environment(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "from-env")
        store = VariableStore()
        store.set(Variable(
            key="MY_VAR",
            value="TEST_VAR",
            mutability=Mutability.ENVIRONMENT,
        ))
        result = store.resolve("MY_VAR")
        assert result.value == "from-env"
        assert result.resolved_from == "environment"

    def test_resolve_reference(self):
        store = VariableStore()
        store.set(Variable(key="BASE", value=100, scope=Scope.GLOBAL))
        store.set(Variable(
            key="ALIAS",
            value="BASE",
            mutability=Mutability.REFERENCE,
            scope=Scope.GLOBAL,
        ))
        result = store.resolve("ALIAS")
        assert result.value == 100
        assert result.resolved_from == "reference"

    def test_list_keys(self):
        store = VariableStore()
        store.set(Variable(key="B", value=2))
        store.set(Variable(key="A", value=1))
        assert store.list_keys() == ["A", "B"]

    def test_list_at_scope(self):
        store = VariableStore()
        store.set(Variable(key="G1", value=1, scope=Scope.GLOBAL))
        store.set(Variable(key="G2", value=2, scope=Scope.GLOBAL))
        store.set(Variable(key="R1", value=3, scope=Scope.REPO))
        globals_ = store.list_at_scope(Scope.GLOBAL)
        assert len(globals_) == 2

    def test_delete(self):
        store = VariableStore()
        store.set(Variable(key="X", value=1))
        assert store.delete("X", Scope.GLOBAL)
        assert store.get("X") is None

    def test_delete_constant_fails(self):
        store = VariableStore()
        store.set(Variable(key="PI", value=3.14, mutability=Mutability.CONSTANT))
        assert not store.delete("PI", Scope.GLOBAL)

    def test_delete_not_found(self):
        store = VariableStore()
        assert not store.delete("MISSING", Scope.GLOBAL)

    def test_serialization_roundtrip(self):
        store = VariableStore()
        store.set(Variable(key="A", value=1, scope=Scope.GLOBAL))
        store.set(Variable(key="B", value="x", scope=Scope.REPO, entity_id="r1"))
        data = store.to_list()
        restored = VariableStore.from_list(data)
        assert restored.get("A") is not None
        assert restored.get("B", scope=Scope.REPO, entity_id="r1") is not None

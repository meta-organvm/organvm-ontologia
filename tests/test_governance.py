"""Tests for the governance layer: policies, revision, signatures."""

import pytest

from ontologia.governance.policies import (
    EvolutionPolicy,
    PolicyAction,
    PolicyCondition,
    evaluate_policies,
)
from ontologia.governance.revision import (
    Evidence,
    Revision,
    RevisionStatus,
    create_revision,
)
from ontologia.governance.signatures import (
    StructuralSignature,
    compute_structural_signature,
    signatures_differ,
)
from ontologia.structure.edges import EdgeIndex, HierarchyEdge, RelationEdge


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------

class TestPolicyCondition:
    def test_eq(self):
        c = PolicyCondition("status", "eq", "active")
        assert c.evaluate("active") is True
        assert c.evaluate("deprecated") is False

    def test_ne(self):
        c = PolicyCondition("status", "ne", "active")
        assert c.evaluate("deprecated") is True
        assert c.evaluate("active") is False

    def test_gt_lt(self):
        assert PolicyCondition("count", "gt", 5).evaluate(10) is True
        assert PolicyCondition("count", "gt", 5).evaluate(3) is False
        assert PolicyCondition("count", "lt", 5).evaluate(3) is True
        assert PolicyCondition("count", "lt", 5).evaluate(10) is False

    def test_gte_lte(self):
        assert PolicyCondition("x", "gte", 5).evaluate(5) is True
        assert PolicyCondition("x", "gte", 5).evaluate(4) is False
        assert PolicyCondition("x", "lte", 5).evaluate(5) is True
        assert PolicyCondition("x", "lte", 5).evaluate(6) is False

    def test_in_not_in(self):
        assert PolicyCondition("t", "in", ["a", "b"]).evaluate("a") is True
        assert PolicyCondition("t", "in", ["a", "b"]).evaluate("c") is False
        assert PolicyCondition("t", "not_in", ["a"]).evaluate("b") is True
        assert PolicyCondition("t", "not_in", ["a"]).evaluate("a") is False

    def test_contains(self):
        assert PolicyCondition("name", "contains", "test").evaluate("my-test-repo") is True
        assert PolicyCondition("name", "contains", "test").evaluate("production") is False

    def test_unknown_operator(self):
        c = PolicyCondition("x", "bogus", 1)
        assert c.evaluate(1) is False

    def test_serialization_roundtrip(self):
        c = PolicyCondition("status", "eq", "active")
        d = c.to_dict()
        c2 = PolicyCondition.from_dict(d)
        assert c2.field == "status"
        assert c2.operator == "eq"
        assert c2.value == "active"


class TestEvolutionPolicy:
    def test_evaluate_all_conditions_met(self):
        p = EvolutionPolicy(
            policy_id="p1",
            name="test",
            conditions=[
                PolicyCondition("status", "eq", "active"),
                PolicyCondition("count", "gt", 3),
            ],
        )
        assert p.evaluate({"status": "active", "count": 5}) is True

    def test_evaluate_partial_conditions(self):
        p = EvolutionPolicy(
            policy_id="p1",
            name="test",
            conditions=[
                PolicyCondition("status", "eq", "active"),
                PolicyCondition("count", "gt", 10),
            ],
        )
        assert p.evaluate({"status": "active", "count": 5}) is False

    def test_disabled_policy(self):
        p = EvolutionPolicy(
            policy_id="p1",
            name="test",
            enabled=False,
            conditions=[PolicyCondition("x", "eq", 1)],
        )
        assert p.evaluate({"x": 1}) is False

    def test_serialization_roundtrip(self):
        p = EvolutionPolicy(
            policy_id="p1",
            name="volatility check",
            description="flag volatile entities",
            conditions=[PolicyCondition("changes", "gt", 10)],
            action=PolicyAction.FLAG,
            scope_entity_type="repo",
            priority=5,
        )
        d = p.to_dict()
        p2 = EvolutionPolicy.from_dict(d)
        assert p2.policy_id == "p1"
        assert p2.name == "volatility check"
        assert p2.action == PolicyAction.FLAG
        assert p2.scope_entity_type == "repo"
        assert p2.priority == 5
        assert len(p2.conditions) == 1


class TestEvaluatePolicies:
    def test_evaluates_all(self):
        policies = [
            EvolutionPolicy(
                policy_id="p1", name="a",
                conditions=[PolicyCondition("x", "gt", 5)],
                priority=1,
            ),
            EvolutionPolicy(
                policy_id="p2", name="b",
                conditions=[PolicyCondition("x", "gt", 3)],
                priority=2,
            ),
        ]
        triggered = evaluate_policies(policies, {"x": 10})
        assert len(triggered) == 2
        # Higher priority first
        assert triggered[0].policy_id == "p2"
        assert triggered[1].policy_id == "p1"

    def test_scope_filter(self):
        policies = [
            EvolutionPolicy(
                policy_id="p1", name="repo-only",
                conditions=[PolicyCondition("x", "eq", 1)],
                scope_entity_type="repo",
            ),
        ]
        assert evaluate_policies(policies, {"x": 1}, entity_type="repo") != []
        assert evaluate_policies(policies, {"x": 1}, entity_type="organ") == []

    def test_no_triggers(self):
        policies = [
            EvolutionPolicy(
                policy_id="p1", name="a",
                conditions=[PolicyCondition("x", "gt", 100)],
            ),
        ]
        assert evaluate_policies(policies, {"x": 1}) == []


# ---------------------------------------------------------------------------
# Revision
# ---------------------------------------------------------------------------

class TestRevision:
    def test_create_revision(self):
        rev = create_revision("Fix naming conflict", action="rename", affected_entities=["e1"])
        assert rev.revision_id.startswith("rev_")
        assert rev.status == RevisionStatus.DETECTED
        assert rev.action == "rename"

    def test_valid_transitions(self):
        rev = create_revision("test")
        assert rev.transition(RevisionStatus.ACCUMULATING) is True
        assert rev.status == RevisionStatus.ACCUMULATING
        assert rev.transition(RevisionStatus.EVALUATING) is True
        assert rev.transition(RevisionStatus.PROPOSED) is True
        assert rev.transition(RevisionStatus.APPROVED) is True
        assert rev.transition(RevisionStatus.APPLIED) is True

    def test_invalid_transition(self):
        rev = create_revision("test")
        # Can't go directly from DETECTED to APPLIED
        assert rev.transition(RevisionStatus.APPLIED) is False
        assert rev.status == RevisionStatus.DETECTED

    def test_cancel_transition(self):
        rev = create_revision("test")
        assert rev.transition(RevisionStatus.CANCELLED) is True
        # Can't transition from CANCELLED
        assert rev.transition(RevisionStatus.DETECTED) is False

    def test_reject_transition(self):
        rev = create_revision("test")
        rev.transition(RevisionStatus.ACCUMULATING)
        rev.transition(RevisionStatus.EVALUATING)
        assert rev.transition(RevisionStatus.REJECTED) is True
        # Terminal state
        assert rev.transition(RevisionStatus.PROPOSED) is False

    def test_add_evidence(self):
        rev = create_revision("test")
        e = Evidence(evidence_type="metric", description="high volatility", data={"count": 15})
        rev.add_evidence(e)
        assert len(rev.evidence) == 1
        assert rev.evidence[0].evidence_type == "metric"

    def test_serialization_roundtrip(self):
        rev = create_revision("test rename", action="rename", affected_entities=["e1", "e2"])
        rev.add_evidence(Evidence(evidence_type="policy_trigger", description="naming conflict"))
        rev.transition(RevisionStatus.ACCUMULATING)

        d = rev.to_dict()
        rev2 = Revision.from_dict(d)
        assert rev2.revision_id == rev.revision_id
        assert rev2.status == RevisionStatus.ACCUMULATING
        assert len(rev2.evidence) == 1
        assert rev2.affected_entities == ["e1", "e2"]


# ---------------------------------------------------------------------------
# Signatures
# ---------------------------------------------------------------------------

class TestStructuralSignature:
    def test_compute_fingerprint(self):
        sig = StructuralSignature(
            entity_id="e1",
            parent_id="root",
            child_count=3,
            outgoing_relation_count=2,
            incoming_relation_count=1,
            depth=1,
        )
        fp = sig.compute_fingerprint()
        assert len(fp) == 16
        assert fp == sig.fingerprint

    def test_deterministic(self):
        """Same inputs produce same fingerprint."""
        sig1 = StructuralSignature("e1", "root", 3, 2, 1, 1)
        sig2 = StructuralSignature("e1", "root", 3, 2, 1, 1)
        sig1.compute_fingerprint()
        sig2.compute_fingerprint()
        assert sig1.fingerprint == sig2.fingerprint

    def test_different_inputs(self):
        sig1 = StructuralSignature("e1", "root", 3, 2, 1, 1)
        sig2 = StructuralSignature("e1", "root", 4, 2, 1, 1)  # different child_count
        sig1.compute_fingerprint()
        sig2.compute_fingerprint()
        assert sig1.fingerprint != sig2.fingerprint

    def test_to_dict(self):
        sig = StructuralSignature("e1", "root", 3, 2, 1, 1)
        sig.compute_fingerprint()
        d = sig.to_dict()
        assert d["entity_id"] == "e1"
        assert d["fingerprint"] == sig.fingerprint


class TestComputeStructuralSignature:
    def test_leaf_node(self):
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge("root", "child", "2025-01-01T00:00:00Z"))
        sig = compute_structural_signature("child", idx)
        assert sig.entity_id == "child"
        assert sig.parent_id == "root"
        assert sig.child_count == 0
        assert sig.depth == 1
        assert sig.fingerprint  # not empty

    def test_root_node(self):
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge("root", "a", "2025-01-01T00:00:00Z"))
        idx.add_hierarchy(HierarchyEdge("root", "b", "2025-01-01T00:00:00Z"))
        sig = compute_structural_signature("root", idx)
        assert sig.parent_id is None
        assert sig.child_count == 2
        assert sig.depth == 0

    def test_deep_node(self):
        idx = EdgeIndex()
        idx.add_hierarchy(HierarchyEdge("root", "l1", "2025-01-01T00:00:00Z"))
        idx.add_hierarchy(HierarchyEdge("l1", "l2", "2025-01-01T00:00:00Z"))
        idx.add_hierarchy(HierarchyEdge("l2", "l3", "2025-01-01T00:00:00Z"))
        sig = compute_structural_signature("l3", idx)
        assert sig.depth == 3


class TestSignaturesDiffer:
    def test_same(self):
        sig1 = StructuralSignature("e1", "root", 3, 2, 1, 1)
        sig2 = StructuralSignature("e1", "root", 3, 2, 1, 1)
        sig1.compute_fingerprint()
        sig2.compute_fingerprint()
        assert signatures_differ(sig1, sig2) is False

    def test_different(self):
        sig1 = StructuralSignature("e1", "root", 3, 2, 1, 1)
        sig2 = StructuralSignature("e1", "root", 5, 2, 1, 1)
        sig1.compute_fingerprint()
        sig2.compute_fingerprint()
        assert signatures_differ(sig1, sig2) is True

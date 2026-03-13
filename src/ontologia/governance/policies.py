"""Evolution policies — rules governing when and how structure changes.

Policies define trigger conditions, required evidence, scope constraints,
and the actions to take when conditions are met. They are the declarative
rules that the revision workflow evaluates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PolicyAction(str, Enum):
    """Actions a policy can recommend."""

    RENAME = "rename"
    RELOCATE = "relocate"
    MERGE = "merge"
    SPLIT = "split"
    DEPRECATE = "deprecate"
    PROMOTE = "promote"
    FLAG = "flag"
    NOTIFY = "notify"


@dataclass
class PolicyCondition:
    """A condition that must be met to trigger a policy."""

    field: str
    operator: str  # "eq", "ne", "gt", "lt", "gte", "lte", "in", "not_in", "contains"
    value: Any

    def evaluate(self, actual: Any) -> bool:
        """Evaluate this condition against an actual value."""
        if self.operator == "eq":
            return actual == self.value
        if self.operator == "ne":
            return actual != self.value
        if self.operator == "gt":
            return actual > self.value
        if self.operator == "lt":
            return actual < self.value
        if self.operator == "gte":
            return actual >= self.value
        if self.operator == "lte":
            return actual <= self.value
        if self.operator == "in":
            return actual in self.value
        if self.operator == "not_in":
            return actual not in self.value
        if self.operator == "contains":
            return self.value in actual
        return False

    def to_dict(self) -> dict[str, Any]:
        return {"field": self.field, "operator": self.operator, "value": self.value}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PolicyCondition:
        return cls(field=data["field"], operator=data["operator"], value=data["value"])


@dataclass
class EvolutionPolicy:
    """A rule governing structural evolution."""

    policy_id: str
    name: str
    description: str = ""
    conditions: list[PolicyCondition] = field(default_factory=list)
    action: PolicyAction = PolicyAction.FLAG
    scope_entity_type: str | None = None  # restrict to certain entity types
    priority: int = 0  # higher = evaluated first
    enabled: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def evaluate(self, entity_state: dict[str, Any]) -> bool:
        """Check if all conditions are met for a given entity state."""
        if not self.enabled:
            return False
        return all(c.evaluate(entity_state.get(c.field)) for c in self.conditions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "name": self.name,
            "description": self.description,
            "conditions": [c.to_dict() for c in self.conditions],
            "action": self.action.value,
            "scope_entity_type": self.scope_entity_type,
            "priority": self.priority,
            "enabled": self.enabled,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EvolutionPolicy:
        return cls(
            policy_id=data["policy_id"],
            name=data["name"],
            description=data.get("description", ""),
            conditions=[PolicyCondition.from_dict(c) for c in data.get("conditions", [])],
            action=PolicyAction(data.get("action", "flag")),
            scope_entity_type=data.get("scope_entity_type"),
            priority=data.get("priority", 0),
            enabled=data.get("enabled", True),
            metadata=data.get("metadata", {}),
        )


def evaluate_policies(
    policies: list[EvolutionPolicy],
    entity_state: dict[str, Any],
    entity_type: str | None = None,
) -> list[EvolutionPolicy]:
    """Evaluate all applicable policies against an entity state.

    Returns the list of policies whose conditions are all met,
    sorted by priority (highest first).
    """
    triggered: list[EvolutionPolicy] = []
    for policy in policies:
        if policy.scope_entity_type and entity_type != policy.scope_entity_type:
            continue
        if policy.evaluate(entity_state):
            triggered.append(policy)
    return sorted(triggered, key=lambda p: -p.priority)

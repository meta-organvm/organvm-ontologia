"""State snapshots — frozen view of entity state at a point in time.

A snapshot captures everything known about an entity at a moment:
its properties, metric values, structural signature, and variable
bindings. Snapshots are the unit of comparison for drift detection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ontologia._ulid import generate_ulid


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StateSnapshot:
    """Frozen view of an entity's state at a specific time."""

    snapshot_id: str
    entity_id: str
    timestamp: str
    properties: dict[str, Any] = field(default_factory=dict)
    metric_values: dict[str, float] = field(default_factory=dict)
    structural_fingerprint: str = ""
    variable_bindings: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "snapshot_id": self.snapshot_id,
            "entity_id": self.entity_id,
            "timestamp": self.timestamp,
        }
        if self.properties:
            d["properties"] = self.properties
        if self.metric_values:
            d["metric_values"] = self.metric_values
        if self.structural_fingerprint:
            d["structural_fingerprint"] = self.structural_fingerprint
        if self.variable_bindings:
            d["variable_bindings"] = self.variable_bindings
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StateSnapshot:
        return cls(
            snapshot_id=data["snapshot_id"],
            entity_id=data["entity_id"],
            timestamp=data.get("timestamp", ""),
            properties=data.get("properties", {}),
            metric_values=data.get("metric_values", {}),
            structural_fingerprint=data.get("structural_fingerprint", ""),
            variable_bindings=data.get("variable_bindings", {}),
            metadata=data.get("metadata", {}),
        )


def create_snapshot(
    entity_id: str,
    properties: dict[str, Any] | None = None,
    metric_values: dict[str, float] | None = None,
    structural_fingerprint: str = "",
    variable_bindings: dict[str, Any] | None = None,
) -> StateSnapshot:
    """Create a new snapshot of an entity's current state."""
    return StateSnapshot(
        snapshot_id=f"snap_{generate_ulid()}",
        entity_id=entity_id,
        timestamp=_now_iso(),
        properties=properties or {},
        metric_values=metric_values or {},
        structural_fingerprint=structural_fingerprint,
        variable_bindings=variable_bindings or {},
    )


def compare_snapshots(
    a: StateSnapshot,
    b: StateSnapshot,
) -> dict[str, Any]:
    """Compare two snapshots and return a diff.

    Returns a dict with keys: changed_properties, changed_metrics,
    fingerprint_changed, changed_variables.
    """
    diff: dict[str, Any] = {
        "entity_id": a.entity_id,
        "from_snapshot": a.snapshot_id,
        "to_snapshot": b.snapshot_id,
        "from_timestamp": a.timestamp,
        "to_timestamp": b.timestamp,
        "changed_properties": {},
        "changed_metrics": {},
        "fingerprint_changed": False,
        "changed_variables": {},
    }

    # Property diff
    all_prop_keys = set(a.properties) | set(b.properties)
    for key in sorted(all_prop_keys):
        old_val = a.properties.get(key)
        new_val = b.properties.get(key)
        if old_val != new_val:
            diff["changed_properties"][key] = {"old": old_val, "new": new_val}

    # Metric diff
    all_metric_keys = set(a.metric_values) | set(b.metric_values)
    for key in sorted(all_metric_keys):
        old_val = a.metric_values.get(key)
        new_val = b.metric_values.get(key)
        if old_val != new_val:
            diff["changed_metrics"][key] = {"old": old_val, "new": new_val}

    # Fingerprint
    diff["fingerprint_changed"] = a.structural_fingerprint != b.structural_fingerprint

    # Variable diff
    all_var_keys = set(a.variable_bindings) | set(b.variable_bindings)
    for key in sorted(all_var_keys):
        old_val = a.variable_bindings.get(key)
        new_val = b.variable_bindings.get(key)
        if old_val != new_val:
            diff["changed_variables"][key] = {"old": old_val, "new": new_val}

    return diff


def has_drift(a: StateSnapshot, b: StateSnapshot) -> bool:
    """Quick check: has anything changed between two snapshots?"""
    if a.structural_fingerprint != b.structural_fingerprint:
        return True
    if a.properties != b.properties:
        return True
    if a.metric_values != b.metric_values:
        return True
    return a.variable_bindings != b.variable_bindings

"""Change detection — compare states to find what changed.

Detectors compare two states (previous vs current) and emit normalized
changes. They work with any data that can be represented as dicts.
"""

from __future__ import annotations

from typing import Any

from ontologia.sensing.interfaces import ChangeType, NormalizedChange


def detect_state_changes(
    entity_id: str,
    previous: dict[str, Any],
    current: dict[str, Any],
    sensor_name: str = "state_detector",
) -> list[NormalizedChange]:
    """Compare two entity state dicts and detect property changes."""
    changes: list[NormalizedChange] = []
    all_keys = set(previous.keys()) | set(current.keys())

    for key in sorted(all_keys):
        prev_val = previous.get(key)
        curr_val = current.get(key)
        if prev_val != curr_val:
            changes.append(NormalizedChange(
                change_type=ChangeType.STATE,
                entity_id=entity_id,
                property_name=key,
                previous_value=prev_val,
                new_value=curr_val,
                source_sensor=sensor_name,
            ))
    return changes


def detect_relation_changes(
    entity_id: str,
    previous_edges: set[str],
    current_edges: set[str],
    sensor_name: str = "relation_detector",
) -> list[NormalizedChange]:
    """Detect added/removed relations for an entity."""
    changes: list[NormalizedChange] = []

    for added in sorted(current_edges - previous_edges):
        changes.append(NormalizedChange(
            change_type=ChangeType.RELATION,
            entity_id=entity_id,
            property_name="relation_added",
            new_value=added,
            source_sensor=sensor_name,
        ))

    for removed in sorted(previous_edges - current_edges):
        changes.append(NormalizedChange(
            change_type=ChangeType.RELATION,
            entity_id=entity_id,
            property_name="relation_removed",
            previous_value=removed,
            source_sensor=sensor_name,
        ))

    return changes


def detect_anomaly(
    entity_id: str,
    metric_value: float,
    expected_range: tuple[float, float],
    sensor_name: str = "anomaly_detector",
) -> NormalizedChange | None:
    """Detect if a metric value falls outside the expected range."""
    low, high = expected_range
    if metric_value < low or metric_value > high:
        return NormalizedChange(
            change_type=ChangeType.ANOMALY,
            entity_id=entity_id,
            property_name="metric_anomaly",
            new_value=metric_value,
            confidence=min(1.0, abs(metric_value - (low + high) / 2) / max(high - low, 1)),
            source_sensor=sensor_name,
        )
    return None

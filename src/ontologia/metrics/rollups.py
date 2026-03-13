"""Hierarchical metric aggregation — roll up child values to parents.

Given a hierarchy (organ→repo→module) and observations at the leaf level,
compute aggregated values at each parent level using the metric's
aggregation policy (sum, avg, max, min, count, latest).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ontologia.metrics.metric import AggregationPolicy, MetricDefinition
from ontologia.metrics.observations import ObservationStore
from ontologia.structure.edges import EdgeIndex


@dataclass
class RollupResult:
    """Aggregated metric value for an entity."""

    entity_id: str
    metric_id: str
    value: float
    child_count: int
    aggregation: AggregationPolicy

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "metric_id": self.metric_id,
            "value": self.value,
            "child_count": self.child_count,
            "aggregation": self.aggregation.value,
        }


def _aggregate(values: list[float], policy: AggregationPolicy) -> float:
    """Apply aggregation policy to a list of values."""
    if not values:
        return 0.0

    if policy == AggregationPolicy.SUM:
        return sum(values)
    if policy == AggregationPolicy.AVG:
        return sum(values) / len(values)
    if policy == AggregationPolicy.MAX:
        return max(values)
    if policy == AggregationPolicy.MIN:
        return min(values)
    if policy == AggregationPolicy.COUNT:
        return float(len(values))
    if policy == AggregationPolicy.LATEST:
        return values[-1]
    return 0.0


def rollup_for_entity(
    entity_id: str,
    metric: MetricDefinition,
    edge_index: EdgeIndex,
    obs_store: ObservationStore,
    at: str | None = None,
) -> RollupResult:
    """Compute an aggregated metric value for an entity from its children.

    Finds all active children in the hierarchy, gets the latest observation
    for each child, and aggregates using the metric's policy.
    """
    children = edge_index.children(entity_id, at=at)
    values: list[float] = []

    for child_edge in children:
        latest = obs_store.latest(metric.metric_id, child_edge.child_id)
        if latest is not None:
            values.append(latest.value)

    return RollupResult(
        entity_id=entity_id,
        metric_id=metric.metric_id,
        value=_aggregate(values, metric.aggregation),
        child_count=len(values),
        aggregation=metric.aggregation,
    )


def rollup_tree(
    root_id: str,
    metric: MetricDefinition,
    edge_index: EdgeIndex,
    obs_store: ObservationStore,
    at: str | None = None,
) -> dict[str, RollupResult]:
    """Compute rollups for an entire subtree, bottom-up.

    Returns a dict mapping entity_id → RollupResult for every non-leaf
    entity in the subtree (including the root).
    """
    results: dict[str, RollupResult] = {}

    def _rollup(entity_id: str) -> float:
        children = edge_index.children(entity_id, at=at)
        if not children:
            # Leaf: use latest observation directly
            latest = obs_store.latest(metric.metric_id, entity_id)
            return latest.value if latest else 0.0

        child_values: list[float] = []
        for child_edge in children:
            child_val = _rollup(child_edge.child_id)
            child_values.append(child_val)

        agg_value = _aggregate(child_values, metric.aggregation)
        results[entity_id] = RollupResult(
            entity_id=entity_id,
            metric_id=metric.metric_id,
            value=agg_value,
            child_count=len(child_values),
            aggregation=metric.aggregation,
        )
        return agg_value

    _rollup(root_id)
    return results

"""Metric definitions — formal metric types with metadata.

Six metric types model different measurement patterns:
- gauge: current value (e.g., repo count)
- counter: monotonically increasing (e.g., total commits)
- delta: change since last observation
- rolling: windowed computation (e.g., 7-day average)
- distribution: histogram/percentiles
- ratio: derived from two other metrics
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ontologia._ulid import generate_ulid


class MetricType(str, Enum):
    GAUGE = "gauge"
    COUNTER = "counter"
    DELTA = "delta"
    ROLLING = "rolling"
    DISTRIBUTION = "distribution"
    RATIO = "ratio"


class AggregationPolicy(str, Enum):
    """How child metrics roll up to parent."""

    SUM = "sum"
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    COUNT = "count"
    LATEST = "latest"
    NONE = "none"


@dataclass
class MetricDefinition:
    """Schema for a metric — what it measures and how it aggregates."""

    metric_id: str
    name: str
    metric_type: MetricType
    unit: str = ""
    description: str = ""
    aggregation: AggregationPolicy = AggregationPolicy.SUM
    entity_type_scope: str | None = None  # which entity types this applies to
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "metric_id": self.metric_id,
            "name": self.name,
            "metric_type": self.metric_type.value,
            "aggregation": self.aggregation.value,
        }
        if self.unit:
            d["unit"] = self.unit
        if self.description:
            d["description"] = self.description
        if self.entity_type_scope:
            d["entity_type_scope"] = self.entity_type_scope
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MetricDefinition:
        return cls(
            metric_id=data["metric_id"],
            name=data["name"],
            metric_type=MetricType(data.get("metric_type", "gauge")),
            unit=data.get("unit", ""),
            description=data.get("description", ""),
            aggregation=AggregationPolicy(data.get("aggregation", "sum")),
            entity_type_scope=data.get("entity_type_scope"),
            metadata=data.get("metadata", {}),
        )


def create_metric(
    name: str,
    metric_type: MetricType = MetricType.GAUGE,
    unit: str = "",
    description: str = "",
    aggregation: AggregationPolicy = AggregationPolicy.SUM,
) -> MetricDefinition:
    """Create a new metric definition with a fresh ID."""
    return MetricDefinition(
        metric_id=f"met_{generate_ulid()}",
        name=name,
        metric_type=metric_type,
        unit=unit,
        description=description,
        aggregation=aggregation,
    )

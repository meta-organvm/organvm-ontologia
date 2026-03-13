"""Sensing interfaces — Protocol classes for change detection sources.

Sensors are pluggable observers that watch different parts of the system
(filesystem, git, registry) and emit normalized events when they detect
changes. Each sensor implements the Sensor protocol.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class ChangeType(str, Enum):
    """Types of detected changes."""

    STATE = "state"           # entity property changed
    RELATION = "relation"     # edge added/removed/modified
    HIERARCHY = "hierarchy"   # structural position changed
    SEMANTIC = "semantic"     # meaning/content changed
    ANOMALY = "anomaly"       # unexpected deviation


@dataclass
class RawSignal:
    """A raw observation from a sensor before normalization."""

    sensor_name: str
    signal_type: str
    entity_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""
    confidence: float = 1.0  # 0.0 to 1.0


@dataclass
class NormalizedChange:
    """A normalized change event ready for the event bus."""

    change_type: ChangeType
    entity_id: str
    property_name: str | None = None
    previous_value: Any = None
    new_value: Any = None
    confidence: float = 1.0
    source_sensor: str = ""
    timestamp: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "change_type": self.change_type.value,
            "entity_id": self.entity_id,
            "confidence": self.confidence,
            "source_sensor": self.source_sensor,
        }
        if self.property_name:
            d["property_name"] = self.property_name
        if self.previous_value is not None:
            d["previous_value"] = self.previous_value
        if self.new_value is not None:
            d["new_value"] = self.new_value
        if self.timestamp:
            d["timestamp"] = self.timestamp
        return d


@runtime_checkable
class Sensor(Protocol):
    """Protocol for change detection sensors."""

    @property
    def name(self) -> str: ...

    def scan(self) -> list[RawSignal]:
        """Scan for changes and return raw signals."""
        ...

    def is_available(self) -> bool:
        """Check if this sensor can operate (dependencies met)."""
        ...

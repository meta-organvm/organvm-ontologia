"""Inference dispatch — route analysis tasks to specialized detectors."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class InferenceType(str, Enum):
    CLUSTER = "cluster"
    INSTABILITY = "instability"
    CONVERGENCE = "convergence"
    EMERGENCE = "emergence"


@dataclass
class InferenceResult:
    """Result of an inference computation."""

    inference_type: InferenceType
    entity_ids: list[str]
    score: float  # 0.0 to 1.0
    description: str = ""
    evidence: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "inference_type": self.inference_type.value,
            "entity_ids": self.entity_ids,
            "score": self.score,
            "description": self.description,
            "evidence": self.evidence,
        }

"""Revision workflow — governed structural change process.

Revisions follow a multi-step lifecycle:
detect → accumulate → evaluate → propose → approve → apply → record

This ensures structural changes are evidence-based and traceable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from ontologia._ulid import generate_ulid


class RevisionStatus(str, Enum):
    DETECTED = "detected"
    ACCUMULATING = "accumulating"
    EVALUATING = "evaluating"
    PROPOSED = "proposed"
    APPROVED = "approved"
    APPLIED = "applied"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Evidence:
    """A piece of evidence supporting a revision."""

    evidence_type: str  # "metric", "event", "policy_trigger", "manual"
    description: str
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=_now_iso)


@dataclass
class Revision:
    """A proposed structural change with evidence trail."""

    revision_id: str
    title: str
    description: str = ""
    status: RevisionStatus = RevisionStatus.DETECTED
    action: str = ""  # "rename", "relocate", "merge", etc.
    affected_entities: list[str] = field(default_factory=list)
    evidence: list[Evidence] = field(default_factory=list)
    triggered_by: str = ""  # policy_id or "manual"
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def add_evidence(self, evidence: Evidence) -> None:
        self.evidence.append(evidence)
        self.updated_at = _now_iso()

    def transition(self, new_status: RevisionStatus) -> bool:
        """Attempt a status transition. Returns True if valid."""
        valid = _VALID_TRANSITIONS.get(self.status, set())
        if new_status not in valid:
            return False
        self.status = new_status
        self.updated_at = _now_iso()
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision_id": self.revision_id,
            "title": self.title,
            "description": self.description,
            "status": self.status.value,
            "action": self.action,
            "affected_entities": self.affected_entities,
            "evidence": [
                {"evidence_type": e.evidence_type, "description": e.description,
                 "data": e.data, "timestamp": e.timestamp}
                for e in self.evidence
            ],
            "triggered_by": self.triggered_by,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Revision:
        return cls(
            revision_id=data["revision_id"],
            title=data["title"],
            description=data.get("description", ""),
            status=RevisionStatus(data.get("status", "detected")),
            action=data.get("action", ""),
            affected_entities=data.get("affected_entities", []),
            evidence=[
                Evidence(
                    evidence_type=e["evidence_type"],
                    description=e["description"],
                    data=e.get("data", {}),
                    timestamp=e.get("timestamp", ""),
                )
                for e in data.get("evidence", [])
            ],
            triggered_by=data.get("triggered_by", ""),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            metadata=data.get("metadata", {}),
        )


# Valid status transitions
_VALID_TRANSITIONS: dict[RevisionStatus, set[RevisionStatus]] = {
    RevisionStatus.DETECTED: {RevisionStatus.ACCUMULATING, RevisionStatus.CANCELLED},
    RevisionStatus.ACCUMULATING: {RevisionStatus.EVALUATING, RevisionStatus.CANCELLED},
    RevisionStatus.EVALUATING: {RevisionStatus.PROPOSED, RevisionStatus.REJECTED},
    RevisionStatus.PROPOSED: {RevisionStatus.APPROVED, RevisionStatus.REJECTED},
    RevisionStatus.APPROVED: {RevisionStatus.APPLIED},
    RevisionStatus.APPLIED: set(),
    RevisionStatus.REJECTED: set(),
    RevisionStatus.CANCELLED: set(),
}


def create_revision(
    title: str,
    action: str = "",
    affected_entities: list[str] | None = None,
    triggered_by: str = "manual",
) -> Revision:
    """Create a new revision in DETECTED status."""
    return Revision(
        revision_id=f"rev_{generate_ulid()}",
        title=title,
        action=action,
        affected_entities=affected_entities or [],
        triggered_by=triggered_by,
    )

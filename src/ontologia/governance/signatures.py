"""Structural and semantic signatures — fingerprints for entities.

Signatures capture the "shape" of an entity's connections and properties.
Comparing signatures over time reveals structural drift and semantic
divergence. Used by tension detection and governance policies.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from ontologia.structure.edges import EdgeIndex


@dataclass
class StructuralSignature:
    """Fingerprint of an entity's structural position and connections."""

    entity_id: str
    parent_id: str | None
    child_count: int
    outgoing_relation_count: int
    incoming_relation_count: int
    depth: int  # distance from root
    fingerprint: str = ""  # hash of the above

    def compute_fingerprint(self) -> str:
        """Compute a deterministic hash of the structural properties."""
        data = json.dumps({
            "parent": self.parent_id,
            "children": self.child_count,
            "outgoing": self.outgoing_relation_count,
            "incoming": self.incoming_relation_count,
            "depth": self.depth,
        }, sort_keys=True)
        self.fingerprint = hashlib.sha256(data.encode()).hexdigest()[:16]
        return self.fingerprint

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "parent_id": self.parent_id,
            "child_count": self.child_count,
            "outgoing_relation_count": self.outgoing_relation_count,
            "incoming_relation_count": self.incoming_relation_count,
            "depth": self.depth,
            "fingerprint": self.fingerprint,
        }


def compute_structural_signature(
    entity_id: str,
    edge_index: EdgeIndex,
    at: str | None = None,
) -> StructuralSignature:
    """Compute the structural signature for an entity."""
    parent_edge = edge_index.parent(entity_id, at=at)
    children = edge_index.children(entity_id, at=at)
    outgoing = edge_index.outgoing_relations(entity_id, at=at)
    incoming = edge_index.incoming_relations(entity_id, at=at)

    # Compute depth by walking up
    depth = 0
    current = entity_id
    seen: set[str] = {current}
    while True:
        p = edge_index.parent(current, at=at)
        if p is None or p.parent_id in seen:
            break
        seen.add(p.parent_id)
        current = p.parent_id
        depth += 1

    sig = StructuralSignature(
        entity_id=entity_id,
        parent_id=parent_edge.parent_id if parent_edge else None,
        child_count=len(children),
        outgoing_relation_count=len(outgoing),
        incoming_relation_count=len(incoming),
        depth=depth,
    )
    sig.compute_fingerprint()
    return sig


def signatures_differ(sig_a: StructuralSignature, sig_b: StructuralSignature) -> bool:
    """Check if two signatures represent different structural positions."""
    return sig_a.fingerprint != sig_b.fingerprint

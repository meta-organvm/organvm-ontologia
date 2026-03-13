"""State recovery — reconstruct entity state by replaying events.

The event log is the canonical audit trail. Recovery reads the event
log up to a timestamp and rebuilds entity identities + names from
the events, independently of the current JSON state files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ontologia.entity.identity import EntityIdentity, EntityType, LifecycleStatus
from ontologia.entity.naming import NameIndex, add_name
from ontologia.events.bus import (
    ENTITY_ARCHIVED,
    ENTITY_CREATED,
    ENTITY_DEPRECATED,
    ENTITY_MERGED,
    ENTITY_RENAMED,
    ENTITY_SPLIT,
    NAME_ADDED,
    OntologiaEvent,
    replay,
)


@dataclass
class RecoveredState:
    """State reconstructed from the event log."""

    entities: dict[str, EntityIdentity] = field(default_factory=dict)
    name_index: NameIndex = field(default_factory=NameIndex)
    events_replayed: int = 0
    recovered_up_to: str = ""


def recover_from_events(
    events_path: Path,
    up_to: str | None = None,
) -> RecoveredState:
    """Replay the event log and reconstruct entity state.

    Args:
        events_path: Path to the events.jsonl file.
        up_to: ISO timestamp — only replay events up to this time.
            If None, replays all events.

    Returns:
        RecoveredState with reconstructed entities and names.
    """
    events = replay(path=events_path, limit=100_000)

    state = RecoveredState()
    for event in events:
        if up_to and event.timestamp > up_to:
            break
        _apply_event(state, event)
        state.events_replayed += 1
        state.recovered_up_to = event.timestamp

    return state


def _apply_event(state: RecoveredState, event: OntologiaEvent) -> None:
    """Apply a single event to the recovered state."""
    if event.event_type == ENTITY_CREATED:
        _apply_entity_created(state, event)
    elif event.event_type == ENTITY_RENAMED:
        _apply_entity_renamed(state, event)
    elif event.event_type == NAME_ADDED:
        _apply_name_added(state, event)
    elif event.event_type == ENTITY_DEPRECATED:
        _apply_lifecycle_change(state, event, LifecycleStatus.DEPRECATED)
    elif event.event_type == ENTITY_ARCHIVED:
        _apply_lifecycle_change(state, event, LifecycleStatus.ARCHIVED)
    elif event.event_type == ENTITY_MERGED:
        _apply_lifecycle_change(state, event, LifecycleStatus.MERGED)
    elif event.event_type == ENTITY_SPLIT:
        _apply_lifecycle_change(state, event, LifecycleStatus.SPLIT)


def _apply_entity_created(state: RecoveredState, event: OntologiaEvent) -> None:
    """Reconstruct an entity from its creation event."""
    uid = event.subject_entity
    if not uid:
        return

    payload = event.payload or {}
    entity_type_str = payload.get("entity_type", "repo")
    try:
        entity_type = EntityType(entity_type_str)
    except ValueError:
        entity_type = EntityType.REPO

    entity = EntityIdentity(
        uid=uid,
        entity_type=entity_type,
        lifecycle_status=LifecycleStatus.ACTIVE,
        created_at=event.timestamp,
        created_by=event.source,
    )
    state.entities[uid] = entity

    # Add initial name if present
    display_name = payload.get("display_name")
    if display_name:
        add_name(state.name_index, uid, display_name, is_primary=True, source=event.source)


def _apply_entity_renamed(state: RecoveredState, event: OntologiaEvent) -> None:
    """Apply a rename event — add new primary name."""
    uid = event.subject_entity
    if not uid:
        return

    new_name = event.new_value
    if new_name and isinstance(new_name, str):
        add_name(state.name_index, uid, new_name, is_primary=True, source=event.source)


def _apply_name_added(state: RecoveredState, event: OntologiaEvent) -> None:
    """Apply an alias addition event."""
    uid = event.subject_entity
    if not uid:
        return

    alias = event.new_value
    if alias and isinstance(alias, str):
        add_name(state.name_index, uid, alias, is_primary=False, source=event.source)


def _apply_lifecycle_change(
    state: RecoveredState,
    event: OntologiaEvent,
    new_status: LifecycleStatus,
) -> None:
    """Apply a lifecycle status change to an entity."""
    uid = event.subject_entity
    if not uid or uid not in state.entities:
        return
    state.entities[uid].lifecycle_status = new_status


def verify_recovery(
    events_path: Path,
    expected_entities: dict[str, EntityIdentity],
) -> dict[str, Any]:
    """Verify that event replay produces the expected entity state.

    Returns a dict with match status, missing entities, and extra entities.
    """
    recovered = recover_from_events(events_path)

    recovered_uids = set(recovered.entities)
    expected_uids = set(expected_entities)

    missing = sorted(expected_uids - recovered_uids)
    extra = sorted(recovered_uids - expected_uids)
    common = recovered_uids & expected_uids

    mismatched: list[dict[str, Any]] = []
    for uid in sorted(common):
        rec = recovered.entities[uid]
        exp = expected_entities[uid]
        if rec.lifecycle_status != exp.lifecycle_status:
            mismatched.append({
                "uid": uid,
                "field": "lifecycle_status",
                "recovered": rec.lifecycle_status.value,
                "expected": exp.lifecycle_status.value,
            })
        if rec.entity_type != exp.entity_type:
            mismatched.append({
                "uid": uid,
                "field": "entity_type",
                "recovered": rec.entity_type.value,
                "expected": exp.entity_type.value,
            })

    return {
        "match": not missing and not extra and not mismatched,
        "events_replayed": recovered.events_replayed,
        "missing_entities": missing,
        "extra_entities": extra,
        "mismatched_fields": mismatched,
    }

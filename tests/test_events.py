"""Tests for the ontologia event bus."""

from pathlib import Path

from ontologia.events.bus import (
    ENTITY_CREATED,
    ENTITY_RENAMED,
    NAME_ADDED,
    OntologiaEvent,
    clear_subscribers,
    emit,
    recent,
    replay,
    subscribe,
    unsubscribe,
)


class TestOntologiaEvent:
    def test_to_dict_minimal(self):
        event = OntologiaEvent(
            event_type=ENTITY_CREATED,
            source="test",
            timestamp="2026-01-01T00:00:00+00:00",
        )
        d = event.to_dict()
        assert d["event_type"] == ENTITY_CREATED
        assert d["source"] == "test"
        assert "subject_entity" not in d  # None fields excluded
        assert "changed_property" not in d

    def test_to_dict_full(self):
        event = OntologiaEvent(
            event_type=ENTITY_RENAMED,
            source="test",
            subject_entity="ent_repo_A",
            changed_property="display_name",
            previous_value="old",
            new_value="new",
            payload={"reason": "rebrand"},
        )
        d = event.to_dict()
        assert d["subject_entity"] == "ent_repo_A"
        assert d["changed_property"] == "display_name"
        assert d["previous_value"] == "old"
        assert d["new_value"] == "new"
        assert d["payload"]["reason"] == "rebrand"

    def test_roundtrip(self):
        event = OntologiaEvent(
            event_type=NAME_ADDED,
            source="test",
            subject_entity="ent_repo_A",
            new_value="alias-name",
        )
        d = event.to_dict()
        restored = OntologiaEvent.from_dict(d)
        assert restored.event_type == event.event_type
        assert restored.subject_entity == event.subject_entity
        assert restored.new_value == event.new_value

    def test_jsonl_no_newlines(self):
        event = OntologiaEvent(event_type="test", source="test")
        assert "\n" not in event.to_jsonl()


class TestEmitAndReplay:
    def test_emit_creates_file(self, tmp_path: Path):
        from ontologia.events import bus
        events_file = tmp_path / "events.jsonl"
        bus.set_events_path(events_file)

        emit(ENTITY_CREATED, source="test", subject_entity="ent_repo_A")
        assert events_file.is_file()
        assert events_file.read_text().strip()

    def test_emit_returns_event(self):
        event = emit(ENTITY_CREATED, source="test")
        assert event.event_type == ENTITY_CREATED
        assert event.timestamp  # non-empty

    def test_replay_returns_events(self):
        emit(ENTITY_CREATED, source="test", subject_entity="e1")
        emit(ENTITY_RENAMED, source="test", subject_entity="e1")
        events = replay()
        assert len(events) == 2
        assert events[0].event_type == ENTITY_CREATED
        assert events[1].event_type == ENTITY_RENAMED

    def test_replay_filter_by_type(self):
        emit(ENTITY_CREATED, source="test")
        emit(ENTITY_RENAMED, source="test")
        emit(ENTITY_CREATED, source="test")
        events = replay(event_type=ENTITY_CREATED)
        assert len(events) == 2

    def test_replay_filter_by_entity(self):
        emit(ENTITY_CREATED, source="test", subject_entity="e1")
        emit(ENTITY_CREATED, source="test", subject_entity="e2")
        events = replay(subject_entity="e1")
        assert len(events) == 1
        assert events[0].subject_entity == "e1"

    def test_replay_limit(self):
        for i in range(10):
            emit(ENTITY_CREATED, source="test", subject_entity=f"e{i}")
        events = replay(limit=3)
        assert len(events) == 3
        # Should be the last 3
        assert events[-1].subject_entity == "e9"

    def test_replay_empty_file(self, tmp_path: Path):
        from ontologia.events import bus
        bus.set_events_path(tmp_path / "empty.jsonl")
        assert replay() == []

    def test_recent(self):
        for i in range(5):
            emit(ENTITY_CREATED, source="test", subject_entity=f"e{i}")
        events = recent(n=3)
        assert len(events) == 3


class TestSubscribers:
    def test_subscribe_and_notify(self):
        received: list[OntologiaEvent] = []
        subscribe(ENTITY_CREATED, received.append)
        emit(ENTITY_CREATED, source="test")
        assert len(received) == 1
        assert received[0].event_type == ENTITY_CREATED

    def test_wildcard_subscriber(self):
        received: list[OntologiaEvent] = []
        subscribe("*", received.append)
        emit(ENTITY_CREATED, source="test")
        emit(ENTITY_RENAMED, source="test")
        assert len(received) == 2

    def test_unsubscribe(self):
        received: list[OntologiaEvent] = []
        subscribe(ENTITY_CREATED, received.append)
        emit(ENTITY_CREATED, source="test")
        assert len(received) == 1
        unsubscribe(ENTITY_CREATED, received.append)
        emit(ENTITY_CREATED, source="test")
        assert len(received) == 1  # no new events

    def test_clear_subscribers(self):
        received: list[OntologiaEvent] = []
        subscribe(ENTITY_CREATED, received.append)
        clear_subscribers()
        emit(ENTITY_CREATED, source="test")
        assert len(received) == 0

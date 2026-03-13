"""Shared test fixtures for organvm-ontologia."""

from pathlib import Path

import pytest

from ontologia.events import bus
from ontologia.registry.store import RegistryStore

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _isolate_events(tmp_path: Path):
    """Redirect all event bus writes to tmp_path for every test."""
    bus.set_events_path(tmp_path / "events.jsonl")
    bus.clear_subscribers()
    yield
    bus.set_events_path(None)
    bus.clear_subscribers()


@pytest.fixture
def store_dir(tmp_path: Path) -> Path:
    """Provide a temporary store directory."""
    d = tmp_path / "ontologia"
    d.mkdir()
    return d


@pytest.fixture
def store(store_dir: Path) -> RegistryStore:
    """Provide a fresh, loaded RegistryStore in tmp_path."""
    s = RegistryStore(store_dir=store_dir)
    bus.set_events_path(s.events_path)
    s.load()
    return s


@pytest.fixture
def registry_path() -> Path:
    """Path to the minimal test registry fixture."""
    return FIXTURES / "registry-minimal.json"

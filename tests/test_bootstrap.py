"""Tests for bootstrap migration from registry-v2.json."""

from pathlib import Path

from ontologia.bootstrap import BootstrapResult, bootstrap_from_registry
from ontologia.entity.identity import EntityType
from ontologia.events import bus
from ontologia.registry.store import RegistryStore


class TestBootstrapResult:
    def test_totals(self):
        r = BootstrapResult(organs_created=2, repos_created=4, organs_skipped=1)
        assert r.total_created == 6
        assert r.total_skipped == 1

    def test_to_dict(self):
        r = BootstrapResult(organs_created=1, repos_created=3)
        d = r.to_dict()
        assert d["total_created"] == 4
        assert d["errors"] == []


class TestBootstrapFromRegistry:
    def test_creates_organs_and_repos(self, store: RegistryStore, registry_path: Path):
        result = bootstrap_from_registry(store, registry_path)
        assert result.organs_created == 2  # ORGAN-I + META-ORGANVM
        assert result.repos_created == 4  # 2 + 2 repos
        assert result.errors == []

    def test_entities_are_persisted(self, store: RegistryStore, registry_path: Path):
        bootstrap_from_registry(store, registry_path)
        organs = store.list_entities(entity_type=EntityType.ORGAN)
        repos = store.list_entities(entity_type=EntityType.REPO)
        assert len(organs) == 2
        assert len(repos) == 4

    def test_names_are_created(self, store: RegistryStore, registry_path: Path):
        bootstrap_from_registry(store, registry_path)
        # Each entity should have a name
        for entity in store.list_entities():
            name = store.current_name(entity.uid)
            assert name is not None, f"Entity {entity.uid} has no name"

    def test_metadata_populated(self, store: RegistryStore, registry_path: Path):
        bootstrap_from_registry(store, registry_path)
        organs = store.list_entities(entity_type=EntityType.ORGAN)
        organ_keys = {o.metadata.get("registry_key") for o in organs}
        assert "ORGAN-I" in organ_keys
        assert "META-ORGANVM" in organ_keys

        repos = store.list_entities(entity_type=EntityType.REPO)
        repo_names = {r.metadata.get("name") for r in repos}
        assert "organvm-engine" in repo_names
        assert "organvm-ontologia" in repo_names

    def test_idempotent(self, store: RegistryStore, registry_path: Path):
        r1 = bootstrap_from_registry(store, registry_path)
        r2 = bootstrap_from_registry(store, registry_path)
        assert r2.organs_skipped == r1.organs_created
        assert r2.repos_skipped == r1.repos_created
        assert r2.total_created == 0

    def test_resolver_works_after_bootstrap(self, store: RegistryStore, registry_path: Path):
        bootstrap_from_registry(store, registry_path)
        resolver = store.resolver()
        result = resolver.resolve("organvm-engine")
        assert result is not None
        assert result.identity.entity_type == EntityType.REPO
        assert result.current_name.display_name == "organvm-engine"

    def test_events_emitted(self, store: RegistryStore, registry_path: Path):
        bootstrap_from_registry(store, registry_path)
        events = store.events()
        # Should have entity.created events + bootstrap.completed
        created_events = [e for e in events if e.event_type == bus.ENTITY_CREATED]
        assert len(created_events) == 6  # 2 organs + 4 repos
        completion = [e for e in events if e.event_type == bus.BOOTSTRAP_COMPLETED]
        assert len(completion) == 1

    def test_invalid_registry_path(self, store: RegistryStore, tmp_path: Path):
        result = bootstrap_from_registry(store, tmp_path / "nonexistent.json")
        assert len(result.errors) > 0
        assert result.total_created == 0

    def test_save_and_reload_after_bootstrap(self, store_dir: Path, registry_path: Path):
        store1 = RegistryStore(store_dir=store_dir)
        bus.set_events_path(store1.events_path)
        store1.load()
        bootstrap_from_registry(store1, registry_path)

        # Reload and verify
        store2 = RegistryStore(store_dir=store_dir)
        bus.set_events_path(store2.events_path)
        store2.load()
        assert store2.entity_count == 6
        resolver = store2.resolver()
        assert resolver.resolve("organvm-engine") is not None

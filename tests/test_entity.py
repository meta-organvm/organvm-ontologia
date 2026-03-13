"""Tests for entity identity — creation, serialization, UID format."""

from ontologia.entity.identity import (
    EntityIdentity,
    EntityType,
    LifecycleStatus,
    create_entity,
    generate_entity_uid,
)


class TestGenerateEntityUid:
    def test_format_repo(self):
        uid = generate_entity_uid(EntityType.REPO)
        assert uid.startswith("ent_repo_")
        assert len(uid) == len("ent_repo_") + 26

    def test_format_organ(self):
        uid = generate_entity_uid(EntityType.ORGAN)
        assert uid.startswith("ent_organ_")

    def test_format_module(self):
        uid = generate_entity_uid(EntityType.MODULE)
        assert uid.startswith("ent_mod_")

    def test_deterministic_timestamp(self):
        ts = 1710000000000
        uid1 = generate_entity_uid(EntityType.REPO, timestamp_ms=ts)
        uid2 = generate_entity_uid(EntityType.REPO, timestamp_ms=ts)
        # Same prefix, different random suffix
        prefix_len = len("ent_repo_") + 10  # prefix + timestamp portion
        assert uid1[:prefix_len] == uid2[:prefix_len]

    def test_uniqueness(self):
        uids = {generate_entity_uid(EntityType.REPO) for _ in range(50)}
        assert len(uids) == 50

    def test_all_types_have_prefixes(self):
        for et in EntityType:
            uid = generate_entity_uid(et)
            assert uid.startswith("ent_"), f"Missing prefix for {et}"


class TestCreateEntity:
    def test_basic_creation(self):
        entity = create_entity(EntityType.REPO, created_by="test")
        assert entity.uid.startswith("ent_repo_")
        assert entity.entity_type == EntityType.REPO
        assert entity.lifecycle_status == LifecycleStatus.ACTIVE
        assert entity.created_by == "test"
        assert entity.metadata == {}

    def test_with_metadata(self):
        meta = {"org": "meta-organvm", "name": "engine"}
        entity = create_entity(EntityType.REPO, metadata=meta)
        assert entity.metadata == meta

    def test_created_at_populated(self):
        entity = create_entity(EntityType.ORGAN)
        assert entity.created_at  # non-empty ISO string
        assert "T" in entity.created_at


class TestEntityIdentitySerialization:
    def test_roundtrip(self):
        entity = create_entity(
            EntityType.REPO,
            created_by="test",
            metadata={"key": "value"},
        )
        d = entity.to_dict()
        restored = EntityIdentity.from_dict(d)
        assert restored.uid == entity.uid
        assert restored.entity_type == entity.entity_type
        assert restored.lifecycle_status == entity.lifecycle_status
        assert restored.created_by == entity.created_by
        assert restored.metadata == entity.metadata

    def test_to_dict_values(self):
        entity = create_entity(EntityType.ORGAN)
        d = entity.to_dict()
        assert d["entity_type"] == "organ"
        assert d["lifecycle_status"] == "active"
        assert isinstance(d["metadata"], dict)

    def test_from_dict_defaults(self):
        d = {"uid": "ent_repo_test", "entity_type": "repo"}
        entity = EntityIdentity.from_dict(d)
        assert entity.lifecycle_status == LifecycleStatus.ACTIVE
        assert entity.created_by == "system"
        assert entity.metadata == {}

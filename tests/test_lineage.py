"""Tests for entity lineage tracking."""

from ontologia.entity.lineage import LineageIndex, LineageRecord, LineageType


class TestLineageRecord:
    def test_roundtrip(self):
        r = LineageRecord(
            entity_id="e2",
            related_id="e1",
            lineage_type=LineageType.DERIVED_FROM,
            metadata={"reason": "migration"},
        )
        d = r.to_dict()
        restored = LineageRecord.from_dict(d)
        assert restored.entity_id == "e2"
        assert restored.related_id == "e1"
        assert restored.lineage_type == LineageType.DERIVED_FROM
        assert restored.metadata == {"reason": "migration"}


class TestLineageIndex:
    def _make_index(self) -> LineageIndex:
        idx = LineageIndex()
        # e2 derived from e1
        idx.add(LineageRecord(entity_id="e2", related_id="e1", lineage_type=LineageType.DERIVED_FROM))
        # e3 split from e1
        idx.add(LineageRecord(entity_id="e3", related_id="e1", lineage_type=LineageType.SPLIT_FROM))
        # e1 merged into e4
        idx.add(LineageRecord(entity_id="e1", related_id="e4", lineage_type=LineageType.MERGED_INTO))
        return idx

    def test_predecessors(self):
        idx = self._make_index()
        preds = idx.predecessors("e2")
        assert len(preds) == 1
        assert preds[0].related_id == "e1"

    def test_predecessors_split(self):
        idx = self._make_index()
        preds = idx.predecessors("e3")
        assert len(preds) == 1
        assert preds[0].lineage_type == LineageType.SPLIT_FROM

    def test_successors(self):
        idx = self._make_index()
        succs = idx.successors("e1")
        # e1 was merged_into e4 → e1 as related_id with MERGED_INTO
        assert any(r.lineage_type == LineageType.MERGED_INTO for r in succs)

    def test_full_lineage(self):
        idx = self._make_index()
        all_records = idx.full_lineage("e1")
        # e1 appears as entity_id in MERGED_INTO and as related_id in DERIVED_FROM, SPLIT_FROM
        assert len(all_records) == 3

    def test_trace_ancestry(self):
        idx = LineageIndex()
        idx.add(LineageRecord(entity_id="e3", related_id="e2", lineage_type=LineageType.DERIVED_FROM))
        idx.add(LineageRecord(entity_id="e2", related_id="e1", lineage_type=LineageType.DERIVED_FROM))
        chain = idx.trace_ancestry("e3")
        assert chain == ["e2", "e1"]

    def test_trace_ancestry_no_predecessors(self):
        idx = LineageIndex()
        assert idx.trace_ancestry("e1") == []

    def test_trace_ancestry_max_depth(self):
        idx = LineageIndex()
        idx.add(LineageRecord(entity_id="e3", related_id="e2", lineage_type=LineageType.DERIVED_FROM))
        idx.add(LineageRecord(entity_id="e2", related_id="e1", lineage_type=LineageType.DERIVED_FROM))
        chain = idx.trace_ancestry("e3", max_depth=1)
        assert chain == ["e2"]

    def test_serialization_roundtrip(self):
        idx = self._make_index()
        data = idx.to_list()
        restored = LineageIndex.from_list(data)
        assert len(restored.all_records()) == 3

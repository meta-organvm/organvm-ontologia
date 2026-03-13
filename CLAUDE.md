# CLAUDE.md

Adaptive structural registry — entity identity, temporal naming, event bus, and governed evolution substrate for the ORGANVM system.

## Commands

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check src/
pyright
```

## Architecture

Foundational substrate providing UID-based entity identity where existing modules use mutable names. Ten layers: entity → structure → variables → metrics → events → sensing → inference → governance → state → registry.

### Key modules (Phase 1)

- **`entity/`** — EntityIdentity (ULID-based UID), NameRecord (temporal aliases), Resolver
- **`events/`** — Enhanced event bus with subject_entity + changed_property tracking
- **`registry/`** — Unified store: JSON for current state, JSONL for append-only logs

### Storage

All data at `~/.organvm/ontologia/`:
- `entities.json` — current entity state
- `names.jsonl` — append-only name history
- `events.jsonl` — append-only event log

### Test isolation

`tests/conftest.py` redirects all storage to `tmp_path`. No test touches `~/.organvm/`.

## Conventions

- `src/` layout — imports are `from ontologia.X import Y`
- Zero runtime dependencies (stdlib only)
- ruff + pyright config matches organvm-engine
- Commit prefixes: `feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`

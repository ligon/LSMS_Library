---
name: root-checkers
description: "Skill for the Root-checkers area of LSMS_Library. 42 symbols across 14 files."
---

# Root-checkers

42 symbols | 14 files | Cohesion: 100%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how digest, load, git work
- Modifying root-checkers-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | digest, load, git, verify_receipt, verify_manifest (+2) |
| `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root.py` | load, sha, receipt, authenticate |
| `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v1_preserved.py` | load, sha, receipt, authenticate |
| `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v2_preserved.py` | load, sha, receipt, authenticate |
| `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_1215_author_root_v3.py` | read, physical, check, binary |
| `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_1188_independent_root_v3.py` | read, receipt, binary |
| `slurm_logs/isa_backlog_2026-10-21/root-checkers/1206-archive1-1173-1186-root.py` | sha, same |
| `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_1204_author_handoff_root.py` | sha, receipt |
| `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_1204_author_handoff_root_v1_preserved.py` | sha, receipt |
| `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_1210_author_handoff_root.py` | sha, check |

## Entry Points

Start here when exploring this area:

- **`digest`** (Function) — `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py:19`
- **`load`** (Function) — `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py:30`
- **`git`** (Function) — `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py:34`
- **`verify_receipt`** (Function) — `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py:38`
- **`verify_manifest`** (Function) — `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py:55`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `digest` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | 19 |
| `load` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | 30 |
| `git` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | 34 |
| `verify_receipt` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | 38 |
| `verify_manifest` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | 55 |
| `verify_source` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | 67 |
| `sources_and_copies` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_849_218_root.py` | 104 |
| `load` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root.py` | 16 |
| `sha` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root.py` | 20 |
| `receipt` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root.py` | 25 |
| `authenticate` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root.py` | 44 |
| `load` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v1_preserved.py` | 16 |
| `sha` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v1_preserved.py` | 20 |
| `receipt` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v1_preserved.py` | 25 |
| `authenticate` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v1_preserved.py` | 44 |
| `load` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v2_preserved.py` | 16 |
| `sha` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v2_preserved.py` | 20 |
| `receipt` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v2_preserved.py` | 25 |
| `authenticate` | Function | `slurm_logs/isa_backlog_2026-10-21/1205-independent/root-checkers/verify_1205_independent_root_v2_preserved.py` | 44 |
| `read` | Function | `slurm_logs/isa_backlog_2026-10-21/root-checkers/verify_1215_author_root_v3.py` | 25 |

## How to Explore

1. `context({name: "digest"})` — see callers and callees
2. `query({search_query: "root-checkers"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`

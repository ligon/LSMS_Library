# Prior-Art Ledger — GH #1277: Malawi 2016-17 `Data/Panel/` is catalog 3819, not 2939

**Search tier used:** ripgrep + git floor (gitnexus MCP did not connect this session).

## §1 Task, restated
Correct the provenance record of `Malawi/2016-17/` (no acquisition). Its
`Data/Panel/` files are 3819's re-release of the IHPS 2016 round, not entry
2939: record `#+CATALOG_ID: 2936, 3819` and `#+CATALOG_COVERS: 2939`, correct
`#+PROVENANCE_NOTE`, leave the primary's `CATALOG_IDNO` / `DOI` alone. Add the
#1276 `#+CATALOG_SUBDIR:` lines and the #1283 `#+CATALOG_HELD:` line (secondary
entry 3819) to 2016-17 and 2019-20, on measured byte identity.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `read_provenance` / `write_provenance` | `lsms_library/provenance.py:636`, `:656` | parse / render one `SOURCE.org` | `tests/test_wave_provenance.py` | reuse (edit via `dataclasses.replace`) |
| `WaveProvenance.extra` | `provenance.py:307` | carries every unknown `#+KEY:` occurrence, in order (GH #1115) | `TestRoundTripIsLossless` | reuse for `CATALOG_SUBDIR` / `CATALOG_HELD` until the framework parses them |
| `covers` / `CATALOG_COVERS` | `provenance.py` | subsumed-not-held relation (GH #600) | `TestCoveredIsNotHeldAndIsNotMissing` | reuse; Tanzania `2008-15/` precedent |
| `discover_waves` | `lsms_library/data_access.py:2068` | `yes` / `covered` / ... per catalog entry | yes | reuse (verification) |
| `scripts/backfill_wave_provenance.py` | — | re-stamp; must not erase a human-established fact | yes | reuse (`--dry-run`, plus a real run on a copy) |

## §3 Definitions & conventions in force
- `CATALOG_ID` is a list of entries whose FILES a directory holds; `CATALOG_COVERS`
  lists entries whose content is subsumed but not held (`CLAUDE.md` §"Data
  Access", "`catalog_id` is NOT a 1:1 key (GH #600)").
- #1283 ruling (@ligon, 2026-10-23): `#+CATALOG_HELD: <id> <idno> [<doi>]` for
  SECONDARY entries only; moved by a complete acquisition or a human,
  evidence-backed (content-validated) edit, never by the catalog alone.
- Precedent: `Malawi/2019-20/Documentation/SOURCE.org` already records its
  Panel as 3819; `Tanzania/2008-15/` is the COVERS pattern.

## §4 Invariants & assumptions
- parse -> render must be idempotent on every shipped record (GH #1115,
  `TestRoundTripIsLossless`); verified on both files before and after.
- A re-stamp must not erase a recorded fact; verified with a dry run and with a
  real backfill run on a copy of the tree (both files byte-identical after).
- `SOURCE.org` is not a build input (`Documentation/`, not `_/`); `CONTENTS.org`
  is excluded as docs: no cache hash should move.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| record edit | reuse provenance API | no hand edit; render is the canonical form |
| byte identity | measured from the downloaded 3819 v06 zip | evidence rule of the #1283 ruling |

## §6 Open questions for the human
- None blocking. Docs still citing `Malawi/2016-17/` as `2936, 2939`
  (`CLAUDE.md`/`AGENTS.md` GH #600 bullet, `.claude/skills/add-wave/SKILL.md`,
  docstrings in `provenance.py` and `data_access.py`) are left for the #1283
  framework branch, which edits those files.

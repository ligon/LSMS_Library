# Prior-Art Ledger - GH #1276 / #1283: a second catalog entry in its own subdirectory

**Search tier used:** ripgrep + git floor (the gitnexus MCP server did not
connect in this session).  Anchors are at `f3ab4a4fc` on
`feat/add-wave-subdir-20261023`.  Code-only: no real acquisition, download,
push or `dvc` command is authorized; synthetic zips and sidecars in `tmp_path`.

## §1 Task, restated

Let a wave directory hold a second, disjoint World Bank catalog entry in its
own `Data/<subdir>/` (the GH #600 pattern of `Malawi/2016-17` and `2019-20`;
Burkina Faso 2021-22's 6224 + 6277 is the motivating case), acquired with
`add_wave(..., subdir=)` / `populate_and_push(..., subdir=)` without touching
the other entry's files.  Record which subdirectory holds which entry
(`#+CATALOG_SUBDIR:`, red team r1) and, per the GH #1283 ruling, a secondary
entry's held and newer release (`#+CATALOG_HELD:` / `#+CATALOG_LATEST:`).  Red
team r2: a re-acquisition into an occupied subdirectory is refresh-only by
default (a `HELD` line never claims the whole entry: Malawi holds one round of
3819 per wave), and a plain acquisition on a mapped record is refused.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_fetch_destination` | `lsms_library/data_access.py:94` | the single decision of where acquisition writes | `tests/test_gh831_*` | reuse; every subdir path routes through it (`_acquisition_data_dir`) |
| `_extract_all_from_zip` | `data_access.py:1323` | GH #1149 staged extraction; `dest_dir` is the sidecar-match scope and the placement root | `tests/test_staged_reacquisition.py` | extend: `refresh_only=` (place only members matching a held sidecar) |
| `_AcquiredRelease`, `_ACQUIRED_RELEASES` | `data_access.py:1485` | the acquisition receipt | `test_staged_reacquisition.py`, `test_wave_provenance.py` | extend: `subdir`, `refresh_only`, `not_placed`; key gains the subdir under `subdir=` |
| `_record_acquired_release` | `data_access.py:1862` | the only writer of held-release keys, behind the complete-release / confirmed-push gates | `test_wave_provenance.py::TestAcquisitionWritesTheHeldRelease` | extend: secondary branch, refresh-only gate |
| `add_wave`, `populate_and_push`, `get_data_file` | `data_access.py:2550`, `:1992`, `:2878` | acquisition entry points; `_source_url` routing (GH #1128) | as above | extend with `subdir=`, `add_members=`, the pre-check |
| `acquired_release`, `write_provenance`, `parse/render_source_org` | `lsms_library/provenance.py:835`, `:649`, `:732` | the record an acquisition writes; lossless parse/render (GH #1115) | `test_wave_provenance.py` | reuse; per-entry fields beside the primary scalars |
| `latest_release`, `release_unknown`, `dropped_by_write` | `scripts/backfill_wave_provenance.py:652`, `:761`, `:602` | the primary's held/latest rules in the backfill | `test_wave_provenance.py` (backfill classes) | mirror per secondary (`secondary_releases`), never writing `HELD` |
| `local_held_idnos`, `discover_waves` | `data_access.py:2272`, `:2354` | held-release ladder (`stale-release`) | `test_wave_provenance.py` | extend to secondaries |
| precedent ledgers | `.coder/ledger/1149-staged-reacquisition.md`, `.coder/ledger/1115-1128-provenance.md` | the same functions' prior contracts | -- | cited, not re-derived |

## §3 Definitions & conventions in force

- **Held release** = the release whose files we hold; moved only by a demonstrated complete acquisition or a human's evidence-backed edit, never by the catalog (GH #1114 / #1128; CLAUDE.md "Wave provenance").  Per entry for secondaries (GH #1283 ruling, @ligon 2026-10-23, option (b), secondaries only).
- **`catalog_id` is not 1:1** (GH #600): `#+CATALOG_ID:` is a list; `#+CATALOG_COVERS:` is subsumed-not-held.
- **`countries_root()` is read-only at runtime except acquisition**, decided by `_fetch_destination` (CLAUDE.md, "one invariant, three issues").
- **Scope of a `HELD` line**: which release the files in that entry's subdirectory came from; NOT that the subdirectory holds the whole entry (GH #1283 ruling).  Malawi: one round of 3819 per wave (`Malawi/_/CONTENTS.org`, "GH #1277").
- **Leftovers** (GH #1149): held sidecars a release does not ship are reported, not retired.

## §4 Invariants & assumptions

- Without `subdir=`, and on a record with no `#+CATALOG_SUBDIR:` line, every acquisition path is byte-for-byte the pre-#1276 one (verified by the red team's default-path trace).
- No `data_access` / `provenance` symbol reaches a build fingerprint (CLAUDE.md, #917); expected census: 0 hashes.
- `.yml`/`SOURCE.org` under `Documentation/` are not build inputs.
- Malawi's 2016-17 and 2019-20 `Data/Panel/` each hold ONE round of 3819 (99 and 98 files of 3819's 396 data members); an unconfined or full re-acquisition imports the other rounds (red team r2).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| subdir confinement | extend `_extract_all_from_zip` via `dest_dir` | it already scoped matching and placement to `dest_dir` |
| entry -> subdirectory | new key `#+CATALOG_SUBDIR:` (red team r1, Sue) | nothing recorded it; wrong-subdir overwrite |
| secondary held / latest release | new keys per GH #1283 ruling | scalars describe the primary only |
| refresh-only | extend the extraction plan (skip members with no held sidecar) | GH #1283 scope rule; the #1149 report already has a leftover list |
| backfill latest per secondary | mirror `latest_release` | same rule, per entry |

## §6 Open questions for the human

- **The refresh-only gate on a PRIMARY entry.**  The red-team decision speaks
  of `HELD`; I apply the same gate to the primary's `#+CATALOG_IDNO:` when the
  primary is refreshed into its own occupied subdirectory: recorded only when
  every held member was verified or replaced (no leftover); members outside
  the held subset are reported, not placed, and do not block.  Precedent: the
  primary scalars are not a whole-entry claim either -- Malawi 2019-20's note
  says `Cross_Sectional/` holds 103 of 108 of 3818's files while
  `#+CATALOG_IDNO:` names 3818's release.  @ligon may want the primary held to
  a whole-entry standard instead.

---
### Phase 3 -- verification

Anchored check against the entries above (head `d9215733b`):

- `_normalise_subdir` / `provenance.normalise_subdir` -- OK (§4, §2 `_fetch_destination`): one directory name; the destination is still `_fetch_destination`'s (`_acquisition_data_dir`).
- `_check_subdir_acquisition`, `_refuse_unconfined` -- OK (§3 #600, §4): refuse before any download; the second is reachable only when a record has a `#+CATALOG_SUBDIR:` line, so the default path is unchanged (red-team default trace byte-identical to base).
- `_extract_all_from_zip(refresh_only=)` -- OK (§2, extends #1149's staged extraction; §3 leftovers): a not-placed member is never staged, never shipped.
- `_AcquiredRelease` (`subdir`, `refresh_only`, `not_placed`), `_receipt_key` -- OK (§2): the existing receipt extended, no parallel ledger; the key is the pre-#1276 one without `subdir=`.
- `_record_acquired_release` (secondary branch, refresh-only gate) -- OK (§3 held release, #1114 per entry): the same complete-release / confirmed-push gates; under refresh-only a leftover blocks.  Open: the gate on a PRIMARY entry (§6).
- `held_secondary_entry`, `with_catalog_subdir`, `held_release_of`, `latest_release_of`, `validate_entry_lines` -- OK (§3 #1283 ruling): secondaries only; the primary's scalars untouched.
- `local_held_idnos` -- OK (§3): secondaries' `HELD` added; nothing else.
- backfill `secondary_releases`, `held_changed` -- OK (§2 mirrors `latest_release`; §3 #1128): never writes `HELD`.
- No REINVENTION found: no new reader, writer or hash path; no CONTRADICTION of §3/§4.
- Census (burst, base `57d255585` vs head): 0 of 549 country, 0 of 40 btf, 0 of 2,109 wave hashes moved (§4).

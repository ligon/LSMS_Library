# Prior-Art Ledger -- GH #1115 / #1128 provenance

Search tier: ripgrep + git + complete GitHub issue/comment reads. No GitNexus or commons query tool is exposed in this session.
Base: frozen integration `852ac2bfcd5f798533f56e7efa1f77db80c0138f`.
Prior implementation: `origin/fix/1128-1115-provenance-latest` at `613611578`, preserved unchanged.
Inherits `STANDING.md` and the provenance/data-access sections of `AGENTS.md` (`CLAUDE.md` is its symlink).

## §1 Task, restated

Port and finish the existing #1115/#1128 code onto the frozen framework candidate: SOURCE.org parsing/rendering must retain unknown/repeated metadata and human notes, primary URLs must not be inferred from metadata/notes, no-URL placeholders must be idempotent, and separate latest-release observations must form a durable queue while held release facts remain protected.
A record without a held IDNO receives no held release from catalog metadata; successful acquisition alone may stamp one and clear the queue.
This is code-only: no live catalog calls, real acquisition, shipped SOURCE.org backfill writes, country config updates, or shared L2 changes.

## §2 Existing machinery

| Machinery | Anchor at base / prior tip | Existing coverage | Decision |
|---|---|---|---|
| `WaveProvenance`, `parse_source_org`, `render_source_org`, `preserved_prose`, `read_provenance`, `write_provenance` | `lsms_library/provenance.py:142,178,256,332,385,405` | `tests/test_wave_provenance.py:TestSourceOrgFormat`, multiple IDs, covered-vs-held; prior `TestRoundTripIsLossless` | Extend by reusing prior implementation |
| Old provenance design / compatibility | `.coder/ledger/600-provenance-1to1.md`, commit `9bcd3c55d` | positional constructor compatibility; many-to-many/catalog relation tests | Reuse; append new dataclass fields |
| Latest release/unknown-held handling | prior `scripts/backfill_wave_provenance.py:latest_release/release_unknown` | fixed literal held-release transition tests and missing-IDNO ruling tests | Reuse; assess old drafted override residual against latest ruling |
| Backfill refusal, per-run review artifact, finally writer | `scripts/backfill_wave_provenance.py:Unresolved/resolve/dropped_by_write/release_changed/write_review/main` | outage/no-source/unknown-id/dry-run/abort/field-preservation tests | Extend existing guards; no alternate writer |
| Discovery and boolean `local` | `data_access.py:local_catalog_ids/local_covered_ids/discover_waves` | `TestDiscoverWavesMatchesOnCatalogId`, catalog relations, repository discovery | Reuse prior sixth `stale-release` state and `held_idnos` |
| Acquisition and actual extraction/push receipts | `data_access.py:_extract_all_from_zip/get_data_file/add_wave/populate_and_push/push_to_cache_batch`; prior `_AcquiredRelease/_record_acquired_release` | hermetic zip/existing-file/download-failure/push-failure tests | Reuse receipts and successful-acquisition stamping; keep staged/nested acquisition #1149 separate |
| Primary URL reader | `data_access.py:1000`, `_read_source_url` | old legacy-reader compatibility test | Reuse the provenance parser's primary URL result rather than introduce another rule |
| Fingerprint machinery | `_build_registry.py`, `Country._table_cache_hash`, `Wave._input_hash`; existing campaign hash probe | `tests/test_build_transform_hash.py`; prior r1 0/1051 observed hash movement | Measure complete current reachability, not assume old counts |

Prior commits to reuse: `df2d74208` (round-trip implementation), `a3cc1fb0c` (queue/discovery/acquisition), `a731c52b4` (r1 fixes/ruling), and generic documentation `d2044a696`/`613611578`.
Do not port `4b78b28a6` queue stamping, six placeholder cleanup SOURCE edits, country CONTENTS edits, unrelated merged source, or acquisition branches #978/#1127.
The original author's attribution is Claude Opus 5.5; the port will cite that provenance explicitly.

Prior independent review read: `/global/scratch/fsa/fc_jevons/ligon/venv_validate/backlog_review_local_40139031_1019/1128-1115-provenance-latest-r1/VERDICT.md`.
Prior follow-up read: sibling `backlog_fix_local_40139031_1019/1128-1115-provenance-latest-r1fix/REPORT.md` and commit bodies.
The old full suite did NOT finish; its progress marks are not a green full-suite result.

## §3 Definitions and authority

- `provenance.py:1`: provenance records which source dataset a wave directory actually holds.
- #1114 owner ruling (2026-10-19): "A wave's recorded catalog release should name the version we hold."
- #1128 owner GO (2026-10-19): "Code-only. Dispatched together with #1115".
- #1128 owner ruling (2026-10-20): "The backfill never supplies a held release from the catalog alone. It leaves `#+CATALOG_IDNO:` absent and lists the record in the review artifact as `release-unknown`." Only acquisition writes the held release.
- Held keys are `CATALOG_IDNO`/`CATALOG_DOI`; latest observation is separate `CATALOG_LATEST_IDNO`/`CATALOG_LATEST_DOI`/first-seen `CATALOG_LATEST_SEEN`.
- Repeated `CATALOG_ID` accumulates holdings; `CATALOG_COVERS` is subsumed content, not held files (existing #600 ledger).
- Unknown metadata and notes remain recorded evidence, not a substitute primary acquisition URL. The explicit `CATALOG_URL` field or pre-metadata body provides the primary URL; note/secondary URLs do not.

## §4 Invariants and boundaries

- Inherit sanctioned IO, country-root identity and cache invariants from STANDING §4. Hermetic acquisition tests operate only on synthetic tmp trees/zip fixtures with mocked network/push operations.
- Never move a held release because a live/latest catalog differs. Preserve missing held IDNO, including repeated backfills; report release unknown.
- Queue observations have their own first-seen date, survive identical reruns, change when the latest changes, disappear when held/latest agree or acquisition succeeds, and never become held facts by inference.
- Network/partial extraction/existing skipped files/push failures cannot falsely certify a new release. Legacy `push=False` is an explicit supported acquisition mode and may record an actually downloaded release.
- Notes include keyword-looking lines and paragraph order. Unknown repeated keys and empty metadata must not disappear. Repeated known scalar handling must be stated honestly rather than advertised as universally lossless if the round-trip still collapses it.
- Backfill dry-run/outage/abort evidence remains durable outside the countries tree; shipped census is strictly read-only and must verify its inputs remain unchanged.
- No country SOURCE or CONTENTS file will be edited, even where the prior branch did so. No stale queue entries will be stamped before separate acquisition dependency landing.
- Nested archive extraction/staging remains #1149; secondary held-entry release metadata remains a separately scoped prior limitation.
- Exact full current country/wave/build fingerprint reach will be measured with config/sidecar reads only, never by rebuilding immutable corpora.

## §5 Reuse decision

| Need | Decision | Reason |
|---|---|---|
| Parse/render and no-URL behavior | Reuse/finish prior provenance patch | Existing tested implementation directly addresses #1115 |
| Latest queue and missing-held-release report | Reuse prior release helpers/review tables | Exact owner ruling is already encoded; no alternative schema needed |
| Acquisition completion | Extend existing receipt/record helper | Close false-success edges without redesigning #1149 staging |
| Primary URL for download routing | Reuse `read_provenance(...).url` | One authoritative URL rule |
| Exhaustive round-trip evidence | Extend shipped SOURCE census; count repeated lines as occurrences | Existing census is useful, but set-only checks cannot prove repeated-line retention |
| Regression evidence | Existing acquisition/provenance/discovery tests + targeted adversarial controls | No served Feature table is changed; full feature/corpus rebuild is unnecessary unless measured reach reveals otherwise |

## §6 Scope questions surfaced to parent

No new user permission is needed for the approved code-only pair.
The prior r1fix explicitly drafted two limitations: `add_wave` stamps before optional push, and a wrong-primary-id correction can still stamp a catalog IDNO without acquisition.
Proposed conservative finish: only record after a complete push when `push=True`, retain successful-download semantics when `push=False`, and refuse a stamped primary-ID correction rather than invent/change held release facts.
Known scalar repeats currently remain parse-last-win with a count-loss refusal on backfill; decide whether to preserve them in the representation here or narrow/report that residual explicitly before claiming universal losslessness.
These checks are for the parent to align the implementation with the accepted brief, not repeated questions to the user.

## Country evidence already read

GhanaSPS CONTENTS records 2017-18 as MIXED-VINTAGE: ten Harvard Dataverse CC0 additions coexist with the original extract under different terms. The five secondary-source lines are substantive provenance and must not become a primary URL.
Its live TODO entries were read, including conversion and legacy feature-script notes; they do not authorize changes here.
Nepal has recorded provenance but missing NSO-hosted microdata. Its CONTENTS still says "2003-04 (NLSS II) -- WB catalog 1000" near the top, while its own BID/caveat section says NLSS II is 74 and NLSS III is 1000. This internal contradiction is recorded, not silently corrected in a code-only task.
Nigeria's held v01 nested layout and Ethiopia's held v02/replacement plot-file guidance were read in the earlier campaign; those acquisitions remain separate and no data-state claim is advanced here.

### Phase 3 verification

Pending implementation and independent review.

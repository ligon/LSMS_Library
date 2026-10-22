# Prior-Art Ledger - GH1149 staged nested re-acquisition

Search tier: direct source/ripgrep, live issue and merged-source reconciliation.
Initial grounding for a queued, UNASSIGNED task; no imports/tests/acquisition.

## Section1 Task

Implement the already decided OptionC from2026-10-20: re-acquisition follows
existing recursive sidecar paths, new members retain zip-relative paths, all
comparison precedes placement, collisions refuse and leftovers remain reported.
Preserve confirmation, push and held-provenance success/failure semantics.
This code-only repair supports Nigeria978/Ethiopia1127 without acquiring them.

## Section2 Existing machinery at releasef8

| Symbol | Anchor | Reuse / required inspection |
|--------|--------|----------------------------|
| _extract_all_from_zip | lsms_library/data_access.py:1141 | Existing fullzip download/extract/report; currently flattens paths and skips top-level existing files |
| _AcquiredRelease | lsms_library/data_access.py:1209 | Existing per-acquisition report carrier; inspect exact callers and completion semantics |
| _record_acquired_release | lsms_library/data_access.py:1562 | Existing held-release gate; partial/failure must preserve prior facts |
| populate_and_push | lsms_library/data_access.py:1652 | Existing whole-release and push completion; reuse batched helpers/locking |
| add_wave | lsms_library/data_access.py:2156 | Existing confirm/push/public flow and source-primary routing |
| get_data_file | lsms_library/data_access.py:2444 | Existing source routing/cache/acquisition entry and caller at2602 |
| acquisition/provenance controls | tests/test_wave_provenance.py:2051 | Existing real extraction test doubles and no-overwrite gate at2104; read before edits |

Read existing canonical MD5/CSV normalization and sidecar helpers before adding
any hashing or path registry. No parallel provenance/download/hash pipeline.

## Section3 Definitions

Inherit STANDING.md and CLAUDE.md. The live1149OptionC ruling is authoritative.
Current add-wave skill explicitly defers staged nested refresh to1149.
The leftover scan in0.16 IS recursive already; its basename-only membership
and flat skip/placement remain unfinished. Do not reinvent the merged recursion.

The held NigeriaW5v01 has106 declared files. The cited v02 membership has104
identical,2revised and2new. EthiopiaW5 currently records v02; citedv03 has67
identical,1removed,1new. Member-list fixtures use actual names with SYNTHETIC
payloads; they are not a claim of new actual release acquisition/validation.

## Section4 Invariants

- Compare all staged members before any placement; refuse ambiguous recursive
  sidecar basename or incoming destination collisions without earlier writes.
- New paths remain withinData and preserve zip-relative structure. Report
  leftovers; no raw/sidecar retirement or source-provenance fiction.
- Keep confirm/push defaults and held/unknown/source-primary facts under
  partial downloads, placement failures and partial pushes.
- Code-only synthetic tests: no WB/S3 calls, real source payloads, DVC CLI,
  credentials, acquisition, cache bypass/promotion/stamp or dependencies.
- Actual549API/39framework/1362wave census before builds; predicted0 moves
  is not evidence and no exclusion may be added to obtain it.
- GH1206: pandas frame exact/zero args alone miss nullableFloat64 changes;
  use explicit EA/numeric/mask and full typed-axis/attrs fault controls.

## Section5 Plan and verification

Writer must first read full current Nigeria/Ethiopia CONTENTS/tasks, report
idiosyncrasies/contradictions and commit a source-specific ledger before code.
Preserve meaningful actual-parent synthetic failures and existing positive
flat/provenance/confirm/push controls. Implement the complete six-part ruling,
targeted and existing acquisition suites, measured fingerprint scope and scoped
held-compatible Feature audit verification. No source-cold country build or
real acquisition is authorized by this queued task.

Candidate/base/private targets and full brief are recorded under
slurm_logs/isa_backlog_2026-10-21/queued-1149-acquisition/.
No runtime verdict, implementation or independent review exists yet.

Sue

# Prior-Art Ledger — GH #978: Nigeria 2023-24, GHS-Panel W5 v01_M -> v02_M

**Search tier used:** ripgrep + git floor (gitnexus MCP did not connect this session).

## §1 Task, restated
Replace the held release of `Nigeria/2023-24/` (WB catalog 6410) with the
catalog's newer release, under @ligon's 2026-10-23 "acquire the newer release,
diff first" ruling on #978. Acquisition only through `data_access.add_wave`;
update `CONTENTS.org`. Do NOT wire the new household geovariables (a separate
served change under #978). No served table changes: no table declares the two
revised members or the two new ones.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `add_wave` | `lsms_library/data_access.py:2259` | download, staged comparison, placement, batched push, held-release stamp | `tests/test_wave_provenance.py`, `tests/test_staged_reacquisition.py` | reuse, unchanged |
| `_extract_all_from_zip` | `data_access.py:1143` | stage whole zip; revised / leftover report against sidecar md5 (GH #1149) | `test_staged_reacquisition.py` | reuse; observed by a spy, not modified |
| `push_to_cache_batch` | `data_access.py:1532` | batched `dvc add` + `dvc push`, lock retry, writer-creds override | writer tests | reuse; gated (raise before push on a membership mismatch) |
| `_record_acquired_release` / `provenance.acquired_release` | `data_access.py:1668`, `provenance.py:582` | moves `CATALOG_IDNO` / `DOI`, clears `CATALOG_LATEST_*` | `test_wave_provenance.py` | reuse |
| `unpushed_blobs` | `data_access.py:1620` | `dvc status --cloud` for named targets | — | reuse (push verification) |

## §3 Definitions & conventions in force
- Held release = the release whose files we hold; only a complete acquisition
  moves it (`CLAUDE.md` §"Data Access", wave-provenance paragraph; #1114/#1128).
- A leftover sidecar is reported, never auto-retired; retirement is a reviewed
  step (`.claude/skills/add-wave/SKILL.md`, GH #1149).

## §4 Invariants & assumptions
- Never run the `dvc` CLI by hand; never `rm` a lock (`CLAUDE.md` §"Data Access").
- `countries_root()` is read-only at runtime except acquisition (`CLAUDE.md`);
  the 108 raw copies `add_wave` placed in the worktree were deleted after each
  matched its sidecar md5.
- `tests/test_staged_reacquisition.py::test_documented_release_member_lists_with_synthetic_payloads`
  pinned every held W5 sidecar, including the two this task revises; narrowed
  to the members both releases ship, on the precedent of
  `TestShippedRecordsSayWhatWeMeasured` ("a pin on the shipped tree's queue
  state breaks the day they merge").

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| acquisition | reuse `add_wave` | sanctioned path; wraps the lock-retrying writer |
| membership gate | new (driver-side spy + gate, not committed) | the task's STOP-on-mismatch must fire before the push; add_wave has no dry-run |
| `2023-24/_/data_info.yml:70-71` stale comment | left alone | a build input: editing it moves every Nigeria hash; belongs to the #978 wiring change |

## §6 Open questions for the human
- None blocking. Wiring `nga_householdgeovariables_y5` into `cluster_features` is the rest of #978; the multi-fix cluster count depends on the cluster key (see CONTENTS.org).

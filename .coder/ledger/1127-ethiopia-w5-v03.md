# Prior-Art Ledger — GH #1127: Ethiopia 2021-22, ESPS-5 v02_M -> v03_M

**Search tier used:** ripgrep + git floor (gitnexus MCP did not connect this session).

## §1 Task, restated
Replace the held release of `Ethiopia/2021-22/` (WB catalog 6161) with the
catalog's newer release, under @ligon's 2026-10-23 "acquire the newer release,
diff first" ruling on #1127. Acquisition only through `data_access.add_wave`;
retire the sidecar of the member v03 no longer ships; correct `CONTENTS.org`.
No served table changes: no table declares either plot geovariables file.

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
  the 68 raw copies `add_wave` placed in the worktree were deleted after each
  matched its sidecar md5.
- `tests/test_staged_reacquisition.py::test_documented_release_member_lists_with_synthetic_payloads`
  pinned every held W5 sidecar, including the one this task retires; narrowed
  to the members both releases ship, on the precedent of
  `TestShippedRecordsSayWhatWeMeasured` ("a pin on the shipped tree's queue
  state breaks the day they merge").

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| acquisition | reuse `add_wave` | sanctioned path; wraps the lock-retrying writer |
| membership gate | new (driver-side spy + gate, not committed) | the task's STOP-on-mismatch must fire before the push; add_wave has no dry-run |
| duplicate retirement | `git rm` of the sidecar | reviewed retirement per GH #1149 |

## §6 Open questions for the human
- None blocking. Serving the new plot geovariables is GH #1150.

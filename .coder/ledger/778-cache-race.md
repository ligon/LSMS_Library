# Prior-Art Ledger -- GH #778 remaining cache disappearance/test isolation

**Search tier:** ripgrep + git, direct function/test/config-document reads; no callable GitNexus query tool used.
Inherits `STANDING.md` sections2-5, `CLAUDE.md` cache behavior and `lsms_library/data_info.yml`.
Base: frozen reviewed candidate `852ac2bfcd5f798533f56e7efa1f77db80c0138f`.
Live issue778 body and all returned comments read2026-10-20 (no comments).

## §1 Task, restated

Finish only the current issue remainder: disappearance of an assumed-fresh country parquet between exists/metadata/data reads must fall through to the ordinary loader on FileNotFoundError. Preserve every other error and the established corruption/coverage contracts. Isolate the actual remaining forced real-country cache mutations/rebuilds using physical private L2 copies with only L1 linked. Do not reimplement merged PR1030, revive the disproven partial-aggregate theory or choose #1071 semantics.

## §2 Existing machinery

| Symbol / fixture | Anchor at852 | Behavior and tests | Reuse |
|------------------|--------------|--------------------|-------|
| `Country._read_assumed_fresh_cache` | country.py:4635 | Read gate excluded from build fingerprint; manifest subset filtering, legacy full pass-through and datetime replay; test_gh1040_wave_read_gates.py:355-522 | Extend only its two get_dataframe reads with FileNotFoundError fallthrough |
| `_read_l2_country_cache` | country.py:4436 | Ordinary path's hash/coverage/degraded grading and read recovery | Reuse unchanged after assumed cache disappearance |
| `read_parquet_cache_waves` / datetime footer helper | local_tools.py:3103 / :3084 | Missing/unreadable footer returns None; does not choose new corruption policy | Reuse unchanged |
| `acf`, `root`, `_plant`, `_FellThrough` | test_gh1040_wave_read_gates.py:52/:364 | Hermetic planted private parquet and observable ordinary descent | Extend existing site3 tests, including exception boundaries |
| PR1030 `34487e7ca` | merged | Poison/destamp tests already private; raw readers use read-is-check; no partial aggregate reproduced | Treat as done |
| `private_country_root` | test_gh866_stale_country_parquet_not_laundered.py:210 | Country subtree copy, L1 symlink, explicit root cache clear/restore, cold skip | Reuse for the remaining Togo control; ensure physical country files |
| Guyana cold regression | test_guyana_index_uniqueness.py:107-164 | Explicit country/wave unlink then seven-table LSMS_NO_CACHE build; category-based collapse assertion | Isolate only this cold test; preserve row/key/category oracles |
| India cold regression | test_india_employment_activity_index.py:100-119 | LSMS_NO_CACHE rebuild of employment | Private copy even if descent does not delete var; preserve ability to build when original cache is cold |
| Togo control | test_gh866...py:292-317 | Warm-gated LSMS_NO_CACHE plot_features build and eviction spy | Reuse same-file private fixture, preserve warm gate and no-country-eviction assertion |
| Named stale issue entries | test_ghanalss_interview_date.py:69; visit_derivations.py:201/:252; gh323_site4_dfs_merge.py:270 | Already private LSMS_DATA_DIR | No edits |
| Build fingerprint exclusion | _build_registry.py:145; test_build_transform_hash.py:663 | Gate edits must not invalidate country/wave hashes | Measure actual hash samples/all-country inventory where available |

## §3 Definitions and conventions

- Per STANDING sections3-4 and CLAUDE cache behavior, cached parquet is pre-finalize and existing finalization remains the sole read-time pipeline.
- `assume_cache_fresh` skips hash/degraded checks by contract; coverage/restriction still applies. Full legacy request remains pass-through; unknown subset coverage falls through.
- test_gh1040...:481 pins corrupt FULL assumed-fresh reads as ParquetFormatError and subset corruption as fallthrough. #1071 is unresolved; preserve both.
- Guyana CONTENTS:4-49: household key(ED,SN,HH), cluster(ED,SN), because EDs repeat across segments; expected no-Makefile warnings for assets/interview_date are documented, not regressions.
- India CONTENTS:276-314: employment is activity grain(t,i,pid,act); wage is a per-day rate that must not be summed across jobs; workplace Rural is not residence.
- Togo CONTENTS:10-14/:616-640: each grappe belongs to one vague, no vague household key; plot_features source lives under Data1, with the source plot grain retained.
- CONTENTS TODO/WAITING/LOGBOOK searches were read. Guyana historical prose says collapse warnings never fire warm, whereas current country.py replays stamped grain reports. Preserve explicit cold coverage rather than relying on that prose or changing data policy.

## §4 Invariants and assumptions

- Catch only FileNotFoundError at the two assumed-fresh DATA reads; do not catch PermissionError, generic OSError, ParquetFormatError, programmer errors, map_index failures or ordinary source failures.
- Optional footer helpers already handle disappearance. Do not broaden their exception policy or new-lock the loader.
- Healthy cache hits must keep existing wave restriction, pass-through, datetime replay, finalization and no-wave-traversal behavior.
- Real-country force-build tests operate under private physical copies, not shared L2, original corpus roots or source-tree parquets. Source cache byte hashes must remain unchanged.
- Only L1 may be linked. Restore LSMS_DATA_DIR and clear memoized data_root on both entry and exit using the proven MonkeyPatch context pattern.
- Guyana/India cold tests must still run cold, including when the source cache is empty; do not introduce a warm-only skip. Togo retains its existing warm prerequisite.
- No source interpretation, country grain, corruption decision or cache writer/fingerprint logic is changed.

## §5 Reuse decisions

| Need | Decision | Reason |
|------|----------|--------|
| Cache disappearance | Extend existing read gate | None already means ordinary loader; no alternate reader/retry loop needed |
| Corruption/error controls | Reuse1040 tests, extend exception boundaries | Narrow catch must not weaken pinned behavior |
| Forced-test isolation | Reuse1030 copy/context pattern in scoped fixtures | No autouse/broad shared fixture changes; only actual remaining test cells |
| Measurement | Reuse existing hash/probe APIs and immutable copied corpus | Verify impact and mutation boundaries without shared writes |

## §6 Questions surfaced to dispatcher

No new human behavior decision needed within the explicitly approved remainder. Proposed source patch catches FileNotFoundError around get_dataframe only (the footer readers already absorb missing files), preserving genuine errors from map/finalize/ordinary source paths.
Test isolation will touch only Guyana's cold test, India's cold test, and the same-file Togo control/private fixture. Existing GhanaLSS/visit/synthetic private fixtures remain untouched.
This ledger is surfaced before implementation per prior-art-ledger; parent can review/surface authorization without asking the human again.

### Phase3 -- verification

Implementation and aggregate report committed/published as `2aef1c2ac`.
The read gate extends its existing two reads with FileNotFoundError-only fallthrough; ordinary grading/finalization and country policy are reused unchanged.
Existing `acf`/`_plant` fixtures cover disappearance before/after footer reads, manifest full/subset and legacy full requests, nonmissing read errors, index-mapping FileNotFoundError, ordinary source failure and successful ordinary-hit finalization.
The initial unchanged-source proof was5 failures/10 passes; the final17 controls with only the frozen852 gate rebound in memory produced6 failures/11 passes, including the newly added before-footer full case.

- Whole1040 read-gate file:48 passed.
- Focused eight-file suite with4 loadfile workers:190 passed,0 skipped/failures/errors,33 warnings in68.77s.
- Three original force-build controls on an expendable private copy:3 passed but15 outer parquet unlinks,16 byte changes and18 file replacements.
- Candidate physical-copy controls:3 passed with zero outer mutation;67 seed files SHA-identical with distinct inodes, and immutable original source bytes/inodes/mtimes unchanged.
- Cold Guyana/India controls:2 passed starting with empty outer L2, without introducing warm-only skips.
- Actual complete inventories:549 country hashes,2139 wave entries (30 existing None) and39 table transform fingerprints identical before/after.
- API surface audit and `git diff --check`:passed.

The §2 reuse/§3 definitions/§4 invariants are preserved.
No source/country build machinery, scientific default, corruption policy or fingerprint definition was changed.
The only test fixture scope changes are Guyana/India cold controls and reuse of the same-file private fixture for Togo; copytree now follows country symlinks so L2 remains physical.
Known #1071 and assume-fresh unstamped-script-output behavior remain explicit in `slurm_logs/framework_campaign_2026-10-20/778/REPORT.org`.
No new survey idiosyncrasy was found; the historical warm-warning documentation contradiction remains reported rather than used to weaken the cold control.
Independent parent review remains required before GitHub PR/landing; source is frozen after this ledger verification commit.

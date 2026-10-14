VERDICT: needs-human -- the fix does what the issue asks on every path I exercised (in-process hook, out-of-process make child, make backend, L2-wave-hit union rebuild, Feature(), strict lever) with no change to served values and a clean full suite, but merging it costs a CORPUS-WIDE cold re-warm of BOTH cache tiers (measured: 10/10 probed country hashes and the wave input hash moved, in countries the branch never touches) against a brief that budgeted one country; a human must accept that bill before merge. No code defect blocks; §6 lists small follow-ups.

Reviewer: red-team element `868-reduce-agreed`, job 39199804, branch `fix/868-reduce-agreed` at `dda202b51`, base `origin/development` at `b5103b64f` (merge-base `242573697`; the extra 22 files in `git diff --stat origin/development..HEAD` are development's own backlog bookkeeping, not this branch -- against the merge-base the diff is exactly the 14 files the brief names).

Environment facts that matter for reading this: the private root was seeded with Togo only. **Uganda had no warm parquets at all** (empty wave dirs), so the Uganda "before" side was built cold on base code in my root first, then snapshotted. `nproc` reported 1 because `OMP_NUM_THREADS=1` was already exported; the affinity mask was 8 CPUs and I used them.

## 1. Refutations

**R1. "Warm read is silent on development" (the issue's symptom) -- held, reproduced.**
Base code, cold build of Uganda `cluster_features`: 8 `GrainConflictWarning`, `grain_reports()` empty, no `lsms_grain_audit` on any parquet. Base warm read in a fresh process: 0 grain warnings of any class, `grain_reports()` empty. Uganda `crop_production` on base: the reducer runs in the `make` child, so the parent saw **no** grain warning even cold, and the warm read was silent too.
Command: `read_tables.py base_cold|base_warm Uganda crop_production,cluster_features` under `PYTHONPATH=<base worktree>`, `LSMS_COUNTRIES_ROOT=<base>/lsms_library/countries`, import asserted.

**R2. "The signal survives the cache" on the branch -- held, on the realistic path.**
No manual eviction; the moved hash drove the rebuild. All 7 `crop_production` wave scripts re-ran (all 7 wave parquets rewritten at 04:00, sizes changed on the 4 conflicting waves), 4 wave parquets carry `cells_blanked` stamps, the country parquet folds them, and a fresh-process warm read emits 4 `GrainCollapseWarning` with `grain_reports(n=4)`, every report `from_cache=True`, `kind=cells_blanked`, `destroyed=0`, `nan_key_rows=0`. `cluster_features`: 8 wave stamps, 8 folded, warm read 8 warnings, `grain_reports(n=8)`. Waves with no conflict (2010-11, 2018-19, 2019-20) carry no stamp, by the same convention Site 1 uses.

**R3. "11 blanked groups" (issue, CONTENTS.org) -- held.** 2009-10: 6, 2011-12: 1, 2013-14: 3, 2015-16: 1 = 11 (`Unit_sold` 9, `Condition_sold` 2).

**R4. "What the reducer returns is unchanged" -- held.** `assert_frame_equal` base vs branch, sorted: Uganda `crop_production` cold and warm (130,606 rows, 452,306 nulls both sides), Uganda `cluster_features` cold and warm (4,805 rows, 3,902 nulls), Togo `cluster_features` base-warm vs branch-cold and branch-warm (540 rows, 10 nulls). All EQUAL. Index unique everywhere. `NullReadWarning` / `NullIndexKeyWarning` counts identical before and after (crop_production 1/1, cluster_features 2/0).

**R5. "Togo: nothing filed, nothing changed" -- held.** 0 reports, 0 grain warnings, no stamp, identical frame on branch cold and warm.

**R6. "The make-run path folds wave stamps into the country stamp" (the brief's hard requirement) -- held, run for real, not on a fixture.** `LSMS_BUILD_BACKEND=make Country('Uganda').crop_production()` in a copy of my root: the country concatenator `_/crop_production.py` ran (hashless `var/crop_production.parquet` written 04:07:27), its stamp holds 4 reports (2009-10:6, 2011-12:1, 2013-14:3, 2015-16:1), and the read replayed 4 `GrainCollapseWarning`. `data_root('Uganda')` works. The commit said "verified on fixtures only"; this is the first non-fixture run of that path.

**R6b. L2-wave-hit union rebuild (the path where the hook never runs) -- held on real data.** Deleted only `Uganda/var/cluster_features.parquet` in my root and re-read in a fresh process: the 8 wave parquets were served from the L2-wave hit, 8 `GrainCollapseWarning` fired, ledger 8 with `from_cache=True`, and the new country stamp carries all 8 waves.

**R7. "Cache cost: corpus-wide, not Uganda + Togo" -- held, and it is the item a human must weigh (see §5).** Table below: 10 of 10 probed `_table_cache_hash` values moved, including Malawi, Ethiopia, Kosovo, Albania, which the branch never touches; `build_transforms_fingerprint` moved for all 4 probed tables; `Wave._input_hash` moved for Kosovo 2000 `housing`; Uganda's `framework_imports_fingerprint` moved (settling the brief's UNVERIFIED question: the in-function import IS reached).

**R8. "Cold-path signal unchanged: still GrainConflictWarning" -- half held.** True for the in-process hook (cluster_features cold: 8 `GrainConflictWarning`, 0 `GrainCollapseWarning`). For the out-of-process `crop_production` the child's `GrainConflictWarning` goes to the child's stderr (4 lines in my log), and the parent **also** emits 4 `GrainCollapseWarning` during the same cold build, because it replays the wave stamp it just caused to be written. Two classes for one finding on a cold `make` build. Not a defect, but the sentence overstates.

**R9. "313 passed unchanged" and the self-report.** The fix agent's final message was "both background tasks ... still in flight"; it never delivered a report, and its wider test run was abandoned when its turn ended. The only regression evidence it left is the commit message. My measurements are §3.

**R10. Strict lever -- held.** `LSMS_GRAIN_STRICT=1` warm read of Uganda `cluster_features` raises `GrainCollapseError` naming `Uganda/cluster_features/2005-06 ... BLANKED 140 cell(s)`. This is a behaviour change for warm reads under strict (they used to pass silently); it is the documented intent and consistent with the cold build, which already raised `GrainConflict`.

**R11. `Feature()` and the harness -- held.** `Feature('cluster_features')(['Uganda','Togo'])` warm: 8 `GrainCollapseWarning`, ledger 8; `crop_production`: 4 and 4. `bench/feature_audit/scan.py` recorded all 12 replayed findings as `runtime_warning` records (2 each, one per phase), severity C, `expected: False`. Note the harness does **not** drain `grain_reports()` structurally (only `null_read_reports()`), so the `dropped` field on the new report kind (see §5) is not misclassified by it today.

**R12. New tests -- 23 of 24 fail on base.** Copied into the base worktree the file fails at collection (`ImportError: cannot import name 'build_context'`). With the three missing names stubbed as no-ops, 23 fail substantively (the reducer rejects `country=`, the ledger is empty, the seven Uganda scripts pass no `grain_audit=`, the de-dup passes no cell) and 1 passes on base: `test_the_context_does_not_move_any_build_fingerprint`, which is a sanity check rather than a discriminator. Acceptable.

**R13. CONTENTS.org -- true.** Uganda: the idiosyncrasy named (two Uganda sites reach the ledger by different routes because they run in different processes) is the actual mechanism, verified in R2 and R6; the 11-group figure is right; "the Uganda re-warm has NOT been paid" is honest. Togo: the note that its alias keeps `on_conflict='raise'` and nothing changes is verified (R5).

## 2. Claimed vs measured

| claim (commit / CONTENTS / brief) | measured |
|---|---|
| 11 blanked crop_production groups | 11 (6+1+3+1 over 2009-10, 2011-12, 2013-14, 2015-16) |
| reducer output unchanged | 6/6 frame pairs `assert_frame_equal` EQUAL |
| warm read replays as GrainCollapseWarning | crop_production 4, cluster_features 8, all `from_cache=True` |
| wave stamps written by the 7 make-run scripts | 4 stamped (the conflicting waves), 3 unstamped (no conflict); all 7 rewritten |
| country concatenator folds wave stamps (make backend) | 4 reports in the hashless `var/` parquet, replayed on read |
| Togo files nothing | 0 reports, identical frame |
| every probed `_table_cache_hash` moved (Uganda x4, Togo x2, Malawi hr, Ethiopia sample) | 10/10 moved (those 8 + Kosovo housing + Albania sample) |
| `build_transforms_fingerprint('household_roster')` moved | moved (and cluster_features, crop_production, sample) |
| Uganda `framework_imports_fingerprint` moved | moved |
| ContextVar moves no fingerprint | held (test passes on branch; `btf` identical inside/outside context) |
| "313 passed unchanged" (targeted) | my superset of that selection: **464 passed** |
| cold signal still GrainConflictWarning only | in-process: yes (8/0); out-of-process: child stderr GrainConflictWarning + parent 4 GrainCollapseWarning |

## 3. Regression

Targeted (`tests/test_gh868_reduce_to_agreed_ledger.py tests/test_gh323_explicit_reducers.py tests/test_grain_collapse*.py tests/test_gh323_togo_cluster_features.py tests/test_uganda_323_grain.py tests/test_build_transform_hash.py tests/test_schema_consistency.py tests/test_feature_canonical_index.py`, `-n 6 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`):

```
464 passed, 18 warnings in 419.82s (0:06:59)
```

Full suite (verbatim recipe, `-n 8 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`, private root):

```
4571 passed, 189 skipped, 3 xfailed, 1 xpassed, 868 warnings in 1732.94s (0:28:52)
(0 failed, 0 errors, 0 OpenBLAS thread-init lines, no worker lost; the xpassed test is `tests/test_pakistan_cluster_features.py::test_extraction_is_cluster_grain`, a pre-existing GH #740 xfail unrelated to this branch, identified by re-running the two files that carry `mark.xfail`: 9 passed, 7 skipped, 1 xpassed)
```

`bench/feature_audit/scan.py --features crop_production cluster_features --countries Uganda Togo`:

```
features: 2  records: 222  skips: 0
by status: {'warn': 222}
candidate findings (warn/fail/error) by severity class: {'C': 222}
```
`ResourceWarning` counts were identical base vs branch on the same reads (crop_production 151/151, Togo 11/11), so the 194 `unclosed file` records are pre-existing. Non-noise findings are all pre-existing class C (`null_content` harvest_month 2009-10/2010-11 and Latitude/Longitude 2019-20, `index_levels_match_scheme` extra `v`, `has_household_index`, Togo `no_constant_columns`); 194 of the 222 are `unclosed file` ResourceWarnings from the wave-script fingerprint walk; 24 are the replayed `reduce_to_agreed` findings this branch makes visible. Nothing fails.

## 4. Scope findings

- 14 files against the merge-base, all named by the brief or implied by it (framework, 7 Uganda wave scripts, Uganda country concatenator, `uganda.py`, two CONTENTS.org, one new test file). No baseline / golden / blessed / lockfile / dependency edit. No existing test touched, so no weakened, skipped, xfailed or moved assertion. `test_uganda_323_grain.py:258`'s destroyed/nan_key filter is untouched and passes.
- `find lsms_library/countries -name '*.parquet'`: 0 in the clone and 0 in the base worktree after all builds. All writes went to my private root (and a scratch copy of it for the make-backend run, deleted); `find ~/.local/share/lsms_library -newer <first script>` is empty, so nothing landed in the default data root. `git status --short` empty at the end; worktree removed.
- The fix went further than "route through the ledger": it added a `ContextVar` build context, a `_file_grain_report` split, wave-stamp replay at three read sites, and wave-parquet stamping on the YAML L2-wave write. Each is exercised by the fixture tests and by R2/R6; none changes served data. Reasonable scope for the issue's "make-run path" clause, which needed the wave-stamp propagation.

## 5. Residual risks and what a human must decide

1. **Corpus-wide cold re-warm (both L2 tiers), against a brief that budgeted "one-country-rebuild".** Measured: every probed hash moved, including four untouched countries, and `Wave._input_hash` moved, so every L2-wave parquet is stale too. The cause is placement, not the ContextVar: `_format_grain_report`, `_record_grain_report`, and `_replay_grain_audit` are reachable from the tagged `_normalize_dataframe_index`, so any edit to them moves everything (the same bill #866/#871 paid). A placement that put the new `cells_blanked` formatter branch and the replay helpers in an `_EXCLUDED_CALLABLES` read-path module might have kept the bill at Uganda + Togo; I did not build that alternative. @ligon decides whether a disclosure-only fix is worth a full corpus re-warm now, or whether it should ride with the next change that already costs one.
2. **`dropped` on a `cells_blanked` report is the collapse count, not a loss.** For the cluster hook it is `n_rows - groups` = the household-to-cluster projection (2,801 for 2005-06); for crop_production it is the de-dup count. It shares a field name with Site 1's row-collapse count. `destroyed`/`nan_key_rows` are zero and the harness does not read the ledger, so nothing misfires today; a future consumer filtering on `dropped` would.
3. **Warm reads under `LSMS_GRAIN_STRICT=1` of Uganda `cluster_features`/`crop_production` now raise.** Intended, and the cold build already did; but anyone who relied on strict passing warm for Uganda will notice.
4. **Warning volume**: one `GrainCollapseWarning` per stamped wave per warm read (8 for Uganda `cluster_features`), and on a cold `make` build both classes fire (R8). Same convention as Site 1, just louder for Uganda.
5. **`_replay_grain_audit`'s docstring says a Nigeria PP/PH wave parquet "holds two rounds' reports, each already naming its round".** Under `build_context` the wave is `self.year` (the folder), and explicit kwargs win over the frame's `t`, so an in-process hook in a two-round folder would file under the folder name, not the round. No live caller today; doc-only inaccuracy.

## 6. Recommended follow-up commits (if the re-warm is accepted)

1. `lsms_library/country.py` `_replay_grain_audit` docstring (and `_resolve_build_context` in `build_transforms.py`): state that under the framework context a report's `wave` is the wave FOLDER (`self.year`); a two-round folder must pass `wave=` explicitly to name the round. One sentence each.
2. `lsms_library/build_transforms.py` `_cells_blanked_report`: rename `dropped` to `collapsed_rows` (or document in the dict's comment that it is the projection/de-dup count, never a loss) so it cannot be read as Site 1's `dropped`; update `test_gh868_reduce_to_agreed_ledger.py::test_replay_is_fatal_under_strict_through_the_shared_lever`'s literal accordingly.
3. Commit message / `Uganda/_/CONTENTS.org`: replace "the cold-path signal is unchanged" with the measured statement: in-process hook unchanged; a cold `make` build emits the child's `GrainConflictWarning` on stderr and the parent's `GrainCollapseWarning` replay in-process.
4. (Decision, not a commit) Record in the merge note that the re-warm is corpus-wide and both-tier, so the next person to warm a root does not attribute the cold builds to something else.

Artefacts: `/local/job39199804/out/*.log|*.json|*.pkl` (job-local, destroyed with the job); the suite log is `/local/job39199804/pytest.log`.

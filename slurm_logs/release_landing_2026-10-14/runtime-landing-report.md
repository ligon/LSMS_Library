SCOPE DEVIATIONS

- The first Windows merge commit triggered the repository post-commit hook,
  publishing `origin/landing/runtime-windows` at `23c890047`. No explicit push
  or GitHub write was run. The coordinator was told immediately. All later
  commits/merges used `-c core.hooksPath=/dev/null`; final heads are local.
- No forbidden source, baseline, fixture-data, lockfile, dependency, DVC-sidecar,
  or shared-cache mutation. No DVC CLI, global clear, force push, assertion
  weakening, or test skipping was introduced. One disposable diagnostic
  comparison encountered the already-reviewed cold/warm StringDtype index
  distinction; it was retained as evidence rather than changing a test.

## Final branches

| Original branch | Landing branch / full SHA | Worktree |
|---|---|---|
| feature/964-windows-script-builds | landing/runtime-windows `396fa215d3b2b9e1e7fdaf83361d932a32ccd449` | runtime/windows |
| fix/810-followup | landing/runtime-810 `917fb4e0bdd6227c10e34abdf268531f4fef6ae8` | runtime/810 |
| fix/868-followup | landing/runtime-868 `e7c494aed6fc680b7055467478b82a0c4a8cc01c` | runtime/868 |
| fix/readers-xport-sniff | landing/runtime-readers `6677925905c5865ff295d2cd455346bb1279dad1` | runtime/readers |
| fix/970-json-make | landing/runtime-970 `7c229350a7fe35913b0151498a67607cd16ace63` | runtime/970 |

Worktree paths are relative to `/local/job39919092/release-work/`. All five
heads are separate descendants of their original branch and include integration
base `452302adcfdbf3af538ef06375df1ac6c5aaa8a0`. All five worktrees are clean.

## Changes and verification

Windows: retained both conflicting subprocess-env guards, all nine deferred
UTF-8 readers, the binary existence probe, no-make helper and its fingerprint
exclusion. Added false-by-default `run_windows_script_build` manual input; the
secret-bearing job permits integration pushes or that explicit dispatch, never
PR events. Following coordinator review, job-only `PYTHONPATH=''` prevents the
workflow checkout path from hiding the installed wheel. The probe now requires
installed code under this interpreter's site-packages, outside GITHUB_WORKSPACE,
and its packaged countries config. Source-checkout negative control fails at
that assertion. Merged current development (#980), then unified its bare manual
dispatch trigger with the Windows input into one YAML key. A new YAML-node
regression rejects duplicate mapping keys, including the exact duplicate-trigger
case, and checks both jobs retain their dispatch conditions. The uncorrected
merge fails two focused checks; final focused check: **69 passed**. Earlier
broader set including build-fingerprint tests: **100 passed**. Native Windows
remains coordinator CI.

Cold Linux builds in distinct empty private roots matched the frozen Windows
expectations both with make and with make absent from PATH:

| Table | Rows | Digest | make / no-make seconds |
|---|---:|---|---:|
| Uganda shocks | 14,457 | 64a53fa7af55 | 61 / 54 |
| Uganda assets | 116,808 | 9202ed542a14 | 58 / 54 |

The no-make run emitted the expected once-only RuntimeWarning. Current Uganda
Makefile expansion has 126 parquet prerequisites and no find errors (the old
handoff's 128 described a previous tree). These were source Linux builds, not
installed-wheel proof.

810: corrected missing-file versus missing-column handling and removed obsolete
skill prose claiming core readers still use dvc.api.open. **9 passed**.

868: reports carry a separate wave_folder from build_context; explicit wave
rounds remain untouched, and legacy reports fall back to wave when selecting
a wave stamp. Added cold/warm synthetic round fixture including an unrelated
folder's same-round report, and an assertion on the reducer's actual
collapsed_rows result. Corrected Uganda's five replays/twelve groups after #848.
**109 passed**. Both new discriminating tests fail on pre-followup b471342e7.
The final combined suite also covers the small None-context fallback refinement.

Real private Uganda measurements: cluster_features **4,805 rows / eight reports**,
cold in-process eight GrainConflictWarnings; warm eight replayed reports.
crop_production **133,331 rows / five reports / twelve blanked groups**; cold
child warnings plus five parent replay warnings; warm five replay warnings.
The initial diagnostic cold/warm DataFrame comparison fails on the pre-existing
index dtype difference (`StringDtype(na_value=<NA>)` vs `na_value=nan`), exactly
as the independent verdict documented. No scientific expectation was changed.

Readers: a claimed .dta with contradictory magic raises before text readers;
a valid Stata header whose two binary decoders fail also raises with the decoder
error chained, preventing latin-1's empty garbage CSV frame. Corrected 36 SSP /
33 unverified counts and all five surveys represented in the byte-3 census.
**89 passed, one credential-gated country guard skipped**; standalone reader
suite **37 passed including real Peru EXPEND.SSP**. On the pre-fix tree, the
four new corruption cases give **3 failed / 1 passed**, including the
truncated latin-1 silent-frame regression. The last commit only corrects ledger
wording identifying the skipped test.

970: development merged cleanly; the JSON direct-read/no-make code and the
synthetic fixture's before/finally root-cache clears remain intact. **69 passed**.
No additional code change was necessary.

## Combined runtime interaction check

The five branches merged cleanly in order Windows → 810 → readers → 868 → 970
into `landing/runtime-combined`. At `0c4c213fca3a040a99616aa1e14503f6a1573936`,
**324 passed, one skipped, one warning in 42.64 seconds** across 14 focused
files, including both no-make branches, all subprocess guards, UTF-8 portability,
reader corruption/XPORT/DAT, grain stamps/replays, hashes and JSON persistence.
The skipped case is `tests/test_null_read_guard.py:387`, requiring S3 credentials.
It is not accepted as credentialed-data proof. Real SSP test ran.

Final combined head `ff2b8dd187629675d77599d4b9938d6636b73c4a` subsequently merges
the Windows installed-identity guard, reader-ledger wording correction, #980,
and the dispatch-key reconciliation. Its final CI / JSON / subprocess /
portability selection gives **68 passed in 15.29 seconds**. It preserves UTF-8
JSON opens/writes and every no-make guard. All constituent final refs remain
available separately; coordinator owns landing.

## Cache scope and isolation

All five original branches belong to the corpus-wide **both-tier** invalidation
bundle. Do not warm the shared corpus between merges. Local follow-ups do not
justify separate shared clears. Private `LSMS_DATA_DIR` roots live at
`/local/job39919092/release-work/runtime/data-*`; only `dvc-cache` is symlinked
to the shared L1 blob cache. No writable parquet was linked or copied back.
Every Python run asserted its worktree import and used worktree countries_root,
CPUs 0–7, BLAS=1, LSMS_BUILD_WORKERS=1; pytest used six loadfile workers with
file output. No PYTHONSAFEPATH was exported into builds. Final enumeration
found **zero in-tree parquets** in all six prepared worktrees.

## Country idiosyncrasies and residual drafts

Uganda: historical comm is a baseline EA carried with movers; district/GPS
conflicts can reflect dispersal and must stay blanked. #848's second harvest
condition slot adds the twelfth sold-condition conflict (Beans, 2018-19 season B).
Its CONTENTS already documented this; the newly added #868 prose saying
"The 11 groups themselves are unchanged" contradicted the current twelve-group
entry and was reconciled explicitly. Assets' country-script versus wave-build
history also explains why values must be checked through the public API.

Peru: 1990 is Lima-only; SSP means SAS XPORT, WT is a real sampling weight,
and N00A.CID is constant 1.0, not a cluster. The prior "other 36 blobs" prose
conflicted with 36 SSP total / three verified and is now "other 33 SSP blobs".
No new survey mapping was inferred.

`RESIDUAL_DRAFTS.org` contains concrete issue dispositions/drafts: import-time
credential sync, non-DTA unreadable binary/text fallback and SPSS retry,
#699's remaining Peru configuration work, and the distinct root-cache test
isolation defect. It reconciles obsolete --no-root work and treats the old
GhanaLSS order-dependent/path-parse findings as unverified investigation
candidates. No issues were posted by this lane.

All logs and diagnostics are in `/local/job39919092/release-work/control/runtime/`.

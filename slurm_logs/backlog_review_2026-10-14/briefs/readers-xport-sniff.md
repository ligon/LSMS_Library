# RED-TEAM review of branch `fix/readers-xport-sniff` @ 77d2b9829 (element `readers-xport-sniff`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/readers-xport-sniff` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Peru; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/readers-xport-sniff/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

## Mandate: REFUTE
Your job is to find the reason this branch must NOT be merged. Default to `verdict: fail` where you are uncertain. A fix agent wrote this branch and its report; the report is a self-report and its "SCOPE DEVIATIONS: none" is a claim, not a proof. Check, by running, not by reading:
1. **Does it fix the issue?** Reproduce the issue's symptom on `origin/development` code (a second clone: `git worktree add /local/job$SLURM_JOB_ID/base origin/development` is allowed -- that is your own scratch, not the shared mirror) and show it gone on the branch, in your private root, cold where the change is a build-path change (`.venv/bin/python -m lsms_library.cli cache clear --country X` against YOUR root; NEVER against the live one).
2. **Do the claimed numbers reproduce?** Every before/after figure in the report and commit messages: re-measure and tabulate claimed vs measured. A number you cannot reproduce is a finding.
3. **Data loss / laundering.** Row counts, index uniqueness, `grain_reports()`, `null_index_reports()`, `null_read_reports()` on every touched table before vs after; any `find lsms_library/countries -name '*.parquet'` hit; any write outside the private root.
4. **Healthy countries intact.** For a framework change: the full suite under the verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest tests -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > /local/job$SLURM_JOB_ID/pytest.log 2>&1`, then read the file), and `bench/feature_audit/scan.py --features <touched feature> --countries <2-3 warm ones>` where a feature is touched. For a country change: that country's tests plus `tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py`.
5. **Scope and stop-list.** `git diff --stat origin/development..HEAD` versus the task's named files; any baseline/golden/blessed/lockfile/dependency edit; any weakened, skipped or xfailed assertion (a moved pin is acceptable ONLY with before/after numbers in the commit message -- check they are there and that they match your measurement).
6. **Are the new tests meaningful?** Would each new test fail on `origin/development`? Run them there (the worktree) and say which do not.
7. **CONTENTS.org**: did the agent name the idiosyncrasy, and is what it wrote true? Anything it claims contradicts CONTENTS.org -- verify against the raw data.
8. **Cache cost claim**: is the stated re-warm scope right? (A `_/*.org`, `_/*.py`, `data_scheme.yml`, wave-module edit moves that country; `get_dataframe`/`_normalize_dataframe_index`-reachable framework edits move the corpus.)

Environment: `PYTHONPATH` is your clone; use `PYTHONSAFEPATH=1` for `-c`/`-m` runs only. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`. `GH_REPO` is set; `gh` reads only, no writes. Read `lsms_library/countries/<C>/_/CONTENTS.org` for each country concerned.

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- You commit NOTHING and push NOTHING: this is a review. `git status --short` must be empty at the end (delete any scratch you made inside the clone).
- Write the report to `$STAGE/VERDICT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.


## Verdict format (write to `$STAGE/VERDICT.md` AND repeat as your final message)
Begin with one line: `VERDICT: pass | pass-with-fixes | fail | needs-human` and one sentence why. Then:
1. **Refutations** -- each a claim from the report/commit, what you ran, what you got (a claim that survives is listed as "held", with the command).
2. **Claimed vs measured** table.
3. **Regression** -- suite/scan summary lines verbatim.
4. **Scope findings**.
5. **Residual risks and what a human must decide** (the branch may be right and still need a decision).
6. **Recommended follow-up commits** (concrete, with paths), if `pass-with-fixes`.

## Rules the fix agent was under (2026-10-14): an open issue must describe exactly the remaining work -- if the branch resolves part of an issue, the report must DRAFT the residual; check that it did.

## The task the fix agent was given
## Scope note from the scrum master
Both edit `local_tools.read_file`/`get_dataframe`, which is in EVERY build fingerprint (CLAUDE.md: any get_dataframe edit moves 37/37). RULING: that corpus re-warm is accepted and will be paid once with #810/#868/#970; state it, do not pay it. Peru 1990's 39 .SSP files are the live instance for #699 (`pandas.read_sas` reads all 10 columns where pyreadstat gave 7 NaN -- see CLAUDE.md 'Silent All-Null Read'); add the magic-byte sniff tests on tmp files, no network.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #699 -- local_tools: `read_file` has no SAS XPORT branch -- Peru 1990 (39 files) is on disk and unreadable
```json
{
 "tier": "B",
 "type": "framework",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 3,
 "summary": "Claim holds. local_tools.read_file (local_tools.py:1259-1340) chains .DAT/.DCT -> read_spss -> read_sav -> read_parquet -> from_dta -> pyreadstat.read_dta -> read_csv -> read_excel -> ... with no SAS XPORT branch and no reference to read_sas/xport anywhere in lsms_library/*.py (the only mention is the null_read_audit.py:18-20 docstring recording the read_xport mis-parse). Re-verified today on pandas 3.0.6: pd.read_sas(blob, format='xport') on Peru/1990/Data/EXPEND.SSP returns 1509x5 with every column fully populated. Peru/1990/ has 37 sidecars on disk here and NO _/ config, so even with the branch the wave stays `absent` until someone writes data_info.yml (a second, Tier-D step). Contradiction to report: Peru/_/data_scheme.yml:32 still says '1990 ships SPSS/PC+ .SSP files, which get_dataframe() cannot read at all' while Peru/_/CONTENTS.org:917-945 corrected that on 2026-08-22 to 'SAS XPORT, readable TODAY'.",
 "evidence": "lsms_library/local_tools.py:1259-1340; grep -rn 'xport|read_sas|SSP' lsms_library/*.py; pd.read_sas on the dvc-cache blob of Peru/1990/Data/EXPEND.SSP.dvc -> (1509, 5) all non-null; ls lsms_library/countries/Peru/1990/_ -> missing; Peru/_/data_scheme.yml:29-34; Peru/_/CONTENTS.org:917-945.",
 "approach": "In read_file, dispatch on the XPORT magic bytes (b'HEADER RECORD*******LIBRARY HEADER RECORD') before the Stata attempt and read with pd.read_sas(f, format='xport') (never pyreadstat.read_xport -- it returns 7/10 all-NaN columns silently; Site R's 1/3 null-fraction guard would catch that mis-wire). Fix the stale comment in Peru/_/data_scheme.yml:32. Wiring Peru/1990/_ (sample from N00A/EXPEND with WT as the weight, PANEL.SSP as the 1985<->1990 person link) is a separate add-wave task.",
 "verify": "Unit test reading EXPEND.SSP (or a tiny synthetic XPORT written with pandas) through get_dataframe asserting all columns populated; existing reader tests. CACHE COST: edit to get_dataframe/read_file moves 37/37 _table_cache_hash (Site R rule) -> corpus-wide re-warm; batch with #712 (same function, same sniff) and the #805 reader change.",
 "contents_org": "Peru/_/CONTENTS.org:126-199, 917-945 -- 1990 is Metropolitan Lima only and re-uses the 1985-86 Lima panel plus a 260-dwelling urbano-marginal expansion; EXPEND.SSP.WT (two-valued, 0.5-1.0) 'appears to be the only real sampling weight anywhere in the Peru series' (1994's _weight_ is identically 1); PANEL.SSP is a person-level 1985<->1990 link; CID is constant 1.0 and not a cluster id.",
 "blocked_by": "none for the reader branch; wiring the wave is Tier D (add-wave, human)",
 "decision_for_human": ""
}
```

### #712 -- get_dataframe's Stata attempt catches only (ValueError, struct.error) -- a CSV whose leading bytes look like a Stata header dies with an uncaught KeyError
```json
{
 "tier": "B",
 "type": "framework",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 3,
 "summary": "Claim holds in substance, with a different exception on today's stack. local_tools.py:1308-1310 still wraps the from_dta attempt in except (ValueError, struct.error) (line 1310 last touched 2026-03-29, 80359af1c; PR #965 Windows readers did not change it). On pandas 3.0.6 the TYPE_MAP KeyError the issue saw is now wrapped by pandas into ValueError('cannot convert stata types'), so that exact path is closed -- but the class is not: a plain CSV with header 'hhid,name,val' and 100,000 rows escapes get_dataframe as an uncaught MemoryError raised from pandas/io/stata.py:1698 (reader.read allocating nobs x nvar from a garbage header) via ligonlibrary/dataframes.py:455 and local_tools.py:1309. 'id,...' and 'region,...' headers of the same size read fine -- content-dependent exactly as described. Sweep of the held corpus: 413 .csv/.CSV sidecars, 93 blobs present in the shared dvc-cache, 0 begin with a byte in {104,105,108,110,111,113,114,115}; 320 blobs not present and not measured.",
 "evidence": "lsms_library/local_tools.py:1308-1310; git blame -L1309,1310; repro: $STAGE/tmp/hhid_100000.csv -> MemoryError traceback (local_tools.py:1358 get_dataframe -> :1309 read_file -> ligonlibrary/dataframes.py:455 from_dta -> pandas/io/stata.py:1698 read); pandas.io.stata.StataReader._read_old_header source shows 'raise ValueError(f\"cannot convert stata types ...\") from err'; CSV first-byte sweep over countries/*/*/Data/**/*.csv.dvc blobs (93 present / 320 missing / 0 hits).",
 "approach": "Sniff, do not widen: in read_file (local_tools.py) only attempt from_dta when the first bytes are a Stata header -- old format: byte0 in 102..115, byte1 in {1,2}, byte2 == 1, byte3 == 0; new format: file starts with b'<stata_dta>' -- and otherwise skip to the next reader. Keep the existing except as-is so a REAL .dta that fails stays loud (a widened bare except would re-create the silent fall-through the repo keeps removing). Same sniff can route .SSP for #699 (magic 'HEADER RECORD*******LIBRARY HEADER RECORD'). Add tests: the hhid_100000 CSV reads via read_csv; a real .dta still reads; a corrupt .dta still raises.",
 "verify": "tests/test_local_tools-style unit tests with synthetic files; pytest -n per Makefile recipe on tests touching get_dataframe; cold build of one small YAML-path country (e.g. Albania) to show the sniff changes nothing for real .dta. CACHE COST: any edit to get_dataframe/read_file moves 37/37 _table_cache_hash (Site R rule) -> corpus-wide re-warm; batch with #699 and the #805 reader change so there is one re-warm.",
 "contents_org": "n/a (framework)",
 "blocked_by": "none (sequencing only: batch with #699/#805)",
 "decision_for_human": ""
}
```


## The branch: diffstat (vs development)
```
 lsms_library/countries/Peru/_/CONTENTS.org    |  15 +-
 lsms_library/countries/Peru/_/data_scheme.yml |  11 +-
 lsms_library/local_tools.py                   | 174 ++++++++++++--
 tests/test_reader_sniff.py                    | 322 ++++++++++++++++++++++++++
 4 files changed, 495 insertions(+), 27 deletions(-)
```
## The branch: commit messages
--- 77d2b9829 test_reader_sniff: census count is 5,674 present blobs, not 5,673
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 4855c09df local_tools: sniff magic bytes before the binary readers -- XPORT branch, Stata gate (GH #699, #712)
`read_file` is a fallback chain, and a chain only works when a reader handed
the wrong kind of file FAILS.  Two ways it did not:

* #712 -- `StataReader` takes the first byte of ANY file as a release
  number; a byte in 102..115 ('f'..'s') enters the old-format header parser,
  which reads a garbage variable count and dies with whatever that garbage
  dictates.  Measured on pandas 3.0.6: a CSV whose header begins `hhid,`
  escapes `get_dataframe` as `KeyError: np.uint8(249)` at TEN rows (the
  triage record expected `struct.error` for small files and `MemoryError`
  for large; neither is what this stack does -- but the class is the same).
  Neither exception is in `except (ValueError, struct.error)`.

* #699 -- no SAS Transport (XPORT) branch at all.  Peru 1990's 39 `.SSP`
  files were on disk and unreadable: the stream pass ended in `ValueError`,
  `get_dataframe` retried with the path, and `pd.read_spss` raised an
  uncaught `ReadstatError`.  A synthetic XPORT with fewer bytes was WORSE --
  it came back as a silent (0, 1) frame.

Fix: sniff, do not widen.  `_sniff_head` reads <= 64 bytes and restores the
stream; `_is_sas_xport` dispatches to `pd.read_sas(format='xport',
encoding=latin-1)` ahead of every other reader (never `pyreadstat.read_xport`,
which returns 7 of 10 columns all-NaN with no exception on these files);
`_is_stata_dta` gates BOTH Stata attempts, which keep their existing
`except` so a genuine `.dta` that fails stays loud.

The Stata predicate deliberately does NOT test the fourth header byte.  The
spec calls it unused-and-zero; a census of every blob held locally
(8,752 sidecars, 5,673 present) found 67 of 4,697 `.dta` blobs -- Kyrgyz
1997, India 1997-98, GhanaLSS `pov_gh5.dta` -- carrying 0x3f..0xff there,
and pandas reads all of them.  A first draft that tested it rejected all
67.  With the landed predicate: 4,697/4,697 `.dta` and 38/38 Stata-written
`.tab` (EthiopiaRHS) pass; 0 of 93 `.csv`, 83 `.sav`, 261 `.dat`, 483
`.dct` do.  Three `.ssp` blobs are XPORT (EXPEND 1509x5, N00A 1528x10,
PANEL 3326x4; every column populated through `get_dataframe`).

Peru docs: `data_scheme.yml` still called the `.SSP` files "SPSS/PC+ ...
cannot read at all"; `CONTENTS.org` had corrected that on 2026-08-22 and
is now updated to say the branch landed.  Wiring `Peru/1990/_` remains a
Tier-D add-wave task.

CACHE COST (stated, not paid): `get_dataframe` is in every build
fingerprint (CLAUDE.md Site R rule, 37/37), so this moves every
L2-country hash and triggers a corpus-wide re-warm; per the scrum-master
ruling it is batched with #810/#868/#970.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>



# Backlog fix element `readers-xport-sniff` -- issues #699 #712

You are a worker agent in a Slurm job with your OWN clone of `development` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the warm parquets of your countries; `dvc-cache` in it is a read-only symlink to the shared blob store. Nothing you rebuild there is seen by anyone else; the live cache must not be touched.

## Guardrails
**Out of scope -- STOP and report if the task would require these**:
- Modifying any file under `tests/fixtures/`, `.coder/coverage/`, or any file whose name contains `baseline`, `golden`, `expected`, `snapshot` or `blessed`
- Modifying `pyproject.toml`, `poetry.lock`, `requirements*.txt`, `dvc.lock`, any `.dvc` sidecar, or any dependency pin
- Weakening, skipping, or xfailing any existing test assertion (moving a pinned baseline the fix legitimately moves is allowed ONLY with the before/after numbers in the commit message)
- Running any "regenerate baseline"/"update snapshot"/coverage-matrix script, or `lsms-library cache clear` against any root other than your private `LSMS_DATA_DIR`
- Any `gh` WRITE (issue close/comment/label, pr create/merge). `gh` READS are fine (`GH_REPO` is set).
- Any git push to a branch other than your own `fix/<key>` on `origin` (the shared mirror); never push to `development`; never force-push; never run checkout/reset/stash/clean on any checkout but your own clone
- Touching files outside the paths the task names, and any DVC CLI invocation (read via `get_dataframe` / `get_data_file` only)
If any of these become necessary, STOP, leave the clone in a committable state, and put the conflict at the top of your report.

## Environment facts
- `PYTHONPATH` is your clone. For `python -c` / `-m` runs add `PYTHONSAFEPATH=1`; do NOT export it into `make`/wave-script subprocesses. Assert once: `PYTHONSAFEPATH=1 .venv/bin/python -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.getcwd()), lsms_library.__file__'`.
- Cores: `$SLURM_CPUS_ON_NODE` (a whole node). Test recipe, verbatim: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <targets> -n $SLURM_CPUS_ON_NODE --dist=loadfile -q > <logfile> 2>&1` -- output to a file, never a pipe. Never `-n auto`.
- `find lsms_library/countries -name '*.parquet'` must stay empty (GH #803).
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/readers-xport-sniff` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

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

## Harness rule (read this twice)
You run under headless `claude -p`. **Your turn ENDS the instant you send a message that contains no tool call**, and everything still pending -- a backgrounded pytest, a build, a push you are "waiting on" -- is abandoned; the job's `/local` clone is then destroyed. Three elements before you lost their reports and uncommitted tests exactly this way. Therefore:
- Run every test and build in the FOREGROUND with a timeout (`timeout 50m ...`), writing output to a file you then read. NEVER background a command and "wait for a notification"; there are no notifications here.
- Commit and push (`git push origin fix/<key>`) BEFORE writing your final message, and `git status --short` must be empty when you do.
- Write the report to `$STAGE/REPORT.md` yourself (the env var `STAGE` is set) AND repeat it as your final message.

## Report format
Your final message MUST begin with a **SCOPE DEVIATIONS** section listing every file touched that the task did not name ("None" is the preferred answer; a stop-list match is prefixed `!!! SCOPE-VIOLATION-CANDIDATE !!!` and left uncommitted). Then, in order:
1. Commit SHA(s) and branch name (`fix/<key>`), and confirmation the branch was pushed to `origin`
2. Verification output: one line per command, with the pytest summary line pasted verbatim; before/after numbers for every data claim
3. Cache cost of the change (which tables/countries re-warm), stated, not paid unless the task says so
4. Surprises, and anything that contradicts CONTENTS.org (quote it)
5. Decisions you had to take, named as decisions
A fix you have only reasoned about, without a green run, is not done -- say so rather than claiming success.

# RED-TEAM review of branch `fix/framework-small` @ de2284da2 (element `framework-small`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/framework-small` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Mali; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/framework-small/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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
Seven small framework items; one commit each, in this order: 694, 915 (half 1 ONLY -- the difflib suggestion; half 2 is a human decision, draft it), 905 (measure before/after on a 400k-row synthetic frame and on Mali food_acquired cold in your private root; result must be identical to within 1 ULP -- cite the GH #871 precedent), 909, 703, 734 (item 1 only), 792 (close the handle; count warnings before/after on a cold Mali build). For each: stash-probe `Country._table_cache_hash` on two countries before/after and report which moved; #905 touches `_finalize_canonical_food_acquired`, which IS build-path -- state its cost.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #694 -- _apply_categorical_mappings nulls non-string values via an unguarded .str.strip() on any column whose name matches a categorical table
```json
{
 "tier": "B",
 "type": "framework",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 4,
 "summary": "Claim holds verbatim: lsms_library/country.py:3048-3050 still reads `if hasattr(df[col], 'str'): df[col] = df[col].str.strip()` before `.replace(rdict)`, and pd.Series(['U','Urban',None,1,4],dtype=object).str.strip() -> ['U','Urban',None,nan,nan] on pandas 3.0.6. The hazard is COLUMN-only: the index-level branch (country.py:3058-3063) uses df.rename(index=rdict, level=...) with no strip. No regression test exists (no test references the strip). Exposure unchanged from the issue (zero live instances).",
 "evidence": "lsms_library/country.py:3048-3050, 3058-3063; python one-liner demo on pandas 3.0.6; grep tests/ for str.strip/_apply_categorical_mappings -> only unrelated hits.",
 "approach": "country.py:3049 -> strip only strings: `df[col] = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)` (or a boolean mask over isinstance), then `.replace(rdict)`. Add tests/test_categorical_mapping_resolution.py case: a numeric-valued column survives an unrelated same-named table unchanged, and a whitespace-padded string label still maps.",
 "verify": "tests/test_categorical_mapping_resolution.py + the new case; warm Country('GhanaLSS').sample() unchanged. CACHE COST: expected 0 -- _apply_categorical_mappings runs under _finalize_result, which is in _build_registry._EXCLUDED_CALLABLES; confirm with the stash-probe (CLAUDE.md measures 0 of N for read-path edits) rather than asserting it.",
 "contents_org": "n/a (framework)",
 "blocked_by": "none",
 "decision_for_human": ""
}
```

### #915 -- Country() validates against the filesystem while countries() documents the accepted set: typos warn once and fail late, non-countries construct silently
```json
{
 "tier": "C",
 "type": "framework",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 3,
 "summary": "Still exactly as filed: country.py:1943-1949 resolves the path, raises only on traversal, and `warnings.warn(f\"Country directory not found ...\")` with default stacklevel and no suggestion; catalog._country_dirs (catalog.py:22-34) is the registered list and its docstring still claims it is 'exactly the strings accepted by Country'. No code constructs Harmonized_LSMS-ISA_Ag / Tanzania_Kegera / KenyaLPS by name (grep over lsms_library, tests, bench, scripts: none).",
 "evidence": "country.py:1943-1949; catalog.py:22-34,71-72; grep -rn \"Country('Harmonized|Tanzania_Kegera|KenyaLPS\" -> none",
 "approach": "Two halves. (1) No decision needed: in Country.__init__ compute close=difflib.get_close_matches(name, _country_dirs(), n=3, cutoff=0.6) with case-insensitive matching, and raise ValueError naming them when the directory does not exist (keep the traversal check). (2) Decision: also raise when the directory exists but lacks _/data_scheme.yml (the 11 dirs incl. .dvc, Afghanistan #702). Fix the two docstrings either way.",
 "verify": "tests: Country('Ghana') raises ValueError mentioning GhanaLSS and GhanaSPS; Country('ghanalss') suggests GhanaLSS; traversal still ValueError. CACHE COST: 0 -- __init__ is not a tagged transform; confirm with a 35-table _table_cache_hash probe.",
 "contents_org": "n/a (framework)",
 "blocked_by": "",
 "decision_for_human": "Raise for an existing directory with no _/data_scheme.yml (the 11 unregistered dirs) -- yes/no?"
}
```

### #905 -- perf: groupby-agg lambda with min_count=1 is ~145x slower than vectorized; used in _finalize_canonical_food_acquired + Panama wave scripts
```json
{
 "tier": "C",
 "type": "perf",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 4,
 "summary": "Still true: build_transforms.py:199-200,209 aggregate Quantity/Expenditure/Quantity_kg with `lambda s: s.sum(min_count=1)`; the three Panama scripts copy it (Panama/1997:197, 2003:196, 2008:179). _finalize_canonical_food_acquired is @build_transform(tables=['food_acquired']) (build_transforms.py:142) and is reached from the country modules of Nigeria, Tanzania, Ethiopia, Malawi, Uganda. ghanasps.py:579 already uses the vectorised `.sum(min_count=1)` form in-repo, so the convention has precedent.",
 "evidence": "build_transforms.py:142-209; countries/Panama/{1997,2003,2008}/_/food_acquired.py; grep -rl finalize_canonical_food_acquired lsms_library/countries -> 5 country modules; ghanasps.py:579",
 "approach": "In _finalize_canonical_food_acquired replace the agg dict with g=out.groupby(idx, dropna=False); res=g[['Quantity','Expenditure'(+'Quantity_kg')]].sum(min_count=1); res['Price']=g['Price'].mean(); then the same edit in the three Panama scripts. Add a tiny equality test (NaN-group stays NaN, mixed group sums).",
 "verify": "pandas equality of the old vs new reducer on a synthetic frame incl. all-NaN groups; cold build of Panama food_acquired (3 waves) and one dedupe=True country (Uganda) with row/column-sum equality against the warm parquet. CACHE COST: build_transforms edit moves food_acquired for every country that imports it (tables=['food_acquired'] tag -> Nigeria, Tanzania, Ethiopia, Malawi, Uganda, Panama re-warm food_acquired + derived); the Panama script edits re-warm Panama only.",
 "contents_org": "Panama/_/CONTENTS.org:168,300 (ENV-97 oversample; pooling unweighted over-represents the comarcas) -- not relevant to the reducer, recorded because the agent must read it.",
 "blocked_by": "",
 "decision_for_human": ""
}
```

### #909 -- test_ghanalss_panel_consistency passes or fails on ambient cache coverage; _panel_spine trusts a partial cache
```json
{
 "tier": "C",
 "type": "test",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 4,
 "summary": "Still true: diagnostics._compute_panel_spine (diagnostics.py:1236-1310) reads var/household_roster.parquet FIRST and trusts it blind -- no read_parquet_cache_waves call, provenance string 'cached household_roster.parquet' carries no coverage. Its own docstring says the API path is preference 1, the code does the opposite. The test at tests/test_canonical_shape_via_cache_miss.py:245 calls check_panel_consistency on whatever cache is ambient. The 'cur (100%)' message wording the issue objects to is in the panel_ids_targets_exist check.",
 "evidence": "diagnostics.py:1236-1310 (parquet-first, no manifest), :1312-1330 (memo); tests/test_canonical_shape_via_cache_miss.py:238-246; local_tools.read_parquet_cache_waves exists (country.py:3672 uses it)",
 "approach": "In _compute_panel_spine: read local_tools.read_parquet_cache_waves(parquet_path); if it is None or does not cover country.waves, skip the parquet and take the API path; put '[k of n waves]' in the provenance string. In the panel_ids_targets_exist check, reword to '<n>/<n> current-wave targets MISSING'. Optionally the test asserts report.provenance covers all waves before judging.",
 "verify": "Reproduce with a private LSMS_DATA_DIR holding a one-wave GhanaLSS household_roster parquet (Country('GhanaLSS').household_roster(waves=['2016-17'])) -> check must not report a 100% miss. CACHE COST: 0 -- diagnostics.py is read-path and in no fingerprint (CLAUDE.md: feature/diagnostic code is excluded).",
 "contents_org": "GhanaLSS/_/CONTENTS.org:1868 records the retired country-level concatenator (#808) and the panel-id `_0` convention; the panel ids themselves are fine, as the issue says.",
 "blocked_by": "",
 "decision_for_human": ""
}
```

### #703 -- test_table_structure.py: clearing a country's cache makes its tests VANISH, not fail -- a green run is not a cold guarantee
```json
{
 "tier": "C",
 "type": "test",
 "tractability": "auto",
 "dedup_status": "open",
 "priority": 4,
 "summary": "Still exactly as filed: CACHED = _find_cached_parquets() at tests/test_table_structure.py:137 enumerates var/*.parquet at collection time; the fixture skips at :143 and test_at_least_one_country_has_cached_data skips (not fails) at :386-390, so a cold root yields zero parametrised cases and a green run.",
 "evidence": "tests/test_table_structure.py:124-143, 384-390",
 "approach": "Parametrise over the EXPECTED (country, table) set from ALL_SCHEMES and pytest.skip('no cache for X/Y') per missing parquet inside the test, so absence shows as a skip with a reason; make test_at_least_one_country_has_cached_data fail unless an explicit cold opt-in (e.g. LSMS_DATA_DIR pointing at an empty root AND --rebuild-caches) is set; print a summary of skipped pairs in test_cached_tables_summary.",
 "verify": "Run against an empty private LSMS_DATA_DIR: N skipped with reasons (not 0 collected); against the warm root: same pass set as today. CACHE COST: none.",
 "contents_org": "n/a (framework)",
 "blocked_by": "",
 "decision_for_human": ""
}
```

### #734 -- pytest cannot run inside a git worktree: lsms_library.pth hardcodes the main checkout, so conftest.py registers twice
```json
{
 "tier": "D",
 "type": "infra",
 "tractability": "auto",
 "dedup_status": "open (no idempotency guard in conftest.py:53-75; CLAUDE.md documents PYTHONPATH/.pth import identity but not the neutral-cwd pytest recipe from the 08-25 comment)",
 "priority": 4,
 "summary": "Root conftest.py (the only one with pytest_addoption) registers --rebuild/--rebuild-caches/--no-purge unguarded. Fix options 1 and 4 from the issue remain the right pair.",
 "evidence": "conftest.py:53-75; ls .venv/lib/python3.11/site-packages/lsms_library.pth (absent in this mounted venv); CLAUDE.md scrum-master addendum 3 (no 'neutral cwd' text); 09-24 triage cluster venv-per-clone-state-in-shared-location",
 "approach": "(1) conftest.py: wrap each parser.addoption in a try/except ValueError (or check parser._anonymous.options) so the second registration is a no-op; (4) add the 08-25 recipe to CLAUDE.md addendum 3: config-only -> LSMS_COUNTRIES_ROOT from the main checkout; library code -> run pytest from a NEUTRAL cwd with PYTHONPATH=<worktree> and assert 'worktrees' in lsms_library.__file__.",
 "verify": "From a worktree: `cd <neutral>; PYTHONPATH=<wt> pytest --collect-only -q <wt>/tests` collects; from inside the worktree with the main .venv it no longer raises 'already added'. CACHE COST: none.",
 "contents_org": "n/a (framework)",
 "blocked_by": "",
 "decision_for_human": ""
}
```

### #792 -- Feature("sample") emits 774 warnings: 746 are one unclosed file handle, 15 are real (waves with no weights/strata)
```json
{
 "tier": "C",
 "type": "bug",
 "tractability": "auto",
 "dedup_status": "partial: nothing landed -- the ResourceWarning is live today at _build_registry.py:460 (moved from :387; my own cold Mali build emitted 12 of them); the live sample parquets confirm GhanaSPS 2013-14/2017-18 weight+panel_weight 100% null, Panama 1997 weight+panel_weight 100% null, Tajikistan 1999 weight 100% null, Serbia and Montenegro 2002 panel_weight 100% null. BUT two of those four are not defects: GhanaSPS CONTENTS.org:449 'waves 2 and 3 have no sampling weight at all ... positive evidence' and Tajikistan CONTENTS.org:132 quotes the 1999 documentation 'there are no weighting variables'.",
 "priority": 4,
 "summary": "Part 1 (auto): `ast.parse(open(path, encoding='utf-8').read())` at _build_registry.py:460 never closes the file. Part 2: the issue's framing 'same class as #713' contradicts the two CONTENTS.org entries above for GhanaSPS and Tajikistan; Panama 1997 (CONTENTS.org:519 'ENV-97 has no documented sampling-frame weighting text at hand') and Serbia 2002 panel_weight remain unadjudicated. Strata/Rural gaps are unchanged and unexamined here.",
 "evidence": "_build_registry.py:460; $STAGE/cold_builds.log (ResourceWarning lines); pd.read_parquet of live {GhanaSPS,Panama,Tajikistan,Serbia and Montenegro,Albania}/var/sample.parquet null fractions by t; GhanaSPS/_/CONTENTS.org:449-466; Tajikistan/_/CONTENTS.org:130-140; Panama/_/CONTENTS.org:519-521",
 "approach": "(1) _build_registry.py:460 -> `tree = ast.parse(path.read_text(encoding='utf-8'))`. (2) Record GhanaSPS 2013-14/2017-18 and Tajikistan 1999 as not-asked in the sample docs (no config change); open a Panama 1997 / Serbia 2002 weight investigation as its own issue with the questionnaire check (C4).",
 "verify": "pytest -W error::ResourceWarning on a small Country() build; a 35-table _table_cache_hash probe before/after the read_text edit. CACHE COST: 0 expected -- _build_registry reads sources to fingerprint them, it is not itself a hashed transform; assert with the probe rather than assume.",
 "contents_org": "GhanaSPS: weights exist for wave 1 only, producer-side (CONTENTS.org:449); Tajikistan 1999: self-weighting by documentation (CONTENTS.org:132-136); Panama: ENV-97 weighting text not examined (CONTENTS.org:519); Serbia and Montenegro: 2002/2003 universe is one joint statement (CONTENTS.org:14).",
 "blocked_by": "",
 "decision_for_human": ""
}
```


## The branch: diffstat (vs development)
```
 conftest.py                                        |  28 ++++-
 lsms_library/_build_registry.py                    |   6 +-
 lsms_library/build_transforms.py                   |  29 +++--
 lsms_library/catalog.py                            |  14 ++-
 .../countries/Panama/1997/_/food_acquired.py       |   9 +-
 .../countries/Panama/2003/_/food_acquired.py       |   9 +-
 .../countries/Panama/2008/_/food_acquired.py       |   9 +-
 lsms_library/country.py                            |  58 +++++++++-
 lsms_library/diagnostics.py                        |  46 ++++++--
 tests/test_catalog.py                              |  39 +++++++
 tests/test_categorical_mapping_resolution.py       |  60 +++++++++++
 tests/test_conftest_addoption_idempotent.py        |  47 +++++++++
 tests/test_gh905_vectorised_min_count_sum.py       | 107 +++++++++++++++++++
 tests/test_gh909_panel_spine_coverage.py           |  83 +++++++++++++++
 tests/test_table_structure.py                      | 117 ++++++++++++++-------
 15 files changed, 587 insertions(+), 74 deletions(-)
```
## The branch: commit messages
--- de2284da2 country: correct the _strip_string_values docstring (categorical .str.strip returns object, as before) (GH #694)
Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- e6c87feba _build_registry: close the source handle in framework_imports_fingerprint (GH #792, part 1)
ast.parse(open(path).read()) leaked one unclosed-file ResourceWarning per
fingerprinted _/*.py source -- 746 of the 774 warnings Feature('sample')
emitted, burying the 15 that carry information.  Now a with-block.

Measured on a cold Mali build (food_acquired + sample) in a private
LSMS_DATA_DIR under -W always::ResourceWarning: 28 'unclosed file'
warnings before, 0 after; the served food_acquired is identical
(431,046 x 3, DataFrame.equals True; sums Quantity 14,587,652.97,
Expenditure 5,839,325,575.16 both times).

Part 2 of the issue (the four waves with null weights) is adjudication,
not code: GhanaSPS 2013-14/2017-18 and Tajikistan 1999 are documented
as having no weights (CONTENTS.org), Panama 1997 and Serbia 2002 are
unexamined -- residual drafted in the element report.

Cache cost: 0 -- _build_registry fingerprints sources, it is not itself
hashed; probe 0 of 55 moved.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- e46b442e4 conftest: make pytest_addoption idempotent so pytest runs inside a git worktree (GH #734, item 1)
Inside a worktree the main checkout's lsms_library.pth puts the main
checkout on sys.path too, so pytest loads BOTH root conftest.py files
under the same module name and pytest_addoption runs twice; the second
parser.addoption raised 'ValueError: option names {'--rebuild'} already
added' and no test could run from a worktree.  Reproduced here by calling
the hook twice on a fresh _pytest Parser: raises before, no-op after.

_addoption_once swallows only the 'already added' ValueError (message
checked); anything else propagates.  tests/test_conftest_addoption_
idempotent.py loads the root conftest by path and pins both.

Items 2-4 of the issue (no-op when __file__ is outside rootdir, a real
editable install, and the CLAUDE.md neutral-cwd recipe) are NOT done
here -- residual drafted in the element report.

Cache cost: 0 (probe 0 of 55 moved).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- e77fb214c tests: test_table_structure parametrises over the DECLARED (country, table) set; a cold cell skips with a reason, an empty cache fails unless opted in (GH #703)
CACHED was enumerated from the parquets on disk at collection time, so
clearing a country's cache made its tests VANISH (zero cases, green run)
and a wholly cold root produced a green run with no structural coverage.

Now EXPECTED is every (country, table) the data_scheme files declare
(453 pairs); the two parametrised classes run over it and _read_or_skip
skips 'no cache for C/T at data_root()' for a parquet that was absent at
collection (the GH #330 vanished-mid-run retry is kept for one that was
present).  test_at_least_one_country_has_cached_data FAILS on an empty
cache unless the run opted into cold mode: --rebuild-caches / --rebuild,
or LSMS_SKIP_AUTH (the no-microdata CI mode, where nothing can be warm --
chosen so .github/workflows/ci.yml needs no edit).  The summary test now
also prints the pairs skipped for want of a cache.  The unused
cached_parquets fixture is gone.

Measured: warm private root (Mali + Panama): 84 passed, 2694 skipped,
449 distinct 'no cache for' reasons.  Fresh empty root, housing class
deselected: no opt-in -> '1 failed, 1 passed, 2718 skipped' with the
message naming the root and 'all 453 declared pairs skipped';
LSMS_SKIP_AUTH=1 -> '1 passed, 2719 skipped'; --rebuild -> same.
Note (pre-existing, unchanged): TestHousingVDtype calls Country().housing()
and so cold-builds ~30 countries on an empty root.

Cache cost: 0 (test file only; probe 0 of 55 moved).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 518b75df3 diagnostics: panel spine trusts the cached roster parquet only when its wave manifest covers every wave (GH #909)
_compute_panel_spine read var/household_roster.parquet first and trusted
it blind, so a one-wave parquet left by household_roster(waves=[w]) became
the spine and panel_ids_targets_exist reported a 100% miss against waves
the spine never held.  New _parquet_covers_waves(path, waves) reads the
GH #891 lsms_cache_waves manifest (footer only, ~1 ms); the parquet is
served only when it names every declared wave, and the provenance string
says so ('cached household_roster.parquet [k of n waves]').  A parquet
with no manifest (pre-#891) cannot prove its coverage and goes to the API
path, which reads the same cache through the normal gate (warm read, no
rebuild).  The fail message of panel_ids_targets_exist now reads
'<n>/<total> current-wave targets MISSING (p%)' instead of 'cur (p%)'.

Reproduced on Mali in a scratch LSMS_DATA_DIR: household_roster(
waves=['2018-19']) leaves a parquet with manifest ['2018-19'] ->
_parquet_covers_waves = (False, 1, 4) -> spine via 'household_roster
(API)' with all 4 waves -> panel_ids_targets_exist: pass, 'All 5011 chain
endpoints present in cached household_roster.parquet [4 of 4 waves]'.
Before the change the one-wave parquet would have been the spine.
Note: the seeded warm Mali roster parquet carries NO manifest -> (False,
0, 4); diagnostics on such parquets now take the (warm) API path.

tests/test_gh909_panel_spine_coverage.py: stub country + tmp LSMS_DATA_DIR,
full manifest serves the parquet (0 API calls), partial and unstamped
parquets take the API path, and the (covered, k, n) counts.

Cache cost: 0 -- stash-probe Mali + Uganda, 0 of 55 moved (diagnostics.py
is read-path, in no fingerprint).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- e95f8d792 build_transforms: vectorised min_count=1 sums in the canonical food_acquired dedupe tail (GH #905)
_finalize_canonical_food_acquired and the three Panama wave scripts
(1997/2003/2008 _/food_acquired.py) reduced Quantity / Expenditure /
Quantity_kg with agg(lambda s: s.sum(min_count=1)) -- one Python call per
group, and pandas deep-copies df.attrs on each, the GH #871 cost.  Now
g[summed].sum(min_count=1) + g['Price'].mean(), with the old column
order (Quantity, Expenditure, Price, Quantity_kg) restored explicitly.

Measured (synthetic 363,682-row frame after the filter, 289,434 groups,
attrs set; $STAGE/probe/bench905.py):
  old 36.04 s -> new 1.06 s (34x)
  index, column order, dtypes, NA pattern identical
  values: Quantity 742 / Expenditure 697 / Quantity_kg 764 cells of
  ~234k differ, max 2 ULP, max relative difference 3.1e-16 (GH #871
  precedent: pairwise cython sum vs in-order Series.sum, 4e-16); Price 0.
Panama food_acquired, cold in a private LSMS_DATA_DIR, before vs after:
  571,354 x 3 both; var parquet byte-identical (DataFrame.equals True),
  API frame 0 cells differ in any column.  Mali does NOT reach this
  function (EHCVM path), so the Mali build is no evidence for it.

tests/test_gh905_vectorised_min_count_sum.py pins the old reducer as the
reference: four Price/Quantity_kg combinations, all-NaN group stays NaN,
NaN key kept (dropna=False), Price averaged, column order, dedupe=False.

CACHE COST (stash-probe, Mali + Uganda, 53 tables + btf): 31 of 55 moved.
build_transforms_fingerprint('food_acquired') moves for EVERY country
(the tables=['food_acquired'] tag is by table name, so Mali/food_acquired
moved although Mali never calls it), and EVERY table of a country whose
module imports the function (Uganda 29/29; also Nigeria, Tanzania,
Ethiopia, Malawi).  Panama re-warms food_acquired via its scripts.  Not
paid here except Panama in the private root.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- e5c168e14 Country(): raise ValueError with near-miss suggestions for a name with no directory (GH #915, half 1)
Before: a typo such as Country('Ghana') emitted a UserWarning once per
call site (Python's default filter), constructed anyway, and died at
first use with a FileNotFoundError naming an install path the user never
chose.  Two teaching-hub users hit exactly this.

Now the constructor raises ValueError naming up to three near misses
from the registered list (catalog._country_dirs, the same list
lsms_library.countries() returns), matched case-insensitively:

  Country('Ghana')    -> Did you mean 'GhanaSPS' or 'GhanaLSS' or 'Uganda'?
  Country('ghanalss') -> Did you mean 'GhanaLSS' or 'GhanaSPS' or 'GhanaAHIES'?

The path-traversal check is unchanged and still runs first.  Half 2 of
the issue -- also raising for an EXISTING directory that lacks
_/data_scheme.yml (11 such dirs, incl. .dvc and Afghanistan #702) -- is
a human decision and is NOT taken here; Country('Afghanistan') still
constructs.  Both catalog docstrings now state that asymmetry instead of
claiming the registered list is 'exactly the strings accepted'.

Cache cost: 0 -- stash-probe of Country._table_cache_hash on Mali +
Uganda (53 tables) plus build_transforms_fingerprint: 0 of 55 moved.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 4ae96f103 country: strip only string values before a categorical replace (GH #694)
_apply_categorical_mappings ran Series.str.strip() on any column whose
name matched a categorical_mapping table.  On an object column the .str
accessor returns NaN for every non-string element, so a numeric column
that merely shared a table's name (Rural = 0/1, a Region code) was nulled
before .replace() ran.  The index branch never stripped, so the hazard was
column-only.

New module-level _strip_string_values strips str values only on object
columns; string/categorical dtypes keep the vectorised path (and their
dtype).  Four regression tests drive _apply_categorical_mappings through a
stub country: numeric values survive, padded labels still map, string /
categorical dtypes unchanged, index branch + attrs unchanged.

Cache cost: 0 -- stash-probe of Country._table_cache_hash on Mali + Uganda
(53 tables) plus build_transforms_fingerprint: 0 of 55 moved.
_apply_categorical_mappings runs under _finalize_result (read path).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>



# Backlog fix element `framework-small` -- issues #694 #915 #905 #909 #703 #734 #792

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
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/framework-small` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

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

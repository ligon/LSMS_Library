# RED-TEAM review of branch `fix/burkina-2014` @ 366788665 (element `burkina-2014`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/burkina-2014` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Burkina_Faso; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/burkina-2014/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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
Read `Burkina_Faso/_/CONTENTS.org` first (EMC 2014 has FOUR passages; only passage 1 fields the quantity grid -- the `Unknown` u sentinel landed 2026-09-09). #864: fix the root in `2014/_/mapping.py::food_acquired` as the record describes, then the message. #794: diagnose-then-fix, Burkina only -- a cp1252/mojibake df_edit hook on food_acquired; measure the C1-signature label count before/after. One Burkina re-warm, private root.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #864 -- food_prices() reports Burkina Faso 2014 as 64.8% unpriceable and blames the wrong cause: three of four EMC passages record money without quantity, so those rows are unpriceable BY DESIGN
```json
{
 "tier": "B",
 "type": "bug",
 "tractability": "diagnose-then-auto",
 "dedup_status": "open",
 "priority": "2 -- the served food_acquired carries a manufactured Quantity=0 on 405,127 money-only rows (0 means 'bought none', not 'not asked'), and the price-loss warning is pinned at 64.8% so a real break in the cell is invisible",
 "summary": "The issue's premise needs one correction that changes the fix. The served Burkina 2014 passages 2-4 do NOT carry `Quantity = NaN`: measured on the warm `Burkina_Faso/var/food_acquired.parquet` (hash-stamped, waves 2014/2018-19/2021-22) and on the wave parquet `2014/_/food_acquired.parquet`, every purchased row of visits 2/3/4 has `Quantity == 0` (139,798 / 136,913 / 128,416 = 405,127 rows; 0 nulls), all with `u = 'Unknown'`; visit 1 adds 44,125 zero-quantity purchased rows. 405,127 + 44,125 = 449,252, within 0.2% of the 449,842 the warning reports. The zero is manufactured by the canonical reshape: `build_transforms.py:102-103` `purchased_qty = (Quantity.fillna(0) - Produced.fillna(0)).clip(lower=0)` turns a not-asked NaN into a purchased 0, and `:135` then drops the p2-4 produced rows (`Produced` is NaN there) -- so the `autocons` VALUE those passages record (CONTENTS.org:465) is not served at all. Downstream, `transformations.py:2960-2962` computes `Price = Expenditure / Quantity_kg` with `Quantity_kg = 0 x NaN(kg factor for 'Unknown') = NaN`, which is the ONLY reason the rows land in the `nan` bucket of `_drop_unpriceable` (:236-262); under `units='unitvalue'` the same rows are `Expenditure / 0 = inf`, the bucket the docstring calls 'ALWAYS a data defect'. Consequences: option 1 as written ('a group where Quantity is 100% null is money-only by construction') cannot fire -- Quantity is 0% null; and CONTENTS.org:465 ('Quantity and Produced are NaN for passages 2--4') describes the SOURCE, not the served table. The taxonomy misattribution the issue describes is real and current (`_drop_unpriceable` has `currency` as its only by-design bucket, :251-258).",
 "evidence": "pd.read_parquet of warm Burkina_Faso/var/food_acquired.parquet and 2014/_/food_acquired.parquet (groupby visit: Q null 0, Q==0 139798/136913/128416, u=='Unknown' 100%); lsms_library/build_transforms.py:98-138; lsms_library/transformations.py:169-262 (_drop_unpriceable), 2952-2962 (kgvalue divide), 3004 (call); Burkina_Faso/2014/_/mapping.py:102-150 (hook: per-passage _food_acquired_canonical + _fill_missing_u); Burkina_Faso/2014/_/data_info.yml:201-250; Burkina_Faso/_/CONTENTS.org:465-472,765-768; commit 1446e4c1b (#851 Phase I)",
 "approach": "Root first, then the message. (1) In `Burkina_Faso/2014/_/mapping.py::food_acquired`, after the per-passage reshape set purchased `Quantity` to NaN for `visit > 1` (the quantity grid is not fielded there; the `Unknown` u sentinel already marks the rows), and stop dropping the passage-2-4 own-consumption VALUE (`autocons`) -- or state in the YAML/CONTENTS that it is deliberately unread. Do NOT change `build_transforms.food_acquired_to_canonical`'s fillna(0) corpus-wide without a census: it is `@build_transform`-tagged and every reshaped country re-warms. (2) In `transformations._drop_unpriceable` add a third by-design bucket beside `currency` -- `money_only`: rows whose `Expenditure > 0` and whose `Quantity` is NaN (after step 1) are excluded from `expected` and `lost` and tallied separately; derive it per `(t, s, visit)` group where Quantity is 100% null (option 1), so it is self-maintaining; name the share in the warning text. Keep `PRICE_LOSS_WARN_THRESHOLD` and never suppress for Burkina.",
 "verify": "tests/test_burkina_faso_2014_passages.py (re-pin per-visit counts: produced rows and NaN Quantity for visits 2-4); a new unit test on `_drop_unpriceable` with a synthetic money-only group; warm `Country('Burkina_Faso').food_prices()` should then report ~44k/0.3-class loss, not 64.8%. CACHE COST: step (2) is in `transformations.py`, which reaches no build fingerprint (tests/test_build_transform_hash.py::test_no_transformations_symbol_reaches_any_fingerprint) -- zero re-warm. Step (1) edits `2014/_/mapping.py`, a `binp:` input of `Wave._input_hash` for every 2014 table and, via `Country._table_cache_hash`, of every Burkina_Faso L2-country hash -> country tier: all Burkina tables re-warm; wave tier: only the 2014 wave parquets. Touching `build_transforms.py` instead would re-warm every country that calls food_acquired_to_canonical.",
 "contents_org": "Burkina_Faso/_/CONTENTS.org:125-130,465-472,765-768 -- the EMC 2014 is a continuous survey: the same households are revisited in four quarterly passages, the 7-day food module is re-asked at each, and ONLY passage 1 fields the quantity grid (qachat/uachat); passages 2-4 record money only (achat, autocons, cadeau). CONTRADICTED on one point by the served table: :465 says Quantity 'is NaN for passages 2--4'; the served purchased rows carry Quantity == 0 (405,127 rows), and the p2-4 autocons value is not served.",
 "blocked_by": "none (design choice between option 1 and 3 can be made by the fix agent as proposed: do both)",
 "decision_for_human": "Serve passage 2-4 purchased Quantity as NaN (not 0) in Burkina_Faso 2014 -- a Burkina-only re-warm -- yes/no?"
}
```

### #794 -- UnicodeWarning in from_dta: the warning is benign; the silent latin-1 path beside it is where the risk lives (and Senegal's mojibake is load-bearing)
```json
{
 "tier": "B",
 "type": "bug",
 "tractability": "diagnose-then-auto",
 "dedup_status": "partial",
 "priority": 2,
 "summary": "Partial. The UPSTREAM half (§D items 1-3) is resolved: the installed ligonlibrary _coerce_label no longer uses errors='ignore', returns a value unchanged when the latin-1->utf-8 round trip fails (which is the format-118 guard the issue asked for: 'Boîte de tomate' survives), and its docstring now says what encoding= actually does. Verified on the venv's ligonlibrary 0.2.2 (dist-info RECORD hash matches dataframes.py) -- but pyproject/poetry.lock pin >=0.2.3, so the fix was verified on a version the lock does not name; confirm 0.2.3 carries it. The IN-REPO half is open and has moved: (1) the live BF leak the comments found (12,933 rows) is now 47,137 rows in the warm cache -- 2014 'Cube (Maggi, Jumbo,\\x85)' 38,982 + 'Viandes de b\\x9cufs fraîches' 6,756, 2018-19 'Viande de bÅ\\x93uf' 1,399 -- because #851 (1446e4c1b) stopped deleting the 2014 p2-p4 passages, so the leak scaled with the row count; (2) Mali food_acquired.j 2018-19 serves 4,447 C1 rows from the same class (not in the issue); (3) the three dead encoding=-passing wave scripts are still on disk (Senegal/2018-19, Burkina_Faso/2018-19, Burkina_Faso/2021-22 _/food_acquired.py); (4) the strategy-B cohort migration (Benin/BF/Senegal/Togo onto _decode_mojibake + crosswalk re-key, ~278,777+ rows) has not happened and nothing marks which country is which; (5) Senegal's u pairing still holds: 41,347 raw mojibake u rows in the parquet, 41,347 match a crosswalk key. Mali vs Senegal asymmetry: Mali's j is mapped at build time (Mali/2018-19/_/data_info.yml:196 harmonize_food on Code), so a C1 value in the parquet is a key that did NOT match, exactly like BF rows 303/304; Senegal's u is mapped at read time (country table named u), so its raw parquet values are expected. OPEN for the diagnose step: Mali/_/categorical_mapping.org:362 appears to hold the same U+009C bytes as the served 'Viande de b\\x9cuf' yet does not match -- whether the warm parquet predates the org edit, df_from_orgfile normalises the key, or the Code column is read differently is unmeasured.",
 "evidence": ".venv/lib/python3.11/site-packages/ligonlibrary/dataframes.py:_coerce_label (strict, latin-1 pinned); ligonlibrary-0.2.2.dist-info/RECORD line 8 sha256 == file; pyproject.toml:26 ligonlibrary (>=0.2.3); pyarrow scan $STAGE/served_mojibake_scan.txt (BF food_acquired j 2014 45,738 / 2018-19 1,399; Mali j 4,447); grep encoding= in the three wave scripts; git log -S'#794' (b2d38da88 docs-only, 1446e4c1b #851); Burkina_Faso/_/CONTENTS.org:655-680; Senegal/_/CONTENTS.org:440-510.",
 "approach": "Diagnose-then-fix, country-scoped: (a) Burkina_Faso -- add a _decode_cp1252 + _decode_mojibake df_edit hook on food_acquired j for 2014 (cp1252 class) and 2018-19 (double-encoded class), re-key harmonize_food rows 296/303/304 to the clean strings, re-measure the leak table in CONTENTS.org (expect 0); (b) Mali -- same for the two harmonize_food Code cells (rows 362/374) and its 2018-19 j; (c) delete the three dead encoding= wave scripts (behaviour-neutral; edits _/food_acquired.py so those tables re-warm); (d) the cohort migration is a separate human decision. If #805 lands at the reader first, (a)'s cp1252 half and (b) are absorbed by it and only the re-key remains.",
 "verify": "Cold build of Burkina_Faso and Mali food_acquired in a private LSMS_DATA_DIR; assert 0 rows of j match [\\x80-\\x9f]|Ã|Å|Â and beef collapses to one label; Senegal food_acquired() still returns 0 mojibake u/j via the API; tests/test_ehcvm_7day.py::test_inputs_join_to_the_served_rows still passes. CACHE COST: wave mapping.py / _/categorical_mapping.org edits are binp: inputs for every table of that wave and country (CLAUDE.md derived-values corollary: a wave's mapping.py is in every table's input hash) -> full Burkina_Faso and Mali re-warm, nothing else.",
 "contents_org": "Burkina_Faso/_/CONTENTS.org:655-680 -- two faults in one country (cp1252 2014, double-encoded 2018-19), 'the crosswalk moves with it: a reader change must land with a re-key AND a re-measurement of the leak table'. Senegal/_/CONTENTS.org:440-510 -- strategy A (repair data: CotedIvoire/Guinea-Bissau/Niger) vs B (key the crosswalk on the damage: Benin/BF/Senegal/Togo); 'Do not clean up these keys in isolation'. The leak table in BF CONTENTS is now stale (12,933 -> 47,137) because of #851.",
 "blocked_by": "#805 (reader-level cp1252 decision) for the cp1252 half; human decision on the cohort migration",
 "decision_for_human": "Migrate the four strategy-B countries (Benin, Burkina Faso, Senegal, Togo) onto the in-repo _decode_mojibake hook with a crosswalk re-key in the same PR (data-affecting, ~278,777+ rows re-keyed, expected served output identical) -- yes/no?"
}
```


## The branch: diffstat (vs development)
```
 .../countries/Burkina_Faso/2014/_/mapping.py       | 161 ++++++++++++++++++++-
 lsms_library/countries/Burkina_Faso/_/CONTENTS.org | 151 ++++++++++++++++++-
 .../Burkina_Faso/_/categorical_mapping.org         |  32 +++-
 lsms_library/transformations.py                    |  96 +++++++++++-
 tests/test_burkina_faso_2014_passages.py           |  86 +++++++++++
 tests/test_burkina_faso_food_labels.py             | 153 ++++++++++++++++++++
 tests/test_unpriceable_price_drop.py               | 105 ++++++++++++++
 7 files changed, 772 insertions(+), 12 deletions(-)
```
## The branch: commit messages
--- 366788665 Burkina Faso 2014 food_acquired: money-only passages are NaN, not 0; repair the truncated crosswalk (GH #864, GH #794)
GH #864.  food_acquired_to_canonical fills a null source quantity with 0, so
every passage-2/3/4 purchased row (405,127) and 43,906 passage-1 rows were
served with Quantity == 0 against a positive Expenditure, and food_prices()
reported the wave as 65.7% unpriceable, blaming a 0-as-missing sentinel.
2014/_/mapping.py::_restore_missing_quantity puts the NaN back wherever the
source qachat was null (qachat is never 0 where achat > 0: 0 of 140,908).
transformations._drop_unpriceable gains a second by-design bucket beside
`currency`: `money_only`, a (t, visit, s) group whose Quantity is null on
every row (>= 30 rows), excluded from both sides of the loss fraction and
named in the warning.  food_prices(kgvalue): 449,431 of 683,713 (65.7%) ->
44,304 of 278,586 (15.9%), money_only 405,127.  Row counts, Expenditure and
Quantity totals unchanged; food_quantities 2014 kg 310,338 -> 306,309 from
the label merge below.

GH #794.  The crosswalk keys for the four damaged labels did not match
because they were never LOADED: an editor turned the U+0085 inside the
`Cube (Maggi, Jumbo,<U+0085>)` key into a line break, df_from_orgfile ends
a table at the first non-`|` line, and the four rows below the break were
dead.  The row is one line again (294 parsed rows, was 291).  Served j with
a C1/mojibake signature: 47,137 -> 0; beef is one label in all three waves;
Cube 38,982 -> `Cube alimentaire`; Feuilles 16,610 -> `Feuilles` via a
whole-value _J_FIX, because that label ENDS in U+0085 and str.strip() makes
it unkeyable in the org file.  _decode_mojibake / _decode_cp1252 run after
the build-time crosswalk as a safety net (0 rows changed today).

Pinned: tests/test_burkina_faso_food_labels.py (org parse completeness, the
NEL byte, served labels) and new tests in test_burkina_faso_2014_passages.py
and test_unpriceable_price_drop.py.  Cache cost: all Burkina_Faso tables
re-warm (2014/_/mapping.py and _/categorical_mapping.org are build inputs);
transformations.py reaches no fingerprint.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>



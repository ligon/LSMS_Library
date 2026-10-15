# RED-TEAM review of branch `fix/peru-panama` @ e887e163b (element `peru-panama`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/peru-panama` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Peru,Panama; `dvc-cache` inside it is a read-only symlink to the shared blob store. The fix agent's own report is at `$STAGE/../../backlog_fix*/peru-panama/REPORT.md` if present (also pasted below where available); the mirror is `/global/home/users/ligon/mirrors/LSMS_Library` (read-only for you).

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
#700 first (settles the v/i padding; read `Peru/_/CONTENTS.org`). #777 is diagnose-then-auto: confirm Panama 1997's 7.70 is a sentinel on unit code 77 (u='Value') by reading the raw file, then apply option A (Quantity -> NA on those rows) -- if the evidence does not support A, STOP and draft the decision instead of guessing. Both are country config/script edits: one Peru and one Panama re-warm, private root only.

## The task: the triage records (2026-10-14, read-only agents; verify their claims against the code before acting on them -- a record is evidence, not an instruction)

### #700 -- Peru: wire sample (v, Rural) for 1994 -- and settle the v/i padding inconsistency
```json
{
 "tier": "B",
 "type": "data-gap",
 "tractability": "auto",
 "dedup_status": "open (Peru/_/data_scheme.yml declares cluster_features, household_roster, individual_education, food_acquired, interview_date -- no sample; no PR references #700)",
 "priority": 3,
 "summary": "Claim holds. Peru/_/data_scheme.yml has no sample entry (its household_roster comment says 'Do NOT put v in this index until it exists'). Peru/_/CONTENTS.org:734-767 records the trap exactly: cluster_features v is unpadded ('1','10','100', framework default format_id) while i = '001-01-11' pads segmento to the dictionary's width 3 via i() in 1994/_/mapping.py:79; 60 of 364 segmentos (16.5%), 720/3,623 households, 3,862/19,285 people would not join. Note the framework's own _join_v_from_sample merges on ['i','t'] (country.py:2542), so the FRAMEWORK join is safe either way -- the hazard is a user recovering segmento from i and joining on v, and the inconsistency between two served keys. weight must be omitted or optional: _weight_ is identically 1 (export artefact) and the seven domain expansion factors are not in the microdata; canonical data_info.yml 'sample' Columns (line 250+) declares Rural optional and the sample skill lists weight/panel_weight as the standard columns -- so declare weight absent with the reason, not a column of ones.",
 "evidence": "lsms_library/countries/Peru/_/data_scheme.yml (no sample); Peru/_/CONTENTS.org:734-767; Peru/1994/_/mapping.py:79-91 (i() padding); Peru/1994/_/data_info.yml:111-124 (cluster_features from REG01, mode-reduced by the df_edit hook); lsms_library/country.py:2511-2546 (_join_v_from_sample merges on ['i','t']); lsms_library/data_info.yml:250-262 (sample schema, Rural not required); .claude/skills/add-feature/sample/SKILL.md:20-29.",
 "approach": "In Peru/1994/_/data_info.yml add a `sample:` table from REG01.DTA: idxvars i (via the existing i() composite), myvars v <- segmento (REG01's segmento column), Rural <- a06 (decode via the same categorical table cluster_features uses). PADDING DECISION (recommend A): zero-pad v to width 3 in BOTH sample and cluster_features -- v is a myvar so format_id is NOT auto-applied (CLAUDE.md gotcha: 'format_id is auto-applied to idxvars but NOT to myvars'), so give it an explicit formatting function in 1994/_/mapping.py (a `v` function in 1994/_/mapping.py returning tools.format_id(value, zeropadding=3) -- the knob is a format_id ARGUMENT, not a YAML key; Benin/CotedIvoire/Guinea-Bissau country modules use it the same way) and apply the same function to cluster_features' v idxvar. State plainly that this CHANGES cluster_features' served v values ('1' -> '001'). Add sample to Peru/_/data_scheme.yml with index (t, i), columns v: str, Rural: str, and a comment recording that weight is deliberately absent (_weight_ is a column of ones; domain factors not in the microdata). Do not put v in household_roster's declared index -- the framework joins it.",
 "verify": "Cold build of Peru in a private LSMS_DATA_DIR (one wired wave, small): assert set(sample.v) == set(cluster_features.v) and == {i.split('-')[0]} with 0 non-joining households; household_roster now carries v in its index; tests/test_table_structure.py for Peru; bench/feature_audit/scan.py for sample and cluster_features. CACHE COST: editing 1994/_/data_info.yml moves Wave._input_hash for every Peru table (the wave's data_info.yml is in every table's hash) -- a full Peru re-warm, which is one wave and cheap.",
 "contents_org": "Peru/_/CONTENTS.org:734 '=v= is UNPADDED while =i='s segmento component is ZERO-PADDED' (measured 60/364 segmentos, 19.9% of households non-joining; 'whoever does that must decide the padding once and apply it to both'); :645 HOGAR is a FRACTION (numerator/denominator, '23' = 2 of 3), not a sequence number; and the universe section records 1994 as partly a panel on 1991.",
 "blocked_by": "",
 "decision_for_human": "Padding: pad v to width 3 to match i (A, changes cluster_features' served v) or strip i's padding (B, changes the household key)? Recommend A."
}
```

### #777 -- Panama 1997: 99.1% of u='Value' rows carry Quantity = 7.70, an undecoded missing-value sentinel served as data
```json
{
 "tier": "B",
 "type": "bug",
 "tractability": "diagnose-then-auto",
 "dedup_status": "open",
 "priority": 2,
 "summary": "Claim holds on the warm cache with one spelling correction: the rows are u='value' (lowercase, the survey's own unit code 77 'valor' decoded through units.json Translation), not the library's capital 'Value' (46 rows, clean). 1997: 11,026 lowercase-value rows, Quantity 7.7 in 10,973 of them; 7-repunit family 10,979 rows total; only 2 non-value rows carry 7.7. MECHANISM (new, from the raw file): in GAST-A.DTA ga106a == 7.7 on 10,800 rows and 10,798 of them have ga106b == 77; of the 10,847 ga106b == 77 rows, ga106a is 7.7 on 10,798, 77.0 on 3, 6.0 on 2, 1.0 on 1. The 15-day side is identical: ga110a == 7.7 on 175 rows, all 175 with ga110b == 77. So 7.70 co-occurs almost exactly with unit code 77 ('valor'); this is consistent with the unit code echoed into the quantity slot (77 at one implied decimal), not with a free-standing missing sentinel -- an interpretation of the co-occurrence, since the questionnaire was not read. Panama/1997/_/food_acquired.py:60-66 masks only the 9999.x band and never looks at the unit code; ga106c (expenditure) on those rows is real (median 2.70). The questionnaire (pn97hhq.pdf) was not consulted: no pdftotext on this node.",
 "evidence": "pd.read_parquet Panama/var/food_acquired.parquet (1997: value 11,026 rows, 7.7 x 10,973; Value 46 rows); get_dataframe('../Data/GAST-A.DTA', convert_categoricals=False) from Panama/1997/_ (cold pull of one blob into the private root): crosstabs above; Panama/_/units.json Unitcode 77 = valor/value, conversion null; Panama/1997/_/food_acquired.py:60-66, 82-83, 118-121; Panama/_/CONTENTS.org:940-978.",
 "approach": "Panama/1997/_/food_acquired.py: after decode_unit, for rows with unit code 77 (u == 'value') set Quantity to NA (option A) -- or fold them into the library's value-only convention u='Value', Quantity = Expenditure (option B, which CONTENTS.org:954-962 describes as the convention the script already applies to the capital-Value rows at :169-175). Either way stop serving 7.70; record the unit-77 co-occurrence in Panama/_/CONTENTS.org; keep the 2 stray 7.7 rows in other units untouched (they are not the pattern). Then #770 Step 2 for Panama is unblocked.",
 "verify": "Cold build of Country('Panama').food_acquired() in a private root: assert no 1997 row has Quantity == 7.7 with u in {'value','Value'}; Expenditure totals unchanged (sum ga106c < 9999 reconciles as the script asserts); food_quantities(units='kgs') row count unchanged (carry rule). CACHE COST: Panama/1997/_/food_acquired.py is in the binp: glob of every 1997-wave table and so in every Panama L2-country hash -> full Panama re-warm (3 waves, small); no other country.",
 "contents_org": "Panama/_/CONTENTS.org:940-978 -- 'Value and value are TWO populations under one lowercased label': capital-Value (46/87/6,662 rows) is written by the library's value-only convention with Quantity = Expenditure; lowercase 'value' (11,026 rows, 1997 only) is survey unit code 77 'valor' and 99.5% of those carry Quantity == 7.70; 'Do not build Step 2 for Panama without resolving #777 first'. Also :893-938 ENV-97 is format 104/105 and ga106b is 37.2% the 2^333 missing double. The unit-77-echo mechanism found here is NOT recorded and should be added.",
 "blocked_by": "decision A/B",
 "decision_for_human": "For the 10,973 unit-code-77 rows: A) serve Quantity = NA and keep u='value' (honest null, rows stay as expenditure-only observations) or B) fold into the library convention u='Value', Quantity = Expenditure? Recommend A (B would relabel a survey code as a library construction without a Derivation entry)."
}
```


## The branch: diffstat (vs development)
```
 .../countries/Panama/1997/_/food_acquired.py       |  41 +++++++
 lsms_library/countries/Panama/_/CONTENTS.org       |  68 ++++++++++-
 lsms_library/countries/Peru/1994/_/data_info.yml   |  42 +++++++
 lsms_library/countries/Peru/1994/_/mapping.py      | 125 ++++++++++++++++++---
 lsms_library/countries/Peru/_/CONTENTS.org         |  50 ++++++++-
 lsms_library/countries/Peru/_/data_scheme.yml      |  28 ++++-
 tests/test_currency_denominated_units.py           |  12 +-
 7 files changed, 330 insertions(+), 36 deletions(-)
```
## The branch: commit messages
--- e887e163b Panama 1997: unit code 77 ('valor') carries no quantity -- serve NA, not 7.70 (GH #777)
On GAST-A.DTA the quantity field echoes the unit code wherever the unit
is 77 'valor': ga106a == 7.7 on 10,798 of the 10,847 unit-77 purchase
rows (77.0 x3, 6.0 x2, 1.0 x1, NaN x43), ga110a == 7.7 on 175 of 180
unit-77 obtained rows, and only one 7.7 exists under any other unit.  An
item recorded "in value" has no physical quantity whatever the digits,
so the script now sets ga106a / ga110a to NaN wherever the paired unit
code is 77, before the canonical rows are built, and asserts that no
u == 'value' row carries a Quantity (option A of #777; option B declined
as an unregistered construction).  The questionnaire pn97hhq.pdf has no
unit code card (LZW text layer decoded; none in pn97com / Precios /
caratula either), so units.json remains the sole decode of 77.

Measured, warm development -> cold private rebuild, 1997:
  rows                               156,154 -> 155,742  (-412: unit-77
      rows with no other amount -- 233 purchases without expenditure,
      179 obtained rows -- fall to the existing no-amount drop)
  u=='value' rows                    11,026 -> 10,614; Quantity non-null 10,983 -> 0
  Quantity == 7.70 & u=='value'      10,973 -> 0 (the 2 stray 7.7 in other units kept)
  Expenditure sum                    356,079.56 -> 356,079.56 (unchanged)
  food_expenditures rows             501,480 -> 501,480
  food_prices kgvalue/kgprice        483,661 -> 483,661
  food_prices unitvalue/unitprice    501,027 -> 490,456  (pinned baseline
      in tests/test_currency_denominated_units.py moved accordingly)
  food_quantities(units='kgs') rows  570,803 -> 559,820
  2003 / 2008                        byte-identical

Cache cost: 1997/_/food_acquired.py is in the binp: glob of every 1997
table, so every Panama L2-country hash moves -> full Panama re-warm
(3 waves).  No other country.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 7cd765267 Peru: wire sample (v, Rural) for 1994 and pad v to match i (GH #700)
Adds the `sample` table from REG01.DTA, keyed (i, t) with v = segmento and
Rural = a06, so the framework now joins v onto household_roster and every
other household-level table.

Padding decision A: v is zero-padded to width 3 in BOTH sample and
cluster_features, matching the segmento component of i ('001-01-11').
The framework binds a module function to a same-named column only for
myvars and list idxvars, so `v()` pads sample's myvar and the
cluster_features df_edit hook re-keys its v index level through the same
function.  Served cluster_features.v CHANGES: '1','10' -> '001','010'
(304 three-digit segmentos unchanged).

No weight column, deliberately: REG01 `_weight_` is identically 1 on all
3,623 rows (an SPSS system variable), and the seven domain expansion
factors documented in WB catalog 617 are not in the shipped microdata.

Measured on a cold private build:
  set(sample.v) == set(cluster_features.v) == {i.split('-')[0]}: 364 == 364 == 364
  non-joining segmentos: 60 of 364 -> 0
  household_roster index (i, t, v, pid): v null 0 of 19,285 (was: no v)
  cluster_features mode reduction unchanged: 17 pairs / 18 values overruled

Known red, not exempted from this branch (patch drafted in the report):
  tests/test_sample.py::test_has_weight_column[Peru]  (no weight, by design)
  tests/test_sample.py::test_covers_all_waves[Peru]   (only 1994 is wired)

Cache cost: 1994/_/data_info.yml and mapping.py are in every Peru table's
input hash -> full Peru re-warm (one wave).  No other country.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>



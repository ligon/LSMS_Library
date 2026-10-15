# Backlog fix element `peru-panama` -- issues #700 #777

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
- Commit as you go; end every commit message with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Publish with `git push origin fix/peru-panama` (origin is the shared mirror; no GitHub push, no PR from here).
- Record any genuine idiosyncrasy you discover in the country's `_/CONTENTS.org`.
- Read `CLAUDE.md` section headings and the sections your files touch; read `.claude/skills/backlog-workflow/SKILL.md` for the rule that an open issue must describe exactly the remaining work: if your fix resolves PART of an issue, DRAFT the residual (title + body + tier) in your report.

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

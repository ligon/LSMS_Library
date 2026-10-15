# RED-TEAM review of branch `fix/811-region-tables` (element `811-region-tables`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/811-region-tables` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Guinea-Bissau,Burkina_Faso,Senegal built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

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

## The task the fix agent was given
## The task (from the triage's dispatch plan)
**What:** Guinea-Bissau: add `#+name: Region` table to _/categorical_mapping.org with the strata table's rows verbatim (sab -> SAB, BOLAMA_BIJAGOS -> Bolama-Bijagos), mirroring efe0199a3. Burkina_Faso (new table 22 -> 13) and Senegal (case-normalise 14) as the sibling issue from step 0.

**Verify:** Served cluster_features.Region vocabulary == served sample.strata vocabulary (9 == 9); tests/test_guinea_bissau_cluster_features.py passes. Cost: one Guinea-Bissau re-warm (one wave, small) -- NOT cache-free.

**Blocked by (as triaged):** nothing

## Triage records for the issues in scope

### #811 -- tier A, auto, dedup open
- country: Guinea-Bissau (siblings measured: Burkina_Faso, Niger, Senegal)
- approach: Add a `#+name: Region` table to Guinea-Bissau/_/categorical_mapping.org with the strata table's rows verbatim (BOLAMA_BIJAGOS -> Bolama-Bijagos, sab -> SAB, lower-case -> title case), mirroring efe0199a3; verify served cluster_features.Region == served sample.strata vocabulary (9 == 9) and that tests/test_guinea_bissau_cluster_features.py still passes; budget the Guinea-Bissau re-warm. Then file or widen to the measured siblings: Burkina_Faso (new table, 22 -> 13), Niger (add lower-case rows to the existing table, 16 -> 8), Senegal (case-normalise, 14 -> 14 title case).
- verify cost: one-country-rebuild
- CONTENTS.org: Guinea-Bissau/_/CONTENTS.org:408-440 ('Cluster Features -- the many-to-one projection (GH #323)'): Region (s00q01) and Rural (s00q04) are 'PERFECTLY CONSTANT WITHIN GRAPPE at source', so re-spelling is safe for the collapse; :139-150 ('SAB is urban-only by design') names SAB as the one region a pooled analysis trips over, which is also the label whose case differs most ('sab' vs 'SAB'); :276-300 leaves 'one vague per grappe, or two?' unresolved -- unrelated to spelling but worth knowing before touching the cluster table. No entry mentions the Region/strata split.
- evidence: lsms_library/countries/Guinea-Bissau/_/categorical_mapping.org:34-44 (strata table, Alternate Spelling -> Preferred Label; no Region table: grep -il '^#+name: *region$' lists Ethiopia, Malawi, Peru, Tanzania, Mali, Nigeria, Niger, GhanaLSS only); Guinea-Bissau/2018-19/_/data_info.yml:16 (strata: s00q01) and :48 (Region: s00q01); raw parquets Guinea-Bissau/var/cluster_features.parquet and var/sample.parquet both hold {'sab','bafata','oio','gabu','cacheu','tombali','quinara','biombo','BOLAMA_BIJAGOS'}; served via Country(c, assume_cache_fresh=True): GB Region raw, GB strata normalised, Niger Region 16 labels, Burkina_Faso 22, Senegal 14; country.py:2885-2894 (_apply_categorical_mappings: table name matched to column name case-insensitively, applied when a Preferred Label column exists); precedent commit efe0199a3 (#832, in merged PR #949); CLAUDE.md cache section lists categorical_mapping.org among hashed `_/*.org` build inputs; open PR #955 (#953, mapping resolution model) is docs+tests and does not change by-name resolution; gh: issue OPEN, 0 comments.
- summary: Confirmed at HEAD on served values: Guinea-Bissau cluster_features.Region is served raw ('sab', 'bafata', ..., 'BOLAMA_BIJAGOS') while sample.strata is served normalised ('SAB', 'Bafata', ..., 'Bolama-Bijagos'), because _/categorical_mapping.org has carried a `#+NAME: strata` table since bc70d3af0 (2026-04-09) and has no `Region` table; both columns read the same source variable s00q01 (2018-19/_/data_info.yml:16,48) and the raw parquet holds identical lower-case values in both. Nothing merged or open addresses it, but the identical fix landed for Ethiopia under #832 (efe0199a3, PR #949, 2026-09-18: a `#+name: Region` table read off the same file's `strata` table). Two corrections to the issue: the fix is NOT cache-free (categorical_mapping.org is among the `_/*.org` build inputs folded into Wave._input_hash per CLAUDE.md's cache section, so every Guinea-Bissau table re-warms -- one wave, small), and the canonical spelling should mirror the existing strata table ('SAB' upper-case, not title case) or the strata<->Region join it wants still misses. The sweep the issue asks for, done on SERVED values: Burkina_Faso 22 labels for 13 regions (no table), Niger 16 for 8 (its Region table at categorical_mapping.org:230-242 lacks the lower-case variants), Senegal 14 with 'Saint-Louis' beside 13 lower-case names (no table); Benin, Togo, CotedIvoire clean.

Read the full issue and its comments first: `gh issue view 811 --comments`.


## The branch: diffstat (vs development)
```
 lsms_library/countries/Burkina_Faso/_/CONTENTS.org | 29 ++++++++++
 .../Burkina_Faso/_/categorical_mapping.org         | 62 ++++++++++++++++++++++
 .../countries/Guinea-Bissau/_/CONTENTS.org         | 28 ++++++++++
 .../Guinea-Bissau/_/categorical_mapping.org        | 52 ++++++++++++++++++
 lsms_library/countries/Senegal/_/CONTENTS.org      | 24 +++++++++
 .../countries/Senegal/_/categorical_mapping.org    | 59 ++++++++++++++++++++
 tests/test_guinea_bissau_cluster_features.py       | 26 +++++++++
 7 files changed, 280 insertions(+)
```
## The branch: commit messages
--- 9ba85575b fix(Senegal): title-case cluster_features.Region (#811)
Sibling of the Guinea-Bissau fix, and cosmetic: `cluster_features.Region`
(s00q01) served 14 labels for 14 regions, identical across 2018-19 and
2021-22 -- thirteen lower-case ('dakar', 'thies', ...) beside one
title-case 'Saint-Louis', the latter only because both waves'
data_info.yml carry an inline SAINT-LOUIS -> Saint-Louis mapping.  No
within-Senegal join was broken; the value is casing consistency with the
other EHCVM countries' Region so a cross-country Feature filter on a
title-case name matches Senegal too.

`_/categorical_mapping.org` now carries a `#+name: Region` table that
title-cases the raw labels with NO accents added ('Thies', 'Kedougou',
'Sedhiou' -- the source has none; adding them would be a judgement beyond
case).  Senegal's `sample` has no `strata` column, so the Guinea-Bissau
"Region == strata" check has no analogue here.  data_info.yml untouched.

MEASURED on the read path (assume_cache_fresh=True): 1,194 rows, index
unique, 14 -> 14 labels, per-wave counts unchanged (Dakar 86/85, Thies
48/48, ..., Sedhiou 34/34).

CACHE COST, stated not paid: `_/categorical_mapping.org` is a build
input, so every Senegal cache hash moves -- two waves, seven script-path
tables.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- b396bcb52 fix(Burkina_Faso): harmonize cluster_features.Region -- 22 spellings, 13 regions (#811)
Sibling of the Guinea-Bissau fix.  `cluster_features.Region` served 22
distinct strings for the 13 regions across three waves; nothing warned
because each wave is internally consistent, so only cross-wave pooling
was wrong.  2014 (EMC `region`) writes unhyphenated mixed case ('Centre
Est', 'Centre sud', 'Plateau central', 'Sud Ouest', 'Hauts Bassins') and
singular 'Cascade'; 2018-19 hyphenates all but 'Hauts Bassins'; 2021-22
hyphenates all AND carries the source typo 'Boucle du Mouhoum'.

`_/categorical_mapping.org` now carries a `#+name: Region` table.  This
one is a DECISION, not a pairing inherited from a strata table: Burkina
has no strata spellings (2014 `strate` is a bare 1-26 code, verified via
get_dataframe; the EHCVM waves ship none -- CONTENTS.org "sample").  The
Preferred Labels are the 2021-22 fully hyphenated official forms, with
'Mouhoum' -> 'Mouhoun' and 'Cascade' -> 'Cascades'.  Which region each
string names is never in doubt.

MEASURED on the read path (Country('Burkina_Faso', assume_cache_fresh=True)):

* 2,035 rows, index unique, 22 -> 13 distinct labels;
* per-wave distinct 13 / 13 / 13 before and after, so the (t, v)
  projection's conflict set cannot move;
* every pooled count is the sum of its variants: Centre 224,
  Hauts-Bassins 84+60+60=204, Boucle du Mouhoun 75+49+44=168,
  Centre-Est 168, Est 152, Centre-Ouest 151, Sud-Ouest 149, Centre-Nord
  147, Nord 143, Centre-Sud 137, Sahel 132, Cascades 131,
  Plateau-Central 129.  Sums to 2,035.

CACHE COST, stated not paid: `_/categorical_mapping.org` is a build
input, so every Burkina_Faso cache hash moves -- three waves, eleven
script-path tables.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>

--- 6733342a0 fix(Guinea-Bissau): serve cluster_features.Region in the strata spelling (#811)
`cluster_features.Region` and `sample.strata` both read s00q01 and the
raw parquets hold identical lower-case values in both, but only `strata`
had a table in `_/categorical_mapping.org` (`#+NAME: strata`, since
bc70d3af0), and the auto-applied mapping matches tables to columns BY
NAME.  So `Region` was served raw ('sab', 'bafata', ..., 'BOLAMA_BIJAGOS')
while `strata` was served normalised ('SAB', 'Bafata', ...,
'Bolama-Bijagos'); `Region == 'Bafata'` matched nothing and a
strata <-> Region join matched zero rows.

`_/categorical_mapping.org` now carries a `#+name: Region` table whose
rows are the `strata` table's rows verbatim plus identity rows -- the
canonical spelling is the one already in force for `strata` ('SAB'
upper-case, NOT the issue's title case: agreement between the two
columns is the point), mirroring Ethiopia's Region table (#832,
efe0199a3).

MEASURED on a COLD build in a private LSMS_DATA_DIR (cache cleared,
lsms_library.__file__ asserted at this clone):

* cluster_features: 450 rows, index unique, 9 distinct Region before and
  after; served vocabulary now == served sample.strata vocabulary
  (SAB 92, Bafata 50, Oio 48, Gabu 48, Cacheu 44, Tombali 42, Quinara 42,
  Biombo 42, Bolama-Bijagos 42);
* the stored 2018-19 wave parquet still holds the raw lower-case values
  (5,351 rows) -- correct: a read-path mapping, as `strata` always was;
* the (t, v) projection cannot move: the mapping runs post-collapse in
  `_finalize_result`, and Region is constant within grappe at source;
* Rural still 281 / 169.

CACHE COST, and a correction to the issue: `_/categorical_mapping.org` is
a build input (every `_/*.org` but CONTENTS.org is folded into
Wave._input_hash, country.py:965), so this moves every Guinea-Bissau
cache hash.  One wave, small (7 s to rebuild the two tables).  "Read-path
only, no cache hash moves" in the issue text is wrong on that point.

tests/test_guinea_bissau_cluster_features.py gains
test_region_and_strata_share_one_vocabulary; the file passes (6 passed).
CONTENTS.org records the idiosyncrasy (the SOURCE mixes eight lower-case
names with one upper-case underscored one).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


## The fix agent's report (a self-report -- refute it)
The only remaining step before the final advisor check is the mapping-test summary, so I'm waiting on that background wait to return.

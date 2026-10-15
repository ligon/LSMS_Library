# RED-TEAM review of branch `fix/848-uganda-condition2` (element `848-uganda-condition2`)

You are an independent, adversarial reviewer in a Slurm job with your OWN clone of `fix/848-uganda-condition2` (your cwd), your OWN mounted venv (`.venv/bin/python`), and a PRIVATE `LSMS_DATA_DIR` (env) seeded with the PRE-FIX warm parquets of Uganda built from `development`; `dvc-cache` in it is a read-only symlink to the shared blob store. `development` is available as `origin/development` in your clone (`git diff --stat origin/development..HEAD`).

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
**What:** Uganda: add a second `conditions` entry to CROP_COLMAPS['2018-19']['B'] (uganda.py:2037-2041) and ['2019-20']['B'] (:2046-2050), harvest + sold sides + kg_factor s5bq06d_2; unit-vs-condition by value-label vocabulary (uganda.py:1703). If @ligon's #647 questionnaire verdict is 'include', add the `_11`/`_21` family in the same PR; if 'exclude', one sentence at uganda.py:1846.

**Verify:** Before/after: +1,436 (2018-19) / +1,307 (2019-20) rows, 14 / 3 additive collisions, harvest_kg delta reported; move tests/test_uganda_crop_condition.py:59 baseline (133,683) and the CONTENTS.org:2076 TODO. Cost: full Uganda re-warm (uganda.py in every Uganda table hash).

**Blocked by (as triaged):** step 1 (park #962 draft); #647 verdict optional. -- the #962 park has been done.

## Triage record
### #848 -- tier B, auto, dedup open
- country: Uganda
- approach: Add a second `conditions` entry to CROP_COLMAPS['2018-19']['B'] and ['2019-20']['B'] (harvest + sold sides, plus kg_factor s5bq06d_2 for 2018-19), deciding unit-vs-condition columns by value-label vocabulary per uganda.py:1703; before/after: +1,436/+1,307 rows, 14/3 additive collisions, harvest_kg delta, then move the CONTENTS.org TODO and test baseline; full Uganda re-warm (uganda.py is in every Uganda table hash).
- verify cost: one-country-rebuild
- CONTENTS.org: CONTENTS.org:1297-1314 'The 5B second slot IS a condition slot, not a season slot -- the old note was wrong … Still not wired: it ADDS ~1 435 rows, which is a mass change requiring its own before/after'; CONTENTS.org:2076-2091 TODO 'Second condition slot dropped for 2019-20 season B … 1 306 non-null conditions'; CONTENTS.org:1318-1321 mixed naming vintages within one file (slot 1 legacy a5bq6{b,c,d}, slot 2 WB s5bq06{b,c,d}_2)
- evidence: lsms_library/countries/Uganda/_/uganda.py:2037-2041, :2046-2050, :2029-2036, :1703-1708 (value-label rule), :1846-1851; raw L1 blobs 62f4e464… (2018-19 AGSEC5B, 84 cols) and 5a62f8e7… (2019-20 agsec5b, 78 cols) column-name + collision probe 2026-09-24; commits b28b50242 (#323), 7a4279da0 (#842); merged_prs.json/open_prs.json: no PR references #848; tests/test_uganda_crop_condition.py:59 (133,683-row baseline that will move)
- summary: Uganda's 2018-20 WIDE-vintage colmaps read one `conditions` slot where the source ships two. At HEAD `CROP_COLMAPS['2018-19']['B']` (uganda.py:2037-2041) and `['2019-20']['B']` (uganda.py:2046-2050) wire slot 1 only, while `['2019-20']['A']` (uganda.py:2029-2036) already wires both slots (commit b28b50242, #323). Raw column probe of the L1 blobs confirms the unread slots: 2018-19 AGSEC5B ships `s5bq06{a..f}_2` + `s5bq07{a,b,c}_2` (1,436 non-null quantities), 2019-20 agsec5b ships the same family labelled '(2019, condition2)' (1,307 non-null) -- a second cell the issue only asked to check and which has no issue number of its own, only `CONTENTS.org:2076` TODO; 2018-19 AGSEC5A has NO `_2` slot (33 columns). So the fix is exactly two cells, harvest and sold sides each. Collision count measured on the raw files (the 'first experiment' CONTENTS.org:2639-2646 asks for): slot-2 records sharing BOTH unit and condition with slot 1 -- 14 of 1,436 (2018-19), 3 of 1,307 (2019-20) -- would be summed by the additive collapse; the rest land as new `condition` rows (slot-2 modal condition 45 vs slot-1 modal 24). That also means slot 2 cannot explain the pre-existing 433 duplicate groups, since it is not read today. Not touched by any merged/open PR; the uncommitted uganda.py diff in the working tree is #962 (food units), unrelated.

Read the full issue and its comments first: `gh issue view 848 --comments`.


## The branch: diffstat (vs development)
```
 .coder/ledger/824-uganda-sold-unit.md              |   4 +-
 .../countries/Uganda/2018-19/_/crop_production.py  |  10 ++
 .../countries/Uganda/2019-20/_/crop_production.py  |  17 +-
 lsms_library/countries/Uganda/_/CONTENTS.org       | 179 ++++++++++++++++-----
 lsms_library/countries/Uganda/_/data_scheme.yml    |   4 +-
 lsms_library/countries/Uganda/_/uganda.py          |  73 +++++++--
 lsms_library/data_info.yml                         |   3 +-
 tests/test_crop_kg_factor.py                       |  30 +++-
 tests/test_gh842_uganda_harvest_unit.py            |  38 ++++-
 tests/test_uganda_crop_condition.py                |   7 +-
 tests/test_uganda_sold_unit.py                     |  15 +-
 11 files changed, 302 insertions(+), 78 deletions(-)
```
## The branch: commit messages
--- cc25a626c Uganda crop_production: wire the season-B second condition slot, 2018-19 and 2019-20 (GH #848)
CROP_COLMAPS['2018-19']['B'] and ['2019-20']['B'] read only slot 1 of
AGSEC5B / agsec5b while the source ships a second harvest record per
plot-crop (s5bq06{a,b,c}_2 + the sold s5bq07{a,b,c}_2 / s5bq08_2), and
2019-20 season A already read both of its slots.  Both B colmaps now
carry a second `conditions` entry; 2018-19's also wires s5bq06d_2 as
KgFactor (2019-20 wires no kg_factor on either slot -- #849's scope).

The slot is a second CONDITION, not a season: harvest-end dates are
June/July 2018 on 1 423 of 1 435 rows like slot 1 (AGSEC5A holds the
2017 second season the variable label names), and the value-label
vocabulary decides the columns -- s5bq06b_2 100% in harvest_units
(69.4% unit-only), s5bq06c_2 100% in harvest_conditions (91.1%
condition-only, modal "Dry - grain" against slot 1's "Fresh").  Slot 1
of the 2018-19 file uses legacy a5bq6{b,c,d} names, slot 2 WB names.

Before -> after, cold private LSMS_DATA_DIR, both reads cold:
  rows                 130,606 -> 133,331
    2018-19 B            7,041 ->   8,463   (+1,422 = 1,436 slot-2 - 14 collisions)
    2019-20 B            7,372 ->   8,675   (+1,303 = 1,307 slot-2 -  4 collisions)
    every other (t, season)         identical
  sum(Quantity)     7,309,085.96 -> 7,434,726.31
  sum(Quantity_sold) 18,134,251.40 -> 18,197,452.70
  sum(Value_sold) 10,709,007,434.04 -> 10,879,808,638.04
  KgFactor non-null     14,149 ->  15,074
  u='Unknown' 2018-19 B      0 ->       4   (slot-2 records with a measure and no unit code)
  harvest_kg rows       78,532 ->  78,571;  sum 28,680,848.595 -> 29,093,322.408 kg
  kg_factor_sources   reported 14,050 -> 14,972; survey_median 57 -> 555;
                      metric 86,978 -> 88,190; none 29,521 -> 29,614;
                      reported_implausible 99 -> 102
  GrainConflictWarning groups 11 -> 12 (one 2018-19 B Condition_sold conflict)
  sale rows knowing both units 40,083 -> 41,162; agreement 0.9606 -> 0.9614;
  largest disagreement cell 162 -> 165; 2009-10 A coverage 0.968 unchanged

Pinned baselines moved with those numbers: tests/test_crop_kg_factor.py
(row counts, Harvest_kg sum, kg_factor_sources), tests/test_uganda_sold_unit.py
(the four shipped counts), and tests/test_gh842_uganda_harvest_unit.py,
whose 2018-19-B sentinel assertion now derives the allowed count (4)
from the raw file and asserts equality.  The same counts are updated
where the prose quotes them (data_info.yml, Uganda data_scheme.yml,
CONTENTS.org, .coder/ledger/824).  CONTENTS.org: TODO -> DONE with the
before/after; the #824 "first experiment" is recorded as run -- slot 2
adds 18 collision groups and cannot explain the pre-existing 433.

Cache cost: uganda.py and data_scheme.yml are in every Uganda table's
input hash, so a full Uganda re-warm follows the merge.

Verified: 224 passed / 8 skipped (nine Uganda test files) and 555
passed / 15 skipped (config-level files), skips all "other country not
warm in the private root".

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>


## The fix agent's report (a self-report -- refute it)
**SCOPE DEVIATIONS** (files touched that the task did not name; none matches the stop list, nothing left uncommitted):
- `tests/test_crop_kg_factor.py`. The live pinned baseline lives here (rows, harvest_kg rows and sum, the full source dict). The triage's `test_uganda_crop_condition.py:59` is a provenance comment, not an assertion.
- `tests/test_gh842_uganda_harvest_unit.py`. Its 2018-19 season-B assertion of exactly zero `Unknown` units was premised on slot 1 alone. It now derives the allowed count from the raw file and asserts equality.
- `tests/test_uganda_sold_unit.py`. Four pinned sale-row counts moved. Nobody anticipated this file.
- `lsms_library/data_info.yml`, `lsms_library/countries/Uganda/_/data_scheme.yml` (comment only), `.coder/ledger/824-uganda-sold-unit.md`. The sold-unit test's own failure message directs updating every place that quotes the 40,083 count. The ledger got a dated bracket appended, not a rewrite.
- `lsms_library/countries/Uganda/2018-19/_/crop_production.py` and `2019-20/_/crop_production.py`. Docstrings only. The 2019-20 one said "KNOWN GAP: CROP_COLMAPS does not read" slot 2, which became false in a file the country rule tells the next reader to read first.

**1. Commit and branch.** One commit, `cc25a626c`, on `fix/848-uganda-condition2`. Pushed to `origin`; `git ls-remote` shows the same SHA.

**2. Verification.** Both reads were cold in the private root (the wave scripts ran; the after-read took 91.6 s). The data_scheme.yml comment landed after the probe, so the third pytest run forced a second independent cold rebuild, and the pinned numbers below were reproduced by it, not read back from the probe's parquet.

| quantity | before | after |
|---|---|---|
| rows | 130,606 | 133,331 |
| 2018-19 B rows | 7,041 | 8,463 (+1,422 = 1,436 slot-2 records − 14 collisions) |
| 2019-20 B rows | 7,372 | 8,675 (+1,303 = 1,307 − 4) |
| every other (t, season) | | identical |
| harvest_kg rows / sum kg | 78,532 / 28,680,848.60 | 78,571 / 29,093,322.41 |
| harvest_kg 2018-19 / 2019-20 kg | 5,904,803 / 3,668,441 | 6,152,896 / 3,832,822 |
| kg_factor_sources reported / survey_median / metric / none / implausible | 14,050 / 57 / 86,978 / 29,521 / 99 | 14,972 / 555 / 88,190 / 29,614 / 102 |
| KgFactor non-null | 14,149 | 15,074 |
| u='Unknown' 2018-19 B | 0 | 4 (slot-2 records with a measure and no unit code) |
| GrainConflictWarning groups corpus-wide | 11 | 12 (one 2018-19 B `Condition_sold` conflict between the slots) |
| sale rows knowing both units / agreement / largest disagreement cell | 40,083 / 0.9606 / 162 | 41,162 / 0.9614 / 165 |

Collisions were measured from the wave builder itself (old colmap vs new): on the 18 summed groups, `KgFactor` and `harvest_month` are slot 1's, six sold labels were completed from NA, one was blanked. Unit-vs-condition for `s5bq06b_2` / `s5bq06c_2` by vocabulary: 100% in `harvest_units` (69.4% / 64.2% unit-only) vs 100% in `harvest_conditions` (91.1% / 84.8% condition-only).

Test runs, written to files:
```
tests/test_uganda_crop_condition.py test_gh842_uganda_harvest_unit.py test_crop_kg_factor.py test_uganda_sold_unit.py test_uganda_99999.py test_uganda_intercropped.py test_gh637_uganda_plot_inputs_season.py test_quantity_screen.py test_863_transforms.py
224 passed, 8 skipped, 41 warnings in 72.69s (0:01:12)
tests/test_build_transform_hash.py test_feature_canonical_index.py test_label_selection.py test_id_format_id_canonical.py test_gh866_stale_country_parquet_not_laundered.py test_derivations.py test_schema_consistency.py test_panel_ids_persistence.py test_shipped_factors.py
555 passed, 15 skipped, 12 warnings in 390.24s (0:06:30)
```
All skips are "other country not warm here" in the private root. `find lsms_library/countries -name '*.parquet'` is empty.

**3. Cache cost.** Full Uganda re-warm of every table, from `uganda.py` alone; the data_scheme.yml comment adds nothing incremental. Paid only in the private root. `data_info.yml` is read-path and moves no hash.

**4. Surprises.** The triage's 133,683 baseline was two fixes stale (live pin 130,606, after #861). The triage's 2019-20 collision count of 3 is 4 on the served key, because keys pass through the code-to-label maps. A fourth pinned test (`test_uganda_sold_unit`) moved and nobody named it. Nothing contradicts CONTENTS.org; the idiosyncrasy that bears on the task is exactly as recorded there: 2018-19 AGSEC5B mixes naming vintages in one file (slot 1 legacy `a5bq6{b,c,d}`, slot 2 WB `s5bq06{b,c,d}_2`), and slot 2 is variable-labelled "second season of 2017" but is a condition slot by its June/July 2018 harvest dates. The `NullIndexKeyWarning` on `v` (926 rows) is an after-only count; I did not capture it before. Remaining quotes of 130,606 elsewhere are dated measurements in ledgers and the quantity_audit timing table, left as history.

**5. Decisions.** No `kg_factor` on either 2019-20 slot although `s5bq06d_2` exists there (921 non-null): that is #849's scope and the gh842 test pins zero KgFactor outside 2018-19. The `_11`/`_21` part-harvest slots stay unwired and the one-sentence note stands verbatim; only the 2018-19 sold-slot claim was removed from that paragraph. The gh842 sentinel assertion moved from a constant to a raw-derived count rather than to a looser threshold. The #824 "first experiment" is recorded in CONTENTS.org as run: slot 2 adds 18 collision groups and cannot explain the pre-existing 433.

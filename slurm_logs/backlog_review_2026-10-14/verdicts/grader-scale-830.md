VERDICT: pass-with-fixes -- the check does what #830 asked, every claimed number reproduces, it is read-path and never moves a tier, and the suite is green except for one pre-existing environment-dependent flake; but the corpus firing rate the agent never measured shows 4 flags, all Ghana: two are genuine GHC-era inflation across a seven-year gap and two (GhanaSPS crop sales, 2009-10 vs 2017-18) are a documented instrument change whose residual above CPI nobody has adjudicated, so the tolerance design needs the human decision the record already asked for, and the docs omit the sentence the issue itself requested.

# 1. Refutations (claim -> what I ran -> result)

| # | claim (report / commit) | command | result |
|---|---|---|---|
| 1 | Uganda `crop_production` served frame: 0 breaks, per-wave medians 48,000 / 50,000 / 80,000 / 90,000 / 96,000 / 120,000 / 135,000, 63 ms | `/local/job39923202/measure.py` on the private warm root (130,606 rows, 7 waves in `t`, 8 in `co.waves`) | **held** exactly: `[]`, 63 ms, medians identical, n 5,473-6,936 |
| 2 | positive control (2015-16 `Value_sold` / 100): exactly one record, median 960 (n=6,882), neighbours 2013-14=90,000, 2018-19=120,000, ratio 0.0098, `per-item[21]` | same script | **held**: `{'wave': '2015-16', 'median': 960.0, 'n': 6882, 'neighbours': [('2013-14', 90000.0, 0.0111), ('2018-19', 120000.0, 0.0084)], 'ratio': 0.00977, 'form': 'per-item[21]'}`; no other wave reported |
| 3 | edge-wave control (2019-20 / 100): only 2019-20, ratio 0.011 | same | **held**: ratio 0.01114, `per-item[20]` |
| 4 | x100 the other way (not claimed; my addition) | same | 2015-16 alone, ratio 97.7 |
| 5 | "a persistent break (A,B vs C,D) is not seen -- stated" | 2015-16 onward / 100 | confirmed blind: `[]`. Stated honestly, and it is the live hazard the check was motivated by ("a wave whose currency was re-denominated without the script following") |
| 6 | `food_acquired` 355,571 rows 0 breaks 355 ms; `nonfood_expenditures` 251,588 rows 0 breaks 149 ms; `plot_inputs` 0 breaks | same | **held** (393 ms / 156 ms / 0 ms; row counts identical) |
| 7 | `tests/test_coverage_matrix.py + tests/test_build_transform_hash.py`: 97 passed | `pytest ... -n 8 --dist=loadfile` on the branch | **held**: `97 passed, 9 warnings in 40.32s` (the `requires_s3` positive-control test ran, not skipped) |
| 8 | cache cost none | `test_build_transform_hash.py` green; `git diff --stat 2ed85b49b..HEAD` touches only `coverage_matrix.py`, `scan.py`, tests, docs, ledger | **held**; no build-path file touched, no parquet written in-tree (`find lsms_library/countries -name '*.parquet'` = 0 after every run) |
| 9 | `scan.py` emits `scale` findings | `bench/feature_audit/scan.py --features crop_production --countries Uganda --jobs 1` on the private root | ran clean (171 records, all warn, none `scale` because the served frame has no break). I then fed `scan._scale_findings` the live GhanaSPS `crop_production` frame: 2 `Finding`s, `check=scale`, severity C, fingerprint `crop_production|GhanaSPS||scale`, and `to_record()` round-trips through `json.dumps` (the `neighbours` metric is a list of tuples) -- the finding path works on real data, not only on the agent's synthetic smoke test. Parity with `_null_read_findings`: neither emits a `pass` record when nothing fires |
| 10 | the 11 new tests are meaningful | copied the branch test file into the `origin/development` worktree, `-k scale_check` | 11 failed / 1 passed on development (all `AttributeError: module has no attribute monetary_scale_breaks`); the one that passes there is `..._uses_the_monetary_registry_not_a_yaml_scan`, which tests `currency._monetary_columns`, pre-existing code. The other ten fail trivially (new symbol), but their bodies pin semantics (tier unchanged, 1.2x silent, redenomination skip, support floor, NA/zero, pooled fallback, item-mix robustness, non-fatal failure, NA currency) rather than existence |
| 11 | "`currency_for` returns pd.NA ... `pd.NA != pd.NA` raises" (second commit) | covered by `test_scale_check_country_without_a_currency_entry_is_still_checked`, green | held |
| 12 | corpus firing rate | **not claimed -- not measured by the agent** ("only Uganda was warm"). I measured it read-only over the live warm `var/*.parquet` (`/local/job39923202/census.py`, `pd.read_parquet` only, no library build path, `df.equals(before)` asserted on every cell) | **87 country-tables judged, 4 records, all Ghana** -- see §5 |

# 2. Claimed vs measured

| quantity | claimed | measured |
|---|---|---|
| Uganda crop_production rows / waves | 130,606 / 8 | 130,606 / 7 in `t` (2005-06 declared, not in the built table) |
| 2015-16 median after /100, n | 960, 6,882 | 960.0, 6,882 |
| ratio, form | 0.0098, per-item[21] | 0.00977, per-item[21] |
| neighbours | 90,000 / 120,000 | 90,000 / 120,000 |
| served-frame breaks / cost | 0 / 63 ms | 0 / 63 ms |
| edge control ratio | 0.011 | 0.0111 |
| food_acquired rows / breaks / ms | 355,571 / 0 / 355 | 355,571 / 0 / 393 |
| nonfood_expenditures rows / breaks / ms | 251,588 / 0 / 149 | 251,588 / 0 / 156 |
| targeted tests | 97 passed | 97 passed |
| corpus firing | (not measured) | 4 of 87 warm monetary country-tables |

# 3. Regression

Full suite, branch, verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 pytest tests -n 32 --dist=loadfile -q`, private root):

    1 failed, 5503 passed, 177 skipped, 3 xfailed, 1 xpassed, 873 warnings in 916.46s (0:15:16)
    FAILED tests/test_feature_canonical_index.py::test_warm_corpus_assembly_loses_no_rows

**Not attributable to the branch.** The assertion is `len(grain_reports()) == before` with `before=1, after=0`: the process-global `_GRAIN_LEDGER` entry was popped by a rebuild inside the `Feature()` call (`country.py:4422`, `_GRAIN_LEDGER.pop((self.name, method_name))`), which the suite triggers in a partially warm private root (Nigeria `crop_production` had no make rule there: `No rule to make target .../Nigeria/var/crop_production.parquet`). It passes in isolation on the branch (`tests/test_feature_canonical_index.py`: `41 passed in 27.04s`) and on the `origin/development` worktree (`1 passed in 22.74s`). The branch cannot reach that path: nothing on the `Feature()` / `Country` build path imports `coverage_matrix`, and the branch's only new import is inside `monetary_scale_breaks` itself.

Targeted: `tests/test_coverage_matrix.py tests/test_build_transform_hash.py`: `97 passed, 9 warnings in 40.32s`.
Scan: `feature-audit scan: 1 features ... records: 171 skips: 0; by status: {'warn': 171}` (Uganda crop_production; no `scale` record, correctly).

# 4. Scope findings

- `git diff --stat 2ed85b49b..HEAD` (merge-base; development is 16 commits ahead, the only overlap is a comment in `data_info.yml`): 5 files, 562 insertions, 1 deletion -- the four task-named paths plus the per-task ledger `.coder/ledger/830-grader-scale.md`, which the report declares. `git merge-tree --write-tree origin/development HEAD` merges clean.
- No baseline / golden / blessed / lockfile / dependency edit. No assertion weakened, skipped or xfailed; no pin moved.
- The docs paragraph does NOT do what the issue text asked: "`docs/guide/coverage.md` gets a paragraph saying what `sane` still does not see after this (item coverage, unit mislabels)". The added paragraph states only the check's own blind spots (persistent break, two-wave frame). Small, but it is the issue's one explicit docs requirement.
- Residual: drafted in REPORT.md (persistent break; `community_prices` grain). It is not on the issue (#830 body unchanged); whoever merges should post it. The draft should also carry the census below, which it says is still to be done.
- CONTENTS.org: framework task; the agent correctly named the motivating idiosyncrasy (Uganda 2015-16 `a5aq8` / 100, `Uganda/_/CONTENTS.org` WAITING entry at line 2142, guarded by `_VALUE_SOLD_SCALE = 100` in `2015-16/_/crop_production.py:83`). Nothing it wrote contradicts that file. No country edit needed.
- Writes: `git status --short` empty at the end; 0 in-tree parquets in both the clone and the base worktree; all suite-created cache dirs are under the private `LSMS_DATA_DIR`.

# 5. Residual risks and what a human must decide

**(a) Corpus firing, measured** (read-only over the LIVE root's raw `var/*.parquet` with `pd.read_parquet`, never through the library's build path; `df.equals(before)` asserted on every cell; monetary columns are untouched by `_finalize_result`, so these medians are what the grader sees; 87 monetary country-tables):

| country | table | column | wave flagged | median (n) | ratio vs neighbour | form |
|---|---|---|---|---|---|---|
| GhanaLSS | community_prices | Price | 1998-99 | 2,000 (63,445) | 5.56x vs 1991-92 (300) | per-item[108] |
| GhanaLSS | food_acquired | Price | 1991-92 | 100 (287,570) | 0.16x vs 1998-99 (1,200) | per-item[45] |
| GhanaSPS | crop_production | Value_sold | 2009-10 | 80 (6,844) | 0.167x vs 2017-18 (500) | per-item[16] |
| GhanaSPS | crop_production | Value_sold | 2017-18 | 500 (3,490) | 6.0x vs 2009-10 (80) | per-item[16] |

Not a firehose (4 of 87), and no known defect in the warm corpus is missed (the only one, #829, is guarded). The two GhanaLSS records are a seven-year gap in GHC-era Ghana (CPI roughly 5-6x over 1992-1999), so a fixed tolerance of 5 applied per *adjacent pair regardless of the years between them* fires on genuine inflation there. The GhanaSPS pair is **unadjudicated and above CPI**: per-crop 2017-18/2009-10 medians run 3.1x (beans) to 7.5x (yam), 4.2x maize, 5.2x cassava, 6.6x cocoa, against a 2010-2018 Ghana CPI of roughly 2.5-3x. `GhanaSPS/_/CONTENTS.org` §"Sales: three instruments, three grains, one honest asymmetry" documents why: 2009-10 `Value_sold` is the season harvest record's A83/A124 total revenue (cedis + pesewas/100, no sold quantity asked), 2017-18 is a sum over nine buyer channels of quantity x unit price, and 2013-14 is NaN by design (no `plotid`; declared `optional`) -- which is why 2013-14 is unjudged and the two remaining waves report each other. An instrument change, not a known scale defect, but nobody has signed off that the residual 2x is real; it is exactly the kind of cell the check exists to queue. Decision for the human: accept the fixed tolerance and its inflation-era cost (the branch's position), or make it gap-aware / CPI-deflated. Warn-only, so nothing is wrong today -- but the first `make matrix` after merge will carry these four notes.
**(b) Redenomination = `currency_for` ISO-code change** (the record's `decision_for_human`): a currency fact already in `data_info.yml`, not a cell allowlist; measured correct on GhanaLSS (2005-06 GHC vs 2012-13 GHS skipped; same-code 10,000x reported). I agree with the agent; confirm.
**(c) The persistent-break blind spot is the motivating hazard.** #830's second sentence is "any wave whose currency was re-denominated without the script following"; a redenomination the config does not know about is a *persistent* break and this check cannot see it (confirmed: Uganda with every wave from 2015-16 onward / 100 reports nothing). The residual names this; it should be filed, not left in a report.
**(d) Minor:** `form` on a record is the first neighbour's form only (`neigh[0][3]`); a record whose two neighbours used different forms is mislabelled.

# 6. Recommended follow-up commits

1. `docs/guide/coverage.md`: one sentence the issue asked for -- what `sane` still does not see after this check (item coverage, unit mislabels, a persistent scale break, an unconfigured redenomination).
2. `.coder/ledger/830-grader-scale.md` (and the residual draft): record the corpus census above (4 of 87, all Ghana: two inflation, two unadjudicated) so the tolerance decision is made on numbers; re-runnable as a `monetary_scale_breaks` sweep over `var/*.parquet`.
3. `lsms_library/coverage_matrix.py`: carry `form` per neighbour (or state in the docstring that `form` is the first comparable neighbour's).
4. Filing candidate from the census: GhanaSPS `crop_production` 2009-10 vs 2017-18 `Value_sold` level (per-crop 3-7.5x over eight years, instrument change documented in `GhanaSPS/_/CONTENTS.org`, residual above CPI unadjudicated) -- a `scale` finding for the audit harness, not part of this branch.
5. On merge: post the residual (persistent break; `community_prices` grain; gap-aware tolerance) to #830 rather than closing it outright, per the 2026-10-14 rule that an open issue describes exactly the remaining work.

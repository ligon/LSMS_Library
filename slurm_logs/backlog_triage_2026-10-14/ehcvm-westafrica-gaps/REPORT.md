# Backlog triage `ehcvm-westafrica-gaps` -- 8 issues, read-only (2026-10-14)

Clone at `a7cf04f2d` (development). Records: `records.json` beside this file. No file outside `$STAGE` was written; `git status --short` is empty; nothing pushed (read-only element -- the brief's push rule does not apply).

## 1. Table

| # | tier | type | tractability | dedup | prio | root_cluster | approach (one line) |
|---|---|---|---|---|---|---|---|
| 941 | B | test | diagnose-then-auto | open | 2 | ci-data-tests-sequential-session | add `workflow_dispatch` to `data-tests` (after PR #973), re-run it; then a sequential cold bisect in CI file order with CI's full stack (incl. pyreadstat) |
| 882 | C | design | human | partial (docs landed in 16c26c65d; nothing in the frame) | 2 | recall-window | human picks a/b/c for the 2011-12 dummies; durable fix is a non-food recall carrier (#851) |
| 864 | B | bug | diagnose-then-auto | open | 2 | food-reshape-zero-fill | Burkina 2014 hook: serve p2-4 purchased `Quantity` as NaN (today 0); then a `money_only` by-design bucket in `_drop_unpriceable` |
| 823 | C | data-gap | human | open (CONTENTS landed; the 'other six' answered: 5 more ship it) | 3 | ehcvm-s05co-unwired | one shared `ehcvm.community_prices_ehcvm` builder, seven 2018 waves, `obs` level per the #840 decision |
| 821 | C | data-gap | human | open (CONTENTS landed; data_scheme comment still wrong) | 3 | ehcvm-s05co-unwired | same builder; fix `Mali/_/data_scheme.yml:333-338` |
| 816 | D | data-gap | human | open | 4 | acquisition-path | decide a 'held, different instrument' coverage marker before any EP ingest; catalog-only SeriesCapability |
| 812 | C | docs | auto | open (PR #971 touches the file, not the topic) | 3 | ehcvm-strata-allocation-undocumented | add the measured allocation beside the verbatim quote; twins for Togo (66.5%) and Benin (31.4%) |
| 706 | D | data-gap | blocked | open | 4 | acquisition-path | acquire the CILSS BID (catalog 83/15551); nothing wireable before that |

## 2. Reconcile

**Nothing in this group can be closed.** All eight are open on today's `development`; none of the merged PRs since 09-18 (#944-#966) nor the in-flight PRs (#967, #969, #971-#973) fixes any of them. Verified by reading the diffs / file lists:

- #941: PR #944 lists `#941` only as a merge note ("Merge of github/development (v0.14.0, #941)"). The `data-tests` job has **not run since the 09-17 failure**: the four later green master runs (through 38270795197, 2026-10-12) are `schedule` events at the same sha `1528fa937`, where `data-tests` is `skipped` (`ci.yml:274`, confirmed with `gh run view --json jobs`). Its state is unknown, not green.
- #882: `partial`. Option (a) "keep + document" landed with the #877 fix (`16c26c65d`): `Niger/_/data_scheme.yml:339-348`, the script docstring, `CONTENTS.org:863-997`. The served dummies are unchanged and undisclosed in the frame. A partial comment is warranted, not a close.
- #821/#823: the CONTENTS.org records are on development (Mali :859-895, Niger :802-823); the `data_scheme.yml` text is still wrong (Mali :333-338; Niger's "ships no community price file"); nothing is wired; `absent_verdicts.csv` has no EHCVM `todo` rows. Partial comment: the "check the other six" is answered below.
- #812: PR #971 (open) edits `Guinea-Bissau/_/CONTENTS.org` for #811 Region spellings; its diff contains 0 mentions of stratification/allocation. Not a fix; sequence after it.
- #864, #816, #706: no landed work at all (0 comments each on #816/#706/#864-related; no PR refs).

## 3. Dispatchable now

Ordered by leverage; "cache cost" cites CLAUDE.md's rules: `transformations.py` reaches no build fingerprint; a `.py`/`data_scheme.yml`/`.org` (non-CONTENTS) edit under a wave or country `_/` dir is a `binp:` input of every table of that wave, and every L2-country hash of the country moves; `CONTENTS.org` is excluded.

1. **#864 step 1 (root) -- Burkina 2014 hook** (`Burkina_Faso/2014/_/mapping.py::food_acquired`, after the per-passage reshape): purchased `Quantity` -> NaN for `visit > 1`; decide whether to serve the p2-4 `autocons` value (today dropped by `build_transforms.py:135`). Verify with `tests/test_burkina_faso_2014_passages.py` re-pinned per visit. Cache cost: **country tier, all Burkina_Faso tables; wave tier, 2014 only**. Do **not** touch `build_transforms.food_acquired_to_canonical`'s `fillna(0)` corpus-wide without a census (it is `@build_transform`; every reshaped country re-warms).
2. **#864 step 2 (symptom) -- `_drop_unpriceable` `money_only` bucket** (`lsms_library/transformations.py:169-262`): rows with `Expenditure > 0` and `Quantity` NaN, detected per `(t, s, visit)` group where Quantity is 100% null, excluded from `expected`/`lost` like `currency`, named in the warning. Cache cost: **zero** (`test_no_transformations_symbol_reaches_any_fingerprint`). Only meaningful after step 1, because today those rows are **0, not NaN** (see §5).
3. **#941 step 1 -- CI config**: add `workflow_dispatch` to `ci.yml` so `data-tests` can run on a chosen ref; run it on development. Cache cost: zero. Sequence after PR #973 (open on `ci.yml`; it does not touch the `data-tests` job). Step 2 (sequential cold bisect) is a human-budgeted run, hours.
4. **#812 -- docs**: measured-allocation paragraph beside `Guinea-Bissau/_/CONTENTS.org:183-190`; twins for Togo and Benin. Cache cost: zero. Sequence after PR #971 merges (same file).

Tier C/D items need the decisions in §4 first. #821+#823 are **one** task across seven countries once decided (cache cost per country wired: every L2-country hash moves, the 2018 wave's parquets at the wave tier -- Benin, Burkina_Faso, Guinea-Bissau, Mali, Niger, Senegal, Togo).

## 4. Escalations

- **#941**: add `workflow_dispatch` to `data-tests` so it can be exercised without a release push -- yes/no?
- **#882**: Niger 2011-12 `people_last7days` dummies: **A** keep as served (30-day/12-month under a 7-day name, docs only), **B** re-point `wage_work` to `ms04q01` (all 30-day; a second parity departure from `NER_ECVMA1.do:1378`), **C** serve NA?
- **#864**: serve Burkina 2014 passage 2-4 purchased `Quantity` as NaN rather than 0 (Burkina-only re-warm) -- yes/no?
- **#821/#823**: register `community_prices` in `data_info.yml` `index_info` with an optional `obs` level (the #840/#822 decision) and wire all seven EHCVM 2018 `s05_co_*` files on one shared builder -- yes/no? (The `todo` rows in `.coder/coverage/absent_verdicts.csv` are a human write under the guardrails.)
- **#816**: should coverage gain a "held, different instrument / not comparable" marker before any pre-2014 Burkina survey is considered -- yes/no?
- **#812** (rider): the Senegal, Mali, Niger and Burkina EHCVM waves (2018-19, 2021-22) declare **no `strata` key in `data_info.yml` at all** (0 occurrences in all eight files -- absent from config, not declared-and-empty), so the allocation check cannot run there -- own issue, yes/no?
- **#706**: acquire and DVC-track the CILSS 1985-88 Basic Information Document (WB catalog 83/15551) -- yes/no?

## 5. Contradictions with CONTENTS.org / CLAUDE.md / the issue threads

1. **`Burkina_Faso/_/CONTENTS.org:465`** -- "All 4 passages are included; =Quantity= and =Produced= are NaN for passages 2--4." **Served table disagrees.** Warm `Burkina_Faso/var/food_acquired.parquet` (hash-stamped) and the wave parquet: visits 2/3/4 purchased rows carry `Quantity == 0` on **139,798 / 136,913 / 128,416** rows (0 nulls), all `u = 'Unknown'`; there are **no** `produced` rows for visits 2-4 and the p2-4 `autocons` **value** is not served. Cause: `build_transforms.py:102-103` `purchased_qty = (Quantity.fillna(0) - Produced.fillna(0)).clip(lower=0)` and `:135`. The sentence is true of the source files, false of what is served.
2. **Issue #864's mechanism** -- "`Price = Expenditure / NaN`" and option 1 "a group where Quantity is 100% null". Quantity is 0% null; the rows reach the `nan` bucket only because `Quantity_kg = 0 x NaN` (no kg factor for `Unknown`, `transformations.py:2960-2962`). Under `units='unitvalue'` the same rows are `Expenditure / 0 = inf`, the bucket the docstring calls "ALWAYS a data defect". 405,127 + visit-1's 44,125 zero-quantity rows = 449,252, within 0.2% of the reported 449,842.
3. **Issue #941, reviewer comment** -- "if `Country.formatting_functions` is a shared/cached dict, wave-level hooks leak". It is a plain `@property` that builds a new dict per access (`country.py:2007-2014`); the in-place `.update` at `:1067-1074` cannot leak. And "the wave `df_edit` hook ... did not run at all" is contradicted by the failing assertion itself: `assert 10798 == 10802` is `tests/test_ehcvm_7day.py:322` `len(inkind) == n` -- the served frame **had 10,798 `s='inkind'` rows**. Among the package's top-level modules and all EHCVM country code the only emitter of inkind rows is `ehcvm._inkind_rows` (`ehcvm.py:352`; the sweep's single other hit is `Mali/2014-15/_/mapping.py`, a non-EHCVM wave; the pre-#876 reshape emits purchased+produced only). So some version of the hook ran and the frame then lacked `Derivation`. Two things to read next: the CI-stack shadow test covered pandas/numpy/pyarrow but **not pyreadstat**; and Senegal 2018-19 / Burkina 2018-19, 2021-22 keep a legacy wave script `_/food_acquired.py` with a live Makefile pattern rule (`Senegal/_/Makefile:36-40`) that writes a hashless L2-wave parquet graded `legacy` -> trust-once (GH #809) -- its shape alone does not match the CI frame, so it is a candidate, not the answer.
4. **`Mali/_/data_scheme.yml:333-338`** -- "the EHCVM waves (2018-19, 2021-22) collect prices only at the region x milieu IHPC grain ... no grappe key": false for 2018-19 (`s05_co_mli2018.dta.dvc` present, 549 grappes), as `CONTENTS.org:880-890` already says. Same for Niger's data_scheme/script text vs `CONTENTS.org:802-823`.
5. **Issues #821/#823 "check the other six"** -- answered: `s05_co_<iso>2018.dta` ships in **seven of eight** EHCVM 2018 waves (Benin 111,148 rows/670 grappes, Senegal 63,312/595, Guinea-Bissau 40,101/449, Burkina_Faso 69,267/585, plus Mali, Niger, Togo in `Data1/`), identical 7-column schema (`grappe, vague, Existe, s05q01-s05q04`); **CotedIvoire 2018-19 does not** (only a `Menage/` dir); **no 2021-22 wave** has a section 5.
6. **`Guinea-Bissau/_/CONTENTS.org:183`** -- "a single two-stage national draw": the issue's numbers reproduce exactly on the warm sample parquet (76.8% between-region share, 0.65x-3.23x). Not a stale file -- the point of the issue -- but the gloss needs the measured counterpart beside it. Togo (66.5%) and Benin (31.4%) carry the same gap.

## Substitutions declared
GitNexus MCP did not connect; blast-radius sweeps were done with `grep`/`rg` over `lsms_library/` and `tests/` as CLAUDE.md's substitute table allows. No `Country()` build was run; all data evidence is `pd.read_parquet`/`pyarrow` on the warm cache plus lock-free `get_dataframe` reads of five `s05_co` files into this job's private root.

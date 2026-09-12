---
name: cross-country-features
description: Use this skill when assembling, auditing, or fixing a table across countries with ll.Feature(...) — the cross-country index assembly, the groupby-collapse semantics, verifying a feature is wired to the RIGHT source column, and the redteam-before-file discipline for Feature glitches. Read it before running a Feature() audit or registering a feature in index_info.
---

# Cross-country Features (`ll.Feature`)

`ll.Feature('table')()` stacks one harmonized table across every declaring country,
prepending a `country` index level. Hard-won lessons from a full cross-feature audit:

## 1. Verify the source column IS the variable (silent-corruption trap)
A feature can be wired to the WRONG column and still "build" fine, returning data
mislabelled as that feature. This recurred ~6× in `individual_education`:
- Mali 2014-15 `s01q06` = *father's roster line code*; real col `s02q23`.
- Mali 2017-18 `s1q07` = a *mother Oui/Non* roster question; real col `s2q06`.
- Guatemala `p10acp` = a *work-module person code*; real `p07b27a`.
- China `s01b10` = the *mother's occupation code* — and the CLSS survey has **no**
  categorical attainment variable at all (only continuous years-of-schooling).
- Tanzania `hh_c04` = *age started school*; real `hh_c07` (grade completed).
- Nigeria `s2q9` = literacy y/n; real `s2aq9`.

**Discipline:** never trust the wired column or the config comment. Read the Stata/SPSS
*variable label* (`pyreadstat` metadata, or `get_dataframe(convert_categoricals=True)`)
and cross-validate the values against `agey` / years-of-schooling. The "numeric
long-pole" features are almost always a *mis-identified column*, not genuinely numeric
data. The `.DCT` dictionaries for legacy fixed-width `.DAT` (GLSS1/2/3, CILSS) are
redundant — `get_dataframe` reads the `.DAT` directly, and `GRADE`/`s2q2` is usually the
shared LSMS letter ladder, so "legacy fixed-width" is NOT high-effort.

## 2. Cross-country index assembly
A feature must be registered in `data_info.yml › Index Info › index_info` (e.g.
`livestock: (t, i, animal)`); otherwise `_canonical_index_levels` is empty,
`feature.py:_harmonize_country_frame` can't name/reorder the levels, and the concat
collapses to a single UNNAMED object-tuple index — `groupby('country')` / `df.loc[c]`
then fail (GH #325/#326).

**Registration alone is not enough** — it needs the per-country level *names* to be
consistent. Heterogeneity blocks it (and naive registration can make it worse):
- Different names for the same level (`crop`→`j`, `plot`→`plot_id`) → **declare them in
  `Index Info › level_aliases`** (GH #569, landed for `crop_production`). This used to
  say "harmonize the per-country index names first", i.e. curate ~14 country configs;
  it is now a two-line config entry applied at assembly by `_rename_index_levels`.
  The `Country()` path keeps its native names, which is why
  `transformations._CROP_LEVELS` / `_PLOT_LEVELS` still exist.
  **The plot axis is `plot_id`** (@ligon, 2026-09-10) — the spelling all 23
  `plot_features` countries already declare — so `crop_production` is registered
  `(t, v, i, plot_id, j, u, condition, season)` and `plot` is the alias, not the
  target. Every `transformations.py` plot-grain transform (`harvest_kg`,
  `nitrogen_kg`, `seed_kg`, `yield_kg`, `fertilizer_rate`) EMITS `plot_id`
  whichever spelling it was handed. A shared axis NAME is not a shared
  VOCABULARY: `crop_production` keys Uganda's plots `{hhid}-{parcel}-{plot}`
  while `plot_features` keys them `{parcel}_{suffix}` — see
  `transformations._parcel_from_crop_plot` and the `on=` kwarg.
- A level only *some* countries carry (`condition`, `season`) → **declare a sentinel in
  `Index Info › missing_level_sentinels`**. `_align_to_canonical_levels` then, per
  country, promotes the level from a COLUMN if one exists (null-filled with the
  sentinel — Mali/Nigeria/Tanzania's `u`) and otherwise adds it as a constant sentinel.
  **Never `pd.NA`** — that is what `fabricate_missing_levels` (#506) does, and a null on
  a declared index level is a deferred silent deletion. Prefer the sentinel for anything
  new.
- Different granularity (one country `(i,t,v)`, another `(i,t,v,pid)`) → registration
  `groupby().first()`-drops the extra level and can INTRODUCE silent data loss
  (people_last7days: registering `(t,v,i)` would drop 87% of Malawi's varying per-`pid`
  counts). Check before registering.
- **Keep `v` in the registered index** unless the table genuinely has none:
  `country._compute_no_v_join` exempts any table whose `index_info` omits `v` from the
  API-time v-join, so a `v`-less registration silently strips `v` from every
  `Country()` frame of that table.
- **EthiopiaRHS** ships COARSE household-level versions of item-level tables (assets,
  livestock, crop_production); its divergent index breaks the concat — treat it as a
  separate table. It is still excluded from `crop_production` (no `plot_id`), by design.

`_harmonize_country_frame` reorders each country's index to canonical order by NAME
(GH #498) — even with no extra level to drop — so positionally-mislabelled indices
(correctly named `[i,t,v]` but order-scrambled under `[t,v,i]`) come out aligned.

**Which shape survives is `_select_kept_shape`, and it is stated, not modal** (GH #775).
The canonical shape wins if any frame has it; otherwise the most frames, then the most
rows, then the lexicographically smallest name tuple. The last three exist because
`Counter.most_common` broke ties by insertion order, so the same countries in a
different ARGUMENT ORDER returned different survivors. Consequence worth knowing: for a
registered feature the canonical shape wins **even as a minority of one** — which is the
point (a country that moves toward the canonical index must not be the one excluded),
but it means registering a canonical shape nobody-but-one country emits would exclude
everybody else. Measure before registering.

## 3. The collapse is `groupby().first()` — check losslessness
When an extra index level is dropped and the index is then non-unique, the frame
collapses via `first()`. Before trusting it:
- Are the collapsed rows IDENTICAL (benign) or distinct (loss)?
  `g.groupby(keys)[measures].nunique().max(axis=1) > 1`.
- For ADDITIVE measures it must SUM, not `first()`: GhanaLSS `food_acquired` has ~12
  per-visit recall rows (summed per CONTENTS.org); `first()` kept only 48% of Quantity.
  Fix sums Quantity/Expenditure and recomputes `Price = Expenditure/Quantity` (price is
  per-unit, NOT additive). See `_ADDITIVE_MEASURE_COLUMNS` / `_collapse_duplicate_index`.
- Dates / non-additive columns have no right sum — decide and document (interview_date
  keeps one household date; per-visit detail stays on `Country(...)`).

## 4. Redteam before filing a Feature "glitch"
Most apparent glitches are misdiagnoses (an audit ruled out ~150 of ~180). Rule out:
- **Stale cache** — re-run with `LSMS_NO_CACHE=1`; if it vanishes cold it was stale.
- **Warning ≠ crash** — `Feature` wraps each country in try/except and continues; a
  per-country `Failed to load …` (esp. the no-microdata countries Nepal/Armenia/
  Timor-Leste 2001/Guatemala) is expected, not a crash.
- **By-design** — GH#323 dup-collapse warnings, the Mover sentinel (GH#268),
  latin-1/DVC infra warnings.

But the redteam can OVER-dismiss (false negative): a collapse-dismissal that didn't
*prove* losslessness deserves a manual look (interview_date's visit-drop was wrongly
ruled benign). Eyeballing high-value features by hand still matters.

## 5. Feature mirrors the Country method
`Feature('bar')` is the country-axis analog of `Country('foo').bar` — same options,
same docstring, swapping *waves → countries* (GH #508). `Feature.__call__` forwards
`**kwargs` to the per-country method by signature introspection; `market=` widens the
canonical index by an `m` level (as `currency` does). Asterisks: the prepended
`country` level, Feature-only defaults (`currency='index'`), property-features
(`panel_ids`/`updated_ids` take no options).

## 6. Verifying a fix — the `.pth` split
- Config under `countries/{C}/_/…` → verify in a worktree with
  `LSMS_COUNTRIES_ROOT=<wt>/lsms_library/countries` (delegatable to worktree agents).
- Framework code AND `lsms_library/data_info.yml` are NOT under `countries/` → the
  `.pth` pins them to the MAIN checkout; verify there (or build a fresh venv).
  This split decides what can be parallel-delegated vs. done in the main checkout.

## 7. Workflow shape that worked
Scout inline → fan out. For N independent countries: profile (read-only) → harmonize
(one worktree agent per country) → review mappings for *semantic* correctness (not just
"no leftovers"; a leftover-check passes `K`=Koranic mapped to Pre-primary). Worktree-
agent commits live in `.git` (cherry-pick by SHA) even after the worktree is cleaned.

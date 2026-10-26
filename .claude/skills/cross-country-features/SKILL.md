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

**Discipline:** verify the wired column and config comment against the Stata/SPSS
*variable label* or questionnaire, then cross-check the response values against
`agey` / years-of-schooling. An approved metadata/header read can supply variable
labels; `get_dataframe(convert_categoricals=True)` decodes value labels instead.
Its `categories_only` dictionaries are value-label maps, not question wording,
and the current `ligonlibrary.from_dta` implementation reads rows before returning
them. Budget the actual header/full-row calls for the pinned reader. The "numeric
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
  `(t, v, i, plot_id, j, u, condition, season, production_obs)` and `plot` is the alias, not the
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
  drops the extra level and selects an observed row under section 3, which can lose detail
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

## 3. Check the observed-row selection and declared reducers
When dropping an extra index level leaves duplicate keys,
`feature._collapse_duplicate_index` selects the most complete observed row via
`country.most_complete_row`; ties retain original order (GH #871).
The old `groupby().first()` selected each column's first non-null value and
could fabricate a composite record. It is no longer the selection rule.
Before treating a collapse as lossless:
- Compare the complete source rows, including missing values and attributes.
  A conflict can destroy information even when an additive total is preserved.
- Consult `_ADDITIVE_MEASURE_COLUMNS`: `food_acquired` sums Quantity and
  Expenditure, while `assets` sums Value. A unit Price is recomputed from
  Expenditure/Quantity; it is not additive. Other columns still come from
  the selected observed row. Additive provenance keys are unioned.
- Dates and other non-additive answers have no automatic sum. Preserve their
  detail at Country grain and consult the documented grain policy before
  changing a reducer. The collapse audit and NA-key behavior remain relevant.

## 4. Redteam before filing a Feature "glitch"
Most apparent glitches are misdiagnoses (an audit ruled out ~150 of ~180). Rule out:
- **Stale cache** -- compare source-backed results in a new private verification root,
  using the add-feature skill's Step 7 and AGENTS.md cache limits.
  `LSMS_NO_CACHE=1` alone does not establish a cold build on every backend.
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

## 6. Verify the intended checkout and configuration
Use the approved interpreter, `cd` into the intended checkout, and set
`PYTHONPATH` to its absolute top level. Assert that the real path of
`lsms_library.__file__` lies under that exact checkout, using the import-identity
check in AGENTS.md. A path substring is insufficient.
- Config under `countries/{C}/_/...`: also set
  `LSMS_COUNTRIES_ROOT=<checkout>/lsms_library/countries`.
- Framework code and `lsms_library/data_info.yml`: `LSMS_COUNTRIES_ROOT` alone
  does not select them; the package import must resolve to the intended tree.
- `python -c` and `python -m` put cwd ahead of `PYTHONPATH`; a script invocation
  puts the script's directory there. Always check cwd and import identity.
The former pinned-`.pth`/main-checkout advice predates GH #957. Older environments
may still carry that file, which makes the identity assertion necessary.
On Savio, mount the approved image with `bin/savio_venv.sh mount` from the local
checkout and follow AGENTS.md; do not build a fresh venv or use the writable master.

## 7. Workflow shape that worked
Scout inline → fan out. For N independent countries: profile (read-only) → harmonize
(one worktree agent per country) → review mappings for *semantic* correctness (not just
"no leftovers"; a leftover-check passes `K`=Koranic mapped to Pre-primary). Worktree-
agent commits live in `.git` (cherry-pick by SHA) even after the worktree is cleaned.

## Guidance update, 2026-10-26
Corrected the obsolete `first()` and pinned-`.pth` advice against GH #871/#957
and current `feature.py` / `country.py`. Earlier wording remains in Git history.

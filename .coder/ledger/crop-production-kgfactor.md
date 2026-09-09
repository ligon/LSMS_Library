# Prior-Art Ledger — crop-production-kgfactor

> Per-task ledger. Adds an optional canonical column `KgFactor` to
> `crop_production` and makes `transformations.harvest_kg` build each row's
> kg factor by a LAYERED procedure that prefers the survey's own reported
> weight. Design fixed by the project owner (2026-09-09) and refined the same
> day; this ledger records the prior art it leans on, not a re-design.

**Search tier used:** ripgrep + git floor. `gitnexus` MCP did not connect this
session (`CONNECTION_CLOSED`), there is no `.gitnexus/` index on this mirror,
and `npx gitnexus` does not complete on Lustre — the substitution CLAUDE.md
("Code intelligence: GitNexus is OPTIONAL") sanctions. Blast radius for
`harvest_kg` swept with `rg -n harvest_kg lsms_library/ tests/ docs/`:
one in-library caller (`yield_kg`, `transformations.py:1820`), zero test
callers, zero country scripts.

## §1 Task, restated

`crop_production` is an item-level registered table at the grain
`(t, i, plot, j, u, condition, season)` (`v` joined at API time). Its
`Quantity` is in the native unit `u`. `transformations.harvest_kg` is a
standalone derived measure — NOT a registered or auto-derived table — that
sums each plot-crop's harvest in kilograms.

Today it converts through ONE channel: the shared unit→kg machinery
(`_kg_factor_series` → `_get_kg_factors`), so a row whose `u` is missing or
unrecognised contributes nothing. Measured on Uganda (warm, 2026-09-09):
28 147 of 133 683 rows (21.1%) convert; 105 536 do not.

Several instruments write the row's weight down. Uganda's UNPS ships
`a5?q6d`, "conversion factor into kg", in every wave, and its 2018-19 season-A
harvest side ships that factor 100%-populated while shipping NO unit code at
all (7 153 rows at the `u='Unknown'` sentinel). Malawi recovers per-row
factors from free-text unit detail (`malawi.py:_extract_kg_conversion`);
Ethiopia and Nigeria carry regional conversion files.

This task adds the canonical carrier for that number and teaches `harvest_kg`
to prefer it. The country wiring (Uganda `a5?q6d` → `KgFactor`) is a SEPARATE
branch and is out of scope here.

## §2 Existing machinery (this task's area)

The antecedent is `food_acquired`'s `Quantity_kg` and the "exact per-row beats
inferred" precedence the food path already implements. Lines, not names:

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `harvest_kg` | `transformations.py:1668` | `Quantity × _kg_factor_series` → `groupby(t,i,plot,j).sum()` | no test names it | **extend** (the task) |
| `yield_kg` | `transformations.py:1820` | sole in-library caller: `harvest_kg(...).reset_index()` then joins area | no | **must not break** — no new column on the returned frame |
| `_kg_factor_series` | `transformations.py:1645` | per-row factor from `_get_kg_factors`; NaN where unknown; handles `u` as level or column | indirectly | **reuse verbatim** — layer (c) |
| `_get_kg_factors` | `transformations.py:1004` | `KNOWN_METRIC` → `_parse_explicit_metric` → price-ratio inference | indirectly | **reuse as-is, do NOT fork** — see §5/§6 |
| `conversion_to_kgs` | `transformations.py:800` | price-ratio inference; `price=`/`quantity=`/`index=`/`unit_col=` are **parameters** | `test_conversion.py` | **not called directly** |
| `conversion_to_kgs` — exact-beats-inferred | `transformations.py:866-872` | `v['Kgs'] = v['Kgs'].where(v['Kgs'].notna(), v['Quantity_kg'])`, then `v_infer = v[v['Quantity_kg'].isna()]` — exact rows anchor the per-kg baseline but are EXCLUDED from what the inference decides | `test_conversion.py` | **the antecedent for precedence** |
| `_apply_kg_conversion` | `transformations.py:1119` | `v['Quantity_kg'] = v['Quantity_kg'].where(notna, factor_kg)` — exact wins, factor fills | indirectly | **antecedent, with a defect not to copy (§4)** |
| `food_quantities_from_acquired` | `transformations.py:1306-1311` | `converted = v['Quantity_kg'].notna()`; `np.where(converted, qty_kg, qty_native)`; `u_new = np.where(converted, 'kg', u_native)` | `test_food_quantities*` | **antecedent for the per-row branch** |
| `_drop_unpriceable` | `transformations.py:116-242` | splits a loss by CAUSE, records the tally on `result.attrs['price_rows_dropped']` (241-242), warns ONCE above a threshold | `test_unpriceable*` | **the disclosure pattern to copy (attrs tally)** |
| `food_prices` attrs re-attach | `transformations.py:1480-1485` | groupby drops `attrs`; the tally is explicitly re-stashed afterwards | yes | **reuse the idiom** — `harvest_kg` also groups |
| `_as_float` | `transformations.py:111` | `pd.NA`-safe Series → float64 ndarray | indirectly | **reuse** |
| `_with_u_in_index` | `transformations.py:1630` | promote `u` from column to level, report whether it promoted | indirectly | **reuse** |
| `PRICE_LOSS_WARN_THRESHOLD` | `transformations.py:66` | module constant for a measured threshold, documented in place | — | **precedent for `_SURVEY_MEDIAN_MIN_REPORTS`** |
| `crop_production.condition` | `data_info.yml:450-506` | canonical block: an optional/index declaration carrying a long `note:` | `test_schema_consistency.py` | **the shape to copy for `KgFactor`** |
| `RecallWindow` | `data_info.yml:567-576` | the `optional: true` + `note:` precedent for a column only some countries carry | `test_schema_consistency.py` | **reuse the shape** |
| `malawi.py:_extract_kg_conversion` | `countries/Malawi/_/malawi.py` | recovers a per-row kg factor from free-text unit detail | — | **out of scope** (country branch), cited as motivation |

### What food does NOT have, and what layer (b) adds

The food antecedent implements exactly ONE preference: an *exact per-row
kilogram* (`Quantity_kg`, supplied by a country script from a survey
conversion TABLE — Nigeria `s10bq2_cvn`, Malawi `cfactor`) beats an inferred
per-unit factor. It has no notion of a *reported factor with gaps*, because a
conversion table has no gaps: it either covers a row or it does not, and where
it does not there is nothing to take a median of.

Crops do. Uganda's `a5?q6d` is reported **per harvest record**, by the
enumerator, and is populated for some rows and not others within the same
wave and unit. That is what layer (b) — the median of REPORTED factors for
the same `(u, condition)` within the same country-wave — is for: the survey's
own weights filling the survey's own gaps. No food code path does this, and
this ledger records it as **new**, not as a reinvention of `conversion_to_kgs`
(which infers from PRICE RATIOS across units, never from reported factors).

## §3 Definitions & conventions in force

Cited, not paraphrased.

- Canonical schema is `lsms_library/data_info.yml`; tests read it and never
  hardcode schema rules — CLAUDE.md "Canonical Schema";
  `tests/test_schema_consistency.py:24-36`.
- `optional: true` is **country-grain**, not wave-grain — CLAUDE.md, the
  `_assert_built_required_columns` / null-read-guard bullets. `KgFactor` is
  optional for exactly that reason: most countries will never carry it.
- `crop_production` columns are REPORTED values only; `harvest_kg` / yield /
  main-crop are transformations — `Uganda/_/data_scheme.yml:166-168`,
  `Mali/_/data_scheme.yml:158`, `Niger/_/data_scheme.yml:161`,
  `Tanzania/_/data_scheme.yml:175`, `Togo/_/data_scheme.yml:158`. Five
  countries say it independently; `KgFactor` must not break it, which is why
  the served column carries **reported, never constructed** values.
- Core never aggregates silently — CLAUDE.md "Grain Collapse". `harvest_kg`
  is an ANALYST-side transform, not core, so its sum is licensed; the
  constraint that binds here is that nothing this task adds may change what
  `Country.crop_production()` serves.
- `data_info.yml` is read-path: `Country._table_cache_hash` /
  `Wave._input_hash` hash the country `_/data_scheme.yml`, `_/Makefile`,
  `_/*.org` and the wave/country modules — **not** the canonical
  `lsms_library/data_info.yml`, and not `transformations.py`
  (`_build_registry._EXCLUDED_CALLABLES` covers read-path code). Verified
  empirically, §Phase 3.
- Disclosure convention: a per-call tally on `result.attrs[...]`, re-attached
  after any groupby — `transformations.py:241-242`, `1480-1485`.

## §4 Invariants & assumptions

- **`harvest_kg`'s output must be byte-identical for any frame with no
  `KgFactor` column.** Layers (a) and (b) are then empty and only (c)/(d) act.
  Pinned two ways: a synthetic test, and the Uganda warm digest
  (`hk_rows=23766`, `hk_sum=4963468.795000811`, hash digest
  `dfec6823…`) measured on the pre-change tree.
- **No new column on the returned frame.** `yield_kg:1820` does
  `harvest_kg(...).reset_index()` and then arithmetic; a provenance column
  would ride into that. Provenance therefore goes to (i) `attrs` and (ii) a
  companion public function — never onto `harvest_kg`'s result.
- **Provenance is per INPUT row, not per contributing row.** `harvest_kg`
  drops zero/NaN products AFTER conversion, so a `reported` row with
  `Quantity=0` is counted `reported` and still contributes nothing. The
  counts sum to `len(crop_production)`.
- **`groupby(..., dropna=False)` for layer (b).** The default `dropna=True`
  deletes rows whose group key is NA (CLAUDE.md "Grain Collapse" §3b, the
  known-open item), and `condition`/`u` can be NA in a raw frame. A row whose
  key is NA must fall through to layer (c), not vanish.
- **An invalid reported factor (≤ 0, ±inf, NaN) counts for nothing** — not
  toward the layer-(b) median, not toward its N, not as `reported`.
- **`_apply_kg_conversion`'s defect must NOT be copied.** It overwrites
  `Quantity_kg` in place (`transformations.py:1119-1128`), so after that call
  an exact survey value and an inferred one share ONE column name and
  `food_quantities_from_acquired` tags both `u='kg'`. The provenance is
  destroyed at the moment of the fill. The new path keeps the resolved factor
  and its provenance in two aligned Series, and never writes a constructed
  value back into `KgFactor`.
- **`u='Unknown'` is a live sentinel in this table.** Uganda uses it wherever
  a wave records no harvest unit label (`Uganda/_/data_scheme.yml:140-142`),
  and 2018-19 season A is 7 153 such rows. See §6.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| layer (a) reported factor | **new** (thin) | no antecedent: food has no reported per-row FACTOR, only an exact per-row kg |
| layer (b) survey median | **new** | §2 last block — nothing in the library takes a median of reported factors |
| layer (c) inferred factor | **reuse `_kg_factor_series` verbatim** | it is the shared machinery `food_quantities_from_acquired` uses; forking it would fork the corpus's factors |
| precedence exact→inferred | **reuse the pattern** at `transformations.py:866-872` / `1306-1311` | same rule, different carrier (factor vs product) |
| disclosure channel | **reuse `attrs` tally** (`241-242`, `1480-1485`) | least invasive for `yield_kg`; no new warning class was asked for |
| per-row provenance | **new public companion** `harvest_kg_factors` | mkdocstrings auto-documents public names in `docs/api/transformations.md` (`filters: ["!^_"]`), so no registry edit; `api_surface_audit.check_kwargs` only inspects `country.py` data methods |
| feeding `Value_sold`/`Quantity_sold` to the inference | **NOT done** — reported, see §6 | `_get_kg_factors` is hardwired; the brief says do not fork it |

## §6 Open questions for the human

1. **Generalising the price-ratio inference to crop sales.** `conversion_to_kgs`
   is column-generic (`price=['Expenditure']`, `quantity='Quantity'`,
   `index=`, `unit_col=` are all parameters, `transformations.py:800`), but
   `_get_kg_factors` is **hardwired** at two points: the presence gate
   `if 'Expenditure' in df.columns and 'Quantity' in df.columns:`
   (`transformations.py:1045`) and the defaulted call
   `conversion_to_kgs(df, index=group_levels)` (`1051`). `crop_production`
   carries `Quantity_sold` / `Value_sold` and no `Expenditure`, so the
   price-ratio layer **never fires for crops today** — layer (c) is
   `KNOWN_METRIC` + the explicit-metric label parser only. Per the brief this
   was NOT forked. A generalisation would need, minimally:
   - `price=` / `quantity=` parameters on `_get_kg_factors`, threaded to both
     the gate and the call;
   - a **different baseline grouping**. `conversion_to_kgs` defaults to
     `index=['t','m','i']` — household-level, pooling across the item `j`.
     For food that is defensible; for crops it is not: maize and coffee
     differ by orders of magnitude in price per kg, so a crop generalisation
     needs `j` in the `pkg` baseline group or the inferred factors are noise;
   - the assumption that `Quantity_sold` is denominated in the SAME `u` as
     `Quantity` (it shares the row's index level, so this looks true for
     Uganda, but it is an assumption no code checks);
   - `Quantity_kg` handling (`866-872`) is a no-op for crops — the column is
     absent — so that branch needs no change.
2. **RESOLVED — layer (b) now REFUSES the missing-unit sentinel.** `Unknown`
   is not a unit, it is the absence of one, so a median over such rows mixes
   80 kg sacks with 5 kg baskets and filling other such rows with it would
   fabricate weights. Excluded from BOTH the median pool and the fill
   targets, and from the disagreement audit; a unit-less row gets kilograms
   from its own reported `KgFactor` (layer a) or from nothing.

   **The sentinel constant is declared in `transformations.py`, and two
   premises behind that instruction did not hold on this tree — reported, not
   worked around** (CLAUDE.md: a finding that contradicts documentation is a
   result to quote). Checked at 28f9243f:
   - `data_info.yml` *does* carry an `Unknown` sentinel, but it belongs to
     the EDUCATION vocabulary (`Columns.household_roster.Education`,
     lines 296/321) — there is **no** machine-readable declaration of the `u`
     sentinel anywhere.
   - There is **no `niger.py:U_NA`**; `rg U_NA --include='*.py'` is empty
     across the repo. Niger's missing-unit sentinel is the literal string
     `Manquant` (`niger.py:602`, `_COMMUNITY_MISSING_UNITS` at `:1183`) and
     has **not** been relabelled onto `Unknown`.

   So `U_UNKNOWN = 'Unknown'` plus `_U_SENTINELS = {'unknown', 'manquant'}`
   live in `transformations.py`. `Manquant` is included deliberately rather
   than assumed away: leaving it out would leave a known hole in the guard
   for Niger the moment that country wires `KgFactor`. Both should be deleted
   in favour of a single canonical declaration when GH #847 lands — that is
   the unification this entry is asking for.
3. **`SURVEY_MEDIAN_MIN_REPORTS = 5` is DECLARED, not measured.** The
   `Quantity_kg` antecedent needed no such threshold — a conversion table has
   no gaps, so there was never a median to license. N is this design's new
   tunable, and no country carries `KgFactor` yet, so there is nothing in the
   corpus to measure it against. The wiring branch is where it gets a
   measured value (Uganda's per-`(u, condition)` report counts are the
   obvious evidence base). Until then it is a `min_reports=` kwarg and a
   documented constant, deliberately not a tuned number pretending otherwise.
4. **The provenance counts POOL across countries.** `attrs` carries
   `{layer: n}` over all input rows, so on a `Feature('crop_production')`
   frame spanning several countries the tally is a total, not coverage. The
   per-row `harvest_kg_factors` frame answers the per-country question with
   one `groupby('country')`; noted in that docstring. Whether the `attrs`
   tally should itself be broken out per country is a question for the first
   multi-country consumer, not something to guess at now.
5. Should `KgFactor` disagreement above the 10% band be a WARNING (the
   `UnpriceableRowsWarning` precedent) rather than only an `attrs` entry?
   Deliberately not added — the refinement asked for disclosure, and a
   warning nobody reads is how GH #323 survived its first fix.

---
### Phase 3 — verification (fill at task end)

Anchored on this ledger only; anything not tied to an entry is out of scope.

- `Columns.crop_production.KgFactor` (`data_info.yml`) — **OK (anchored on
  §2/§3)**. Copies the `RecallWindow` `optional: true` + `note:` shape; the
  note says "reported, never constructed", which is what keeps the five
  configs quoted in §3 ("REPORTED values only") true.
- `harvest_kg_factors` — **OK (anchored on §5)**. Layer (c) is
  `_kg_factor_series` called verbatim, so the shared factors are not forked;
  layers (a)/(b) are new for the reason §2's last block gives (food's exact
  values come from conversion TABLES, which have no gaps to take a median
  of). Not a reinvention of `conversion_to_kgs` — that infers from PRICE
  RATIOS across units, never from reported factors.
- `_survey_median_factors` — **OK (anchored on §4)**. `dropna=False` per the
  §4 invariant; `count` counts USABLE reports because `_valid_factor` runs
  first; the fine→coarse fallback is `(u, condition)` → `u`, both inside the
  country-wave.
- `_valid_factor` — **was REINVENTION, now OK (§2)**. First cut inlined
  `pd.to_numeric(...).to_numpy(dtype='float64', na_value=np.nan)`, which is
  exactly `_as_float` (`transformations.py:111`, listed in §2 as *reuse*).
  Corrected to call `_as_float`; the finite/`> 0` filter on top is the new
  part.
- `_disagreement` — **OK (§5)**. Denominator is rows where BOTH layers have a
  usable factor; the gap is relative to the *reported* factor, because the
  question is how wrong the library is where the survey answered.
- `_level_or_column` — **OK (§4)**. Index-level-or-column access, the same
  ambiguity `_with_u_in_index` already handles for `u`; kept separate because
  that function *promotes* and this one only *reads*.
- `harvest_kg` — **OK (anchored on §4)**. Returned frame still carries only
  `Harvest_kg` (test pins it, protecting `yield_kg:1820`); tallies re-stashed
  after the groupby per the §2 `food_prices` idiom. The no-`KgFactor` path is
  bit-identical, proved twice: structurally against the old algorithm in
  `tests/test_crop_kg_factor.py`, and on Uganda's warm build — 23 766 rows,
  sum 4 963 468.795000811, `hash_pandas_object` digest `dfec6823…` before and
  after.
- **No cache hash moved** (§3, "data_info.yml is read-path"):
  `Country(c)._table_cache_hash('crop_production', c.waves)` byte-identical
  for Uganda / Ethiopia / Malawi between the main checkout at 28f9243f and
  this worktree. Records in
  `/global/scratch/fsa/fc_jevons/ligon/tmp/kgfactor_scratch/`.
- **Nothing in the stop-list was touched.** `git diff --name-only` against
  28f9243f: `.coder/ledger/crop-production-kgfactor.md`,
  `lsms_library/data_info.yml`, `lsms_library/transformations.py`,
  `tests/test_crop_kg_factor.py`. No country config or script, no
  `country.py` / `feature.py` / `local_tools.py` / `build_transforms.py`, no
  change to `food_acquired`'s `Quantity_kg` convention, no test weakened,
  no `pyproject.toml`.

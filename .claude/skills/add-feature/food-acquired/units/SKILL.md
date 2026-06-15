---
name: food-acquired-units
description: This skill should be used when adding a new wave or country to food_acquired and the unit (`u`) index needs to be clean native labels — decoding numeric/coded units to names, borrowing labels across sibling surveys, and avoiding the gotchas that leak raw codes. It is distinct from kg conversion (covered by the parent food-acquired skill, Step 3) — it is about making `u` itself a clean set of native unit labels before any kg factor is applied.
---

# Decode & clean food_acquired unit labels (`u`)

The `u` index level of `food_acquired` must be a clean set of **native unit
labels** — `Heap`, `Pail (Small)`, `American Tin`, `Bunch`, `kg`, `Value` —
with **no leaked numeric codes** (`19`, `4A`), **no item names** (`Banana`),
and **no case/spelling duplicates** (`PIECE` vs `Piece`). This is a separate
job from converting units to kilograms (parent skill, Step 3): you decode and
clean the *label* first; the kg factor is applied to a clean `u` afterward.

The #223 "Layer 2" effort drove every existing country to this state. When
you add a new wave or country, repeat the checklist below — the same handful
of gotchas recur every time.

## The audit: is `u` clean?

Run this on the built `food_acquired` before you consider the unit work done:

```python
from lsms_library import diagnostics
diagnostics.food_acquired_u_code_leaks('YourCountry')   # codes still in u
```

`food_acquired_u_code_leaks` flags any `u` value matching `^\s*\d` (a leaked
raw code). The regression net is `tests/test_u_code_leak.py` — add the
country there once it is clean (and read its header comment: it is the
running ledger of every country's leak count and which residuals were
*accepted*, see below).

A leak count > 0 means the decode below is incomplete (or silently dead — see
the gotchas).

## Decode toolkit — sources in priority order

When `u` comes through as numeric codes (or coded strings), find the label
from the first source that has it:

1. **The source file's own value labels.** Stata `.dta` / SPSS `.sav` carry
   value labels. Read with `get_dataframe(fn, convert_categoricals=True)` and
   the codes resolve to names for free. *Check this first* — many "code leaks"
   are just `convert_categoricals` not being set, or a CSV export that
   dropped the labels (the `.dta` of the same survey often still has them —
   Nigeria GHS).

2. **A sibling survey in the same program.** If this survey's `.dta` lost its
   labels but uses a *standardized* coding shared across a survey family,
   borrow the labels from a sibling that kept them. The EHCVM family does
   this: `lsms_library/categorical_mapping/ehcvm_units.org` (`#+name:
   ehcvm_units`) is the union of `s07bq03b` value labels across all 8 EHCVM
   countries, applied via `mappings: ['ehcvm_units','Code','Preferred Label']`
   in `data_info.yml`. Regenerate it with
   `slurm_logs/build_ehcvm_unit_codebook.py` if a new sibling is added.

3. **The questionnaire / codebook.** Survey documentation (Excel `Unites`
   sheets, IHPS "CODES FOR UNIT" PDF pages) lists the code→label map. Pull it
   from `{Country}/{wave}/Documentation/` via DVC (`.venv/bin/dvc pull …`) and
   read with `pdftotext` or `pd.read_excel`. **Watch the scheme:** the
   questionnaire's generic list may use *different numbering* than the data
   (EHCVM's `Unites` sheet is 1–57; the data codes are 100–700 — they do not
   align, so the questionnaire could not decode the data codes there).

4. **A country-level `units` / `unit_labels.org` table.** Some countries ship
   a hand-curated `Code | Label` table (`{Country}/{wave}/_/categorical_mapping.org`
   `#+name: units`, or GhanaLSS's standalone `_/unit_labels.org`). Apply via
   `get_categorical_mapping` or the country `food_acquired.py`.

**Never fabricate a label.** If no source defines a code, leave it as the code
and *document it as an accepted residual* (see below) — do not guess. Wrong
unit labels silently corrupt downstream prices and kg aggregates.

## Accepted residuals

Some codes are genuinely undecodable from any available source: out-of-range
data-entry codes (Malawi's 19), or item-specific local units whose names no
codebook records (Togo `659/660/662` = banana/meat/yam portions; Burkina
`254/255` = meat portions, size in a separate column). The convention:

- Leave them as codes (do **not** invent a label).
- Document them in `{Country}/_/CONTENTS.org` — what they are, why undecodable.
- Note them in the `tests/test_u_code_leak.py` header ledger as ACCEPTED.

They are not a functional problem: per the `units=` modes (parent skill,
*Downstream API*), these rows still contribute to `food_expenditures`
(unit-independent) and `food_prices(units='unitvalue')` (per-native-unit);
only the kg aggregate (`food_quantities(units='kgs')` → `u='kg'`) excludes
them.

## Gotchas that actually bit us

- **Float-stringification (the #1 cause of silent leaks).** A numeric code
  column reaches `myvars` as a float (`1.0`), but dict/table keys are string
  codes (`'1'`), so `.map`/`.replace` misses *every* row. `format_id` is
  auto-applied to `idxvars` but **NOT** to `myvars`. Normalize before lookup:
  `u=('s8hq13', lambda x: unitsd.get(format_id(x), pd.NA))` — the same pattern
  used for the `j` (item) mapping. (GhanaLSS #348.)

- **`get_categorical_mapping(tablename='units')` returns `{}` without a value
  column.** It defaults `idxvars='Code'` but takes the value column from
  `**kwargs`; with none, `df_data_grabber` yields a column-less frame → empty
  dict → the decode is silently dead and *all* codes leak. Pass the value
  column: `get_categorical_mapping(tablename='units', Label='Label')`.
  (GhanaLSS #348.)

- **In-table `#` comments break org tables.** A `#` line *between* `#+name: u`
  and the table, or *inside* the table rows, severs the table from its name
  and `df_from_orgfile` silently returns nothing. Put comments **above**
  `#+name:` or after the whole table — never between or inside.

- **Reserved `u` sentinels.** `kg` and `Value` are reserved
  (`_RESERVED_U_SENTINELS`). A country `#+name:u` table must not remap them
  (e.g. tagging `Kg` so it collides with the `kg` conversion sentinel — #361).
  `food_quantities` / `food_prices` are sentinel-protected; don't fight it.

- **`u` categorical mappings are additive.** `u` is in
  `_ADDITIVE_CATEGORICAL_TABLES`, so a country `#+name:u` table is *row-unioned*
  with the global `categorical_mapping/u.org` (kg-variant → `Kg`) rather than
  replacing it. Add only the country-specific rows; don't re-declare the
  global kg variants.

- **Metric-string magnitude safety.** A "unit" like `10Kgs` is a *quantity in
  kg*, not a unit name. Relabeling it to `Kg` without scaling makes the
  quantity wrong by 10×. *Convert* it: scale `Quantity` by the factor and tag
  `u='kg'` (Malawi `_metric_kg_factor`). The greedy-token guard must reject
  non-metric look-alikes (`10Giraffes` ≠ 0.01). Item-*independent* metric
  magnitudes convert cleanly; item-*dependent* containers (`sachet`, `bottle`)
  need a per-item factor and should stay native if you don't have one.

- **Case / spelling dupes.** Title-case and fold variants (`PIECE`/`Piece`,
  `american tin`/`American Tin`) via a `spellings` inverse dict or a
  `Preferred Label` table, so they collapse to one `u`.

## Worked references

- **`.dta` value labels present** → Nigeria `_/units.py`, `categorical_mapping.org` (#375).
- **Sibling-borrow codebook** → `categorical_mapping/ehcvm_units.org`, Togo/Burkina (#373).
- **Questionnaire decode** → Malawi `2013-14/Documentation/IHPS…PDF`, `_/categorical_mapping.org` `#+name:u` (#383/#399).
- **Country `units` table + float gotcha** → GhanaLSS `1991-92/_/food_acquired.py` (#348).
- **Free-text cleanup + magnitude safety** → Malawi `_/malawi.py` `_clean_freetext_unit` / `_metric_kg_factor` (#382/#391).

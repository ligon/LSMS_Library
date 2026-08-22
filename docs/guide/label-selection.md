# Label selection (`labels=`)

Surveys often classify the same thing at two resolutions: a canonical one the
library harmonizes across countries, and a finer one the producer actually
recorded. `labels=` lets a caller ask for the finer one at query time, without
the canonical vocabulary ever changing.

## The problem it exists for

Iraq's IHSES 2006-07 stratifies every household into a settlement tier. The
library delivers the canonical binary. Measured, cold, on the shipped data
(17,822 households; `strata` is the raw `xstrat`, whose suffix is the tier):

| raw tier | households | delivered `Rural` |
|---|---:|---|
| `urban center` | 5,413 | Urban |
| `other urban` | 5,736 | Urban |
| `al-karekh` (Baghdad) | 314 | Urban |
| `al-rasafah` (Baghdad) | 318 | Urban |
| `al-sader` (Baghdad) | 321 | Urban |
| `extra` | 92 | Urban |
| `rural` | 5,628 | Rural |

Seven tiers become two values; 12,194 households land in one bucket. The
canonical fold is right — `Rural ∈ {Urban, Rural}` is what makes the column
comparable across 47 countries — but the detail should not be *unreachable*.

The same shape recurs in Ethiopia ESS2/ESS3 (large town / small town),
Mali (Bamako / autre urbain / rural), Niger (Communauté Urbaine / urbain /
rural) and Kazakhstan 1996 — see the corpus-wide audit in
[#682], class A.

And a shared *vocabulary* would not fix it. A later corpus sweep ([#704]) found
that what defeats cross-country settlement harmonisation is variation in the
**entity being classified** — locality vs. enumeration area vs. commune — not
variation in the population threshold. Two surveys can agree on the word "town"
and still be classifying different objects. That is an argument for exactly
this design: a per-country label column, selected by name at query time, with
no pretence that one country's ladder means the same thing as another's. The
canonical binary stays the only cross-country claim the library makes.

`labels=` is the query-time escape hatch: the canonical value stays canonical,
and a caller who wants the tier asks for it.

```python
import lsms_library as ll

c = ll.Country('Someland')
c.sample()['Rural'].unique()
# array(['Urban', 'Rural'], dtype=object)                      canonical

c.sample(labels={'Rural': 'Settlement'})['Rural'].unique()
# array(['City', 'Large Town', 'Medium Town', 'Small Town',
#        'Large Village', 'Small Village', 'Other'], dtype=object)
```

**No country is wired for this yet.** This page documents the core mechanism
and how to curate a table for it; the per-country wiring is separate work. Note
in particular that Iraq's `Rural` currently comes from an inline `mapping:`
dict in its wave `data_info.yml`, not from a `categorical_mapping.org` table —
using this mechanism there means moving that decode into a table (see
*Curating a table* below).

## The two forms

| form | targets | applied by |
|---|---|---|
| `labels='Aggregate'` (scalar) | the `j` index level, and **only** `j` | `Country._relabel_j`, on the finished frame |
| `labels={'Rural': 'Settlement'}` (dict) | any column or index level, named explicitly | `Country._apply_categorical_mappings`, at the raw code |

A scalar keeps its historical meaning byte for byte: it renames `j` from
`Preferred Label` to `<variant> Label` and, for `food_expenditures` /
`food_quantities`, re-aggregates. It never touches a column, even on a table
that has a mapped one.

A dict names its targets. Keys match columns and index levels
case-insensitively. The variant resolves to the `'<variant> Label'` column of
the same-named `categorical_mapping` table, falling back to a bare
`'<variant>'` — the same rule the scalar form uses.

The key `'j'` in a dict is routed to the scalar behaviour, so
`labels={'j': 'Aggregate'}` and `labels='Aggregate'` are one request and there
is exactly one mechanism per face. The two compose:
`labels={'j': 'Aggregate', 'Rural': 'Settlement'}`.

## Which decode path this extends — and which three it does not

A raw survey code becomes a label by one of **four** routes. `labels=` extends
only the first.

1. **Auto-dispatch on a name match** — a `categorical_mapping` table whose name
   matches a column or index level (case-insensitively) and which has a
   `Preferred Label` column. Applied in `Country._apply_categorical_mappings`.
   **This is the one `labels=` extends.**
2. **An explicit `mappings:` key** in a wave's `data_info.yml`, for a table
   whose name does *not* match the target (`harmonize_food` → `j`). Rare — two
   occurrences in the corpus.
3. **Direct Python consumption** — `tools.code_label_map('rural', …)` in a
   wave's `mapping.py`, consumed by that wave's own formatter. This is where
   GhanaLSS GLSS3/GLSS4 `Rural` actually comes from
   (`GhanaLSS/1991-92/_/mapping.py`, `1998-99/_/mapping.py`).
4. **An inline `mapping:` dict** under a `myvars` entry in a wave's
   `data_info.yml` — Iraq's `Rural`, Malawi's housing columns.

Routes 2–4 are untouched by this change and remain the right tool where they
are used. A country that wants query-time selection has to move the decode to
route 1.

**Scope: route 1 is country ∪ global, never wave.**
`_apply_categorical_mappings` reads `Country.categorical_mapping`, which merges
`lsms_library/categorical_mapping/*.org` with the country's
`_/categorical_mapping.org` — and never opens a wave directory. A table in
`{country}/{wave}/_/categorical_mapping.org` is visible to
`Wave.categorical_mapping` but is **not** auto-applied to a `Country(...)`
read, with or without `labels=`.

## Curating a table for it

Add a label column to the country's `categorical_mapping.org` table. `Code`
stays first, `Preferred Label` stays canonical:

```org
#+name: Rural
| Code | Preferred Label | Settlement Label |
|------+-----------------+------------------|
|    1 | Urban           | City             |
|    2 | Urban           | Large Town       |
|    3 | Urban           | Medium Town      |
|    4 | Rural           | Small Town       |
|    5 | Rural           | Large Village    |
|    6 | Rural           | Small Village    |
|    7 | Rural           | Other            |
```

Nothing else is needed: the auto-dispatch already applies a table whose name
matches a column or index level. Default reads are unaffected — they still
resolve `Code → Preferred Label`.

Two rules about that table:

- **Keep the code column first.** The key column is "the first column that is
  neither `Preferred Label` nor the requested variant". Reordering it so a
  label column comes first makes the *default* mapping key on the wrong column
  and silently decode nothing. This is pre-existing and unguarded; it is pinned
  by `tests/test_label_selection.py::test_source_cols_ordering_is_the_key_column`.
- **A `Code`-keyed table belongs at country or wave scope, never global.** Codes
  are survey-specific — Iraq's `1` is not Ghana's `1` — and
  `_augment_numeric_code_keys` additionally registers `'1'` / `'1.0'`, widening
  the blast radius. The global tables under `lsms_library/categorical_mapping/`
  are label-keyed (`Alternate Spelling`, `Original Label`) for this reason. The
  one exception is `ehcvm_units.org`, defensible because EHCVM is a single
  multi-country instrument with one shared code list. No assertion enforces
  this yet.

## Errors

| situation | raises | `Feature` behaviour |
|---|---|---|
| country curates no such mapping table, or no such label column | `LabelUnavailableError` (a `KeyError` subclass) | drops the country, one aggregated warning, `df.attrs['labels_unavailable']` |
| the result has no such column or index level | `LabelUnavailableError` | **same** — drops the country |
| table exists but has no `Preferred Label`, or no key column left | plain `KeyError` | surfaces as a per-country "Failed to load" warning |
| `labels=` is neither a `str` nor a dict, or a dict value is not a `str` | `TypeError` | as above |

The distinction that matters is **absence vs. defect**. *Missing curation* — a
country that curates no such label column, or has no such column at all — is a
normal state of the corpus, and `Feature` degrades over it. A *malformed table*
is a defect in curated config, and stays loud.

The first two rows are deliberately one case. A caller cannot tell them apart:
`Rural` is not a required column of `sample` — "many countries' sample table
carries only (v, weight, strata) and has no urban/rural indicator at all"
(`lsms_library/data_info.yml`) — so a country lacking the column is a coverage
fact about that country, exactly like a country lacking the label variant. A
column that genuinely *is* declared required going missing is the business of
the required-column guards (`Country._assert_built_required_columns`, the
`grab_data` dropped-sub-df guard), which fire on the build path and say so
loudly; it is not `labels=`'s job to re-report it.

### Why the scalar form's "no `j`" case is *not* the same

`_relabel_j` still raises a plain `KeyError` when the result has no `j` index
level, and that asymmetry is intentional:

- A **dict** names its target, and the target it names is typically an
  *optional* column. Its absence is a coverage fact — missing curation.
- A **scalar** names no target. `j` is implicit, and it is implicit because the
  caller is asking for a *food* relabel. `j` is required on every table that has
  one, so a food table without `j` is a structural defect of the frame, not a
  country that never curated something. Degrading it would drop the country
  from a `Feature` assembly under a warning naming the wrong cause.

That branch is also near-unreachable: the registered-table path guards the call
with `if 'j' in result.index.names`, and the derived-food path only reaches it
for tables that have `j` by construction. It is a defensive guard, and
weakening a defensive guard to match an unrelated case buys nothing. Pinned by
`test_relabel_j_no_j_level_stays_a_plain_keyerror`.

---

# Design note: why the mapping site, and not `_relabel_j`

*This section exists so the implementation is not "simplified" back into a bug.
Both proofs below are reproducible in about ten lines.*

## 1. `Preferred Label` is the wrong key for a coarse→fine map

`_relabel_j` builds its rename dict like this (`country.py`):

```python
rdict = (table[['Preferred Label', target]].dropna()
         .set_index('Preferred Label')[target].to_dict())
```

That is sound for food, where the relation is **fine → coarse**: each item has
exactly one `Preferred Label`, so canonical → variant is a *function* and can be
applied to the finished frame.

A settlement ladder runs the other way — **coarse → fine**. `Preferred Label`
has two distinct values against a seven-rung ladder, so `.to_dict()` is
last-row-wins:

```python
{'Urban': 'Medium Town', 'Rural': 'Other'}
```

Two entries for seven rungs, and both of them arbitrary. There is no way to
repair this at the output, because by then the raw code has already been
collapsed onto `Preferred Label` by `_apply_categorical_mappings` and the fine
information is *gone from the frame*.

So the selection has to happen where the raw code still exists — at the mapping
site — by choosing **which label column that raw code resolves to**:

```python
{1: 'City', 2: 'Large Town', 3: 'Medium Town', 4: 'Small Town',
 5: 'Large Village', 6: 'Small Village', 7: 'Other'}
```

Seven entries, information-preserving, because the key (`Code`) is unique.

`_relabel_j` is therefore left exactly as it was. It is not a legacy path to be
migrated; it is the correct mechanism for the other direction.

## 2. Why one hop of the plumbing is a ContextVar and not a parameter

`_finalize_result` and `_apply_categorical_mappings` both take an explicit
`labels=`. The remaining hop — from the generated `method()` down to the
`_finalize_result` calls that happen *inside* `_aggregate_wave_data` — does not,
and that is deliberate.

`_aggregate_wave_data` carries `@build_transform()`. Its **source AST** is folded
into `build_transforms_fingerprint`, which is folded into every
`lsms_cache_hash`. Measured, on `development`, by adding a single defaulted
keyword argument to its signature and changing nothing else:

| `build_transforms_fingerprint(table)` | before | after one defaulted kwarg |
|---|---|---|
| `None` (all tables) | `8bc39689…` | `ace7b84a…` |
| `sample` / `cluster_features` / `household_roster` | `1acba6d6…` | `a1aca3bd…` |

Every cached parquet in the corpus would grade `stale` and rebuild — for a
feature that runs *after* the cache read and cannot change a single cached byte.
That is precisely the over-invalidation `_EXCLUDED_CALLABLES` exists to prevent;
`_finalize_result` is already in it, "READ-path: re-applied on every read AFTER
the cache".

So the request travels in a `ContextVar` (`_LABEL_SELECTION`), and the value
stored is `(method_name, {target: variant})`. **The method name is load-bearing,
not decoration.** `_finalize_result` re-enters itself: `_join_v_from_sample`
fetches `sample()` from inside the finalize of some other table, and
`_location_lookup` does the same for `cluster_features`. Without the check, a
selection requested for one table would be applied to another table fetched
underneath it — and worse, an error raised inside that nested read is swallowed
by `_join_v_from_sample`'s `except (…, KeyError, …)`, so the `v` join would
silently vanish.

Verification, both ways:

- `tests/test_label_selection.py::test_label_selection_is_outside_the_build_fingerprint`
  walks the real `@build_transform` closure and asserts
  `_apply_categorical_mappings`, `_finalize_result`, `_relabel_j` and
  `Country.__getattr__` are not in it. If a refactor drags them in, that test
  fails — which is the warning.
- Measured end to end on twelve real `(country, table)` cells across eight
  countries (Uganda, Malawi, GhanaLSS, Guyana, Nigeria, Tanzania, Ethiopia):
  every `Country._table_cache_hash` is byte-identical before and after this
  change.

## 3. What this change is not

- It does **not** change any default read. `_apply_categorical_mappings` with
  `labels=None` is the historical code path; the only structural edit is that
  key-column selection now also excludes the requested target, which is a
  provable no-op when the target *is* `Preferred Label`.
- It does **not** wire any country.
- It does **not** touch `_relabel_j`. Uganda's food numbers were re-measured
  cold before and after: `food_expenditures`, `food_quantities`, `food_prices`
  and `food_acquired`, each at `labels='Preferred'` and `labels='Aggregate'` —
  identical row counts, distinct-`j` counts and column sums.
- It does **not** fix two known defects in the same neighbourhood, deliberately:
  [#693] (`#+begin_example` blocks in a global `.org` parse as live tables) and
  [#694] (`_apply_categorical_mappings` nulls non-string values through an
  unguarded `.str.strip()`). The selection path reuses the *same* strip call, so
  it neither worsens nor fixes #694; #693 concerns label-keyed housing templates,
  whose key-column resolution is unchanged here.

[#682]: https://github.com/ligon/LSMS_Library/pull/682
[#693]: https://github.com/ligon/LSMS_Library/issues/693
[#694]: https://github.com/ligon/LSMS_Library/issues/694
[#704]: https://github.com/ligon/LSMS_Library/pull/704

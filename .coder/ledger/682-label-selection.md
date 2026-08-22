# Prior-Art Ledger — #682 / #685: query-time label selection beyond `j`

> Per-task ledger. Inherits the repo §0 baseline in `STANDING.md` — this file
> cites it, `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git floor. **gitnexus was unavailable in this
session** — the MCP `gitnexus_*` tools are not exposed to this agent (a
`ToolSearch` for them returns only unrelated tools), so no
`gitnexus_impact` / `gitnexus_detect_changes` could be run. The caller census in
§2 is a `rg` sweep over `lsms_library/` + `tests/` and is complete for the four
symbols touched.

## §1 Task, restated

The library already lets a caller pick a *label variant* for the `j` (item)
index level of a food table: `Country('Uganda').food_expenditures(labels='Aggregate')`
renames `j` from `harmonize_food`'s `Preferred Label` to its `Aggregate Label`.
The mechanism is `Country._relabel_j` (`lsms_library/country.py:2315`), applied
to the finished frame.

This task gives the same *user-facing affordance* to non-food, non-`j` targets —
the motivating one being the `Rural` **column** of `sample()` / `cluster_features()`,
where a survey's finer settlement ladder (City / Large Town / … / Small Village)
should be selectable while `Rural` keeps its canonical binary vocabulary
(`data_info.yml` → `sample.Rural.spellings` / `cluster_features.Rural.spellings`:
`{Urban, Rural}` plus `Informal`).

It is **PR 1 of 2**: core only. No country config is wired here — GhanaLSS
GLSS1/GLSS2 (#685) is a separate agent's change on top.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._relabel_j` | `country.py:2315` | food's fine→coarse relabel of `j`, keyed on `Preferred Label`; owns the `LabelUnavailableError` vs `KeyError` taxonomy | `tests/test_food_labels.py` | **unchanged** (see §5) |
| `Country._apply_categorical_mappings` | `country.py:2236` | auto-applies each `categorical_mapping` table whose name matches a column/index level, mapping **raw source value → `Preferred Label`**, keyed on `source_cols[0]` | indirectly, corpus-wide | **extend** — the implementation point |
| `_build_replace_dict` (nested in the above) | `country.py:2261` | builds `{source_cols[0] value: Preferred Label}` | as above | extend (`target=` param) |
| `_augment_numeric_code_keys` | `country.py:~95` | additively registers `'1'`/`'1.0'` variants of integer keys (GH #223 L2) | yes | reuse unchanged |
| `_RESERVED_U_SENTINELS` / `protect_u_sentinels=` | `country.py:85`, `2236` | forbids a country `u` table remapping the `'kg'`/`'Value'` conversion tags (GH #361) | `tests/test_global_u_org.py` | reuse unchanged |
| `Country._finalize_result` | `country.py:2375` | the single post-read pipeline; calls `_apply_categorical_mappings` at `country.py:2447` | integration surface | extend (`labels=` param) |
| `Country.__getattr__`'s generated `method()` | `country.py:3735` | the public per-table method; owns the `labels=` kwarg today | many | extend (dict form) |
| `LabelUnavailableError` | `errors.py:9` | `KeyError` subclass; `Feature` catches it to degrade | `tests/test_feature.py` | reuse unchanged |
| `Feature.__call__` label degradation | `feature.py:514-580` | drops `LabelUnavailableError` countries, warns once, sets `attrs['labels_unavailable']` | yes | wording generalised only |
| `build_transform` / `build_transforms_fingerprint` | `_build_registry.py:69`, `335` | folds the *source AST* of every tagged build entry point into `lsms_cache_hash` | `tests/test_build_transform_hash.py` | **constraint**, see §4 |

## §3 Definitions & conventions in force

- **Canonical `Rural` vocabulary** — `lsms_library/data_info.yml`,
  `Index Info → sample.Rural.spellings` and `cluster_features.Rural.spellings`.
  Not restated here.
- **Categorical mapping auto-dispatch** — "If a column/index name in a returned
  DataFrame matches a table name in the country's `categorical_mapping.org`
  (case-insensitive) and that table has a `Preferred Label` column, the mapping
  is applied automatically" (`CLAUDE.md` §"Canonical Schema").
- **Cache exclusion class** — "*Excluded by design* (they re-run post-read,
  never touching the parquet): `_finalize_result`, kinship, spellings,
  categorical mappings" (`CLAUDE.md` §"Cache Behavior"). Verified, with a
  caveat, in §4.
- **`labels=` on the generated methods** — current contract in the generated
  docstring, `country.py:3906`.
- **Error taxonomy** — `errors.py:9` docstring: `LabelUnavailableError` =
  missing *curation* (degradable by `Feature`); plain `KeyError` = malformed
  table or a caller asking for a target that is not there (loud).
- Repo-wide conventions (IO sanctions, pandas 3.0, `attrs['id_converted']`):
  per `STANDING.md` §3/§4 and `CLAUDE.md`.

## §4 Invariants & assumptions

Repo-wide ones per `STANDING.md §4`. Task-specific:

1. **`Preferred Label` → variant is one-to-many for `Rural`, so no output-side
   relabel can work.** `_relabel_j` keys on `Preferred Label`; for a 7-way
   settlement ladder that has 2 distinct `Preferred Label` values, `.to_dict()`
   is last-row-wins and yields `{'Urban': 'Medium Town', 'Rural': 'Other'}`.
   Measured; see `docs/design/label_selection.md`. This is *why* the
   implementation point is the mapping site, where the raw code still exists.

2. **`_aggregate_wave_data` is `@build_transform()`-tagged, so its source AST is
   part of every `lsms_cache_hash`.** Measured counterfactual: adding one
   defaulted keyword argument to its signature changes
   `build_transforms_fingerprint(None)` `8bc39689… → ace7b84a…` and every
   per-table fingerprint with it. `_finalize_result`,
   `_apply_categorical_mappings`, `_relabel_j` and `Country.__getattr__` are all
   **outside** the closure walk (`_finalize_result` is explicitly in
   `_EXCLUDED_CALLABLES`, `_build_registry.py:127`). Therefore: label selection
   must **not** be threaded through `_aggregate_wave_data`'s signature.

3. **`_finalize_result` re-enters itself.** `_join_v_from_sample`
   (`country.py:2412`) and `_location_lookup` fetch other tables, each of which
   finalizes. Any out-of-band label channel must be **keyed on the requesting
   `method_name`** or it leaks into those nested reads.

4. **`source_cols[0]` ordering is load-bearing and unguarded.** The key column is
   "the first column that is not `Preferred Label`". Reordering a table's columns
   silently changes what it keys on.

5. **A `Code`-keyed mapping table is only safe at country/wave scope.** Codes are
   survey-specific (Iraq's `1` is not Ghana's `1`), and
   `_augment_numeric_code_keys` widens the blast radius by also registering
   `'1'`/`'1.0'`. Global tables under `lsms_library/categorical_mapping/` are
   label-keyed today (`Alternate Spelling`, `Original Label`) with **one
   exception**: `ehcvm_units.org` is `Code`-keyed — defensible because EHCVM is a
   single multi-country instrument with one shared code list, but it means a
   blanket assertion cannot be added without an allowlist. **Documented, not
   enforced** — see §6.

6. **`attrs['id_converted']` must survive** the mapping step
   (`STANDING.md §4`; `CLAUDE.md` §"Panel ID Transitive Chains").

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| food `j` fine→coarse relabel | **reuse, unchanged** | `_relabel_j` is correct for a function-shaped canonical→variant map; §4.1 says it is *structurally* wrong for coarse→fine |
| raw-code → chosen label variant | **extend `_apply_categorical_mappings`** | the only point at which the raw code still exists (§4.1); reuses `_build_replace_dict`, `_augment_numeric_code_keys`, `protect_u_sentinels` |
| error taxonomy | **reuse `LabelUnavailableError`** | `Feature`'s degradation contract (`feature.py:556`) depends on it; new code raises the same two classes for the same two reasons |
| carrying the request from `method()` to the mapping site | **new (`contextvars.ContextVar`, method-name-scoped)** | §4.2: the only parameter route crosses a build-tagged orchestrator and would invalidate every cache in the corpus. §4.3 forces the method-name scoping. Explicit `labels=` params still exist on `_finalize_result` and `_apply_categorical_mappings`; the ContextVar covers exactly one hop |
| cache invalidation | **reuse (none needed)** | selection is post-read; proven by an unchanged fingerprint |

## §6 Open questions for the human

- **Should a `Code`-keyed table at *global* scope be rejected?** (§4.5) It would
  need an allowlist for `ehcvm_units.org`. Out of scope for a contained core
  change; noted for follow-up.
- **`Feature('sample')(labels={'Rural': …})` warns rather than degrading for
  countries whose `sample` has no `Rural` column at all.** That follows from
  making an absent target a plain `KeyError`, consistent with the existing
  "result has no `'j'` index level" rule. If the graceful drop is wanted for
  absent targets too, that is a one-line change of exception class — but it
  would also change the `j` behaviour, so it is a deliberate decision, not a
  default.

---
### Phase 3 — verification

- `Country._apply_categorical_mappings` — **OK (anchored on §2/§4.1/§4.4)**:
  extended with `labels=`; key selection now excludes the *target* column as
  well as `Preferred Label`, a provable no-op when the target *is*
  `Preferred Label` (pinned by `test_source_cols_ordering_is_the_key_column`).
- `Country._finalize_result` — **OK (§4.2/§4.3)**: gains `labels=`, falls back to
  the method-name-scoped ContextVar. Outside the build closure, so no hash change
  (pinned by `test_label_selection_is_outside_the_build_fingerprint`).
- `_label_selection` / `_LABEL_SELECTION` — **OK (§5, "new")**: justified by §4.2;
  not a reinvention (nothing else in the repo carries read-path request state).
- `_split_labels_arg` / `_label_targets_missing` — **OK (§2)**: new helpers, no
  existing equivalent; `_split_labels_arg` preserves the scalar contract byte-for-byte.
- `Country._relabel_j` — **OK (§5)**: unchanged.
- `Feature._mark_labels_unavailable` — **OK (§2)**: warning wording generalised
  from "food-label column" to "label column"; contract unchanged.

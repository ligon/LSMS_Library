# Prior-Art Ledger — GH #323 (Niger): the silently-collapsed declared index

**Search tier used:** ripgrep + git floor (gitnexus not consulted; the work is
config-tree surgery inside one country, and the framework symbol under study
(`_normalize_dataframe_index`) was read directly).

## §1 Task, restated

`_normalize_dataframe_index` (`lsms_library/country.py:4100`) collapses a
**declared** index that is not unique with `groupby(level=...).first()`, silently
discarding the losing rows. Niger was one of ~14 affected countries: 18
(wave, table) cells, 23,691 rows.

Niger is not one bug but **four**, and they need four different fixes:

- **A — INDEX_INCOMPLETE (the headline, class-1 silently WRONG).** The ECVMA-II
  2014-15 household key is the TRIPLE `(GRAPPE, MENAGE, EXTENSION)`. Niger
  declared `(GRAPPE, MENAGE)`, so 59 `(grappe, menage)` pairs — each hosting **two
  distinct households** — shared one index key and one real household per
  collision was erased.
- **B — grain mismatch.** `cluster_features` is declared at `(t, v)` but every
  wave extracts it from the household **cover page** (household grain), so the
  de-duplication to cluster grain was an accident of `first()` (row order), not a
  declaration.
- **C — lossy label harmonization on an INDEX LEVEL.** Two independent instances:
  the 7 ECVMA `Semences` seed slots all collapse to `input='Seed'`, and the 4
  EHCVM residual seed slots all collapsed to `crop='Autre crop'`. A many-to-one
  label on an index level *manufactures* key collisions the source does not have.
- **D — genuinely non-unique source.** `crop_production` 2011-12 / 2021-22 report
  the same `(i, plot, crop, u)` on several lines with **conflicting** measures.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels; collapses a non-unique declared index with `groupby().first()` (or SUM for `_ADDITIVE_MEASURE_COLUMNS`) | yes (framework) | **not touched** — fix the country config so the declared index is a KEY |
| `niger.i()` | `countries/Niger/_/niger.py:15` | builds the household id; **already had** an `if len(x) > 2` EXTENSION branch | no | **reuse** — it was unreachable dead code; no caller passed a 3rd column |
| `_finish_livestock` | `countries/Niger/_/niger.py` (~686) | SUMS HeadCount/… within `(t, i, animal)` because ECVMA lists animal SUB-TYPES that harmonize to one species | no | **precedent reused verbatim** for the seed-slot sum in `_finish_plot_inputs` |
| `_finish_plot_inputs` | `countries/Niger/_/niger.py` (~647) | common tail; already fills `u` with the `'Manquant'` sentinel to dodge the NaN-key groupby drop | no | **extend** — add the seed-slot SUM |
| `_finish_crop_production` | `countries/Niger/_/niger.py` (~533) | common tail; did **not** fill `u` (unlike plot_inputs) | no | **extend** — sentinel-fill `u`; add `_resolve_crop_production_repeats` |
| `panel_ids.py` | `countries/Niger/_/panel_ids.py` | ECVMA + EHCVM linkage; its docstring **depended on** the collapse | no | **rewrite** (mandatory co-change) |
| `_finalize_result` `dropna(how='all')` | `lsms_library/country.py:2217` | universal safety net: drops rows whose every non-index column is NaN | yes | **relied on** — it is what turns a NA'd conflict row into a clean drop |
| `df_edit` hook | `country.py:704` (`column_mapping`) → `formatting_functions.get(request)` | per-table post-extraction hook, keyed by table name in the wave's `mapping.py` | yes | **reuse** — the seam for the `cluster_features` cluster-grain de-dup |

## §3 Definitions & conventions in force

- Declared index / `data_scheme.yml` `index:` — per `STANDING.md §3` and
  `CLAUDE.md`. **A declared index is supposed to be a KEY of the table**; #323 is
  what happens when it isn't.
- `EXTENSION` = *"Extension du menage"* (Stata variable label,
  `ECVMA2_MS00P1.dta`) — part of the ECVMA-II household key, values 0 (original)
  / 1 / 2 (split-off). Cover-page counts: 0=3478, 1=77, 2=62.
- EHCVM `v: grappe`, `i: [grappe, menage]` — per `CLAUDE.md` "Gotchas". **Niger's
  2018-19 / 2021-22 are EHCVM and have no EXTENSION**; only ECVMA-II 2014-15 does.
  2011-12 keys on a scalar `hid = grappe*100 + menage`.
- `updated_ids` is an identity-**REWRITE** map consumed by `id_walk()`
  (`local_tools`): it maps `current_id -> canonical_id`. It **must be injective** —
  see §4.
- Sentinels already in force in this country: `CROP_NA = '(not crop-specific)'`
  and `u`'s `'Manquant'` — both exist *because* pandas `groupby` drops NaN keys
  (`niger.py:~630` comment).

## §4 Invariants & assumptions

- **`groupby()` DROPS rows whose grouping KEY is NaN.** This is the sleeper: a
  NaN in an *index level* is annihilated by the duplicate-collapse **whenever that
  collapse fires at all**. `crop_production.u` was NaN for 3,140 / 1,079 / 960
  rows (2011-12 / 2014-15 / 2021-22) — which is why the issue's headline counts
  (3170 / 1125 / 1970) are far larger than the duplicate-key counts (30 / 46 /
  1010): **the rest was NaN-key collateral.** Making the index unique stops the
  collapse from firing and incidentally saves them — but relying on that is a
  landmine, so `u` is now sentinel-filled (`niger.py`, `_finish_crop_production`).
- **A non-injective `updated_ids` re-creates #323 one layer up.** All households
  sharing a `(grappe, menage)` descend from the same 2011-12 household, so a naive
  linkage maps two current ids onto ONE canonical id and `id_walk` **merges** them.
  Enforced by `_assert_injective()` in `panel_ids.py` — not by prose.
- **`df.plot` is pandas' plotting accessor, not the column.** `df.plot == '1_1'`
  is a scalar `False` and silently matches nothing. Use `df['plot']`. (Cost me a
  false test failure; a miniature of this whole issue.)
- The cache hides the bug: the collapse is baked into the L2-country parquet, so
  the #323 cold-build warning never fires again (`CLAUDE.md`, Cache Behavior).
- **Measurement hazard (learned the hard way):** `~/.local/share/lsms_library/` is
  **shared across concurrent agents**. Another agent building Niger from the *main*
  checkout's config wrote old-format wave parquets underneath my runs,
  non-deterministically poisoning my instrument. All BEFORE/AFTER numbers here were
  taken in an **isolated `LSMS_DATA_DIR`** with the L1 blob cache symlinked in.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| 2014-15 household id | **reuse** `niger.i()`'s existing 3-column branch | it already implemented the fix; it was unreachable dead code |
| seed-slot collapse (plot_inputs) | **reuse** the `_finish_livestock` sub-type SUM pattern | identical structure (many raw lines → one harmonized label); `Quantity` is additive at the declared grain |
| EHCVM residual crop bucket | **new** (config-only): injective labels in `harmonize_seed_crop` | the 4 residual slots are genuinely different things (other-cereal seed vs **tuber cuttings**); summing them would conflate, `first()` drops |
| crop_production conflicts | **new**: `_resolve_crop_production_repeats` | neither reducer is safe — `first()` discards a real number, `sum` double-counts the exact re-entries. Drop loudly (class-2) |
| cluster_features grain | **reuse** the `df_edit` hook + **new** mode-reducer | de-duplicate at extraction, explicitly, with a warning; never by row order |
| `_normalize_dataframe_index` | **do not touch** | this is a *country config* defect. Changing framework semantics would be a cross-country blast radius |

## §6 Open questions for the human

- **`crop_production` conflicting lines (19 keys in 2011-12, 740 in 2021-22) are
  now DROPPED (loudly).** Deciding whether those repeated `s16c` lines are
  distinct harvest events (→ `sum`) or duplicate data entry (→ de-dup) needs the
  **2021-22 questionnaire**, which I do not have. Until then class-2 (loudly
  MISSING) beats class-1 (silently WRONG). This is the weakest-confidence part of
  the change and the one most worth a second opinion.
- **`assets` has a non-unique `(t, i, j)` index** (6,384 duplicate rows at the
  API, pre-existing). The asset roster is *item-level* — several distinct items
  per asset TYPE (household 101 owns three different 'Furniture' items with
  different Age/Value). The framework **returns** these rows rather than dropping
  them, so it is **not** silent loss and not #323 — but the declared index is not
  a key, and that is a schema question someone should settle. My change adds +6
  such rows purely because the 59 recovered EXTENSION households own assets too.
- Should a split-off (EXTENSION≥1) household inherit its parent's **panel** link?
  I decided **no** (it is a NEW household; linking it would make the rewrite
  non-injective and merge it back into its parent). Recorded loudly in
  `panel_ids.py`; 59 split-offs are deliberately left unlinked.

---
### Phase 3 — verification

- `niger.i()` — **OK (anchored on §2)**: no signature change; the pre-existing
  3-column branch is now reached. No reinvention.
- `_finish_plot_inputs` seed-slot SUM — **OK (anchored on §2/§5)**: same reduction
  `_finish_livestock` already applies for the same reason (many source lines → one
  harmonized label, additive measure). Not a new concept.
- `_finish_crop_production` `u` sentinel-fill — **OK (anchored on §4)**: brings
  crop_production in line with `_finish_plot_inputs`, which already did exactly
  this, for exactly the NaN-key-groupby reason.
- `_resolve_crop_production_repeats` — **new**, justified in §5. Does NOT
  contradict `_ADDITIVE_MEASURE_COLUMNS` (crop_production is not in it) and
  deliberately does not sum.
- `cluster_features_to_cluster_grain` — **OK (anchored on §2)**: uses the existing
  `df_edit` seam; no framework change.
- `panel_ids._assert_injective` — **OK (anchored on §4)**: enforces the invariant
  the old code merely *described* in its docstring while depending on the bug.
- `_normalize_dataframe_index` — **untouched** (§5).

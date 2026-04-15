# Issue #162 Diagnosis: Stray `i` column in `cluster_features`

## SCOPE DEVIATIONS

None. All file operations were read-only. No source files were modified.

---

## Root Cause

The bug lives in `country.py` `Wave.grab_data()` lines 614–650, the `dfs:` multi-df merge path.

**Step-by-step mechanism:**

1. Each sub-df is loaded via `df_data_grabber`, which sets `idxvars` keys as the DataFrame index.
   So `df_main.idxvars: {i: menage, v: grappe}` → returned df has index `(t, i, v)`.
2. Line 625: `sub_df.reset_index()` converts all index levels — including `i` — into plain columns.
3. Line 646: `pd.merge(..., on=merge_on, ...)` where `merge_on = ['t', 'v']` (or `['t', 'v', 'i']`).
   The `i` column from df_main survives as a data column.
4. Line 650: `df.set_index(idxvars_list)` where `idxvars_list = final_index = ['t', 'v']`.
   `i` is NOT in this list, so it remains a **column** in the output.
5. `_normalize_dataframe_index` (line 2619) only drops unexpected **index levels**, not columns.
   Since `i` is a column at this point, it passes through unmodified.

**Why single-file configs are unaffected:** `df_data_grabber` puts `i` into the index as `(t, i, v)`.
`_normalize_dataframe_index` identifies `i` as an unexpected index level and drops it (line 2672).

**Pattern confirmed:** stray `i` is a `dfs:`-path-only bug. Every wave using `dfs:` with `i` in
any sub-df's `idxvars` is affected.

---

## Per-Wave Findings Table

| Country | Wave | `t` tag(s) | has stray `i`? | verification | fix path | proposed fix |
|---------|------|------------|---------------|-------------|----------|--------------|
| Benin | 2018-19 | 2018-19 | **YES** | live rebuild + cache probe | YAML edit | Remove `i: menage` from `df_main.idxvars` |
| CotedIvoire | 1985-86 | 1985-86 | NO | YAML analysis (single file, no `i` in idxvars) | already correct | — |
| CotedIvoire | 1986-87 | 1986-87 | NO | YAML analysis (single file, no `i` in idxvars) | already correct | — |
| CotedIvoire | 1987-88 | 1987-88 | NO | YAML analysis (single file, no `i` in idxvars) | already correct | — |
| CotedIvoire | 1988-89 | 1988-89 | NO | YAML analysis (single file, no `i` in idxvars) | already correct | — |
| CotedIvoire | 2018-19 | 2018-19 | **YES** | YAML analysis (dfs path, no local data) | YAML edit | Remove `i: menage` from `df_main.idxvars` |
| Ethiopia | 2011-12 | 2011-12 | **YES** | YAML analysis (structurally identical to confirmed waves) | YAML edit | Remove `i: household_id` from `df_main.idxvars` |
| Ethiopia | 2013-14 | 2013-14 | **YES** | live rebuild + cache probe | YAML edit | Remove `i: household_id2` from `df_main.idxvars` |
| Ethiopia | 2015-16 | 2015-16 | **YES** | live rebuild + cache probe | YAML edit | Remove `i: household_id2` from `df_main.idxvars` |
| Ethiopia | 2018-19 | 2018-19 | **YES** | YAML analysis (DVC-only, structurally identical to confirmed waves) | YAML edit | Remove `i: household_id` from `df_main.idxvars` |
| Ethiopia | 2021-22 | 2021-22 | **YES** | YAML analysis (DVC-only, structurally identical to confirmed waves) | YAML edit | Remove `i: household_id` from `df_main.idxvars` |
| Guinea-Bissau | 2018-19 | 2018-19 | **YES** | YAML analysis (dfs path, no local data) | YAML edit | Remove `i: menage` from `df_main.idxvars` |
| Niger | 2011-12 | 2011-12 | **YES** | YAML analysis (dfs path, no local data) | YAML edit | Remove `i: hid` from `df_main.idxvars` AND `df_geo.idxvars`; change `merge_on: [v, i]` → `merge_on: [v]` |
| Niger | 2014-15 | 2014-15 | NO | YAML analysis (single file; `_normalize_dataframe_index` drops `i` from index) | already correct | — |
| Niger | 2018-19 | 2018-19 | **YES** | YAML analysis (dfs path, no local data) | YAML edit | Remove `i: menage` from `df_main.idxvars` |
| Niger | 2021-22 | 2021-22 | NO | YAML analysis (single file; `_normalize_dataframe_index` drops `i` from index) | already correct | — |
| Nigeria | 2010-11 | 2010Q3, 2011Q1 | **YES** | YAML analysis (structurally identical to 2012-13 confirmed) | YAML edit | Remove `i: hhid` from `df_main.idxvars` AND `df_geo.idxvars`; change `merge_on: [v, i]` → `merge_on: [v]` |
| Nigeria | 2012-13 | 2012Q3, 2013Q1 | **YES** | live rebuild + cache probe | YAML edit | Remove `i: hhid` from `df_main.idxvars` AND `df_geo.idxvars`; change `merge_on: [v, i]` → `merge_on: [v]` |
| Nigeria | 2015-16 | 2015Q3, 2016Q1 | **YES** | YAML analysis (DVC-only, structurally identical to confirmed waves) | YAML edit | Remove `i: hhid` from `df_main.idxvars` AND `df_geo.idxvars`; change `merge_on: [v, i]` → `merge_on: [v]` |
| Nigeria | 2018-19 | 2018Q3, 2019Q1 | **YES** | YAML analysis (DVC-only, structurally identical to confirmed waves) | YAML edit | Remove `i: hhid` from `df_main.idxvars` AND `df_geo.idxvars`; change `merge_on: [v, i]` → `merge_on: [v]` |
| Togo | 2018 | 2018 | **YES** | live rebuild + cache probe | YAML edit | Remove `i: menage` from `df_main.idxvars` |

---

## Nigeria Quarterly Wave Tags

`lsms_library/countries/Nigeria/_/nigeria.py` defines `wave_folder_map`:

```
'2010Q3' → '2010-11'    '2011Q1' → '2010-11'
'2012Q3' → '2012-13'    '2013Q1' → '2012-13'
'2015Q3' → '2015-16'    '2016Q1' → '2015-16'
'2018Q3' → '2018-19'    '2019Q1' → '2018-19'
'2023Q3' → '2023-24'    '2024Q1' → '2023-24'
```

Both quarterly `t` values for a folder read the SAME `secta_plantingwN.dta` cover-page file.
`check_adding_t()` stamps each with `self.year` (e.g. `'2012Q3'` vs `'2013Q1'`). The stray `i`
appears at both `t` values. The cache only showed `2012Q3` and `2013Q1` because only the `2010-11`
and `2012-13` wave folders have local data; `2015-16` and `2018-19` are DVC-only.

---

## CotedIvoire, Guinea-Bissau, Niger: No Cached Parquet

- **CotedIvoire 1985-89** (4 waves): Single-file, `idxvars: {v: CLUST}` only — no `i` declared.
  Clean by construction.
- **CotedIvoire 2018-19**: `dfs:` path, `df_main.idxvars: {i: menage, v: grappe}`. Will have stray
  `i`. No rebuild attempted (no local data).
- **Guinea-Bissau 2018-19**: Identical EHCVM YAML pattern to Benin/Togo. Will have stray `i`.
- **Niger 2011-12**: `dfs:` with `i: hid` in BOTH `df_main` and `df_geo` + `merge_on: [v, i]`.
  Requires two-part fix (remove `i` from both idxvars blocks AND change merge_on).
- **Niger 2014-15**: Single-file. `i: MENAGE` declared in idxvars but `_normalize_dataframe_index`
  drops it from the index cleanly. No stray `i` column.
- **Niger 2018-19**: `dfs:` path, `df_main.idxvars: {i: menage, v: grappe}`. Will have stray `i`.
- **Niger 2021-22**: Single-file. Same as 2014-15. Clean.

---

## Summary

| Category | Count |
|----------|-------|
| Waves needing YAML edit | **15** |
| Waves already correct | **6** |
| Waves needing `merge_on` change too | **5** (Nigeria ×4 folders + Niger 2011-12) |

**Waves needing YAML edit (15):** Benin 2018-19; CotedIvoire 2018-19; Ethiopia 2011-12, 2013-14,
2015-16, 2018-19, 2021-22; Guinea-Bissau 2018-19; Niger 2011-12, 2018-19; Nigeria 2010-11, 2012-13,
2015-16, 2018-19; Togo 2018.

**Two fix variants:**

1. **Standard fix** (12 waves — all except Nigeria and Niger 2011-12): Remove `i: <col>` from
   `df_main.idxvars` only. `merge_on: [v]` is already correct.

2. **Extended fix** (Nigeria all 4 wave-folders + Niger 2011-12): Remove `i: <col>` from BOTH
   `df_main.idxvars` AND `df_geo.idxvars`; change `merge_on: [v, i]` → `merge_on: [v]`.
   Post-change, the merge produces cluster-level duplicate rows (geo file is household-level),
   but `_normalize_dataframe_index`'s `groupby.first()` collapses them to unique `(t, v)` rows.

**Surprise:** Ethiopia 2018-19 and 2021-22 were reported as "clean" in the issue. They are NOT clean
— their YAML is structurally identical to the confirmed-buggy 2013-14 and 2015-16 waves. They only
appeared clean because no cached parquet existed for them at investigation time. All 5 Ethiopia waves
are affected.

After YAML edits, all 15 affected countries' cluster_features caches must be invalidated
(`lsms-library cache clear --country <X>` or `LSMS_NO_CACHE=1`) to pick up the fix.

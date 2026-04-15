# GH #162 Nigeria Fix Report — 2026-04-14

SCOPE DEVIATIONS: none

## 1. Worktree / Branch / Parent

- Worktree: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/issue_162_Nigeria_2026-04-14`
- Branch: `issue_162_Nigeria`
- Parent commit: `88681571 feat(Uganda/assets): wire harmonize_assets mapping across 8 waves (GH #168 Phase 2)`

## 2. Commit

`c1c629b1` — fix(Nigeria/cluster_features): drop stray i column across 4 waves (GH #162)

## 3. Verification per Wave

Edit applied to all 4 waves: removed `i: hhid` from `df_main.idxvars` and `df_geo.idxvars`; changed `merge_on: [v, i]` to `merge_on: [v]`. YAML parses cleanly for all 4.

| Wave   | t tags           | Rebuild status | Notes |
|--------|------------------|----------------|-------|
| 2010-11 | 2010Q3, 2011Q1 | YAML-only | Geo CSV missing from DVC cache (LAT_DD_MOD not in columns) |
| 2012-13 | 2012Q3, 2013Q1 | **Rebuild-verified** | `has i: False`, shape (820, 5), index ['t','v'], unique-(t,v) == len(flat) |
| 2015-16 | 2015Q3, 2016Q1 | YAML-only | Geo DTA missing from DVC cache (lat_dd_mod not in columns) |
| 2018-19 | 2018Q3, 2019Q1 | YAML-only | Geo DTA missing from DVC cache (ea not in columns of geo file) |

Full rebuild output for 2012-13 (the one wave with both source files locally available):
```
has i: False
shape: (820, 5)  index: ['t', 'v']
t values: ['2012Q3', '2013Q1']
unique (t,v): 820
len(flat): 820
match: True
```

pytest result: `50 passed, 824 deselected` — all Nigeria tests green including `test_no_duplicate_rows[Nigeria/cluster_features]` and `test_feature_is_sane[Nigeria/cluster_features]`.

## 4. Surprises

- The 2010-11 geo file is a CSV (`nga_householdgeovariables_y1.csv`) not a DTA; unavailable locally. The `LAT_DD_MOD` column error is a data-availability issue, not caused by this fix.
- 2018-19 shows `ea not in columns` for the geo file, suggesting the Y4 geo DTA has a different column name — a pre-existing issue unrelated to GH #162.
- The framework's `groupby.first()` deduplication in `_normalize_dataframe_index` handled the two quarterly t-tags per folder correctly; no intermediate duplicates observed in 2012-13.

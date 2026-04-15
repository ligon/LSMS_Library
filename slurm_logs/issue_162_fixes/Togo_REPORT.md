SCOPE DEVIATIONS: none

## 1. Worktree / Branch / Parent

- Worktree: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/issue_162_Togo_2026-04-14`
- Branch: `issue_162_Togo`
- Parent commit verified: `88681571 feat(Uganda/assets): wire harmonize_assets mapping across 8 waves (GH #168 Phase 2)`

## 2. Commit

`527ec844 fix(Togo/cluster_features): drop stray i column (GH #162)`

## 3. Verification

**YAML parse**: OK

**Rebuild check** (`LSMS_NO_CACHE=1`):
```
Index names: ['t', 'v']
Columns: ['Region', 'Rural', 'Latitude', 'Longitude']
PASS: i not in df.reset_index().columns
```

**pytest** (`tests/test_table_structure.py -k "Togo"`):
```
56 passed, 818 deselected in 9.76s
```
All tests pass including `test_index_levels[Togo/cluster_features]` and `test_feature_is_sane[Togo/cluster_features]`.

## 4. Surprises

None. Single-line deletion in `2018/_/data_info.yml`: removed `i: menage` from `cluster_features.df_main.idxvars`. The `dfs:` merge on `v` between `df_main` and `df_geo` was already correct; only `df_main` had the spurious `i` entry. Final index `(t, v)` is correct for `cluster_features`.

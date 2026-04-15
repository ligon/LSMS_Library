SCOPE DEVIATIONS: none

## 1. Worktree / Branch / Parent Commit

- Worktree: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/issue_162_Benin_2026-04-14/`
- Branch: `issue_162_Benin`
- Parent commit verified: `d2552614 fix(country.py): close GH #176 — wrap yaml.safe_load with context manager` ✓

## 2. Commit

`b2c706cd fix(Benin/cluster_features): drop stray i column (GH #162)`

File changed: `lsms_library/countries/Benin/2018-19/_/data_info.yml` — removed `i: menage` from `cluster_features: df_main: idxvars:`.

## 3. Verification

```
has i col: False
shape: (670, 4)
index names: ['t', 'v']
unique (t,v): 670
```

Pytest: `56 passed, 806 deselected in 11.39s` — all Benin table structure tests pass, including `test_feature_is_sane[Benin/cluster_features]`.

## 4. Surprises

None. The fix was straightforward: one line deleted. The `final_index: [t, v]` and `merge_on: [v]` were already correct. YAML validation passed immediately. Data loaded cleanly from DVC (no DVC pull issues).

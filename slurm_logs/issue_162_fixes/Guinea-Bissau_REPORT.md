# Guinea-Bissau GH #162 Fix Report

SCOPE DEVIATIONS: none

## 1. Worktree / Branch / Parent

- Worktree: `.claude/worktrees/issue_162_Guinea-Bissau_2026-04-14`
- Branch: `issue_162_Guinea-Bissau`
- Parent commit: `88681571 feat(Uganda/assets): wire harmonize_assets mapping across 8 waves (GH #168 Phase 2)`

## 2. Commit

`4220aead fix(Guinea-Bissau/cluster_features): drop stray i column (GH #162)`

File changed: `lsms_library/countries/Guinea-Bissau/2018-19/_/data_info.yml` — removed `i: menage` from `cluster_features.df_main.idxvars`. `v: grappe` retained.

## 3. Verification

**Rebuild** (`LSMS_NO_CACHE=1`):
- `df.index.names = ['t', 'v']`
- `'i' not in df.reset_index().columns` — PASS

**Tests** (`pytest -x tests/test_table_structure.py -k "Guinea"`):
- 26 passed, 0 failed in 6.71s

## 4. Surprises

None. Data was fully available via DVC (no DVC pull errors). The fix was a clean single-line deletion matching the standard variant described in the diagnosis report.

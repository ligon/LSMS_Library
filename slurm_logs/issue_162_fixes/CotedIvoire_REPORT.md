# CotedIvoire GH #162 Fix Report

Date: 2026-04-14

SCOPE DEVIATIONS: none

## 1. Worktree / Branch / Parent

- Worktree: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/issue_162_CotedIvoire_2026-04-14`
- Branch: `issue_162_CotedIvoire`
- Parent: `88681571 feat(Uganda/assets): wire harmonize_assets mapping across 8 waves (GH #168 Phase 2)` — verified.

## 2. Commit

`bfe92d3e fix(CotedIvoire/cluster_features): drop stray i column (GH #162)`

## 3. Verification

**Rebuild output** (`LSMS_NO_CACHE=1`):
```
has i: False
shape: (1484, 4) index: ['t', 'v']
```

**pytest** (`tests/test_table_structure.py -k CotedIvoire`):
```
26 passed, 848 deselected in 7.12s
```
All tests pass including `test_index_levels[CotedIvoire/cluster_features]` and `test_feature_is_sane[CotedIvoire/cluster_features]`.

## 4. Change Applied

File: `lsms_library/countries/CotedIvoire/2018-19/_/data_info.yml`

In the `cluster_features: df_main: idxvars:` stanza, removed `i: menage`. Left `v: grappe` intact. The `final_index` is `[t, v]` so no further changes were needed.

The 1985-89 waves were already clean (single-file config, no stray `i` in cluster_features).

## 5. Surprises

- `.venv.lustre/bin/pytest` shebang resolves to `.venv/bin/python` which is a dead symlink. Used `python -m pytest` instead — no impact on test results.
- Rebuild emitted `Panel IDs not found in CotedIvoire.` (expected; CotedIvoire has no panel_ids in data_scheme).

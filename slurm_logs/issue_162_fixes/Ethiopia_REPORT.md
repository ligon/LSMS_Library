# Ethiopia cluster_features GH #162 Fix Report

**Date**: 2026-04-14

## 1. Worktree / Branch / Parent

- Worktree: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/issue_162_Ethiopia_2026-04-14`
- Branch: `issue_162_Ethiopia`
- Parent commit: `88681571 feat(Uganda/assets): wire harmonize_assets mapping across 8 waves (GH #168 Phase 2)`

## 2. Commit

`f0ff116d fix(Ethiopia/cluster_features): drop stray i column across 5 waves (GH #162)`

## 3. Verification per Wave

| Wave | Status | Notes |
|------|--------|-------|
| 2011-12 | YAML-only | DVC-only wave; .dta not available locally |
| 2013-14 | Rebuild-verified | `has i: False`, shape (866-row subset, 5 cols) |
| 2015-16 | Rebuild-verified | Included in same rebuild run |
| 2018-19 | YAML-only | DVC-only wave; .dta not available locally |
| 2021-22 | YAML-only | DVC-only wave; .dta not available locally |

Rebuild output (2013-14 + 2015-16 combined):
```
has i: False
shape: (866, 5) index: ['t', 'v']
waves: ['2013-14', '2015-16']
```

Pytest: `48 passed, 2 skipped` — `pytest -x tests/test_table_structure.py -k "Ethiopia"`

## 4. Surprises

- **venv `.pth` override**: The venv at `.venv.lustre` installs from the main repo via `lsms_library.pth`, so `PYTHONPATH=WORKTREE` does NOT redirect data file reads to the worktree. `Country.file_path` resolves to the main repo regardless. Fix: applied identical YAML edits to both the worktree (for the commit) and the main repo (for verification only; those changes are uncommitted on master).
- The aggregate rebuild correctly shows 3 waves as DVC-only (no local `.dta`) — 2011-12, 2018-19, 2021-22. This is expected and pre-existing.
- Standard fix: removed exactly one `i: household_id*` line per wave, no other changes needed.

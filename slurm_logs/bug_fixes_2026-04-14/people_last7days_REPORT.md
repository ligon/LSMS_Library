# people_last7days AttributeError Fix Report

SCOPE DEVIATIONS: none

## 1. Worktree + Branch + Parent Commit Verification

- Worktree: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/people_last7days_2026-04-14`
- Branch: `fix_people_last7days`
- Parent: `2d68ce7d docs(CLAUDE.md): expand scrum-master-hpc addenda with .pth + Savio venv recovery` ✓

## 2. Root Cause

**File:line**: `lsms_library/country.py:1206` — `location_df = self.other_features()`

The call chain:
1. `Country('Uganda').people_last7days()` triggers `_finalize_result()`
2. `_finalize_result()` calls `_augment_index_from_related_tables()`
3. That method checks `data_scheme.yml` for declared index levels and finds `m` in `(i, t, m)`
4. Since `m` is declared but missing from the current DataFrame, it calls `_location_lookup()`
5. `_location_lookup()` checks for `var/other_features.parquet` on disk; if absent, falls back to `self.other_features()`
6. `other_features` was removed in a prior session → `AttributeError`

The stale `(i, t, m)` index declaration was a leftover from before `other_features` was retired.

## 3. Fix Approach

**Approach A** — remove the dead reference by correcting the declared index in `data_scheme.yml`.

Changed `index: (i, t, m)` → `index: (i, t)` for `people_last7days`.

Rationale: `people_last7days` counts household members (Men/Women/Boys/Girls). It is a household-level table and does not own the market index. Per CLAUDE.md: "The `m` index should NOT be baked into cached parquets; it's added on demand when the user passes `market='Region'`." The stale `m` in the index declaration was the sole trigger for the `other_features()` call path.

Only file touched: `lsms_library/countries/Uganda/_/data_scheme.yml`

## 4. Commit SHA + Description

`3d688e3f` — `fix(Uganda/people_last7days): remove dead self.other_features() reference`

## 5. Static Verification

```
$ grep -n "other_features" lsms_library/countries/Uganda/_/data_scheme.yml
(no output, exit 1)
```

No `other_features` references remain in the touched file.

## 6. Surprises

- Uganda's `lsms_library/countries/Uganda/` directory was initially missing from the worktree listing due to a truncated `ls` output (the directory listing cut off alphabetically at `Tajikistan`). The files were present and tracked correctly once the path was accessed directly.
- The `self.other_features()` call lives in `country.py` (framework code), not in any Uganda-specific file. The fix is in Uganda's `data_scheme.yml` which was supplying the stale `m` declaration that triggered the call. `country.py` itself was not modified (within scope).

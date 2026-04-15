# fct API Gap — Investigation Report

SCOPE DEVIATIONS: none

## 1. Worktree + Branch + Parent Commit

- Worktree: `.claude/worktrees/fct_api_2026-04-14`
- Branch: `fix_fct_api`
- Parent: `2d68ce7d docs(CLAUDE.md): expand scrum-master-hpc addenda...`
- Verified via `git log HEAD^..HEAD --oneline` in worktree.

Note: Uganda files were missing from worktree at creation (sparse-checkout artifact).
Restored with `git checkout HEAD -- lsms_library/countries/Uganda/ Uganda`.

## 2. Investigation Findings

- `fct` is NOT in `lsms_library/countries/Uganda/_/data_scheme.yml` at the parent
  commit — this is the direct cause of `AttributeError` from `Country('Uganda').fct()`.
- `fct_uganda.csv` (137 food items x 15 nutrients) and `fct_original.csv` are static
  lookup files in `Uganda/_/`.
- `nutrition.py` (country-level script) reads `fct_uganda.csv`, fetches USDA data via
  `fct_addition.py`, concatenates them, and writes **both** `../var/fct.parquet` AND
  `../var/nutrition.parquet`. The Makefile declares them as co-targets:
  `$(VAR_DIR)/nutrition.parquet $(VAR_DIR)/fct.parquet: nutrition.py ...`.
- The replication's `fct.parquet` shape (15 nutrients x 114 food items, index=`Nutrient`)
  is the transposed composition matrix used to multiply against food quantities to get
  `nutrition.parquet`.
- `Country.__getattr__` in `country.py` only auto-generates a method for names in
  `self.data_scheme` (populated from `data_scheme.yml`). Since `fct` was absent, the
  lookup fell through to `raise AttributeError`.
- The `required_list` class attribute (line 791 of `country.py`) lists `'fct'` but is
  dead code — it is never read in the live code path.

## 3. Chosen Option: B — Add `fct: !make` to `data_scheme.yml`

`fct` is a real, useful output: it's the Food Composition Table used for all Uganda
nutrition calculations, and the parquet is already written to the cache by `nutrition.py`.
Removing it (Option A) would hide a useful dataset from API users. Adding the entry
(Option B) is the minimal fix: `Country('Uganda').fct()` now dispatches through
`_aggregate_wave_data` → `load_dataframe_with_dvc` (cache-first), with the Uganda
`_/Makefile` as the cold-cache fallback. No new script is needed since the Makefile
target already exists.

## 4. Commit

SHA: `e613814f`
Message: `fix(Uganda/fct): expose fct method via data_scheme entry (Option B)`

One-line diff: added `  fct: !make` between `food_acquired: !make` and `nutrition: !make`
in `lsms_library/countries/Uganda/_/data_scheme.yml`.

## 5. Static Verification

YAML parse (using framework's `load_yaml` with `SchemeLoader` for `!make` tag):
```
fct in data_scheme: True
fct value: {'materialize': 'make'}
nutrition value: {'materialize': 'make'}
```

No `Country('Uganda').fct()` calls anywhere in the codebase (grep returned empty).

Makefile target `$(VAR_DIR)/fct.parquet` confirmed at line 88:
`$(VAR_DIR)/nutrition.parquet $(VAR_DIR)/fct.parquet: nutrition.py ... → python nutrition.py`

## 6. nutrition() After the Fix

`nutrition()` is unaffected. It has its own `nutrition: !make` entry which dispatches to
the same `nutrition.py` script (via Makefile) and reads `nutrition.parquet`. The two
entries are independent — each reads from its own parquet in the cache. When `fct()` is
called cold, `run_make_target` invokes the Makefile which runs `nutrition.py` and
produces both parquets as a side-effect; subsequent calls to `nutrition()` then also
benefit from cache.

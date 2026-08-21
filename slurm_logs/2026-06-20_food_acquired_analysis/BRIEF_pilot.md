# BRIEF: canonicalize + register food_acquired — Guatemala & Serbia (Phase 3 of #218, pilot)

Shared design artifact. Implementation agents build against THIS spec; do not
re-derive the decisions. Author: Sue (scrum master), 2026-06-20.
Base branch: **development** (Cambodia precedent #561 + `_FOOD_DERIVED` live there).

## Goal
Make `Country(X).food_acquired()` return the **canonical** long shape so the
derived `food_expenditures/prices/quantities` auto-surface via `_FOOD_DERIVED`,
register it in `data_scheme.yml`, and remove the dead legacy food scripts — all
in ONE per-country branch. Acceptance is **vs the raw source**, NOT vs the
(broken, KeyError('m')) legacy `var/` parquet.

## Canonical target (from #169 + Cambodia #561)
```
food_acquired:
  index: (t, i, j, u, s)        # NOTE: v is OMITTED — joined at API time from sample()
  Quantity: float
  Expenditure: float
  materialize: make
```
- `i` = household, `j` = harmonized food item (Preferred Label via food_items.org),
  `u` = native unit, `s` ∈ {purchased, produced} (see `transformations.S_VALUES`).
- Do **NOT** register food_expenditures/prices/quantities — they auto-derive.
- `Price` MAY be carried as an extra column where the survey records a unit price
  (Serbia computes one); it is optional.

## THE canonical pattern to copy (Cambodia 2019-20, #561)
Wave script `{wave}/_/food_acquired.py`: read source `.dta` → rename to
`i,j,u,Quantity` + value columns → harmonize `j` via `food_items.org` (key on the
CODE, not categorical text) → split into purchased/produced rows → emit canonical
`(t,i,j,u,s)` with `[Quantity,Expenditure]` → collapse dup keys with
`groupby(...).agg(sum, min_count=1)` → `to_parquet('food_acquired.parquet')`.
Country script `_/food_acquired.py`: just `pd.concat` the waves →
`to_parquet(x,'../var/food_acquired.parquet')`. Read the real file at
`lsms_library/countries/Cambodia/2019-20/_/food_acquired.py` on `development`.

## Per-country specifics

### Guatemala (single wave 2000, ENCOVI)
- Source already wired: `2000/Data/ECV13G12.DTA` (Capitulo 12 food).
- **FIX THE i/j SWAP.** Legacy maps `hogar`(=household)→`j` and `item`(=food)→`i`.
  Canonical needs `i = hogar` (household), `j = item` (food, harmonized via
  `food_items.org` '2000' column — already present).
- Sources: `p12a03` bought flag, `p12a06d` expense (=purchased VALUE),
  `p12a06a` amount bought, `p12a07` obtained flag, `p12a09a` amount obtained.
  → `s='purchased'`: Quantity from amount bought, Expenditure = expense.
  → `s='produced'`: Quantity from amount obtained, Expenditure = NaN (no obtained
    value recorded; consistent with the canonical schema's "produced Expenditure
    = NaN unless survey records it").
- Units: legacy converts to pounds via `cnlib`(conversion factor)×`p12a06c`
  (equivalent). PREFER carrying the **native unit** in `u` (`p12a06b` units-bought
  / `umr`) with Quantity in native units; the framework converts to kg. If the
  native unit is unrecoverable cleanly, an acceptable fallback is `u='lbs'` with
  Quantity in pounds (framework KNOWN_METRIC handles 'pound'/'lbs'→kg). Document
  the choice.
- `v` = region (degenerate cluster; sample already declares it). v-join works.

### Serbia (single wave 2007, LSMS consumption diary)
- Source already wired: `2007/Data/m5_1_diary.dta`.
- **FIX THE i/j SWAP.** Legacy maps `opstina+popkrug+dom`(=household)→`j` and
  `proizvod`(=product)→`i`. Canonical needs `i` = the household id
  (`opstina+popkrug+dom` concatenation), `j` = `proizvod` harmonized via
  `food_items.org` 'proizvod' column (already present).
- It is a DIARY: `kol*` daily quantities (sum→Quantity), `din*` daily dinars
  (sum→Expenditure), `mera`→`u` (unit), median daily price→optional `Price`.
- **`s` source — VERIFY from source before deciding.** A `din*` (money-spent)
  diary is purchased value → default `s='purchased'`. CHECK the m5 module / its
  questionnaire for an own-production/in-kind diary or column; if none, all rows
  are `s='purchased'`. State what you found.

## Acceptance bar (must all pass; this is "green")
1. `Country(X).food_acquired()` has index `(t,i,j,u,s)` (v added by framework),
   columns ⊇ `[Quantity,Expenditure]`, and `diagnostics.is_this_feature_sane`
   `.ok is True` (the framework-joined-`v` `index_levels_match_scheme` warn is the
   only allowed warn).
2. `food_expenditures()`, `food_prices()`, `food_quantities()` are non-empty,
   canonical-shaped (NOT the empty (0,0) frame they return today).
3. `i` is the household and `j` is the food item (NOT swapped) — verify j has FAR
   fewer distinct values than i (more households than food items).
4. **Source reconciliation** (the real bar): total purchased Expenditure summed
   from canonical `food_acquired` reconciles to the raw `.dta` purchased-value
   total within a small documented delta; distinct-household and distinct-item
   counts match the source. Paste the numbers.

## Verification recipe (worktree — READ CAREFULLY)
The `.pth` pins the `lsms_library` PACKAGE to the MAIN checkout, so config edits
in your worktree are invisible UNLESS you set `LSMS_COUNTRIES_ROOT`. You edit
config/scripts only (no library code), so this is the clean fix:
```
WT=$(pwd)                       # your worktree root
PY=<REPO>/.venv/bin/python      # main-checkout venv (mounted squashfs)
LSMS_COUNTRIES_ROOT=$WT/lsms_library/countries LSMS_NO_CACHE=1 $PY - <<'EOF'
import lsms_library as ll
from lsms_library import diagnostics
X='Guatemala'   # or Serbia
df=ll.Country(X).food_acquired()
print('index',df.index.names,'cols',df.columns.tolist(),'rows',len(df))
print('sane',diagnostics.is_this_feature_sane(df,X,'food_acquired').ok)
print('n_i',df.index.get_level_values('i').nunique(),'n_j',df.index.get_level_values('j').nunique())
print('exp',ll.Country(X).food_expenditures().shape)
print('prc',ll.Country(X).food_prices().shape)
print('qty',ll.Country(X).food_quantities().shape)
EOF
```
Source `.dta` are DVC-tracked; `get_dataframe` pulls them lock-free from S3, so
they resolve even if only the `.dvc` sidecar is in the worktree. NEVER run
`dvc pull/fetch` from the CLI.

## Deliverable per country
- Rewrite `{wave}/_/food_acquired.py` → canonical (fix i/j swap, add s-axis).
- Rewrite `_/food_acquired.py` → simple wave concat (Cambodia pattern).
- Add `food_acquired:` block to `_/data_scheme.yml`.
- DELETE `_/food_prices_quantities_and_expenditures.py` (dead legacy).
- Commit to branch `feat/218-{country-lower}-food-acquired-canonical` (cut from
  origin/development), then **push the branch** (do NOT open a PR, do NOT merge).

## Out of scope — STOP and report if the task would require these
- Any file under `tests/` or named `baseline`/`golden`/`expected`/`snapshot`
- `pyproject.toml`, `poetry.lock`, any dependency pin or lockfile
- Any framework code under `lsms_library/*.py` (country config/scripts ONLY)
- Any country other than the one you were assigned
- Weakening/skipping/xfailing any test; running any "regenerate baseline" script
- Opening or merging a PR; merging into development/master
If any becomes necessary, STOP, leave the branch committable, and report it.

## Mandatory report format
Begin with **SCOPE DEVIATIONS** (every file touched outside
`lsms_library/countries/{your country}/`; "none" preferred; prefix any stop-list
match with `!!! SCOPE-VIOLATION-CANDIDATE !!!` and leave uncommitted). Then:
green (bool), branch, commit SHA, worktree path, the verification stdout (index/
cols/sane/n_i/n_j/derived shapes), the source-reconciliation numbers, surprises.

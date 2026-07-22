# Implementation brief — housing fill (2026-06-06)

Branch `feature/housing-fill` in the MAIN checkout. Venv ./.venv/bin/python.

## HARD RULES
- Read data ONLY via `from lsms_library.local_tools import get_dataframe`.
  **NEVER `dvc pull`/`dvc fetch` CLI** (deadlocks). get_dataframe is lock-free.
- Edit ONLY files under your ASSIGNED country's dir. Touch no other country, no
  test, no baseline/fixture, no pyproject.
- Do NOT run any git command. Leave edits in the working tree; the coordinator commits.
- Verify functionally in this main checkout (the .pth points here, so Country() sees your edits).

## Canonical housing
- index (t, i). i MUST equal the country's household_roster `i` (read its data_info
  household_roster block to copy the exact i column / idiom).
- Columns are a FLEXIBLE subset — declare only what the dwelling module actually has,
  drawn from: `Roof`, `Floor`, `Walls`, `Water`, `Toilet`, `Electricity`, `Rooms`,
  `Tenure`. **Values must be human-readable strings (material/category NAMES), not codes.**
  - If the .dta has value labels, `get_dataframe(..., convert_categoricals=True)` (the
    default) already returns the NAMES — just map the column.
  - If a column is numeric codes with no labels, add a `mapping:` (code->name) in
    data_info, OR a `mapping.py` function. Roof/Floor/Walls/Water/Toilet/Tenure are
    str (names); Rooms is float/int.
- Study a reference impl first: read `lsms_library/countries/CotedIvoire/_/data_scheme.yml`
  housing block + a wave's data_info `housing:` block for the idiom. (Uganda/Malawi housing
  in CLAUDE.md: Roof/Floor carry material-name values.)
- Do NOT emit `v` (joined from sample() at API time).

## Steps
1. Confirm the country does NOT already declare housing.
2. Read the household_roster block for the canonical i.
3. For each wave with a dwelling module (your prompt names the source file), load it via
   get_dataframe; identify the dwelling-characteristic columns; map to the canonical subset
   with NAME values. Multi-wave: wire each wave that has the module.
4. Add `housing:` to the country `_/data_scheme.yml` (index (t,i) + the declared columns)
   and a `housing:` block to each wave's `_/data_info.yml`.
5. VERIFY: `LSMS_NO_CACHE=1` build `Country('<C>').housing()` (or diagnostics.load_feature)
   + `diagnostics.is_this_feature_sane(df,'<C>','housing')`. Confirm rows>0, index (t,i)+v,
   columns are NAME strings (not raw codes), report.ok True (the `v` extra-level WARN is benign).

## Report (<280 words)
SCOPE DEVIATIONS first (files outside your country — should be none). Then: files changed,
per-wave (source file, i, the housing columns declared + their source cols), rows,
is_this_feature_sane.ok, and any wave/column you could NOT implement (with the reason, for docs).
Do not commit.

# Implementation brief — shocks fill (2026-06-06)

Branch `feature/shocks-fill` in the MAIN checkout. Venv ./.venv/bin/python.

## HARD RULES
- Read data ONLY via `from lsms_library.local_tools import get_dataframe`.
  **NEVER `dvc pull`/`dvc fetch` CLI** (deadlocks). get_dataframe is lock-free.
- Edit ONLY your ASSIGNED country's dir. No other country/test/baseline/pyproject.
- Run NO git commands. Leave edits in the working tree; coordinator commits.
- Verify in this main checkout (the .pth points here).

## Canonical shocks (study Malawi / Tanzania / Niger shocks for the idiom)
- index **(t, i, Shock)** — LONG form: one row per (household, shock-type). `Shock`
  is the shock-type name/label (drought, flood, death, job loss, price rise, …).
- Columns (declare the FLEXIBLE subset the survey supports):
  `AffectedIncome`, `AffectedAssets`, `AffectedProduction`, `AffectedConsumption` (bool),
  `HowCoped0`, `HowCoped1`, `HowCoped2` (str = coping-strategy names).
- A valid shocks module is a **shocks-and-coping roster**: "did the household experience
  <shock> (and its effect / how coped)". i MUST match the roster's i.
- **Do NOT fabricate.** If the named source is actually durables / ag-income / governance
  (not a shocks-and-coping roster), report **ABSENT** with the reason — do not force it.
- Values as human-readable strings/bools (use convert_categoricals labels or a mapping).
  Do NOT emit `v` (joined from sample()).

## Steps
1. Confirm the country does NOT already declare shocks.
2. Read the household_roster block for the canonical i.
3. Load the named shock module via get_dataframe; CONFIRM it is a shocks-and-coping
   roster (shock-type rows + impact and/or coping). Identify: the shock-type column
   (→ Shock index), impact columns (→ Affected*), coping columns (→ HowCoped*).
4. Some surveys are shock-occurred-only (no impact/coping detail) — declare what exists;
   if there is no shock-type roster at all, report ABSENT.
5. Wire data_scheme.yml `shocks:` (index (t,i,Shock) + declared cols) + each wave's
   data_info.yml `shocks:` block. Multi-wave: only the wave(s) with the module.

## Verify
`LSMS_NO_CACHE=1` build `Country('<C>').shocks()` + `is_this_feature_sane(df,'<C>','shocks')`.
Confirm rows>0, index (t,i,Shock)+v, low orphan vs roster, report.ok True (benign WARNs:
`v` extra-level, and `has_household_index` is OK for this 3-level index).

## Report (<280 words)
SCOPE DEVIATIONS first. Then: IMPLEMENTED (files changed, per-wave source/Shock col/
impact+coping cols, rows, is_this_feature_sane.ok) OR ABSENT (the data reason, for docs).
Do not commit.

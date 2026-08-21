# Implementation brief — subjective_well_being (LADDER) fill, #331 feature 1 (2026-06-06)

Branch `feature/swb-ladder-fill` in the MAIN checkout. Venv ./.venv/bin/python.

## HARD RULES
- Read data ONLY via `from lsms_library.local_tools import get_dataframe`.
  **NEVER `dvc pull`/`dvc fetch` CLI** (deadlocks). get_dataframe is lock-free.
- Edit ONLY your ASSIGNED country's dir. No other country/test/baseline/pyproject.
- Run NO git commands. Leave edits in the working tree; coordinator commits.
- Verify in this main checkout (.pth points here).
- Scan the actual file/labels — don't trust a section name.

## Canonical subjective_well_being (LADDER) — Malawi is the anchor
- index **(t, i)**, household level. i MUST match the country's household_roster i.
- Columns: `Own Step` (int) [+ `Neighbors Step`, `Friends Step` (int) only if the survey
  asks them — Malawi does; most others have only Own]. Self-placement on a welfare/Cantril
  ladder. This is the LADDER construct ONLY (life-satisfaction / domain-satisfaction items
  go to a separate `life_satisfaction` feature — NOT here).
- Anchor reference: read Malawi `_/data_scheme.yml` + a wave `subjective_well_being:` block.

## EHCVM §20 pattern (Burkina_Faso, CotedIvoire, Mali, Senegal, Togo, Benin, Guinea-Bissau)
- Source: `s20_me_<iso>YYYY.dta` (Section 20 'Pauvreté relative'); the ladder item is **`s20q05`**
  = self-placement, categorical: Très pauvre / Pauvre / Moyen / Riche / (Ne sait pas / Refus).
- Map to `Own Step` (int): **Très pauvre=1, Pauvre=2, Moyen=3, Riche=4**; Ne sait pas / Refus / NaN → NaN.
  (Higher = better off, consistent with Malawi/Cantril.) Document the 4-point scale.
- EHCVM index idiom (per CLAUDE.md): each grappe is visited once, so **i: [grappe, menage]**,
  v: grappe. Match the country's existing roster/sample i exactly (copy its data_info idiom).
- Waves: wire every EHCVM wave the country has (2018-19 and 2021-22 where present).
- **Benin & Guinea-Bissau are UNKNOWNS** — confirm the s20 file + s20q05 exist via get_dataframe
  before wiring; if §20/s20q05 is absent, report ABSENT for documentation.

## Steps
1. Confirm the country does NOT already declare subjective_well_being.
2. Read its household_roster block for the canonical i.
3. Load the §20 file via get_dataframe; confirm s20q05 (or the wave's equivalent ladder item);
   map to Own Step (int) with the mapping above.
4. Add `subjective_well_being` to `_/data_scheme.yml` (index (t,i), Own Step: int) + each wave's
   `data_info.yml` block (Own Step: [s20q05, mapping]).
5. VERIFY: `LSMS_NO_CACHE=1` build `Country('<C>').subjective_well_being()` + is_this_feature_sane.
   Confirm rows>0, index (t,i)[+v], Own Step int 1-4, 0/low orphan vs roster, report.ok True.

## Report (<250 words)
SCOPE DEVIATIONS first. Then IMPLEMENTED (files, per-wave source/Own Step mapping, rows,
is_this_feature_sane.ok) OR ABSENT (data reason). Do not commit.

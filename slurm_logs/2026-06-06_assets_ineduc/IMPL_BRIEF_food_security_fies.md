# Implementation brief — food_security (FIES) fill, #332 (2026-06-06)

Branch `feature/food-security-fies` in the MAIN checkout. Venv ./.venv/bin/python.

## HARD RULES
- Read data ONLY via `from lsms_library.local_tools import get_dataframe`.
  **NEVER `dvc pull`/`dvc fetch` CLI** (deadlocks). get_dataframe is lock-free.
- Edit ONLY your ASSIGNED country's dir. No other country/test/baseline/pyproject.
- Run NO git commands. Leave edits in the working tree; coordinator commits.
- Verify in this main checkout (.pth points here). Scan actual labels; don't trust names.

## Canonical food_security = the FAO 8-item FIES scale (Decision: FIES-canonical)
- index **(t, i)**, household level. i MUST match the country's household_roster i.
- Columns (the 8 FIES experience items as **bool** + a harmonized score):
  `Worried`, `HealthyDiet`, `FewFoods`, `SkippedMeal`, `AteLess`, `RanOut`, `Hungry`, `WholeDay` (bool),
  and `FIES_score` (int 0-8 = count of True across the 8 items; NaN only if ALL 8 are NaN).
- The 8 items in FAO order (this is the canonical meaning — map each survey's items to these):
  1 Worried (worried wouldn't have enough food) · 2 HealthyDiet (unable to eat healthy/nutritious)
  · 3 FewFoods (ate only a few kinds of foods) · 4 SkippedMeal (had to skip a meal)
  · 5 AteLess (ate less than you thought you should) · 6 RanOut (household ran out of food)
  · 7 Hungry (hungry but did not eat) · 8 WholeDay (went a whole day without eating).
- Binary FAO FIES: map yes→True, no→False; "Ne sait pas"/"Refus"/NaN→NaN. (If a survey records a
  frequency instead of yes/no, binarize: any occurrence→True. Note it.)
- Document the **recall period** per country (EHCVM = last 12 months).

## EHCVM §8A pattern (Benin, Burkina_Faso, CotedIvoire, Mali, Senegal, Togo, Guinea-Bissau, Niger)
- Source: `s08a_me_<iso>YYYY.dta` (Section 8A 'Sécurité alimentaire'). Items **s08aq01..s08aq08**
  map IN ORDER to Worried..WholeDay (verified: s08aq01=worried … s08aq08=whole day). Values
  Oui/Non/Ne sait pas/Refus → True/False/NaN. (s08aq07a/q08a are frequency follow-ups — IGNORE for
  the binary scale, or note.) Guinea-Bissau may use Portuguese Sim/Não.
- 2021-22 EHCVM may split §8 into sub-modules → the file/var may be `s08a_me_<iso>2021.dta` still,
  but CHECK (cf. the §20→§20a split). Wire every EHCVM wave the country has (2018-19 + 2021-22).
- index idiom: **i: [grappe, menage]**, v: grappe (match the country's roster).
- `FIES_score`: add a row-wise count function in the wave's mapping.py (count of "Oui"/True across
  the 8 item columns) — OR compute via a small script if cleaner. Document the approach.

## Steps
1. Confirm the country does NOT already declare food_security.
2. Read roster block for canonical i.
3. Load §8A file; map s08aq01..08 → the 8 canonical bool items; compute FIES_score.
4. Wire `food_security` in `_/data_scheme.yml` (index (t,i), 8 bools + FIES_score:int) + each wave's
   data_info.yml block.
5. VERIFY: `LSMS_NO_CACHE=1` build `Country('<C>').food_security()` + is_this_feature_sane.
   Confirm rows>0, index (t,i)[+v], items bool, FIES_score 0-8, 0/low orphan, report.ok True.

## Report (<260 words)
SCOPE DEVIATIONS first. Then IMPLEMENTED (files, per-wave source/item mapping, FIES_score method,
rows, is_this_feature_sane.ok) OR ABSENT (data reason). Do not commit.

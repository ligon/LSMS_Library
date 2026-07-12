# Implementation brief — life_satisfaction (NEW feature), #331 feature 2 (2026-06-07)

Branch `feature/life-satisfaction` in the MAIN checkout. Venv ./.venv/bin/python.

## HARD RULES
- Read data ONLY via `from lsms_library.local_tools import get_dataframe`.
  **NEVER `dvc pull`/`dvc fetch` CLI** (deadlocks). get_dataframe is lock-free.
- Edit ONLY your ASSIGNED country's dir. No other country/test/baseline/pyproject.
- Run NO git commands. Leave edits in the working tree; coordinator commits.
- Verify in this main checkout. Scan ACTUAL variable labels; don't trust names.

## Canonical life_satisfaction (NEW — long-form, the #331 'two features' design)
- index **(t, i, Domain)** — one row per (household, life-domain). i MUST match the roster i.
- Column: `Satisfaction` (str) — the ordinal satisfaction rating, human-readable (keep the
  survey's native ordinal labels, e.g. "Very satisfied"…"Very dissatisfied", or "Satisfied"/
  "Neutral"/"Dissatisfied"; do NOT invent a numeric scale — preserve the label).
- **Domain** = the life aspect, mapped to a canonical name. Use these where they fit:
  `Overall`, `Health`, `Finances`, `Housing`, `Food`, `Job`, `Education`, `Safety`,
  `Environment`, `Community`. (A single overall-life-satisfaction item → Domain='Overall'.
  A subjective financial-position / economic-situation rating → Domain='Finances'.)
- **Household level.** If the module is INDIVIDUAL-level (e.g. Iraq §23 ~7 rows/HH; Timor 2001
  S13A), reduce to the household head (or a documented representative); note the choice.
- This is the SATISFACTION-RATING construct (NOT the Cantril/welfare ladder — that's the separate
  `subjective_well_being` feature, already wired). Don't duplicate ladder items here.
- Do NOT emit `v` (joined from sample()).

## Steps
1. Confirm the country does NOT already declare life_satisfaction.
2. Read roster block for canonical i. Identify the satisfaction module + its items.
3. Map each satisfaction item → a (Domain, Satisfaction) row. Reshape WIDE items → LONG
   (t, i, Domain). Multi-wave: wire each wave with the module. (Script path if a reshape/
   individual-reduction is needed; YAML only if already one-row-per-(HH,domain).)
4. VERIFY: `LSMS_NO_CACHE=1` build `Country('<C>').life_satisfaction()` + is_this_feature_sane.
   Confirm rows>0, index (t,i,Domain), Satisfaction populated, low orphan vs roster, report.ok True.
   (is_this_feature_sane: a 3-level index gives a benign has_household_index warn — OK.)

## Report (<260 words)
SCOPE DEVIATIONS first. Then IMPLEMENTED (files, per-wave source, the item→Domain map, unit
reduction if any, rows, is_this_feature_sane.ok) OR ABSENT (data reason). Do not commit.

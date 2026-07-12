# Implementation brief — non-FIES food-security families, #332 (2026-06-07)

Branch `feature/food-security-nonfies`. Venv ./.venv/bin/python.
Three per-family features (the #332 "per-family features" decision). Each agent does ONE
country + its assigned family.

## HARD RULES
- get_dataframe ONLY (lock-free); NEVER dvc pull/fetch CLI. Edit ONLY your country dir.
- No git. Leave edits in tree. Verify in main checkout. Scan ACTUAL labels; don't trust names.
- i MUST match the country's household_roster i. Do NOT emit v (joined from sample()).

## Family A — `food_security_hfias` (HFIAS 9-item Access Scale)
- index **(t, i)**. The 9 standard HFIAS occurrence questions, each coded as ordinal
  frequency 0-3: columns `HFIAS1`..`HFIAS9` (int; 0=Never, 1=Rarely(1-2×/4wk),
  2=Sometimes(3-10×), 3=Often(>10×)). Map the survey's occurrence(yes/no)+frequency pair
  to 0-3 (no→0; yes+freq→1/2/3). Plus `HFIAS_score` (int 0-27 = sum of the 9) and
  `HFIAS_category` (str: 'Food secure'/'Mildly'/'Moderately'/'Severely food insecure',
  per the FANTA algorithm). Recall: 4 weeks (30 days).
- Countries: **Nigeria** (HFIAS/coping in the Food Security section, post-planting &/or
  post-harvest — find the 9 items), **Tajikistan** (2009 m8a HFIAS 9-item M8AQ11.. + 2007).

## Family B — `food_coping` (coping-strategies / rCSI)
- index **(t, i, Strategy)**, column `Days` (int 0-7 = days in last 7 the HH used the
  strategy). One row per (HH, coping strategy). Map each survey's coping items to a Strategy
  name; use these canonical names where they fit: `LessPreferred`, `BorrowFood`,
  `LimitPortion`, `RestrictAdults`, `ReduceMeals` (the 5 rCSI strategies) + keep survey-
  specific ones descriptively. Recall: last 7 days (note if different).
- Countries: **Tanzania** (§H `hh_h02a..` coping battery — every wave incl. multi-round
  2008-15; head/HH level), **Nepal** (NLSS coping/adequacy), **Ethiopia** (W1-3 2011-12/
  2013-14/2015-16 §7/§8 rCSI day-count battery — NOT W4-5 which are FIES already wired).

## Family C — `months_food_inadequate` (months of inadequate provisioning)
- index **(t, i)**. `MonthsInadequate` (int 0-12 = number of months in last 12 the HH could
  not meet food needs) + `AnyInadequate` (bool). If the survey lists WHICH months, you MAY
  also emit a companion but the (t,i) count is the core. Recall: last 12 months.
- Countries: **India** (§8A vulnerability `v08a*` months-inadequate), **Liberia** (§16
  sect16a_public S16_* months faced food shortage), **Uganda** (GSEC17 food-deprivation:
  months/which-months — every UNPS wave), **Timor-Leste** (2001 §13C `S13C` number of months
  with inadequate food provision).

## Steps
1. Confirm the country doesn't already declare your feature.
2. Read roster for i; load the module; map items to the family schema above (binarize/score
   faithfully; if the survey's instrument doesn't actually match the family, report ABSENT).
3. Wire `_/data_scheme.yml` + wave data_info.yml (or a script if reshape/score needed).
4. VERIFY: `LSMS_NO_CACHE=1` build `Country('<C>').<feature>()` + is_this_feature_sane;
   rows>0, correct index, low orphan, report.ok True.

## Report (<260 words)
SCOPE DEVIATIONS first. Then IMPLEMENTED (feature name, files, per-wave source/mapping/score,
rows, is_this_feature_sane.ok) OR ABSENT (reason). Do not commit.

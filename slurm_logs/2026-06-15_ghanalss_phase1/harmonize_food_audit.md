# GhanaLSS `harmonize_food` food-item mapping audit

Date: 2026-06-15 · Worktree: `/local/job35035156/wt-harmonize` · HEAD `64f3d188`
Scope: investigation + a reviewable draft extension of the food-item mapping
(DESIGN D6). No wave scripts touched; mapping/data only. Do NOT commit.

## 1. Where `harmonize_food` lives + how each wave reads it

`harmonize_food` is a **per-wave** Org table inside each wave's
`GhanaLSS/<wave>/_/categorical_mapping.org` (there is no single country-level
copy). Columns: `Preferred Label | Aggregate Label | Code_9b | Label_9b |
Code_8h | Label_8h` for 1991–2016; the two 1980s waves use `Code_12A/Label_12A
| Code_12B/Label_12B`. The wave's `food_acquired.py` reads it and **drops rows
whose code does not map to a non-blank Preferred Label** (`j != ''`).

| Wave    | Code scheme (purch / prod) | How the script reads it                               |
|---------|----------------------------|-------------------------------------------------------|
| 1987-88 | Code_12A / Code_12B        | `df_from_orgfile(name='harmonize_food')`, `.replace`  |
| 1988-89 | Code_12A / Code_12B        | `df_from_orgfile(name='harmonize_food')`, `.replace`  |
| 1991-92 | Code_9b / Code_8h          | `get_categorical_mapping('harmonize_food', Code_9b/8h)` |
| 1998-99 | Code_9b / Code_8h          | `df_from_orgfile`, `.isin(food_9b.values())` filter   |
| 2005-06 | Code_9b / Code_8h          | `df_from_orgfile`, `.isin(...)` filter                |
| 2012-13 | Code_9b / Code_8h          | `get_categorical_mapping('harmonize_food', Code_9b/8h)` |
| 2016-17 | Code_9b / Code_8h          | `get_categorical_mapping('harmonize_food', Code_9b/8h)` |

## 2. Current code coverage (distinct codes present in source vs. mapped)

Row counts are visit-stacked source rows (per the script's reshape grain).

| Wave    | Module          | codes | rows      | mapped rows | %     | unmapped codes / rows |
|---------|-----------------|-------|-----------|-------------|-------|-----------------------|
| 1987-88 | purchase (12A)  | 61    | 77,111    | 77,111      | 100.0 | 0 / 0                 |
| 1987-88 | production (12B)| 49    | 20,808    | 20,808      | 100.0 | 0 / 0                 |
| 1988-89 | purchase (12A)  | 61    | 78,545    | 78,545      | 100.0 | 0 / 0                 |
| 1988-89 | production (12B)| 49    | 23,374    | 23,374      | 100.0 | 0 / 0                 |
| 1991-92 | purchase (9b)   | 107   | 104,068   | 104,068     | 100.0 | 0 / 0                 |
| 1991-92 | production (8h) | 56    | 32,052    | 32,052      | 100.0 | 0 / 0                 |
| 1998-99 | purchase (9b)   | 108   | 167,803   | 136,568     | 81.4  | 18 / 31,235           |
| 1998-99 | production (8h) | 58    | 44,603    | 44,603      | 100.0 | 0 / 0                 |
| 2005-06 | purchase (9b)   | 167   | 1,436,956 | 1,015,798   | 70.7  | 49 / 421,158          |
| 2005-06 | production (8h) | 63    | 424,052   | 424,052     | 100.0 | 0 / 0                 |
| 2012-13 | purchase (9b)   | 170   | 2,851,237 | 2,012,639   | 70.6  | 50 / 838,598          |
| 2012-13 | production (8h) | 63    | 1,056,265 | 1,056,265   | 100.0 | 0 / 0                 |
| 2016-17 | purchase (9b)   | 485   | 6,762,748 | 4,448,136   | 65.8  | 166 / 2,314,612       |
| 2016-17 | production (8h) | 67    | 934,527   | 934,527     | 100.0 | 0 / 0                 |

**Headline:** every PRODUCTION (8h/12B) module is already 100% mapped. All drops
are on the PURCHASE side, and — once classified — are almost entirely the
**non-food expenditure section** that section 9B also records (9B is a combined
food + non-food purchase module). The unmapped *rows* are large because the
non-food block is large, NOT because real food is being lost.

## 3. Classification of every unmapped purchase code

Method: read each code's Stata **value label** from the FULL `.dta`
(`convert_categoricals=True`; the `g7sec9b_small.dta` lacks labels, so the full
file was used per the 2016-17 script's own note). Codes whose value label is
just the numeric code echoed back (no real label in the data) → UNRESOLVABLE.

### 1998-99 — 18 unmapped codes / 31,235 rows → **all UNRESOLVABLE**
Codes **108–125** carry NO value label in `SEC9B.DTA` (the label is the number
echoed, e.g. `108.0`). The wave's `fdexpcd` codebook table only documents codes
up to 107 (it ends at "107 Other Tobacco Products"); 108–125 are not in it.
Per the units-skill discipline ("never invent a label for an undecodable code")
these are **left undecoded and flagged for a codebook-PDF pass** — NOT guessed.
(Documentation PDFs exist: `1998-99/Documentation/GHA_1998_GLSS_Dataentry_EN.pdf`,
`glss4_report.pdf` — a future pass should decode 108–125 from these.)

### 2005-06 — 49 codes / 421,158 rows → **all NON-FOOD**
Codes 165–277: refuse disposal, public toilets, charcoal/firewood/kerosene/
petrol/diesel, soaps & bleaches & disinfectants, insecticides, matches, candles,
medicines (painkillers, antibiotics, anti-malarials, condoms), transport (rail,
bus, trotro/taxi, ferries, porters, luggage), postage/telephone/internet,
lotteries, exercise books/textbooks/newspapers/magazines, barbers/wigs/grooming,
toothpaste/razor/combs. Correctly dropped.

### 2012-13 — 50 codes / 838,598 rows → **all NON-FOOD**
Codes 165–277, same non-food block as 2005-06 plus 165 "other tobacco products".
Correctly dropped.

### 2016-17 — 166 codes / 2,314,612 rows → **1 FOOD, 165 NON-FOOD**
Every unmapped code is **≥ 650**, i.e. inside the non-food expenditure section
(food codes run 1–~647). The one exception:
- **650 "Kola Nuts" — FOOD** (13,944 rows). It sits immediately after the
  beer/tobacco stimulant group (635–647) and before the utilities block (651+
  refuse/fuel). Kola nut is a consumed caffeine nut. It was already a *row* in
  the table but with a **blank Preferred Label** → dropped.

All 165 others are NON-FOOD and were already present in the table with blank
Preferred Label (i.e. the original author deliberately listed-and-dropped them):
fuels (655–662), detergent/soap brands (665–691: Omo, Ariel, Sunlight, Lux,
Rexona…), bleach/disinfectant/insecticide/matches/candles/brooms (692–699),
paper/cleaning (703–708), medicines & medical (710–725, 800–810), transport
(820–851), phone/internet (853–866), print/copy services (868–870), recreation/
lottery (871–877), newspapers/magazines/books/stationery (880–903), hotel/
grooming (923–929), and the **929–990 brand block** (see §4). 905/906 "Tinned
Dog/Cat Food" = pet food → NON-FOOD. 719 "Oral rehydration salts" = medical →
NON-FOOD. 941/942/943 "Orange/Tango/Rose" are scent brands in the toilet-roll/
grooming block (between 939 Wigs and 944 Other toilet roll) → NON-FOOD.

## 4. The 929–990 verdict: **NON-FOOD** (correctly dropped)

The 2016-17 codes 929–990 are **brand-level non-food** consumer items, already
present in the table with blank Preferred Label. Evidence (Stata value labels):
929 Other personal grooming; 935–939 hair (Darling, Auntie lizy, Ultra, Wigs);
941–944 air-freshener/toilet-roll scents (Orange, Tango, Rose, Other toilet
roll); 949–952 toothpaste (Pepsodent, Close up, Colgate); 960–964 deodorant
(Blue ice, Sure, Rexona, Fa); 968–971 razors (Bic, Lord, Gillette); 975–977
skin (Queen Elizabeth, Vasline); 980–983 skin powder; 984–987 sanitary
(Tampons, Always, Yazz, Other sanitary pad); 990 "Fee for money transfer".
None is food. They are correctly excluded; **no codebook PDF is needed** — the
data's own value labels resolve them.

## 5. Draft mapping extension

**One FOOD code added** (the only genuine food among all unmapped codes across
all 7 waves):

| Wave    | File / table                               | Code | Label_9b   | Preferred Label | Aggregate Label |
|---------|--------------------------------------------|------|------------|-----------------|-----------------|
| 2016-17 | `2016-17/_/categorical_mapping.org` `harmonize_food` | 650 | Kola Nuts | **Kola Nut**    | Pulses, Nuts    |

Preferred Label `Kola Nut` and Aggregate Label `Pulses, Nuts` are **reused** —
identical to the existing 1991-92 / 1998-99 `Kola Nut` rows (Code_9b 24). No new
label coined; existing format/columns/ordering preserved; no row removed/renamed.

(`food_items.org` master carries both `Kola Nut` and a separate `Cola Nut` —
the latter used by 2005-06/2012-13. `Kola Nut` is the spelling closest to the
2016-17 source label "Kola Nuts" and the one the older 9b waves already use.)

## 6. VERIFY — 2016-17 purchase retention before → after

```
total purchase rows                : 6,762,748
retained BEFORE (650 blank)        : 4,448,136  (65.77%)
retained AFTER  (650 = Kola Nut)   : 4,462,080  (65.98%)
rows recovered as food             : 13,944
existing non-blank mappings changed: []   (none)
codes newly mapped                 : ['650']
```

The remaining ~34% dropped purchase rows are the **legitimately non-food**
section-9B block (fuels, soaps, transport, medicines, etc.) — correctly excluded.
The food-item mapping for purchases was already essentially complete; Kola Nuts
was the sole missing real food.

## 7. Lists for a human / codebook pass

**UNRESOLVABLE (need 1998-99 codebook PDF — do NOT guess):**
1998-99 purchase codes **108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118,
119, 120, 121, 122, 123, 124, 125** (31,235 rows total). No value label in
`SEC9B.DTA`; `fdexpcd` codebook ends at 107. Decode from
`1998-99/Documentation/GHA_1998_GLSS_Dataentry_EN.pdf` / `glss4_report.pdf`.

**AMBIGUOUS:** none. (650 Kola Nut resolved to FOOD; 941/942/943 and 905/906
resolved to NON-FOOD from value-label + positional evidence.)

## Files edited (uncommitted, for coordinator review)
- `lsms_library/countries/GhanaLSS/2016-17/_/categorical_mapping.org` — 1 line
  (code 650 Preferred/Aggregate Label populated; additions only).
- `harmonize_food_audit.md` — this report.

(Helper scripts left at worktree root: `enum_codes.py`, `enum_8788.py`,
`classify.py`, `verify_retention.py`; outputs `enum_codes_modern.csv`,
`unmapped_classified.csv`. Not part of the mapping deliverable.)

---
name: skill
description: "Skill for the _ area of LSMS_Library. 75 symbols across 22 files."
---

# _

75 symbols | 22 files | Cohesion: 100%

## When to Use

- Working with code in `lsms_library/`
- Understanding how fix_food_labels, harmonized_food_labels, prices_and_units work
- Modifying _-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `lsms_library/countries/Ethiopia/_/ethiopia.py` | harmonized_food_labels, _sum_expenditures_from_file, prices_and_units, food_expenditures, nonfood_expenditures (+6) |
| `lsms_library/countries/Tanzania/_/tanzania.py` | harmonized_food_labels, _sum_expenditures_from_file, prices_and_units, food_expenditures, food_quantities (+4) |
| `lsms_library/countries/GhanaLSS/_/ghanalss.py` | change_id, concate_id, _sum_expenditures_from_file, food_expenditures, food_quantities (+3) |
| `lsms_library/countries/GhanaSPS/_/ghanasps.py` | _sum_expenditures_from_file, food_expenditures, food_quantities, _household_roster_from_file, age_sex_composition (+1) |
| `lsms_library/countries/Togo/_/togo.py` | _sum_expenditures_from_file, food_expenditures, food_quantities, _household_roster_from_file, age_sex_composition |
| `lsms_library/countries/Malawi/_/malawi.py` | _household_roster_from_df, age_sex_composition, get_household_characteristics, _extract_kg_conversion, handling_unusual_units |
| `lsms_library/countries/Uganda/_/uganda.py` | harmonized_unit_labels, harmonized_food_labels, food_acquired, nonfood_expenditures |
| `lsms_library/countries/Niger/_/niger.py` | _household_roster_from_df, age_sex_composition, _safe_int, fill_func |
| `lsms_library/countries/Ethiopia/_/food_acquired.py` | fix_food_labels, id_walk |
| `lsms_library/countries/Senegal/_/senegal.py` | _household_roster_from_df, age_sex_composition |

## Entry Points

Start here when exploring this area:

- **`fix_food_labels`** (Function) — `lsms_library/countries/Ethiopia/_/food_acquired.py:12`
- **`harmonized_food_labels`** (Function) — `lsms_library/countries/Ethiopia/_/ethiopia.py:30`
- **`prices_and_units`** (Function) — `lsms_library/countries/Ethiopia/_/ethiopia.py:153`
- **`food_expenditures`** (Function) — `lsms_library/countries/Ethiopia/_/ethiopia.py:250`
- **`nonfood_expenditures`** (Function) — `lsms_library/countries/Ethiopia/_/ethiopia.py:264`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `fix_food_labels` | Function | `lsms_library/countries/Ethiopia/_/food_acquired.py` | 12 |
| `harmonized_food_labels` | Function | `lsms_library/countries/Ethiopia/_/ethiopia.py` | 30 |
| `prices_and_units` | Function | `lsms_library/countries/Ethiopia/_/ethiopia.py` | 153 |
| `food_expenditures` | Function | `lsms_library/countries/Ethiopia/_/ethiopia.py` | 250 |
| `nonfood_expenditures` | Function | `lsms_library/countries/Ethiopia/_/ethiopia.py` | 264 |
| `food_quantities` | Function | `lsms_library/countries/Ethiopia/_/ethiopia.py` | 276 |
| `harmonized_food_labels` | Function | `lsms_library/countries/Tanzania/_/tanzania.py` | 159 |
| `prices_and_units` | Function | `lsms_library/countries/Tanzania/_/tanzania.py` | 280 |
| `food_expenditures` | Function | `lsms_library/countries/Tanzania/_/tanzania.py` | 314 |
| `food_quantities` | Function | `lsms_library/countries/Tanzania/_/tanzania.py` | 325 |
| `harmonized_unit_labels` | Function | `lsms_library/countries/Uganda/_/uganda.py` | 27 |
| `harmonized_food_labels` | Function | `lsms_library/countries/Uganda/_/uganda.py` | 37 |
| `food_acquired` | Function | `lsms_library/countries/Uganda/_/uganda.py` | 47 |
| `nonfood_expenditures` | Function | `lsms_library/countries/Uganda/_/uganda.py` | 101 |
| `id_walk` | Function | `lsms_library/countries/GhanaLSS/_/household_roster.py` | 13 |
| `change_id` | Function | `lsms_library/countries/GhanaLSS/_/ghanalss.py` | 321 |
| `concate_id` | Function | `lsms_library/countries/GhanaLSS/_/ghanalss.py` | 400 |
| `id_walk` | Function | `lsms_library/countries/GhanaLSS/_/food_acquired.py` | 25 |
| `food_expenditures` | Function | `lsms_library/countries/Togo/_/togo.py` | 132 |
| `food_quantities` | Function | `lsms_library/countries/Togo/_/togo.py` | 145 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Get_household_characteristics → _household_roster_from_df` | intra_community | 3 |
| `Id_walk → Concate_id` | intra_community | 3 |
| `Id_walk → Concate_id` | intra_community | 3 |

## How to Explore

1. `gitnexus_context({name: "fix_food_labels"})` — see callers and callees
2. `gitnexus_query({query: "_"})` — find related execution flows
3. Read key files listed above for implementation details

---
name: tanzania-1497
description: "Skill for the Tanzania_1497 area of LSMS_Library. 23 symbols across 4 files."
---

# Tanzania_1497

23 symbols | 4 files | Cohesion: 91%

## When to Use

- Working with code in `bench/`
- Understanding how cover, roster, select work
- Modifying tanzania_1497-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `bench/research/tanzania_1497/test_head_tracking.py` | cover, roster, select, test_current_head_with_previous_member_one_selects_split_over_frame, test_previous_head_non_head_presence_is_distinct_from_current_selector (+7) |
| `bench/research/tanzania_1497/classify.py` | location_kind, link_kind, nonblank, main, locations |
| `bench/research/tanzania_1497/capture.py` | digest, attach_build_tripwires, confined_dta_probe, main |
| `lsms_library/countries/Tanzania/_/tanzania.py` | map_08_15, _map_with_head_tracking |

## Entry Points

Start here when exploring this area:

- **`cover`** (Function) — `bench/research/tanzania_1497/test_head_tracking.py:28`
- **`roster`** (Function) — `bench/research/tanzania_1497/test_head_tracking.py:32`
- **`select`** (Function) — `bench/research/tanzania_1497/test_head_tracking.py:36`
- **`test_current_head_with_previous_member_one_selects_split_over_frame`** (Function) — `bench/research/tanzania_1497/test_head_tracking.py:40`
- **`test_previous_head_non_head_presence_is_distinct_from_current_selector`** (Function) — `bench/research/tanzania_1497/test_head_tracking.py:46`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `cover` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 28 |
| `roster` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 32 |
| `select` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 36 |
| `test_current_head_with_previous_member_one_selects_split_over_frame` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 40 |
| `test_previous_head_non_head_presence_is_distinct_from_current_selector` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 46 |
| `test_no_previous_member_one_current_head_fallback_depends_on_cover_order` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 54 |
| `test_multiple_eligible_current_heads_are_not_silently_classified_unique` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 60 |
| `test_numeric_head_code_requires_own_label_dictionary_before_selector` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 66 |
| `test_non_split_keeps_simple_link_without_inventing_head_evidence` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 73 |
| `test_missing_previous_link_is_excluded_without_invented_history` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 78 |
| `test_native_selector_conserves_input_payloads_and_roster_order_irrelevant` | Function | `bench/research/tanzania_1497/test_head_tracking.py` | 83 |
| `location_kind` | Function | `bench/research/tanzania_1497/classify.py` | 15 |
| `link_kind` | Function | `bench/research/tanzania_1497/classify.py` | 26 |
| `nonblank` | Function | `bench/research/tanzania_1497/classify.py` | 31 |
| `main` | Function | `bench/research/tanzania_1497/classify.py` | 35 |
| `locations` | Function | `bench/research/tanzania_1497/classify.py` | 145 |
| `map_08_15` | Function | `lsms_library/countries/Tanzania/_/tanzania.py` | 29 |
| `digest` | Function | `bench/research/tanzania_1497/capture.py` | 12 |
| `attach_build_tripwires` | Function | `bench/research/tanzania_1497/capture.py` | 19 |
| `confined_dta_probe` | Function | `bench/research/tanzania_1497/capture.py` | 28 |

## Connected Areas

| Area | Connections |
|------|-------------|
| _ | 3 calls |
| Tests | 2 calls |

## How to Explore

1. `context({name: "cover"})` — see callers and callees
2. `query({search_query: "tanzania_1497"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`

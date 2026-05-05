---
name: skunkworks
description: "Skill for the SkunkWorks area of LSMS_Library. 59 symbols across 6 files."
---

# SkunkWorks

59 symbols | 6 files | Cohesion: 100%

## When to Use

- Working with code in `SkunkWorks/`
- Understanding how make_constructor, register_yaml_tags, load_yaml_with_tags work
- Modifying skunkworks-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `SkunkWorks/yaml_tags_implementation.py` | TypedColumn, RuralColumn, UrbanColumn, RegionColumn, DistrictColumn (+18) |
| `SkunkWorks/test_tag_as_name_final.py` | TypedColumn, RuralColumn, UrbanColumn, SexColumn, AgeColumn (+5) |
| `SkunkWorks/yaml_tags_prototype.py` | TypedColumn, DatetimeColumn, BinaryColumn, RuralColumn, UrbanColumn (+4) |
| `SkunkWorks/test_yaml_tags.py` | TypedColumn, DatetimeColumn, RuralColumn, UrbanColumn, SexColumn (+1) |
| `SkunkWorks/test_tag_as_name.py` | TypedColumn, RuralColumn, UrbanColumn, SexColumn, DateTimeColumn (+1) |
| `SkunkWorks/test_list_based_tags.py` | TypedColumn, RuralColumn, SexColumn, AgeColumn, DateTimeColumn |

## Entry Points

Start here when exploring this area:

- **`make_constructor`** (Function) — `SkunkWorks/yaml_tags_implementation.py:239`
- **`register_yaml_tags`** (Function) — `SkunkWorks/yaml_tags_implementation.py:278`
- **`load_yaml_with_tags`** (Function) — `SkunkWorks/yaml_tags_implementation.py:293`
- **`register_yaml_tags`** (Function) — `SkunkWorks/yaml_tags_prototype.py:201`
- **`load_yaml_with_tags`** (Function) — `SkunkWorks/yaml_tags_prototype.py:215`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `TypedColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 17 |
| `RuralColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 48 |
| `UrbanColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 65 |
| `RegionColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 87 |
| `DistrictColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 95 |
| `SexColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 107 |
| `AgeColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 127 |
| `RelationColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 135 |
| `DateTimeColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 147 |
| `YearColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 159 |
| `MonthColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 167 |
| `DayColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 175 |
| `IntColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 187 |
| `FloatColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 194 |
| `StringColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 201 |
| `CategoryColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 208 |
| `HouseholdIdColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 219 |
| `PersonIdColumn` | Class | `SkunkWorks/yaml_tags_implementation.py` | 227 |
| `TypedColumn` | Class | `SkunkWorks/test_tag_as_name_final.py` | 8 |
| `RuralColumn` | Class | `SkunkWorks/test_tag_as_name_final.py` | 21 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Load_yaml_with_tags → Make_constructor` | intra_community | 3 |

## How to Explore

1. `gitnexus_context({name: "make_constructor"})` — see callers and callees
2. `gitnexus_query({query: "skunkworks"})` — find related execution flows
3. Read key files listed above for implementation details

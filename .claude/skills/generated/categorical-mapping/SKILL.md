---
name: categorical-mapping
description: "Skill for the Categorical_mapping area of LSMS_Library. 10 symbols across 2 files."
---

# Categorical_mapping

10 symbols | 2 files | Cohesion: 100%

## When to Use

- Working with code in `lsms_library/`
- Understanding how regularize_string, preprocess, get_label_vector work
- Modifying categorical_mapping-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `lsms_library/categorical_mapping/ai_agent.py` | get_payload, get_response, parse_information_with_gpt, convert_df_to_str, food_label_prompt (+1) |
| `lsms_library/categorical_mapping/categorical_mapping_helper.py` | regularize_string, preprocess, get_label_vector, get_cosine_similarity |

## Entry Points

Start here when exploring this area:

- **`regularize_string`** (Function) — `lsms_library/categorical_mapping/categorical_mapping_helper.py:40`
- **`preprocess`** (Function) — `lsms_library/categorical_mapping/categorical_mapping_helper.py:75`
- **`get_label_vector`** (Function) — `lsms_library/categorical_mapping/categorical_mapping_helper.py:94`
- **`get_cosine_similarity`** (Function) — `lsms_library/categorical_mapping/categorical_mapping_helper.py:103`
- **`get_payload`** (Function) — `lsms_library/categorical_mapping/ai_agent.py:36`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `regularize_string` | Function | `lsms_library/categorical_mapping/categorical_mapping_helper.py` | 40 |
| `preprocess` | Function | `lsms_library/categorical_mapping/categorical_mapping_helper.py` | 75 |
| `get_label_vector` | Function | `lsms_library/categorical_mapping/categorical_mapping_helper.py` | 94 |
| `get_cosine_similarity` | Function | `lsms_library/categorical_mapping/categorical_mapping_helper.py` | 103 |
| `get_payload` | Function | `lsms_library/categorical_mapping/ai_agent.py` | 36 |
| `get_response` | Function | `lsms_library/categorical_mapping/ai_agent.py` | 50 |
| `parse_information_with_gpt` | Function | `lsms_library/categorical_mapping/ai_agent.py` | 63 |
| `convert_df_to_str` | Function | `lsms_library/categorical_mapping/ai_agent.py` | 84 |
| `food_label_prompt` | Function | `lsms_library/categorical_mapping/ai_agent.py` | 88 |
| `unit_prompt` | Function | `lsms_library/categorical_mapping/ai_agent.py` | 120 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Get_cosine_similarity → Regularize_string` | intra_community | 4 |

## How to Explore

1. `gitnexus_context({name: "regularize_string"})` — see callers and callees
2. `gitnexus_query({query: "categorical_mapping"})` — find related execution flows
3. Read key files listed above for implementation details

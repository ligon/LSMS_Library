---
name: scripts
description: "Skill for the Scripts area of LSMS_Library. 59 symbols across 17 files."
---

# Scripts

59 symbols | 17 files | Cohesion: 78%

## When to Use

- Working with code in `scripts/`
- Understanding how data_method_kwargs, check_kwargs, check_unused work
- Modifying scripts-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `scripts/backfill_wave_provenance.py` | held_changed, changed_by_write, unchanged_by_write, _wave_dirs, _may_write (+6) |
| `scripts/api_surface_audit.py` | _identifier_census, data_method_kwargs, check_kwargs, check_unused, check_untracked_inputs (+3) |
| `scripts/promote_recall_records.py` | basis_counts, _clean, _num, validate, render (+1) |
| `scripts/refresh_fct.py` | _module, _sha, _country_inputs, _bounded_requests, refresh (+1) |
| `scripts/extract_sampling_evidence.py` | _sentences, from_population_yml, from_contents_org, main |
| `scripts/promote_population_records.py` | api_waves, render, build, main |
| `slurm_logs/isa_backlog_2026-10-21/1245-id-walk/scripts/harness.py` | _classify, _site, wrapped_walk, wrapped_fin |
| `scripts/sampling_fingerprint.py` | _fingerprint_wave, fingerprint_country, main |
| `scripts/check_uganda_invariance.py` | check, main |
| `tests/test_wave_provenance.py` | test_default_review_artifact_lives_outside_countries_root, test_dropped_by_write_refuses_a_lost_duplicate_or_empty_key |

## Entry Points

Start here when exploring this area:

- **`data_method_kwargs`** (Function) — `scripts/api_surface_audit.py:90`
- **`check_kwargs`** (Function) — `scripts/api_surface_audit.py:178`
- **`check_unused`** (Function) — `scripts/api_surface_audit.py:201`
- **`check_untracked_inputs`** (Function) — `scripts/api_surface_audit.py:229`
- **`main`** (Function) — `scripts/api_surface_audit.py:269`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `data_method_kwargs` | Function | `scripts/api_surface_audit.py` | 90 |
| `check_kwargs` | Function | `scripts/api_surface_audit.py` | 178 |
| `check_unused` | Function | `scripts/api_surface_audit.py` | 201 |
| `check_untracked_inputs` | Function | `scripts/api_surface_audit.py` | 229 |
| `main` | Function | `scripts/api_surface_audit.py` | 269 |
| `basis_counts` | Function | `scripts/promote_recall_records.py` | 67 |
| `validate` | Function | `scripts/promote_recall_records.py` | 95 |
| `render` | Function | `scripts/promote_recall_records.py` | 136 |
| `main` | Function | `scripts/promote_recall_records.py` | 174 |
| `refresh` | Function | `scripts/refresh_fct.py` | 149 |
| `main` | Function | `scripts/refresh_fct.py` | 223 |
| `render_source_org` | Function | `lsms_library/provenance.py` | 752 |
| `held_changed` | Function | `scripts/backfill_wave_provenance.py` | 744 |
| `changed_by_write` | Function | `scripts/backfill_wave_provenance.py` | 843 |
| `unchanged_by_write` | Function | `scripts/backfill_wave_provenance.py` | 875 |
| `pytest_terminal_summary` | Function | `conftest.py` | 79 |
| `check` | Function | `scripts/check_uganda_invariance.py` | 17 |
| `main` | Function | `scripts/check_uganda_invariance.py` | 30 |
| `summary_lines` | Function | `tests/uganda_invariance_reporting.py` | 47 |
| `run_country` | Function | `scripts/backfill_wave_provenance.py` | 1170 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Run_country → _body` | cross_community | 6 |
| `Run_country → Parse_id_list` | cross_community | 6 |
| `Run_country → _regions` | cross_community | 5 |
| `Run_country → One` | cross_community | 5 |
| `Run_country → Source_org_path` | cross_community | 4 |
| `Main → _clean` | intra_community | 4 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Lsms_library | 14 calls |
| Tests | 11 calls |

## How to Explore

1. `context({name: "data_method_kwargs"})` — see callers and callees
2. `query({search_query: "scripts"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`

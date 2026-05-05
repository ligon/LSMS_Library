---
name: feature-scan-2026-04-13
description: "Skill for the Feature_scan_2026-04-13 area of LSMS_Library. 11 symbols across 3 files."
---

# Feature_scan_2026-04-13

11 symbols | 3 files | Cohesion: 100%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how fmt_int, load_recs_for_feature, prior_audit_exists work
- Modifying feature_scan_2026-04-13-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | fmt_int, load_recs_for_feature, prior_audit_exists, get_prior_audit_summary, make_memo (+2) |
| `slurm_logs/feature_scan_2026-04-13/run_feature_scan.py` | countries_with_feature, main, worker |
| `slurm_logs/feature_scan_2026-04-13/probe_one_feature.py` | probe |

## Entry Points

Start here when exploring this area:

- **`fmt_int`** (Function) — `slurm_logs/feature_scan_2026-04-13/aggregate_features.py:21`
- **`load_recs_for_feature`** (Function) — `slurm_logs/feature_scan_2026-04-13/aggregate_features.py:30`
- **`prior_audit_exists`** (Function) — `slurm_logs/feature_scan_2026-04-13/aggregate_features.py:37`
- **`get_prior_audit_summary`** (Function) — `slurm_logs/feature_scan_2026-04-13/aggregate_features.py:41`
- **`make_memo`** (Function) — `slurm_logs/feature_scan_2026-04-13/aggregate_features.py:54`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `fmt_int` | Function | `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | 21 |
| `load_recs_for_feature` | Function | `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | 30 |
| `prior_audit_exists` | Function | `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | 37 |
| `get_prior_audit_summary` | Function | `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | 41 |
| `make_memo` | Function | `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | 54 |
| `make_summary` | Function | `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | 266 |
| `main` | Function | `slurm_logs/feature_scan_2026-04-13/aggregate_features.py` | 356 |
| `countries_with_feature` | Function | `slurm_logs/feature_scan_2026-04-13/run_feature_scan.py` | 34 |
| `main` | Function | `slurm_logs/feature_scan_2026-04-13/run_feature_scan.py` | 76 |
| `worker` | Function | `slurm_logs/feature_scan_2026-04-13/run_feature_scan.py` | 52 |
| `probe` | Function | `slurm_logs/feature_scan_2026-04-13/probe_one_feature.py` | 18 |

## Execution Flows

| Flow | Type | Steps |
|------|------|-------|
| `Main → Load_recs_for_feature` | intra_community | 3 |
| `Main → Fmt_int` | intra_community | 3 |
| `Main → Prior_audit_exists` | intra_community | 3 |
| `Main → Get_prior_audit_summary` | intra_community | 3 |

## How to Explore

1. `gitnexus_context({name: "fmt_int"})` — see callers and callees
2. `gitnexus_query({query: "feature_scan_2026-04-13"})` — find related execution flows
3. Read key files listed above for implementation details

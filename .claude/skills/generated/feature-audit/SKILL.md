---
name: feature-audit
description: "Skill for the Feature_audit area of LSMS_Library. 23 symbols across 4 files."
---

# Feature_audit

23 symbols | 4 files | Cohesion: 65%

## When to Use

- Working with code in `bench/`
- Understanding how phase1b_assembly, phase1a_country_sanity, phase2_kwargs work
- Modifying feature_audit-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `bench/feature_audit/scan.py` | _build, _assembly_invariants, phase1b_assembly, _scan_one_feature, _warnings_to_findings (+12) |
| `bench/feature_audit/audit.workflow.js` | ctx, triagePrompt, redteamPrompt |
| `bench/feature_audit/cluster.py` | cluster, main |
| `lsms_library/feature.py` | _all_known_features |

## Entry Points

Start here when exploring this area:

- **`phase1b_assembly`** (Function) — `bench/feature_audit/scan.py:547`
- **`phase1a_country_sanity`** (Function) — `bench/feature_audit/scan.py:365`
- **`phase2_kwargs`** (Function) — `bench/feature_audit/scan.py:658`
- **`resolve_features`** (Function) — `bench/feature_audit/scan.py:711`
- **`main`** (Function) — `bench/feature_audit/scan.py:867`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `phase1b_assembly` | Function | `bench/feature_audit/scan.py` | 547 |
| `phase1a_country_sanity` | Function | `bench/feature_audit/scan.py` | 365 |
| `phase2_kwargs` | Function | `bench/feature_audit/scan.py` | 658 |
| `resolve_features` | Function | `bench/feature_audit/scan.py` | 711 |
| `main` | Function | `bench/feature_audit/scan.py` | 867 |
| `run` | Function | `bench/feature_audit/scan.py` | 784 |
| `emit` | Function | `bench/feature_audit/scan.py` | 802 |
| `ctx` | Function | `bench/feature_audit/audit.workflow.js` | 83 |
| `triagePrompt` | Function | `bench/feature_audit/audit.workflow.js` | 98 |
| `redteamPrompt` | Function | `bench/feature_audit/audit.workflow.js` | 124 |
| `cluster` | Function | `bench/feature_audit/cluster.py` | 47 |
| `main` | Function | `bench/feature_audit/cluster.py` | 92 |
| `_build` | Function | `bench/feature_audit/scan.py` | 196 |
| `_assembly_invariants` | Function | `bench/feature_audit/scan.py` | 427 |
| `_scan_one_feature` | Function | `bench/feature_audit/scan.py` | 727 |
| `_warnings_to_findings` | Function | `bench/feature_audit/scan.py` | 216 |
| `_null_read_findings` | Function | `bench/feature_audit/scan.py` | 230 |
| `_scan_one_country` | Function | `bench/feature_audit/scan.py` | 304 |
| `_grand_totals` | Function | `bench/feature_audit/scan.py` | 566 |
| `_nan_fractions` | Function | `bench/feature_audit/scan.py` | 574 |

## Connected Areas

| Area | Connections |
|------|-------------|
| Tests | 3 calls |
| Lsms_library | 2 calls |
| Ghanasps | 1 calls |
| Final-cache-validation | 1 calls |

## How to Explore

1. `context({name: "phase1b_assembly"})` — see callers and callees
2. `query({search_query: "feature_audit"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`

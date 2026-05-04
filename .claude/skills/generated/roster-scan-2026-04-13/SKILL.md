---
name: roster-scan-2026-04-13
description: "Skill for the Roster_scan_2026-04-13 area of LSMS_Library. 6 symbols across 3 files."
---

# Roster_scan_2026-04-13

6 symbols | 3 files | Cohesion: 100%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how countries_with_roster, main, worker work
- Modifying roster_scan_2026-04-13-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/roster_scan_2026-04-13/run_scan.py` | countries_with_roster, main, worker |
| `slurm_logs/roster_scan_2026-04-13/aggregate.py` | fmt_int, main |
| `slurm_logs/roster_scan_2026-04-13/probe_one.py` | probe |

## Entry Points

Start here when exploring this area:

- **`countries_with_roster`** (Function) — `slurm_logs/roster_scan_2026-04-13/run_scan.py:23`
- **`main`** (Function) — `slurm_logs/roster_scan_2026-04-13/run_scan.py:64`
- **`worker`** (Function) — `slurm_logs/roster_scan_2026-04-13/run_scan.py:41`
- **`probe`** (Function) — `slurm_logs/roster_scan_2026-04-13/probe_one.py:21`
- **`fmt_int`** (Function) — `slurm_logs/roster_scan_2026-04-13/aggregate.py:11`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `countries_with_roster` | Function | `slurm_logs/roster_scan_2026-04-13/run_scan.py` | 23 |
| `main` | Function | `slurm_logs/roster_scan_2026-04-13/run_scan.py` | 64 |
| `worker` | Function | `slurm_logs/roster_scan_2026-04-13/run_scan.py` | 41 |
| `probe` | Function | `slurm_logs/roster_scan_2026-04-13/probe_one.py` | 21 |
| `fmt_int` | Function | `slurm_logs/roster_scan_2026-04-13/aggregate.py` | 11 |
| `main` | Function | `slurm_logs/roster_scan_2026-04-13/aggregate.py` | 20 |

## How to Explore

1. `gitnexus_context({name: "countries_with_roster"})` — see callers and callees
2. `gitnexus_query({query: "roster_scan_2026-04-13"})` — find related execution flows
3. Read key files listed above for implementation details

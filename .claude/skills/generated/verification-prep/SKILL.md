---
name: verification-prep
description: "Skill for the Verification-prep area of LSMS_Library. 13 symbols across 3 files."
---

# Verification-prep

13 symbols | 3 files | Cohesion: 73%

## When to Use

- Working with code in `slurm_logs/`
- Understanding how record, validate_run, wait_for_success work
- Modifying verification-prep-related functionality

## Key Files

| File | Symbols |
|------|---------|
| `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | record, test_success, test_all_non_success_conclusions_refused, test_identity_mismatches_fail_closed, test_exact_get_only_command (+4) |
| `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/ci_gate.py` | validate_run, wait_for_success |
| `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/prepare_copy.py` | digest, snapshot |

## Entry Points

Start here when exploring this area:

- **`record`** (Function) — `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py:15`
- **`validate_run`** (Function) — `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/ci_gate.py:7`
- **`wait_for_success`** (Function) — `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/ci_gate.py:27`
- **`digest`** (Function) — `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/prepare_copy.py:14`
- **`snapshot`** (Function) — `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/prepare_copy.py:18`

## Key Symbols

| Symbol | Type | File | Line |
|--------|------|------|------|
| `record` | Function | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 15 |
| `validate_run` | Function | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/ci_gate.py` | 7 |
| `wait_for_success` | Function | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/ci_gate.py` | 27 |
| `digest` | Function | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/prepare_copy.py` | 14 |
| `snapshot` | Function | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/prepare_copy.py` | 18 |
| `test_success` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 22 |
| `test_all_non_success_conclusions_refused` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 25 |
| `test_identity_mismatches_fail_closed` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 30 |
| `test_exact_get_only_command` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 37 |
| `test_wait_records_local_success_then_exact_ci_success` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 45 |
| `test_repeated_status_prints_once` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 57 |
| `test_timeout_refused` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 63 |
| `test_workflow_failure_refused` | Method | `slurm_logs/release_v0.16.0_2026-10-21/verification-prep/check_ci_gate_synthetic.py` | 70 |

## How to Explore

1. `context({name: "record"})` — see callers and callees
2. `query({search_query: "verification-prep"})` — find related execution flows
3. Read key files listed above for implementation details
4. `explain({target: "<file or symbol>"})` — persisted taint findings (source→sink data flows), when indexed with `--pdg`

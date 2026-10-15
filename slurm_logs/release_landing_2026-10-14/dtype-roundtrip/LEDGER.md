# Read-only ledger — Liberia/Mali cold versus warm dtype comparison

§1 Diagnose Liberia interview_date datetime64[s]→[ms] and Mali household_roster
MonthsSpent object→string on the same copied parquet bytes. No tracked edits,
source/shared-cache API calls, or GitHub writes. Mali interview_date coverage
loss is owned by the runtime agent and is not a dtype normalization.

§2 Prior art: STANDING.md sanctioned IO/cache-finalization rules;
.coder/ledger/645-to-parquet-null-coercion.md: writer's object→Arrow string
conversion is deliberate and load-bearing, Wave rebinds writer result while
Country discards it; country.py::_finalize_result; tests/generate_baseline.py
fingerprint (dtype-sensitive pandas row hash). Reuse actual writer, reader,
finalizer and country API only on new private physical copies.

§3 Country CONTENTS read first, including all headings/TODO/WAITING/LOGBOOK
search (neither file currently has task-state entries beyond the TODO keyword
declaration). Liberia NHFS is forest-proximate, not nationally representative;
its discarded distance stratification cannot be inferred from missing strata.
Mali mixes EAC-I2014/2017 with EHCVM2018/2021; grappe not vague is the cluster
identifier. CONTEXT contradiction to quote: MonthsSpent notes say EHCVM filters
“do not give a continuous duration measure” and analysts should use binary
variables “rather than expecting a MonthsSpent column”; existing YAML does map
Oui→12/Non→0 into MonthsSpent. Preserve that existing scientific policy and
report the contradiction; no value or schema changes authorized.

§4 Prove row values, null masks, index and wave coverage independently of dtype.
Matching dimensions alone is insufficient. Full original served frames were not
saved; reproduce their exact recorded fingerprints from private API outputs
and isolate serialization dtype effects. Never weaken the material Mali date
coverage check by normalizing away missing rows.

§5 Use private copies of only required target/sample/cluster tables and JSON,
assert import/config/root identity, pinned build/BLAS1 and CPU16–23. Keep raw
rows and logs private; publish aggregate evidence only. Any actual fix requires
separate parent authorization.

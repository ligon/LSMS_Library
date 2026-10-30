# Burkina Faso #1244/#1276 cached namespace repair

## §1 Task
Reproduce independent EMC 2014/EHCVM 2018 household collisions using declared,
already-held sources, then recover the smallest ruled EMC_/CS_ repair compatible
with public d65e8b136. Verify keys, weights, cardinality and API behavior.

## §2 Existing machinery
Search tier: git + ripgrep; no GitNexus tools available.
- `Burkina_Faso/2014/_/mapping.py`: wave-specific YAML formatters/hooks.
- `Burkina_Faso/_/burkina_faso.py:i`, `ehcvm_i`: distinct native 2021/2018
  formatters; tested by `tests/test_burkina_faso_panel_ids.py`.
- `local_tools.id_walk`: existing single-pass rename; `tests/test_id_walk.py`.
- `Country._normalise_sample_weights`: per-wave mean normalization; raw
  parquet retains source weights. Reuse unchanged.
- `get_dataframe`, `_dvc_cache_path`: sanctioned read and local sidecar lookup.
- Public #1293 acquisition subdir and #1282 panel work already landed.
- Sibling context/Burkina_Faso confirms draft #1307 (7a0bbf00) plot repair:
  separate work, not reimplemented. #1119/#1188/#1240/#1245/#1189 landed.

## §3 Definitions
Inherit `.coder/ledger/STANDING.md`, `CLAUDE.md`, canonical
`lsms_library/data_info.yml` and country `_/CONTENTS.org`.
2014 and 2018 are independent draws despite overlapping serials.
User confirms EMC_/CS_ namespace policy already selected. 2018 formatter is
`grappe + '0' + menage.zfill(2)`; 2021 is `grappe + menage.zfill(3)`.

## §4 Invariants
Single-pass, exactly-once mapping; preserve component source weights and CS
missing panel weight. Preserve all within-wave household membership and payload.
Country CONTENTS says “Also measured, not reconciled” for sample-count differences
and “A food-row count cannot count interviews.” Neither is adjudicated here.
No shared cache mutation/acquisition, framework edits, food exposure or geography
policy change. Tests serial under prepared image with private identity assertions.

## §5 Reuse decision
Extend wave-specific formatting with ruled namespace; reuse Country/Wave readers,
weight normalization, joins and id_walk. Recover only reviewed product fragments
from retained private source if accessible, never adopt the broad private branch.
No B-sample7 admission framework or historical control replay.

## §6 Open dependencies
Old v6 checkout is absent on receiving node. Request ordinary retained product
source location while advancing cached EMC subcase. CS declaration/blob absence
will be reported exactly, never filled by acquiring files. Historical failed
receipts remain failed/unverified. Root owns integration and external actions.

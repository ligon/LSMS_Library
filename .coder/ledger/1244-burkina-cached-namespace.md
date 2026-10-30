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
Old v6 checkout is absent on receiving node. Ordinary retained results.git
contains94d12f2f93dd2e55c7da0158db0a76e69b2f4396; recovered minimum product
fragments and3 CS sidecars. All selected12 blobs exist and match their declared
MD5. No acquisition is needed. Source provenance is in recovered-source.json. Historical failed
receipts remain failed/unverified. Root owns integration and external actions.

## Verification and metadata dependency

OK (§2–§5): actual sample keys and raw/normalized weights match an independent
source oracle; 2014/2018 sample overlap583→0, roster604→0; 2021 sample3227→10403
with7176 CS households and missing CS panel_weight. All existing roster payload
and membership preserved. Single-pass JSON unchanged; warm/Feature checks pass.
The two live EMC script consumers preserve actual-source payloads exactly.

The recovered source also contains the needed population/sampling definitions.
Reuse its existing mixed-national+panel/rotating-panel descriptions, qualified
to the sample/roster scope and public flat panel layout. Canonical population
source and its generator histogram move together; regenerate only Burkina via
the existing renderer. This is necessary disclosure of the served components,
not a new scientific definition or a test-baseline relaxation. SOURCE records
partial secondary declarations, never an acquisition-complete held release.
Full other-module and acquisition-layout migration remains root integration work.


## Independent source review corrections

The frozen1e8cb8c source/evidence remains unchanged. Review found a concrete
config-root binding defect: 2014 mapping and two script consumers import a bare
burkina_faso module after sys.path append, although the framework loads country
formatters from an exact path without registering/clearing that bare module.
A cached module from another root can therefore supply the wrong helper or lack
emc_i entirely. Reuse existing get_formatting_functions at each source-relative
country path; add poisoned-bare-module regression cases. No namespace/science
formula changes are needed. Original source-based API evidence does not cover
this corrected import binding; targeted corrected-source tests must be separate.

The supported get_data_file path also has a concrete secondary-source hazard
when local/DVC data are missing: its ordinary WB fallback uses the primary6224
SOURCE URL and selects ZIP members by basename, which could place the primary
panel cover at a requested Cross_Section destination. get_dataframe's existing
sidecar branch does not itself take this fallback. Adding CATALOG_ID alone does
not select6277. Full-release acquisition currently refuses duplicate sidecars
before Data writes; do not claim that an overwrite was observed.

Add a truthful partial CATALOG_SUBDIR6277Cross_Section declaration, and fail closed
before WB credential/provider access for ordinary reads of explicitly mapped
secondary subtrees, reusing read_provenance/_subdir_mapping. Preserve local and
DVC hits and primary fallback. No secondary download or full acquisition layout
is enabled. Mocked misses/basename collision, primary compatibility and early
local/DVC hit controls must verify this narrow shared-file boundary. Full Panel
relocation/acquisition and missing-secondary healing remain separate residuals.

# GH1602 — prewarm source-path alignment

Search tier: git/rg/AST floor. Fresh authenticated GitHub development fetch and
ls-remote both resolve8505ee8a05359f66a820534c5af2ab3873671a27. New isolated
branchfix/1602-prewarm-source-paths-20261108; other trees/actors remain untouched.
Inherit STANDING.md§§3–4, CLAUDE.md DataAccess/CacheBehavior and canonical schema.
Fresh issue/body/comments frozen in B/reports/1602-prewarm-source-paths-20261108,
where B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103.

## §1 Task

Make best-effort YAML-source prewarming locate the same declared source
sidecars as actual Wave loading/hashing, including bare Data-relative per-file
overrides. This is a shared mechanical resolver correction, not a new extractor,
namespace/confinement policy, acquisition campaign or scientific rule. Preserve
logical-wave folder maps, absolute/current relative paths, first-key overrides,
DFS sibling precedence, dedup/cache no-repeat and unavailable-source behavior.

## §2 Existing machinery

| Symbol | Existing behavior/test | Decision |
|---|---|---|
| local_tools._collect_file_paths_from_block:807 |1510collector consumes first key in override list entries, excludes variable payloads, visits named DFS siblings; tests/test_gh1510_source_overrides.py | reuse unchanged; no parallel parser |
| Wave.column_mapping:1509 |Actual per-file grammar, first mapping key and override payload; same1510tests | authority; no grammar change |
| Wave.grab_data:get_data:1989 |Builds self.file_path / Data / file and first tries os.path.relpath, yielding lexical normalized loader paths | reuse path semantics; fake-provider actualWave controls |
| Wave._input_hash:1434 |Path(os.path.normpath(self.file_path / Data / fn)), then source_fingerprint(sidecar) | reuse exact declared-source identity; no real raw read |
| Country.__getitem__:3137 /wave_folder_map |ActualWave.file_path contains physical folder behind logical label | reuse, never reconstruct from logical label |
| local_tools._warm_dvc_cache_for_feature:683 |Existing collector, but resolves under_/ and uses resolve(); dedups MD5, checks both cache layouts, lazy remote, best effort | extend only path composition to match existingData/normpath semantics |
| tests/test_gh1510_source_overrides.py:150 |FakeFS positive../Data paths and no-repeat, no real remote | retain; add bare/absolute/DFS/map/unavailable controls |
| B/reports/framework-1510-local-20261107/test_prewarm_resolver_residual.py |Retained actualWave-hash/fakeFS witness: bareData-sidecars exist but oldprewarm findsnone | preserve original; base-negative evidence, no rewriting history |
| Country._aggregate_wave_data -> prewarm and _build_registry closures |Prewarm currently contributes to build fingerprints through caller;1510measuredGLOBALinvalidations | measure actual footprint; no exclusion-policy adjustment |
| Existing1510hash-census-v3.py and guarded operator |450declaredcountry/1895wave/35fingerprints, plus separate553/2313/39runtime surface at thenbase; metadata-only guards | reuse/adapt at current frozen heads, enumerate exactnewscope/None/errors/exclusions |

## §3 Definitions and country grounding

STANDING.md§4/CLAUDE.md require config paths through countries_root/Wave.file_path
and private runtime data roots. The existing YAML authority says Data-relative,
not script-stack _resolve_data_path semantics. No new root/Namespace rule is
introduced; absolute declarations retain existing pathlib behavior.

The prior FULL Burkina_Faso/Malawi/Nigeria CONTENTS reads included all
LOGBOOK/WAITING/TODO. Byte-checked against their retained1510receipts; Nigeria
is identical. All changed text in Burkina_Faso(+19lines) and Malawi(+19lines)
was read; exact old/new hashes/diffs are in country-grounding.json/.diff.
Relevant idiosyncrasies remain: Nigeria logical PP/PH labels share physical
folders, and WAITING recall quotes both '12 months reference period' and
'PAST 6 MONTHS' without authorizing reinterpretation. Malawi panel current
geography differs from stayer-grounded baseline EA coordinates; its new1174
note preserves the19blank fertilizer amounts and differentiates deletedgroups
from original lines. Burkina2014 has four passages; the new1528 note explicitly
retains the contradiction 'The printed form is not what was fielded.' BFA's
historical 'A gap in the hash, not fixed here' entry is not authority to undo
1510; that hashing fix and the separately filed prewarm miss are distinguished.
No country configuration or policy is edited; tests use invented Control only.

## §4 Invariants

- Keep declaration collector, loader, hash assembly, schema, country configs,
  source/refusal grammar and hash exclusions unchanged.
- Prewarm sidecars follow the existing lexical Data path, including absolute
  and parent-relative inputs; use actualWave.file_path for mapped folders.
- First-key override and DFS sibling semantics remain1510authority. No ignored
  override/variable values or unrelated files are prefetched.
- Existing local2.x/3.x blob means no remote client or repeat download; shared
  MD5 across declarations/waves is fetched once. Missing/corrupt metadata and
  unavailable sources stay best effort; per-call loading remains fallback.
- No real source/cache acquisition, shared L1/L2 write/rewarm, DVC CLI,
  credentials, network, dependency change, country/science/default/1509/48 change
  or GitHub write. FakeFS bytes only. Metadata census refuses real raw/
  harmonized/provider/build accesses, permitting only audited tracked config.
- Exactcwd/PYTHONPATH/config/import/mountedvenv, explicit empty private roots,
  credential gateFALSE, unshare, BLAS/build1,pytest2loadfile. Frozen offlineCI
  as resources permit; root owns independent review/publication/configuredCI.

## §5 Reuse decision

One path-composition correction in prewarm reuses existingData/normpath
semantics. No helper/extractor/resolver abstraction is needed. New tests are
justified by the retained bare-path counterexample and untested positive cache/
provider identity boundaries. Reuse current1510declaration/hash controls and
guarded metadata census. Any broad fingerprint closure cost is measured and
reported without changing exclusion policy or doing a rewarm.

## §6 Open questions

No scientific ruling or missing implementation authority. Unexpected loader/
hash disagreement, source selection ambiguity or unrelated regression will be
reported before broadening this scope. Ledger is surfaced before code.

### Phase3

Pending implementation/frozen checks. No test, source-prefetch or footprint
success is claimed by this ledger-first commit.

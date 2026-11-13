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

### Phase3 — final anchored verification

Implementation0994e131659aaa5854887f3c7af1416a06145a8c; originalbase8505.
All author CI/census/test and independent review actors closed before ordinary
composition with currentGitHubc780562b299bb100cdd006037ba424c8ad3264e9, yielding
integrationaa5dde08cbc437d972e895e735488376a460f1cd. Final own docs-only commit
must preserve that integration's library/config/test bytes and owned0994bytes.
All receipts/old reports remain immutable under the report directory in§1.

- **OK (§§2–5reuse):** only prewarm's source expression changes from_/resolve
  to existing actualWaveData/normpath. Independent review proves restoring
  that expression makes the complete local_tools AST identical to base.
  Collector1510, loader, hash/exclusion/schema policy, country configs, MD5
  selection, lazy remote/cache/failure logic and public API remain unchanged.
- **OK (§§3–4definition):** physical mapped Wave folders, bare/dot/relative/
  absolute/parent paths, lexical normalization before symlinks, first override
  key and named DFS sibling precedence hold. No namespace/confinement or
  scientific policy was invented; no source bytes authenticated by prewarm.
- **OK (§4tests):** untouched8505tree runs initial14externalcontrols with
  8expected resolver failures/6compatibility passes. Authored18newcontrols plus
  102existing controls pass120precommit; independent exact0994review reruns
  them plus17externalcontrols:137PASS/1inheritedsourcecredentialskip, noP1/P2.
  ActualWaveprovider/cache writer and fakeFS exercise source identity, ignored
  payloads/unrelated sources, mapped scopes/dedup, both local/remote layouts,
  no-client/no-repeat, malformed/unavailable sources and partial cleanup.
- **OK (§4offlineCI):** freeco_carletonjob41017138,4CPU24G,--cold, exact0994,
  fresh allocation-local EMPTYdata/config (helperL1linkunused), falsecredentials,
  own mountedprefix/package/config and entire-actorunshare/BLASbuild1.
  Exactcommand: mountedpython-mpytest-n2--dist=loadfile--no-purge-q
  -m'not requires_s3'tests.11604PASS/245skip/4xfail,692warnings,1350.66spytest;
  wrapper1353.63s,sacctCOMPLETED0:0/24:46,cleanhead0994before/after.
  Separatecollect-only executes no bodies:17260total/11853selected/5407
  deselected. This filtered unit run is not source-marked or configuredGHA CI.
- **OK (§5metadata):** audited1510operator reused EXACTSHA2c14de9078c622a5233d5849a3975ac735b2963f33ff029b57f5a049d916bf4d.
  Bothfrozenheads:38registeredcountries,450declaredtable/1895wave/35build/
  186import fingerprints,0errors/denials; separate runtime553/2313/39surface,
  not additional persistedcaches.11unregisteredcountry/3noncountry exclusions
  explicitly listed;18NonePeruwavecells unchanged.28overrides/9configs,
  0unsupported. Only exact tracked conversionconfigCSVblob28619ec2allowed;
  actualraw/harmonized/provider/build/acquisition accesses refused.
- **OK (§5reachqualification):**762physicalconfig/feature/sourcebindings in
  100configs;753loader-sidecar vs227old-prewarm occurrences;526missedbindings,
  9neither,0old-only.526bindings cover421paths/415declaredMD5s WITH AT LEAST
  ONE missedbinding;12ofthosepaths and12MD5salreadyhad anotheroldreachable
  binding. GLOBALunion184→593paths and184→587MD5s gains409paths/403MD5s.
  Original census/sidecar receipts remain unchanged; additive
  binding-global-union-qualification.json derives this from closed metadata
  only. No runtime/performance/sourceavailability claim or newcensus follows.
- **OK (§5footprint):** measured8505→0994 GLOBAL450/450countryhashes,
  1877/1895wavehashes,35/35build fingerprints move;0/186importclosures move.
  The18Nonecells remainNone. Existing caller closure reachability explains
  this cost; no hash-exclusion change/schema bump or shared rewarm hides it.
- **OK (§4composition):** normalc780merge introduces upstream1535canonical
  optionalmonthstrings and1480test/operator/docs only; ownedlocal_tools and
  newresolver test remain EXACT0994.195hermeticrelevantcompositionchecks
  pass,4warnings,280.67s,privateemptyroots/falsecredentials/unshare. Uganda
  sourcebaselinecoverage is explicitly INCOMPLETE0/53entries/0of265fields:
  all53sourceentries DESELECTED, sourceverification NOTPASSED/unclaimed.
  Do not call this a real source invariance run. StrictAPIaudit0 on0994and
  integrationaa5; first invalid python-mpoetry operator failure is preserved,
  corrected existingPoetrytool uses mountedruntime without installs/newvenv.

No sourcecampaign or GitHub write was performed. Root owns final-head configured
unit/Windows CI, publication/merge and any release/rewarm. Local implementation,
independentreview, filteredofflineCI and boundedcomposition gates are complete;
science/1509/weights48 and other frozen research artifacts remain untouched.

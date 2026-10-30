# Independent Mali review: Ethiopia #1119 structural crop marker

**Qualified acceptance recommended. No concrete source/result blocker remains for the emitted structural-marker change.** This review read the completed author report, original and v3/v4 failures, full released deltas, actor/observer/comparator/source, aggregate receipts and both closed manifests. No reviewer project import, frame deserialization, test, decoder, API or data replay occurred.

Country idiosyncrasy: §3 field-level fertilizer/organic inputs have no crop, while §4 seed/pesticide observations are crop-specific; §5 purchase attachment uses holder/crop identity only when uniquely attributable to a field. CONTENTS historically says “these rows carry j = 'Whole Plot'”; its dated #1119 entry explicitly supersedes that spelling with “j = '(not crop-specific)'” for the structural branches. The Panel I / Panel II split, W2/W3 household_id2, full holder/parcel/field keys and #1183 retention remain. The settled #1357 option 1 excludes anonymous rows; no identity is inferred from holder prefixes. Historical mean/centroid prose also does not supersede the later observed-row rule.

## Exact source and custody

Baseline `edc9b080c86c9f51fc5a17773681e189aef1c505`, candidate `f180929ee1a5e4ce64c196883ea20c7fb1f0ef42`. Both 1,197-member source archives authenticate against released bindings. All 60 country function/class ASTs are identical; the sole production AST difference is `WHOLE_PLOT_SENTINEL = '(not crop-specific)'`. Five drivers, colmaps and compiler remain unchanged. The country schema delta is comment-only; no canonical shared-schema edit is proposed.

Independently authenticated exact v3 binding `ee3f12bfbd007da282fdecf31bfd876824ab9f0c5599c504e0fd80a604704736`, v4 binding `9376ebe6756967cfde6e85bd0dfbb8c22ffe321146e10810543b5cb0542424d8`, and eight operator files for each. Hash-only review authenticated all 110 v3 and 62 v4 closed files, 399,530,004 bytes, with regular/non-symlink and unchanged metadata checks. Manifests are `5683f81d8693688586d1be222d557a82576995112904c063a2d7417269790bda` and `929494c9518a4c0fc7cb755c735b6716d38a90350f1585021a7c690d7cc9f3af`. Both final 16-input hash/epoch closures agree with original staged bindings. Original selected input payloads were not reopened by this reviewer.

## What actually passed

All 20 matching-stage structural-only comparisons pass: five native CALL/RETURN/production-storage triples, compiler triple and Country/Feature pair. Totals are 180,900 rows: exactly 69,024 structural rows receive only the authorized j marker; 111,876 Seed/Pesticide rows are unchanged. Per-wave totals and structural counts are 45,149/15,162; 47,416/17,044; 47,426/17,340; 23,271/11,147; 17,638/8,331.

The independent expectation changes the j dictionary/codes after source-defined native ordering, preserving non-j unused domains/codes, masks, row multiplicity, payload, nullable classes/dtypes and recursively typed attrs. GH849/GH1206 comparator code independently checks ExtensionArrays and exact nonmissing NumPy values. Actual operands are not normalized. Different pickle commitments for typed-equal index objects are not a failure of this semantic contract and do not establish byte equality.

Compiler expectations use existing concat/id_walk at the CALL stage. Both Country/compiler-finalizer and independent named-axis/full-attrs singleton Feature compositions pass. Candidate strict Country passes. Baseline Country is an authentic previously saved return, verified in v4 without rebuilding it. Fresh-process candidate Country/Feature are exact to their closed cold returns; seven selected caches preserve hashes, identity, modes, nanosecond times and coverage. Observed warm activity is four sanctioned readers/eight aggregate hooks, with zero named source/parser/native/Wave-build/writer entrypoints. This is not universal zero I/O.

## Failures and acceptance boundary

All 14 transport failures remain false: 12 RETURN/bare-pandas NA-policy mismatches plus both compiler CALL/RETURN differences. Of 36 transport predicates, 22 pass, including all 12 RETURN/production-storage checks using the original sanctioned reader. The successful matching-stage product comparisons do not turn these cross-stage failures green.

The original W1 bare-reader failure, the first dispatch's missing-environment import failure, and v3's unexecuted list-versus-dict accessor error remain failed. V4 substitutes the source-correct `_materialization_entry` accessor and reuses completed stages; it exits 0 in 167.415 seconds after the stopped v3's 247.679 seconds. All descendants/cleanup receipts close. V4's seven absent export names are optional baseline strict/sample-before or failure-only artifacts; baseline sample-before remains inherited in v3. All mandatory operands and receipts exist.

Original baseline W1 consumed three saved source loads. V3 consumed the remaining 27 once plus its first baseline sample; v4 consumed zero source loads and its first candidate sample. Original67, import-failure64, v3 60 and v4 33 selected-stream histories remain separately reported. These are saved numeric source operands and explicitly composed, privately stamped sample/native prerequisites. They are not a clean raw cold build, fresh sample/cache/weight validation, actual GNU Make acceptance, or independent population/instrument/eligibility science. Current public extraction must preserve #1364 schema and all later work.

The original inherited synthetic result is 28 passes / 5 fixture failures followed by failed-five-only 5 passes: 33 distinct cases, not one clean run. Continuation controls are 5 passes / 1 failure followed by failed-control-only 1 pass. Actual data contain no crop-specific literal `Whole Plot`; its preservation is established by the focused invented test, not claimed as an actual observation. Existing missing/unmapped crop exclusions and #1183 retention are unchanged. No whole #1119, #1120, #1287 or scientific umbrella closure follows.

Recommend a minimal public constant/comment/test/documentation extraction with this qualified result, retaining every historical failure. No further source/data replay is needed for that extraction.

Author report SHA256: `8a6151ee80633b8e040ee014c832237d9a190a488e019964a8e93c3cb72c72ec`. Independent review SHA256: `bd2d229ba8ed6e318b2e5f05dcf51fe67fbf5974f7ade3f3c0ca4ecb12c1bc7b`. No test or data replay occurred during public extraction.

Late publication addendum: structured task status is completed and native exit
is 0, while supervisor/Slurm exit is 74. Git workspace publication completed at
`6c3d8033a6c3c48d95221f06299a369a29c1b067`; ordinary evidence publication
has 120 size-cap errors at the 256 MiB ceiling. Both directly authenticated
shared private witness sets remain durable. This is qualified actual evidence,
not a fully successful infrastructure publication or proof of pre-emption.
No data replay was performed to address this publication failure.

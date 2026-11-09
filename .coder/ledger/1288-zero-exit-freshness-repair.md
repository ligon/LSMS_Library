# Prior-Art Ledger — GH1288 zero-exit Make freshness repair

**Current cold-stage base:** d2b1c070d03da8b25daa6d1fe2b88a1c5ea89fe2,
 tree 56cf7143ad466ea27a14af1992ef740e02edfb61 (asserted with own Git).
**Branch:** dispatch/dd3c3ebd650a4b919b834565a36c0cf1.
**Historical stage-1 base:** 5695e0855e9182b9e61559d802ce7249102818be,
 tree aac6725ec7a05b77c9dcf78689f903797c186882; historical branch
 dispatch/f779ad1ac1c94d2b823af3f96f5b2a22. Inherited source is an UNREVIEWED
 draft, not an accepted implementation.
**Search tier:** Git + rg + stdlib AST/text, the AGENTS-approved substitute.
The supplied REPAIR-CONTRACT is selected authority; historical refusal-only
reports are qualified history. This stage permits NO task runtime.

## §1 Task, restated

Repair the direct non-DVC, declared materialize:make country-builder route's
zero-exit stale-wave and stale-country acceptance. Execute selected work safely
or reuse current declared-input/coverage evidence. Preserve the native builder,
scientific content, GH1303 failures, GH1327 selection, GH479/GH866 placement,
reviewed JSON sources and unrelated prerequisite bytes/availability.

## §2 Existing machinery

| Machinery | Current source | Tests read (UNRUN) / decision |
|---|---|---|
| Country._aggregate_wave_data / nested run_make_target | country.py:5411,5523 | GH1288, GH1590: extend direct route only |
| _make_target_is_missing / _reemit_make_stderr | country.py:203,232 | GH1288: reuse exact target/prerequisite distinction and original cause/stderr |
| _has_country_builder / _probe_country_builder | country.py:5303,5365 | GH1039: reuse; no Make parser or rule probe execution here |
| Wave._input_hash / Country._table_cache_hash | country.py:1354,4517 | test_cache_hash_invalidation: reuse declared input hashes, including content rather than mtimes |
| cache_freshness / stamp_parquet_hash | local_tools.py:3266,3294 | cache hash tests: reuse/extend writer with optional coverage, retaining all other metadata |
| _cache_covers_waves / _read_l2_country_cache | country.py:4653,4784 | GH891: reuse existing freshness, coverage, drops, replay and selection gates |
| Wave.grab_data / generated wave getters | country.py:1921 | GH1292/GH1590: reuse for YAML contribution, including explicit successful empties |
| scan_script_data_refs / _collect_file_paths_from_block / _dvc_cache_path | local_tools.py:3078,807,503 | reuse declared sources and existing L1 lookup; no dependency parser |
| _evict_hashless_wave_caches / _evict_stale_country_cache | country.py:5060,5089 | GH479 cache tests / GH866 source guards: unchanged |
| _restrict_to_waves / _finalize_result / _assert_built_required_columns | country.py:4690,4025,5168 | GH1327, GH479: reuse, validate before promotion |

Historical stage-1 reading record (not current-stage runtime evidence):
Read full STANDING, existing1288/1590/make-country-wave-subset ledgers,
GH866/GH1039 source/tests, supplied issue body/comment, source proof and all four
historical1288 artifacts. The repository has no separately named479/866/1039
ledger; their current contracts are recorded in full AGENTS/CLAUDE and tests.
No actual country configuration/module/fixture is used; Alpha/Beta are invented.

## §3 Definitions & conventions in force

Inherit STANDING §§3–4, CLAUDE Cache Behavior / read-only countries_root,
and canonical data_info.yml; no schema/science change is intended.

- Existing strict_country_make is enabled only on the direct declared Make
  country-builder route. Exit0 and output_identity are not input validation.
- GH891 coverage means BUILT logical waves, including empty contributions;
  emitted t values are not a coverage declaration. Country builders own their
  full-country output; requested filtering remains GH1327's existing helper.
- The current native runner accepts the first existing candidate after exit0.
  A copy/new inode/touch is insufficient if an old contributing wave was reused.
- GH479 eviction stays inside load_from_waves; GH866 eviction stays immediately
  before its guarded country fallback. JSON crosswalks remain reviewed sources.

## §4 Invariants and selected algorithm

1. At the strict direct route only, reuse a country output only with a non-None
   current hash and known sufficient coverage through the existing read gate.
2. Otherwise create an isolated temporary build data root. Never remove, rename,
   hardlink or overwrite a live prerequisite. Seed copies only of registered
   parquets with current hashes and sufficient declared coverage, distinguishing
   selected country output (always absent) from wave/other-table inputs.
   Do not seed unverified managed caches. Hashless raw/DVC provenance remains
   eligible. Copy declared available L1 blobs using the
   existing lookup; do not link L1, fetch, clean, or change acquisition policy.
3. Prepare the selected table's declared physical wave contributions, once per
   physical script folder. Reuse current seeded inputs; run missing script
   contributions through the existing runner in explicit wave-only strict mode
   (no shared country fallback). YAML contributions reuse actual Wave getters
   and sanctioned writers in an isolated child, not a second table transform.
   An explicit produced empty is valid; zero-exit/no output is not.
4. Run the existing country builder with isolated output candidates/environment
   and an absent selected output. Exact no-rule/missing-Make fallback remains;
   real recipe/prerequisite failures retain original cause and stderr. No -B,
   FORCE, Make-text parsing, or blanket live-cache eviction.
5. Check current declared source hashes before/after; ensure copied live inputs
   and selected output have not changed concurrently. Validate the actual frame
   with existing wave selection, finalization and required-column checks before
   acceptance. Stamp only this verified output with input hash and explicit
   all-wave builder coverage; preserve Arrow/pandas/audit metadata. Atomically
   promote only selected country output; discard all transaction companions.
   Byte-identical regenerated content remains valid; no inequality heuristic.

Unknown computed input paths remain the existing hash boundary. This design
supports root-honoring producers using declared managed inputs, not arbitrary
absolute-path or out-of-band reads. An unproven OTHER cache cannot be made current merely by copying it. Literal
registered Other inputs use their existing producer privately; declared raw/DVC
inputs retain source provenance. Truly computed/unregistered/unverifiable inputs
can retain the native missing-prerequisite refusal under ROOT's explicit narrow
availability ruling below, without any claim that the live bytes are stale.

## §5 Reuse decision

Extend country.py at the direct acceptance boundary, retaining one native runner
with private root/wave-only options defaulting off. Add small Country helpers
for transaction setup/source stability and YAML-child preparation. Existing
hashes, declaration APIs, getters, compiler scripts and sanctioned readers/writers
remain authoritative. Extend stamp_parquet_hash only with optional coverage;
existing callers/default behavior unchanged. No new module is needed.

This changes fingerprinted orchestration: conservatively expect corpus-wide
country AND wave invalidation via ALL-scoped build transforms. No hash exclusions,
registry/schema bumps or country edits. A later independent metadata census must
measure actual scope; historical counts are not current evidence.

## §6 Open questions / boundaries

No blocking question inside the selected charter. Report the dynamic-input and
stale rule-less OTHER-prerequisite boundaries with code evidence. No runtime,
provider/source/cache access, Make, pytest, package import or census is granted.
Prepared runtime placement is not a runtime grant. All regression drafts UNRUN.

### Phase 3 — verification

Source implementation/review and static checks completed in the new cold stage
as recorded below. Every runtime gate remains UNRUN. No executed behavior is
inferred from drafted assertions or historical runtime evidence.

### Coordinator refinements at implementation checkpoint

ROOT's shared steering accepts the transaction approach and requires validation
on a deep copy, leaving promoted bytes/attrs PRE-finalize. No post-finalize
serialization or second id_walk is permitted. Existing read-gate replay/drop and
coverage behavior remains authoritative.

Hashless OTHER is UNPROVEN, not necessarily stale. Explore ordinary registered
Other builders in the private transaction: discover literal managed references
with existing scan_script_data_refs and declared paths, and ask the actual
Country API to build a referenced unproven table under the private root. Never
substitute generic concatenation for the selected country compiler. Unknown
dynamic dependencies are not guessed, and unknown missing prerequisites remain
native failures. A genuinely hashless, rule-less, dynamically referenced Other
with no established builder/provenance is a remaining availability boundary for
ROOT review, not evidence that it is stale. Broader default/JSON/legacy failure
paths and real exposure remain explicit partial-GH1288 residuals.

### Static closure refinement

Current _table_cache_hash folds selected wave hashes but not another table's
raw-source sidecars, and country-only literal raw references are not separately
folded there. A warm Make result therefore also needs its recorded declared
Other-table hashes and literal/YAML source_fingerprint values. Extend the existing
parquet metadata writer/reader with an optional lsms_make_inputs manifest used
ONLY by direct Make reuse; do not change _table_cache_hash grammar, default cache
read policy, registry exclusions or scientific content. Compare this finite
manifest before/after building and on direct warm reuse. This is freshness input
evidence, not a second data/cache tier. Known literal Other closure is collected
with the existing scanner/registry; computed-path residual remains explicit.

### ROOT ruling on the concrete availability discriminator

The shared channel explicitly accepts the invented computed/hashless/rule-less
Other case ONLY as an availability boundary/residual, not stale-source evidence
or actual-country exposure. Narrow contextual refusal may protect genuinely
computed/unregistered/unverifiable inputs. Ordinary discoverable prerequisites
must retain builder/script fallback. The required healthy literal dependency
must privately rebuild and yield exact15,25; both cases preserve live Other
bytes/availability. Both tests remain UNRUN. GH1288 remains OPEN for broader
exposure, other callers and declaration questions; no scientific/config contract
is added. The new draft names both cases explicitly.

The extra direct-Make manifest records Other table hashes and country-only/raw
literal references not already covered by Wave._input_hash. YAML and ordinary
wave literal sources already folded by the existing hash remain eligible for
normal verified reuse without a redundant manifest. Declared source paths are
also used to copy only already-available L1 blobs, including literal Other
closure. Existing raw/DVC provenance remains eligible.

### New cold-stage ledger-FIRST checkpoint (2026-11-08)

This is a distinct source-only completion, not resumption or retry of native
41031186: native/supervisor143, SIGTERM15, Slurm FAILED15:0 at43:02 of45min;
ROOT's single extension was denied. Only the old ledger checkpoint was published;
four required reports were missing. Original source/evidence and failures are
immutable. Old ledger-FIRST bbe8ea6621ee3a98d33b4efd70d0b95720455f22 remains an
ancestor. Current d2b1 is currentdd40 plus exactly five salvaged WIP paths; the
supplied SOURCE-PROOF/SALVAGE-COMPOSITIONS are authentication, not behavior proof.
The submission clone timeout happened before manifest/sbatch; its infrastructure
amendment supplies no task runtime, cache, source-data or acquisition grant.

The selected algorithm remains productive private transactions, actual native
country builder, current declared-input closure and logical coverage independent
of emitted rows. ROOT-TRANSACTION-REFINEMENTS requires deep-copy validation with
PRE-finalize promoted bytes/attrs, preserved read-gate evidence/warnings/degraded
and coverage semantics, no global eviction or force, and discoverable Other
builder/script fallback. ROOT-AVAILABILITY-RULING allows only the narrow truly
computed/unregistered/unverifiable Other availability residual. The discriminating
healthy literal Other must produce independent exact15,25 privately; both it and
the computed refusal conserve live Other bytes. Hashless alone is no refusal.
Declared current raw/DVC provenance stays eligible. GH1288 remains OPEN.

Only this ledger, caching.md, country.py, local_tools.py and the new GH1288 test
module are owned. First NEW commit is ledger ONLY; final ledger will be separate.
Five external evidence reports were initialized UNFINISHED/UNRUN before code.
Verification is Git/rg/stdlib AST/JSON/text/compile-no-pyc only, using the prepared
interpreter with -I -S; no imports, collection, tests, Make, producers, data/cache
reads, providers/network, census, additional agents or native-state inspection.
Generic Alpha/Beta fixtures only. All meaningful inherited assertions remain;
necessary fixture corrections will be named precisely. No new hash exclusions.

Phase3 is PENDING. Review will specifically inspect private closure vs false
copy/touch proof, physical/logical alias coverage, raw vs managed Other handling,
pre-finalize payload/typed attrs, warning and ledger side effects, original
nonzero cause, source drift/interruption and unaffected call-site defaults.
Static ALL-scoped fingerprint impact is expected across country and wave tiers;
no measured counts or corpus success can be claimed here.

### Cold-stage source completion and anchored review (2026-11-08)

Ledger-FIRST commit: 0348f58d44e1f06a5c12cfefb6c08526c595fa07 (ledger only).
Code/test/docs commit: 04d9d0ea5f6709d0f58146800e35e08226694486.
Published solely to this dispatch's private results.git. This final ledger update
is a separate commit. Current source/config identity is the asserted d2b1/tree56cf
union, not historical stage1 source or a stale filesystem branch.

Actual static defects corrected in the inherited draft (all anchored on §2–§4):

- A native Other-country transaction promotes only its country output; it cannot
  supply an explicitly required Other-wave companion by accident. Extend the
  same runner with default-off internal wave-only preparation, preserving all
  physical/logical declarations. Keep true native country transforms for an
  Other-country input. No second extraction or scientific transform was added.
- Raw/DVC provenance could be mistaken for a managed Other cache. Keep sidecar-
  declared raw inputs distinct; copy already-available L1 bytes at private
  redirected paths where needed, without demanding or manufacturing L2 hashes.
  All live Other/source bytes remain intact. No global drop/force/cleanup.
- The inherited cold path replayed unscoped country reports, did not start its
  selected ledgers clean, and could lose subprocess wave evidence in a native
  concatenator. Reuse existing scoped grain/date replay, drop warning/healing
  primitives and per-load selected ledgers. Preserve wave/native footer evidence
  through the optional metadata-only writer, and the existing just-written
  degraded-cache retry policy. Existing read-gate definitions are untouched.
- A producer could replace its output between DataFrame validation and stamping.
  Freeze a parent-owned private pre-finalize snapshot, validate a deep copy,
  check stable selected-wave bytes, sources and original output, then stamp and
  promote the snapshot. Warm validation uses a deep copy too. A cheap warm file
  identity check is only a race receipt; declared hashes/coverage establish
  freshness. Neither changed bytes nor stat identity is a freshness oracle.
- Propagate the selected build stack through native/YAML children so recursive
  API builds cannot lose the existing cycle check. Preserve exact Make target
  absence vs failed prerequisite classification, native stderr and original
  exception cause. No unknown probe is reclassified as absence.

Regression drafts now have 46 test-function definitions, not collected cases or
passes. All 103 inherited Assert AST nodes are retained exactly, with no new
skip/xfail or relaxed expected payload. Added drafts cover the literal Other-wave
path, declared raw/DVC input, snapshot substitution race, private-wave mutation,
interruption, exactly-once IDs, cold/warm scoped diagnostic ledgers, degraded
healing, a grouped-wave companion that cannot satisfy a no-output country build,
and an actual native Other builder yielding independently literal15,25. Its
Other native transform converts wave values1,2 to5 and carries a builder marker;
generic concatenation would produce11,21 and fail. The computed/hashless/no-
producer availability refusal remains separate; both conserve frozen liveOther.

Explicit fixture corrections: the Other IO-leaf child mock now consumes the
extended table/wave-path/country-input arguments and emits only requested
physical paths. Per-fixture process-ledger keys are saved/restored to prevent
cross-test contamination. Expected values and inherited assertions were not
changed. Added numeric assertions compare exact nonmissing NumPy values and
masks as well as full keys/dtypes and typed producer attrs. Native toy scripts,
Makefiles and tests remain UNRUN; they require a separate ROOT runtime plan.

Actual source-only outcomes: three top-level modules AST-parse and compile in
memory without pyc/execution. Nine finite child-command/script-template families
were rendered as text and compiled without execution; dynamic script-edit
expressions remain explicitly unverified. Against actual currentdd40, 168/169
existing Country functions and 86/87 local_tools functions retain their ASTs;
only the tagged aggregate and metadata stamp writer change. Seven existing
nested default loaders/helpers are unchanged, including load_from_waves and
load_dataframe_with_dvc. GH479/GH866 eviction placement, GH1303 classifier/probe,
GH1327 restriction, read gates, finalizer and scientific helpers retain their
actual original ASTs. Git diff --check passed. New helpers extend the declared
input/metadata/producer boundary; no schema, registry, lock or hash exclusion was
changed. This is an anchored source review, not a functional acceptance verdict.

One static audit attempt asked own Git for original WIPa33c, which is deliberately
outside this worker's ancestry, and failed with 'not a tree object'. No fetch or
base change followed. The corrected audit compares actual initial d2b1 objects
AND bytes with the supplied immutable WIP salvage manifest, and compares every
nonowned mode/type/blob with actual currentdd40. Final frozen identities/counts
are in source-byte-proof.json. No failure was relabeled as a pass.

Source-only completion leaves no identified blocking conflict in the selected
contract. It does NOT establish runtime correctness, actual-country exposure,
scientific equality or a universal input-declaration solution. All imports,
collection/tests, Make/native/toy producers, raw/data/cache reads, API/Feature,
cold/warm runs, hash census, corpus rewarm and CI/GitHub gates are UNRUN here.
The read-only Git/rg/stdlib AST/text substitution was used; no extra agents,
providers, acquisition, installs or native-state inspection. The existing runtime
profile supplies an interpreter, not task-execution authority.

The risk remains HIGH in footprint: the aggregate and explicit child dispatchers
are ALL-scoped build transforms. Expect broad L2-country AND L2-wave invalidation
and extra private copy/snapshot disk/IO, without claiming measured counts or
rewarm success. A future metadata census must establish exact scope. Conservative
offered-input manifests can also cause a rebuild after an unused offered Other
changes; they are evidence over available declared inputs, not a generic Make
parser. Arbitrary computed/outside-root reads remain outside established closure.

GH1288 remains OPEN. Residual: measure broader actual-country/table exposure;
review default/JSON/legacy callers' failure paths; review declaration coverage;
retain the narrow computed/unregistered/unverifiable Other AVAILABILITY tradeoff
without calling correct-looking live bytes stale. No original failed report,
source143, denied extension, submission-timeout package or witness is overwritten.
Coordinator and independent acceptance remain pending.
### Current integration and portable regression checkpoint (2026-11-09)

Root accepted original7d6ccd1e through the separately frozen v2 actual24:
24PASS (22normative+2late observations),73active profiles,0denials, clean
source; original v1's22PASS/2FAIL plus24link denials is not accepted evidence.
The v2 fixture repair restores four original owned local_tools function objects
before parent seeding/fingerprinting, avoiding a parent/child closure mismatch.
It changes no native/oracle/product policy. The genuine computed/hashless/
rule-less Other availability residual remains separate and whole1288 stays open.

This NEW integration starts on frozen current stack
9d267417b544cd8834f736cf93dc8ae7fba617fb, ordinarily merging exact7d6ccd1e.
Actual merge base is dd40aae5, not6863. A pre-source external ledger plan and
complete original task-ledger read preceded the merge. Git's preview/actual
merge is conflict-free; no resolution or source rewrite was made. Five original
1288 paths are admitted, preserving all current1501 no-cache semantics,
1468JSON reader and both publisher guards, two corrected legacy tests and the
1495 comments/docstring. Current/development/GitHub merging remains Root-owned.

Before adding the sixth owned path, freeze this tracked checkpoint: deliver
accepted v2 controls as tests/test_gh1288_native_original_contract.py. All nine
existing test functions, helper/value/typed-attrs oracles,24producer families,
24semantic parameters and four-owned-helper restoration fixture stay exact.
Only module documentation/import portability and the existing genuine
shutil.which('make') availability mark may differ. No external report/grant/
guard dependency or pytest current-symlink workaround belongs in repo tests.
Normal pytest convenience links remain normal; external old guards stay intact.

Root's finite current-composition choice is ONLY the two previously affected
nativeOther Alpha/Beta cases as ordinary private smoke, following explicit
import/collection/identity and actual Make-availability checks. Do not run a
third private24 or whole original46. Configured exact-current fullCI must run
all portable24 plus original author tests and the full offline suite; Linux
release acceptance must establish executed24, not a Make-absence skip pass.
Genuine absence may use the same capability convention as existing native Make
tests. No source/real-country/API/cache/census campaign or new exclusions.
This checkpoint's portable module and current smoke are UNRUN/unwritten here.

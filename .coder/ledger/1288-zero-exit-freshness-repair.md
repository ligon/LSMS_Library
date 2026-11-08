# Prior-Art Ledger — GH1288 zero-exit Make freshness repair

**Base:** 5695e0855e9182b9e61559d802ce7249102818be, tree
 aac6725ec7a05b77c9dcf78689f903797c186882.
**Branch:** dispatch/f779ad1ac1c94d2b823af3f96f5b2a22.
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
   Do not seed hashless/stale inputs. Copy declared available L1 blobs using the
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
absolute-path or out-of-band reads. A stale OTHER rule-less prerequisite cannot
be made current merely by copying it: leave the live file intact, omit the stale
copy, and preserve native missing-prerequisite refusal. No new declaration or
availability contract is silently invented for that unsupported case.

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

Pending implementation and static-only conservation/syntax checks. No executed
result is inferred from a drafted assertion or historical runtime evidence.

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

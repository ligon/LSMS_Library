# Prior-Art Ledger - GH1091 Tanzania community builder diagnosis

Sue (GPT-6), 2026-10-21. Search tier: ripgrep + git floor.
Baseline: reviewed Tanzania PR1185 head
`e75be9c56fa6c318ce32cea5a8be6d448578bed1`.
Inherit `.coder/ledger/STANDING.md` sections 3-4, `CLAUDE.md`, and canonical
`lsms_library/data_info.yml`; do not duplicate or redefine their contracts.
This ledger is committed before new diagnosis functions or analysis.

## Section 1: Task, restated

Diagnose the Tanzania community-price country concatenator and actual Make
DAG, including normal default/Make all, supported-singleton, unsupported and
combined requests at target-cold, fresh-process warm and populated stages.
Produce a concrete minimal retirement proposal and discriminating test plan;
do not implement a candidate. GH788 settles framework authority. GH840/1086
settles the source variety/visit/observation representation. No science,
eligibility, unit basis, identifiers, grain or household crosswalk changes.

## Section 2: Existing machinery

| Symbol / file | Location at baseline | Existing behavior / test | Decision |
|---|---|---|---|
| Country concatenator | `Tanzania/_/community_prices.py` | Loops only 2019-20/2020-21 native parquets and concatenates; no source transform or ID walk | Diagnose retirement |
| Country Make block | `Tanzania/_/Makefile:145-149` | Sole-use discovery variables and explicit var target invoke country script | Audit all references and actual make -n |
| Independent wave rule | `Tanzania/_/Makefile:151-152` | Runs actual requested wave community script | Preserve |
| Generic rule | `Tanzania/_/Makefile:154-155` | `%.parquet: %.py CONTENTS.org tanzania.py` can affect legacy paths/discovery | Measure actual route; no regex inference |
| `_has_country_builder` | `country.py:4893` | Script existence, then real fresh-empty-root Make probe; GH1039 tests | Reuse |
| `_aggregate_wave_data` / `load_from_waves` | `country.py:4980/5249` | Native wave loading, ID walk/normalization, authoritative concatenation | Reuse |
| `_empty_declared_frame` | `country.py:1050` | Existing strict-subset no-builder empty contract | Measure; no normalized comparison |
| `_assert_built_required_columns` | `country.py:4817` | Existing post-finalize script-path column enforcement | Preserve |
| Native helper | `Tanzania/_/tanzania.py:1953` | Positive village price/quantity basis, resolved source item/unit, source-code variety, reported unit price | Preserve byte-exact |
| Both modern wave scripts | `Tanzania/{2019-20,2020-21}/_/community_prices.py` | Read own CM_SEC_F_ID, raw codes plus aligned item labels; sanctioned IO | Preserve |
| Sanctioned IO/hash/coverage | `local_tools.py`, `Country._table_cache_hash`, `Wave._input_hash` | Existing gates and native legacy script cache contracts; STANDING sections 2/4 | Reuse |
| Relevant tests/audit | `tests/test_community_prices_variety.py`, `tests/test_gh1039_make_backend_no_country_rule.py`, `bench/feature_audit/` | Existing source-code grain, Make routing and Feature checks | Reuse narrowly if prerequisites gated |
| Reviewed unit correction | ledger `1118-tanzania-plot-input-units.md`, source/API evidence under `slurm_logs/.../1118-Tanzania/` | PR1185 normalized 86+322 missing plot-input units, preserving 9698 rows | Verify exact reviewed blobs/prior receipts only; no AG staging/rebuild |

## Section 3: Definitions and country idiosyncrasies

Complete current Tanzania CONTENTS read: 2142 lines, Git blob
`9424a5042b9880d6c48aba17af0e8d3cadc45ccb`, including TODO/WAITING/LOGBOOK.
AGENTS/CLAUDE match the previously read complete guidance (SHA256
`c1205513db01fb743ec3c8b8c0ec69615a2bfcd9521fb7ed2d3195bd1a7e20dd`).
Read backlog-workflow, cross-country-features, multi-round-waves and
tanzania-panel-design; retain the current task's stricter no-bypass/no-acquisition
constraints over older skill examples suggesting cache clears or NO_CACHE.

- UPD4 back-casts split-off descendants into earlier waves. UPHI is the
  round4 panel identity; r_hhid is the interviewed round household. The
  many-to-many relationship is deliberate, and a unique key does not prove
  correct grain. The live WAITING #114 remains; no incidental key repair.
- Four logical waves share 2008-15/ via wave_folder_map. Use actual folders
  in scripts/caches; do not invent unsupported community sources there.
- 2014-15 pools Extended and Refresh panels; 2019-20 follows Extended,
  2020-21 follows Refresh plus 545 urban booster households. Zero household
  overlap between the latter waves is by design.
- Community v is native interview__key, not household sample cluster v.
  No i-bearing household join belongs on this table. Retain the native
  i==v compatibility shim until normal framework normalization, if produced
  by the existing helper; verify actual layers rather than assume it.
- Source item code variety preserves separately priced codes merged by j;
  Description retains each questionnaire's label. Price is cm_f063/cm_f062
  per native cm_f061 unit; district-capital prices are a different instrument
  and remain excluded. Positive price/quantity and resolved item/unit/key
  eligibility remains unchanged.
- CONTENTS says the geography-only join is "infeasible without a lossy,
  unverifiable crosswalk", then records an implemented confidence-tagged
  community_cluster_xwalk with deterministic 2020 matching. The schema's
  older region-only paragraph also differs from its adjacent implemented
  crosswalk section. Quote that contrast, do not silently erase either or
  rewrite the crosswalk. Native v remains authoritative for these prices.
- PR1185's 408-unit correction concerns plot_inputs, not community prices:
  86 in 2019-20 and 322 in 2020-21, including 19 no-amount records. Its existing
  conflicting-unit duplicate groups belong to #637, not this retirement.
  Root clarified that the original brief conflated these counts. Retain this
  as an immutable prior-proof invariant; stage only the two community price
  modules, and measure their actual counts independently. No AG source work
  or plot_inputs API rebuild is authorized in this diagnosis.

## Section 4: Invariants and validation boundary

- Read-only production/config/tests. The only tracked edit authorized now is
  this task ledger. No candidate patch or candidate build is authorized.
- New private root only; never reuse or mutate the frozen Niger/1078/849/Mali
  source or evidence roots. Safe exports get a dedicated safe-exports/
  directory; never overwrite a private aggregate basename.
- Exact cwd/import/PYTHONPATH/countries/config/data identities; mounted image,
  one numeric/build/Make thread and at most two explicit loadfile workers.
  Active socket/write guards plus unshare -rn before any source decoding/API.
- Stage only held declared sources using MD5/SHA/size/mode/mtime and physical
  copy/nlink/inode gates, both DVC layouts. Terminal successful preparation
  precedes dependent probes. No acquisition, DVC CLI, shared-cache operation,
  bypass, stamp, in-tree raw/parquet, household join or xwalk rewrite.
- Compare complete native/walked/normalized/raw-country/public and CLI
  stages, with check_exact=True, rtol=atol=0, all axes/levels/codes/dtypes/
  masks/order and recursively typed full attrs. No attr stripping or numeric
  tolerance. Capture per-call cache receipts BEFORE assertions.
- Default and Make are separately measured. Do not preserve any actual
  wrong-wave Make output as an acceptance target, or promise blanket zero
  warm writes where native hashless scripts naturally rebuild.
- Healthy comparator L2 requires exact current hash/full coverage and
  physical provenance gate at this source pin; d602 Niger is not assumed
  compatible. Avoid unrelated food/FCT builds and broad test/corpus sweeps.
- Any expected candidate hash scope is explicitly a prediction until an
  approved real patch and actual census exist. No source-guess shortcut.
- Raw frames/IDs/coordinates/warnings/tracebacks stay private. An allowlisted
  aggregate wrapper exports only bounded counts/statuses; never cat/head/tail
  or rg detailed failure logs. Preserve failed launches and exact limits.

## Section 5: Reuse decision

Reuse existing authoritative wave aggregation, fresh Make discovery, required
column guards, source helpers and cache machinery. No new concatenator,
grain/ID design, selector or scientific transform is needed. Diagnostic
wrappers and independent source/count receipts are private analysis only.
The concrete eventual removal allowlist and baseline-negative tests will be
proposed after measurement, not inferred from Niger or Mali.

## Section 6: Open scope and stop

No new human authority/grain decision is requested: framework authority is
settled. Finish the read-only diagnosis/proposal and stop for ROOT review
before implementation. Source gaps or contradictions are evidence to report,
not permission to acquire, infer a fallback or extend scope.
No GitHub write, public merge or issue closure.

## Phase 3: Verification

Read-only diagnosis complete; concrete proposal awaits ROOT review before any
implementation. Safe evidence lives in
`/local/job40255479/control/isa-backlog-20261021/Tanzania/1091/safe-exports/`.

- OK (sections 2/5): actual fresh and populated Make DAGs show both wave
  prerequisites and the country concatenator, plus the independent generic
  in-tree route. The country file is 45 concatenate-only lines; Make145-149
  is the sole-use variable/country-rule block. Proposal deletes only those
  two runtime paths, retaining wave151-152/generic154-155/shared variables.
  No implementation or selector patch was made.
- OK (sections 3/4): two held source modules/sidecars passed terminal MD5/SHA/
  size/mode/mtime gates, with 40 initial and four later physical source
  copies. The healthy Mali cache was genuinely current/full-coverage at the
  compatible source pin before copying, not assumed fresh.
- OK (sections 3/4): independent source binding retains 3,076/14,623 rows
  (17,699), all source-code varieties and exact own-price/own-basis values.
  Actual writer-stage native frames and normalized default country concat
  are fully exact at zero numeric tolerance. Coarse conflicts261/1281 and
  price disagreements243/1149 remain distinguished by variety; no first
  reducer, household link, unit policy or xwalk repair was introduced.
- OK (section 4): 20 cold,20 independent fresh-warm and20 populated request
  probes completed. Default respects all ten request sets; Make returns
  all17,699 rows for every one. All same-request cold/warm full frames,
  MultiIndex internals and recursively typed attrs are exact. Default warm/
  populated writes zero data-root cache changes; Make rewrites all three
  files and overwrites a verified populated country cache with hashless
  native output. All per-call receipts precede comparisons/assertions.
- LIMIT (section 4): strict layer distinctions remain. Native source-memory
  index dtype declarations differ at parquet serialization; native/walked
  values agree but the latter adds id_converted. Public all-wave default/
  Make differ only by that attr. Existing empty-helper Description is object,
  versus string from the old fallback; covered-cache slices carry the flag.
  CLI JSON serialization changes typed sampling/population/recall containers,
  with eight exact matched full Arrow footer/schema/file-byte controls.
  No relaxed dtype diagnostic is counted as a strict pass.
- OK (section 4): first/fresh warm pooled Feature frames match separately
  for default/currency=None (57,917 Tanzania+Mali;17,699 Make Tanzania-only).
  The four actual scanner records are individually classified, zero fail/
  error/skip records; there is no community_prices phase2 kwargs grid.
  No first cold Feature snapshot or Mali source rebuild is claimed.
- OK (sections 2/4): existing focused pytest selection58pass/0skip/0fail.
  A following make api-audit attempt was guarded during Poetry environment
  setup and returned2; the exact existing audit script passed once with the
  mounted interpreter. No dependency/guard change or pytest repeat occurred.
- LIMIT (section 4): an initial source-helper harness used the wrong cwd and
  failed curation lookup; preserved/corrected with no product/expected-data
  change. An auxiliary saved-artifact unpickle launch omitted the verified
  runner/import environment and failed ModuleNotFoundError. Its generic
  traceback reached tool output, without records/IDs/coords/source warnings;
  no successful decode, API/source/test call or cache mutation is claimed for
  it. Exact script/call/incident are retained; original stdout/stderr was not
  captured privately. Subsequent tally used only existing artifacts through
  the guarded identity-asserted runner.
- OK (sections 3/6): reviewed PR1185 runtime/test blobs and prior408 unit
  receipts are pinned; zero AG staging/plot_inputs rebuild. Source/runtime
  remains e75, with only this ledger changed in the checkout. The predicted
  25 Tanzania/524 others/39 framework hash scope is explicitly unmeasured
  until an actual approved patch. No broad corpus/suite or multi-country
  composition claim; stop for ROOT review before implementation.

## Approved implementation phase - 2026-10-22 UTC

Root accepted the frozen diagnosis/proposal after reading the actual Make DAG,
source oracle, strict comparisons and safe-export implementation. The decision
is pinned privately at `Tanzania/1091-fix/ROOT-ENGINEERING-DECISION.md`.
Implementation starts from clean `99c824079da480ab2646e499c06ac70e457ff271`;
its runtime/tests equal reviewed PR1185
`e75be9c56fa6c318ce32cea5a8be6d448578bed1`. The separate parent checkout stays
immutable, and the completed diagnosis/evidence stays frozen.

The current CONTENTS blob is exactly
`9424a5042b9880d6c48aba17af0e8d3cadc45ccb`; carry the complete 2142-line
reading and named/quoted findings in section 3, including TODO/WAITING/LOGBOOK.
Live PR1185 is open/unmerged on its actual
`fix/1118-tanzania-unknown-units-20261021` branch at e75; do not imply a merge.

### Production allowlist and reuse

- Delete only the 45-line `Tanzania/_/community_prices.py` country concat.
- Remove only original Make145-149 (two sole-use discovery variables, blank,
  explicit country rule and recipe). Preserve blank150, wave151-152,
  generic154-155 and every other Make byte.
- Reuse `_has_country_builder`, authoritative wave aggregation, finalizer,
  required Price guard, sanctioned IO/hash/coverage machinery from section 2.
  No new concatenator, selector patch, Quantity addition or science policy.
- Keep both native scripts, tanzania.py, units/maps, eligibility, own price/
  basis, native interview v/i==v, variety/grain, IDs, xwalk, schemas/materialize,
  framework and dependencies byte-exact. Keep the separate reviewed 408-unit
  plot-input proof without AG staging or plot-input rebuild.
- Additional tracked scope: one meaningful targeted test file, this ledger,
  a dated CONTENTS addition at the actual file end, and safe aggregate reports.

### Required measurements before acceptance

Measure all 549 API hashes and 39 framework fingerprints against the exact
immutable parent in separate source-asserted processes BEFORE source rebuilds.
The predicted 25 Tanzania/524 other/39 framework split is not a result until
measured. Preserve any country-wide re-warm cost; do not change registry/schema.

New tests use actual source Makefile/country-script presence, six logical waves
and the real four-to-one older folder map, two distinct native script outputs,
real selectors/exists/Make and no fabricated absence. Run the same new test
file unchanged on parent/candidate; meaningful parent failures must have zero
fixture errors. Cover supported/unsupported/combined/mixed requests, optional
Description and required Price without Quantity, protected existing country
cache, generic/wave/all DAGs, full native grain and attrs.

Fresh physical held sources pass terminal MD5/SHA/size/mode/mtime/nlink/inode
gates before any decoding/API. Use both backends at cold, separate-process
new-object warm and genuinely current populated stages, all six logical waves.
Persist endpoints before assertions; classify added/deleted/modified files.
Default zero writes require receipts. Natural Make hashless native eviction/
rewrites are measured, while Make must neither create nor serve/overwrite a
country cache on the new wave route.

All frame comparisons retain strict dtypes/classes, axis names/order/unused
levels/codes, nulls and recursively typed full attrs, including immutable record
`_FIELDS` presence/values; pandas float tolerance is not enough. Use exact
checks with rtol=atol=0. Preserve measured empty Description/id_converted and
JSON attr serialization distinctions without normalization. Reuse the direct
source oracle, all native/walked/normalized/raw/public layers, separate default/
currency=None Feature controls, current full-coverage healthy comparator, CLI
prewrite/postread/complete Arrow controls and individual scanner accounting.

### Operational boundary and final stop

Only the new candidate/parent ignored DVC tmp resolutions are metadata-write
exceptions. All data/config/XDG/temp and diagnostics are new/private, mounted
image with exact cwd/package/countries/config/data identities, offline/write
guards plus unshare -rn, numeric/build/Make threads one, at most two explicit
loadfile pytest workers. No acquisition, DVC CLI, shared mutation, cache bypass/
clear/stamp, source/fixture/golden/dependency change or broad suite/corpus build.
Keep safe exports separate from private basenames. Report substantive unexpected
behavior before expansion. Freeze a clean attributed published author head,
strict evidence and zero consumers for a DIFFERENT independent reviewer;
no author PASS, GitHub write, public merge or issue closure.

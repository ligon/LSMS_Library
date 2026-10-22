# Prior-Art Ledger -- GH1198 Uganda production observations

**Search tier:** git + ripgrep floor. This ledger precedes all new prototype
functions. Source baseline: f8d236694c26fe9e709c5548f02c70f74ea72c33.
Inherits [STANDING.md](STANDING.md), the repository CLAUDE.md and data_info.yml.

## §1 Task, restated

Implement the root-approved GH1198 observation coordinate for Uganda
crop_production on design head989fbf3d98439be802d1f9cebd8ab5197c56b002.
The 2026-10-22 ROOT-ENGINEERING-DECISION.md supersedes the private metadata
proposal: append string production_obs after season, preserve every eligible
native record/condition slot, and declare unresolved_observation for other
countries without asserting that their served row is one native observation.
Keep quantities, factors, sales and existing typed attrs at matched stages.
GH849's twelve factor mappings remain separate and unchanged from f8.

## §2 Existing machinery

| Machinery | Source | Existing coverage and decision |
|---|---|---|
| crop_production_for_wave / CROP_COLMAPS | countries/Uganda/_/uganda.py:1230,1620 | Reuse pure field decoding, eligibility and dtypes; tests/test_crop_kg_factor.py and test_uganda_crop_condition.py. Private projection must intercept before the seven-key sum/first block at1579. |
| Source identity and field oracles | frozen private Uganda/849/{DIAGNOSIS.org,PROPOSAL.md,source-identity.json,all-production-key-audit.json} | Reuse only after receipt gates and guarded physical copies; source release/row/slot locators are evidence, not an approved API. Strict artifact addendum separately accepted. |
| _normalize_dataframe_index / Country._finalize_result | country.py:8394,3815 | Reuse with deep-copied declaration in private controls; existing grain/index/population tests. Preserve id_walk and id_converted, never invent an ID formatter. |
| _align_to_canonical_levels / _harmonize_country_frame | feature.py:277,649 | Reuse canonical union/sentinel alignment and extra-level/drop controls; tests/test_feature_canonical_index.py. A country-only coordinate is insufficient. |
| harvest_kg_factors / harvest_kg / yield_kg | transformations.py:4282,4698,4888 | Reuse reported-factor screens, ladder, survey medians and explicit analyst reduction; tests/test_shipped_factors.py and test_crop_kg_factor.py. No new conversion estimator. |
| get_dataframe / get_data_file | local_tools.py; STANDING §2/§4 | Reuse sanctioned source access only if decoding is needed. Saved raw/native artifacts may instead be copied with exact receipts. |
| source_fingerprint / format_id | local_tools.py:2954; Uganda/_/uganda.py | Reuse declared-sidecar identity and native identifier formatting. Actual loaders pass explicit references from the same literal source paths they read; synthetic callers provide explicit synthetic references. |

## §3 Definitions and conventions

- CLAUDE.md:515: "Duplicates on a declared index mean the IDENTIFIER IS BROKEN
  or a LEVEL IS MISSING -- fix the index; do not declare a reducer."
- data_info.yml:107 currently declares crop grain
  `(t,v,i,plot_id,j,u,condition,season)`; sentinels at276 and Feature's
  alignment implementation are existing extension points, not approval to edit.
- Grain policy, SkunkWorks/grain_aggregation_policy.org:23: maximal instrument
  grain belongs in access/composition; explicit transformations may aggregate.
  The document labels its broader composition mechanism partly implemented.
- Uganda CONTENTS full f8 SHA256
  `774d5bc13c35ca5a6d9543766648f88dbc1efe508d73aa388d5ac6e81146256c`
  (3604 lines, including task/logbook entries). A/B names modules rather than
  calendar-season order; 2013 value vocabularies override swapped labels;
  2018 mixes legacy/wide names and A has no harvest-unit source; 2019 slots
  are condition observations. Sales unit/condition is separate.
- KgFactor is a rate on the SAME observation, not additive or a reported total.
  Existing finite/positive, cap250, kilogram1% and calendar-year screens stay.
- CONTENTS:2945 already identifies missing record ordinal; historical claims
  of "small and in the right direction" (2435), harmless repeated rows
  (helper1573) and older ladder wording conflict with frozen849 findings.
  Treat these as dated contradictions, never permission to discard evidence.

## §4 Invariants and assumptions

- STANDING §4 applies. Only the root-approved helper/callers/crop declarations,
  canonical axis/sentinel/docs, tests and country notes may change. No cache
  stamp, bypass, acquisition, shared-cache mutation or promoted private parquet.
- Retain EVERY eligible observation, even equal payloads, absent factors or
  missing native IDs. A physical row ordinal is CONSTRUCTED/source-order and
  bound to exact file release provenance, never survey-reported identity.
- 2011/2013/2015 production IDs and 2018/2019 actual slots require separate
  namespace and uniqueness auditing. Unkeyed2009/2010 require full-row-repeat
  classification, not automatic deletion. Future ID conflicts stay explicit.
- Existing null-key losses360/43 and 2015 sales x100 remain separate. No
  Quantity/factor/unit/eligibility correction is included in this phase.
- Feature drops undeclared extras and collapses; the approved declaration must
  be tested through actual Country/Feature normalization and alignment.
- No production_observation attrs carrier is introduced. Existing metadata
  machinery remains authoritative; canonical_alignment may record the new
  declared sentinel. Unequal unregistered attrs do not survive natural concat.
- Capture physical zero-based row positions before selection/join/melt. Tokens
  row:<source-fingerprint>:<ordinal> identify the same declared release/order;
  dvc fingerprints contain the source name and SHA256 of the declared sidecar,
  not raw MD5. Nonempty constructed identities reject missing: fingerprints.
- Missing declared native-ID columns raise CropColmapError; missing values use
  constructed fallback. Duplicate IDs/slots suffix EVERY occurrence, including
  equal payloads. Repeated identical (source,row,slot) locators fail loudly.
- Preserve all403 old null-key exclusions and928 eligible null-v records.
- Before builds, measure all549 API/39 framework/API-wave hashes independently.
  Use physically private gated held L1 sources, normal empty-L2 cold builds and
  separate fresh-process warm checks; at most two loadfile workers/threads1.
- Explicit zero-tolerance full-frame, axis, dtype, mask and recursive typed
  attrs comparisons; pandas default floating tolerances are insufficient.

## §5 Reuse decision

| Quantity | Decision | Reason |
|---|---|---|
| Source decoding, slots, screens | reuse | Pure helper and frozen source oracles already encode them. |
| Observation coordinate | extend | Native IDs/actual slots/explicit release-row tokens fill the missing instrument grain; remove only the final lossy reducer. |
| Country / Feature alignment | extend declarations | Existing hooks consume the new axis and honest unresolved sentinel; no framework code/flag/carrier. |
| Per-row kg then harvest/yield | reuse | Existing transformations implement both conversion and analyst reduction. |
| Comparator | reuse real gated operands | Actual healthy comparator requires full current-hash/coverage receipts and matching arguments; synthetic controls are labelled separately. |

## §6 Approved boundary and remaining verification

Root approved the revised implementation in the 2026-10-22 task brief and
ROOT-ENGINEERING-DECISION.md, after reading the full frozen design. This ledger
update is committed BEFORE production edits or new analysis. No additional
survey decision is required. Production callers, source references, native
identity collisions, real area/yield controls, both backend subset behavior,
typed attrs, scanner findings and cold/warm dtype behavior require measurement.
Report an unheld prerequisite or existing Make subset defect before expanding
scope. Independent review precedes any PR; this lane has no GitHub writes.

### Prior frozen design verification (not implementation acceptance)

- OK (§2/§4): all-record private projection retains133761 nonnull-key native
  observations,430 more than133331 baseline keys;403 existing null-key rows
  remain separately excluded. Zero full-source-row repeats across14 source
  frames;33 equal-helper-payload groups remain separate.
- OK (§2/§3):99 independent paired-field checks;35 private semantic/mutation
  controls, including seven exact-comparator negatives. Existing normalization,
  finalizer and declared Feature alignment retain all projected observations.
  Current undeclared Feature route collapses428 nonnull rows and deletes928
  null-v rows; this is evidence for declaring the coordinate, not an ID fix.
- OK (§2/§5): whole-frame row conversion and explicit harvest reduction plus
  independent report-count/median oracles use existing machinery. Current-wiring
  projection changes2018 by2091kg; separate849 factor-wiring measurement is
  labelled independently. Yield checks use explicitly synthetic areas; actual
  source-area yields remain a concrete future prerequisite/test.
- OK (§4):101 physical artifact copies and17 current-cache parquets retain
  bytes/modes/mtimes. Runtime/config/schema/tests remain exactly f8. No new
  DTA decode or source-cold build, candidate hash claim, production/schema edit
  or public observation contract is made. Safe DESIGN/PROTOTYPE/commands are
  the engineering handoff; original strict stops and handling qualifications
  remain private and are described there. Sue (GPT-6).

### Implementation verification, 2026-10-22

- OK (§2/§4): runtime/test commit `929149222e43a8fe500cc08420cdc68357170af3`
  follows ledger-first `9144f5085`. All seven real loaders fingerprint the same
  literal sources they read. Reported IDs, physical release rows, reordered
  declared slots, missing values/columns and duplicate locators have controls.
  The conditional old nonunique-index null boundary is exact, including an
  otherwise-unique null-key frame; 403 held-source exclusions remain separate.
- OK (§2/§5): 59 held DTA prerequisites were physically copied to private L1,
  both layouts MD5/SHA/size/mode/mtime/inode/nlink-gated before decode. Forty-one
  sidecars omit a declared size; actual copy size is recorded, not invented.
  Normal empty-L2 source builds retain133761 records on both backends. Fresh
  default warm changes0/144 file receipts; Make changes17 mtimes only, exactly
  the measured existing parent route. No cache stamp/bypass/acquisition occurs.
- OK (§4): 85 complete raw/native/public/sample/area frame comparisons use
  check_exact=True,rtol=atol=0 plus every index level/code/class/dtype/null mask
  and recursively typed full attrs. They connect the corrected final builds to
  the independently decoded source oracle and prior downstream operands. No
  production_observation metadata is manufactured. All928 null-v records survive.
- OK (§2/§5): actual healthy Niger46376 and pooled Feature180137, both currency
  branches, with fresh-process API pairs and a separately labelled artifact
  oracle. Country bare default is None; Feature default is index. The earlier
  parent Country operand labelled default explicitly requested index and is
  preserved as such. No original Niger bare-cold API was saved; its actual
  explicit-None operand is available. Only canonical_alignment gains the
  Niger unresolved_observation entry; other typed attrs are exact.
- OK (§2/§3): current-wiring whole harvest78571 rows, +2091kg; factor-layer
  counts and median pool effects measured using existing transformations.
  Actual plot_features42841 and real parcel-yield14022 full frames are exact
  parent/candidate. The documented100% parcel-match claim is contradicted by
  14022/40409 converted keys and0 literal-plot matches. Separate residual only.
- OK (§4): actual inventories before final source builds:28/549 Uganda country
  hashes move,521 others exact;0/39 framework fingerprints;171/1362 declared
  wave hashes and224/2139 API-wave pairs move, all Uganda. All old CROP_COLMAPS
  entries including kg_factor are AST-exact f8 after removing identity fields.
- OK (§2/§4):392 focused cases pass,10 genuine unstaged unrelated warm-corpus
  skips,0 errors/failures. Final39 observation controls give36 failures/3 passes
  on actual parent989; the three passes include the preserved unique-null,
  duplicate-null and empty-input boundaries. Strict API audit passes.
- OK (§4): scoped unmodified scanner51→41 WARN,0FAIL/ERROR, no phase2 grid.
  Exact full-record multiset22 unchanged,29 removed,19 added: ten old lossy
  label-reducer warnings vanish; remaining differences are quantity diagnostics,
  row-count/index declaration text and the declared sentinel alignment. Every
  occurrence/digest remains available without warning normalization.
- Residual (§6): Make country crop builder ignores strict wave subsets on both
  heads; default unsupported2005 public0 still comes from a whole raw country
  fallback with requested-only coverage metadata. No builder retirement here.
- Evidence limits: original34-pass v2 log survives, its XML was overwritten by
  a new39-pass run; no recovery/replay claim. Original source snapshots, null/
  slot/attrs stops, premature hash check, scanner-wrapper argument stop and
  area-oracle axis-construction stop remain pinned separately. Corrected runs
  use unique names/fresh roots; safe exports never replace private operands.

Independent review is still required; these are author verification results.
No public PR or GitHub write was made. Sue (GPT-6).

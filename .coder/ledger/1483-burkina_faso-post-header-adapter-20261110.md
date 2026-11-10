# Prior-Art Ledger — #1483 Burkina Faso post-header adapter

Status: source implementation candidate frozen at
`850cb762871711098d8a58b3b8ab0b3f08fd0685`; every pure/runtime control UNRUN.
Ledger-first commit: `c6c1533270254e8c06916b0d5e09236bd1583b70`.
Search tier: rg + git (no exposed GitNexus). Exact initial
HEAD4381d701ffb06ab447a37c66362119d1c1e23009, checkout and branch asserted.
Tracked initial source was clean; two untracked channel shadows are untouched.
Only the dispatch's absolute mutable channels were used.

## §1 Task, restated

Implement own2021-22 household s16b behind the existing `plot_inputs` table,
with a separate raw-answer projection, source-backed applied-input membership
and independent used/gift/purchased units. Retain native codes/unknowns, no
parcel allocation, preserve2018 source and all existing assertions. Freeze
portable invented-frame controls and a finite dispatcher command before any
verification. No package imports, tests/collection, providers, runtime setup,
Make/API/cache/native or document reads are admitted to this worker.

ROOT's later steering explicitly authorizes actual private TierB wiring,
without another approval loop. A supplementary own header now labels q09c
Montant; it resolves the initial q09a carrier gap. Earlier inactive freezes
and source-only receipts remain historical, not failed runtime attempts.

## §2 Existing machinery

| machinery | coverage / reuse decision |
|---|---|
| `2018-19/_/plot_inputs.py:_input_labels,_seed_crop_labels,_unit_labels,_finish_plot_inputs` | reference for existing #1123/#1119/#1188 wide-column semantics and collision refusal; module has top-level IO, not importable as a pure helper; unchanged |
| `_/burkina_faso.py:i` and `local_tools.py:format_id` | actual own2021 formatter; reuse by injection; controls extract these exact production definitions without package imports |
| `_/categorical_mapping.org:harmonize_input,harmonize_seed_crop,u` | reuse exact own-label keys; never feed2018 bare tail codes to a2021 map; existing file unchanged |
| `local_tools.py:get_categorical_mapping,get_dataframe,to_parquet` | existing Org resolver and sanctioned wrapper leaves; injected only in pure controls |
| `.coder/ledger/1123-burkina-input-identity.md` | distinct native slots must not collide, even with equal payloads; count-only refusal |
| `.coder/ledger/1188-burkina-input-purchase-routing.md` | same-country accepted wide representation, independent nullable amounts/flags/units; no false/zero/u substitution |
| `.coder/ledger/1119-burkina-seed-crop-labels.md` | source-settled crop identity meets existing vocabulary, never allocation |
| `transformations.py:_normalise_input_label` | exact nutrient lookup candidates, not substrings; extract actual helper in the code6 pure control; no transform/provider execution here |
| closed v3 source5f3533d0706c1fc497fe9d10b8497b0d45955109 ledger/plan and five canonical evidence files | reuse exact own2021 source binding and historical key/gate findings; no old documents/framework transplanted or diagnostic replay |

New `_/s16b_2021.py` is necessary: the2018 driver executes IO at import and
its decoded source meanings are2018-specific; `_map_codes` silently returns
NA for unknowns. The new pure projection/adapter explicitly preserves raw
answers and unknown identities. No2018 refactor or shared helper is needed.

## §3 Definitions & source conventions in force

Inherit `STANDING.md` §§3–4, CLAUDE's Canonical Schema, Grain Collapse,
Panel ID and Data Access contracts, and `lsms_library/data_info.yml`'s input
axes. Personally read FULL3385-line original country CONTENTS, including all
TODO/WAITING qualifications. Its SHA256 is
`9fef29cf986d4918a21f49b9abbe25c4a4e101b6c7b5a0647ebd626899ec73b4`.

s16b is household × input, no parcel allocation. Vague is a sample split.
Own2021 native i is grappe+menage.zfill(3), before exactly one framework walk.
CONTRADICTION retained: generic ehcvm_i prose says new EHCVM features “MUST
use this helper”; own2021 plot wrapper says it “must build ... i with the
SAME formatter” as sample, country i. The own-wave contract is operative.
The old scheme's “no parcel-level input data” is too broad for agriculture:
#1483 separately names s16a parcel questions. This adapter asserts only s16b
grain and does not promote or adjudicate s16a. Original notes are preserved.

Own header SHA256
`61045df1365ad170216e088ef02794a5d8aa950746907ba77fc0369eeb912f52`
binds source MD5ed8625abcf1d679669d7d231cf77f58c. It supplies22 input codes,
22 labels on EACH unit field and1Oui/2Non flags. q01/q02 variable labels are
None; q02's own card plus closed own2021 diagnosis bind use. q03a/b are use,
q05/07a/b gifts, q08 purchases. Preserve truncated question labels verbatim.

Supplement SHA256
`8656adfce39210322208af352622866b77f828b8091fe2130800d5ade92f14c9`
binds the SAME source: q09c literally “16B.09c. Montant”; q09b “Unité” and
q09a's combined quantity/value stem remain. That own structural split assigns
quantity to q09a and amount to q09c under #1188's accepted representation.
This is not dtype-based meaning or2018 borrowing. q09c is retained native;
no canonical money derivation. Both packets returned zero records; observed
multiplicities, masks/counts, missing tags, CAPI and release cause do not follow.

## §4 Invariants and authorized boundary

- Raw layer retains all admitted No/blank/unknown answers and original types,
  masks/order/attrs. Public layer admits only own q02=1; explicit2 excluded
  and counted; missing/unknown gate refuses, no amount/acquisition gate.
- Missing flags remain NA; unknown nonmissing flags refuse. Negative, zero
  and missing quantities survive; no scale, clipping, sentinel guess or fill.
- Missing/duplicate native identity and mapped collisions refuse before write;
  unknown integral slots/units remain source-qualified plus raw native codes.
- Own code6 stays FULL `DAP et autres engrais inorganiques`: ROOT explicitly
  prohibits extending the legacy2018 pure-DAP fold/0.18 assumption to2021.
  No existing2018 map or NPK grade policy changes. Combined rubber/teak stays
  literal and distinct; no single crop/physical Seed classification invented.
- All2018 code/maps and numerical oracle values stay exact. ROOT explicitly
  authorized updating only the obsolete2021-empty availability expectation. New raw
  columns widen combined output; old seven payload columns/keys must remain
  exact in later verification. The existing2018 code21 count remains602 and all payload assertions remain;
  that oracle now queries2018 explicitly.2014 stays empty;2021 is required
  nonempty with its requested t. New2021 groundnut slot19 no longer enters
  the2018-only count. No real count, tolerance or golden hash is repinned.
- No strata/weights/population/season/exclusions/anonymous route/shared schema,
  other-country/framework or source-sidecar edit. Entire original CONTENTS
  survives as an exact226693-byte prefix. No new native/producer/API run.
- Return attrs are preserved by code and tested prospectively; no composed
  Country/Feature/storage/warm transport proof is claimed. Build hashes are
  expected to move for this country, not measured. No cache census/rewarm.

## §5 Reuse decision

Reuse existing index, public table, optional-column contract, country formatter,
Org resolver and sanctioned leaves. Extend only source-settled identities:
`Semences d'arichide` -> existing Seed/Arachide; rubber/teak retains its literal
combined identity. Code6's full native label follows ROOT's explicit hold.
New pure raw/application functions provide provider-free controls without
changing2018 or global machinery. New2021 wrapper is the minimal serving
wiring; fourteen native optional columns provide a direct inverse route.
No new estimator, reducer, physical factor, nutrient grade or allocation.

## §6 Remaining questions and next gate

1483-Q09A was recorded once and closed in the absolute channel after ROOT's
supplement. No missing source/representation decision blocks this household
binding. ROOT owns review, independent verification, availability-baseline review and
any PR; no GitHub write/publication/merge is performed here.

Next gate is exactly the new test file, under the MAIN/exact cwd/PYTHONPATH/
LSMS_COUNTRIES_ROOT/private false credential profile, BLAS/build1 and
`-n2 --dist=loadfile --noconftest`, as frozen in `_/s16b_2021_candidate.md`.
No native rows are needed. Subsequent native/storage/ID-walk/API work needs a
separate finite plan. #1483 other objectives, #1573/#1442/#1148/#1437/#1436/
#1482/#1485 holds remain. #587/#594 is already done; no matrix rerun.

### Phase 3 — anchored source checks; runtime UNRUN

- OK (§2/§5), static: production driver uses current sanctioned leaves and
  own-wave formatter; existing2018 machinery stays byte-exact. New controls
  execute actual extracted formatter/normalizer and actual wrapper call only
  with invented frames/injected leaves. They have NOT been collected/run.
- OK (§3/§4), static: own cards and q09c authority are authenticated metadata;
  combined q09a wording and raw answers remain. No borrowed tails, units,
  parcel allocation or pure-DAP fold. Numerical/native equivalence UNRUN.
- OK (§4), source closure at the final freeze: exactly9 owned changed paths,
  19261 nonowned Git mode/type/blob entries exact; original country prefix
  exact. Existing scheme bytes are exact after removing ONLY the added raw
  field block. Four changed Python files parse using stdlib only; no definitions
  execute. git diff --check passes. Package/config import identity UNRUN.
- Retain initial first-freeze diff-check exit2: a literal blank patch context
  carried trailing whitespace.30b9cc177 corrected it; executable code/tests
  were unchanged then. The unapplied patch passed git apply --check before
  own q09c resupply superseded it with real wiring. Neither is runtime proof.
- OK (§2/§4), static: own source path now occurs as a real literal argument in
  the wave wrapper so existing `scan_script_data_refs` discovers the source
  sidecar. Driver refuses alternate paths; the actual wrapper-call control
  pins this argument. No runtime cache fingerprint was computed.
- ROOT's last steering explicitly broadens test ownership for the obsolete
  availability assertion in `test_burkina_faso_plot_inputs_identity.py`.
  Old: all-wave source21 oracle, then2014/2021 both empty. New: same2018
  numerical oracle explicitly scoped to2018,2014 empty,2021 nonempty and
  requested t only. This preserves the original2018 oracle population when
  new2021 Arachide slot19 appears. Exact old/new source diff is saved in the
  final SOURCE-PROOF; existing/new test execution remains UNRUN.
- No runtime REINVENTION/CONTRADICTION verdict is claimed. Pure controls,
  pytest/collection/JUnit, native/header/record parsing, Make/Country/Feature,
  provider/acquisition/cache/census/full CI are all UNRUN. On-disk skill
  frontmatter was enumerated; make api-audit was not run under this profile.
  Prior CLOSED v3 failed audit/native/supervisor attempts retain their states.

## Coordinator independent source P2 and selected correction (2026-11-10; before execution)

Original2f059aef/850cb762 source and all worker UNRUN receipts remain immutable. Independent full9path review found a membership contradiction: adapt_s16b_2021 validates all raw nativekeys and refuses missing/unlisted q02 before gating, whereas its approved public membership is exactly q02==1 (samecountry2018script filters first). Rawnonapplication/unknown answers can therefore abort the new public wave. Root accepted the correction: retain whole rawprojection byte/types/order/attrs, select positiveown q02==1 first, then keep all existing appliedidentity/flag/amount/card/collision guards. ExplicitNon count stays literalq02==2; missing/unlisted answers are excluded from the appliedtable without being relabelledNon, zero or applications. No amount/content gate, DAPpolicy, allocation or othercountrychange. Meaningful purecontrols will include malformed/duplicate nonappliedpayloads versus still-loud malformedappliedpayloads. BFApure/native phases are UNRUN at this ledger-first correction freeze; Root separately grants one held source read ONLY after correctedpureacceptance.

### Independent nullable-card correction, 2026-11-10

The first pure run retained 1 pass and 41 setup errors from the format_id receiver allowlist. The fixture-only tail correction then yielded 39 passes and 2 failures among those 41 cases. One is a real label bug: pandas nullable-integer map callbacks can present integral codes as floats when missing values occur. The adapter now canonicalizes already-validated integral codes for the card lookup and unknown identity; no rounding or source mutation is introduced. The other failure occurred before the adapter: assigning None through a pandas3 float64 multi-column setter. Its fixture now explicitly constructs nullable Float64 columns. The full independent literal frame oracle remains unchanged; a direct nullable-code/card control is added. Only these two failed cases and the new control are selected next; native validation remains pending.

### Closed independent controls and native adapter acceptance

Corrected runtime f4fb4080922652d05d4b0dfae3bfecf474893318 is independently checked. Preserve the actual histories: initial 42 cases = 1 pass/41 setup errors; receiver-only fixture correction then 41 affected cases = 39 passes/2 failures; nullable-card product correction plus nullable fixture construction then ONLY the 2 failed cases and 1 new nullable-card control = 3 passes. This is 43 distinct pure cases across attempts, not a fresh 43-green cohort. All actual runs used exact MAIN/source/countries/private identities, network isolation, zero guard denials and no shared/private top-level data/config writes.

Root's separate finite native grant was executed once: exact own2021 source ed8625abcf1d679669d7d231cf77f58c (20,891,535 bytes), one sanctioned get_dataframe(convert_categoricals=False), actual injected driver/current own cards/country maps/source-bound i. 70,994 native rows yielded 12,223 explicit-q02=1 applications. Exact native selected raw projection, household identity, separate used/gift/purchase quantity/unit/flag projections, input/RAM/typed attrs and original/distinct0400copy seals all passed; one private output only. Actor60751 exit0/one pytest pass/27.55s, source clean/bindings equal/zero denials. Evidence: reports/review-1483-bfa2021-native-adapter-20261110; pure corrections in review-1483-bfa2021-nullable-card-20261110. This validates the native adapter and driver, not final Country/Feature/data-CI, Cross_Section, fullwave, nutrient-grade or conversion authority. No source acquisition/shared cache/real API calls.

### Current Country-route writer placement correction (2026-11-10; ledger first)

The exact d47 failed-node normal reproduction passed all2018 source/card/602 and2014-empty assertions, then failed on2021 at test line219. The guarded writer attempted an in-tree2021 parquet because the country helper calls directly supplied to_parquet, whose existing stack_depth2 sees the country helper instead of the wave. Correct only the wave callback location; adapter/frame/field/membership/cards/maps unchanged. Preserve original private1FAIL and30physicalseals in reports/1687-bfa-source21-normal-reproduction-20261110. This is an observed current placement defect; the original public CI retained no failing line, so it is not retroactively identified. Source review/pureactualwriter control precedes any newly granted real-source continuation.

The narrow writer runtime237d126c7819e909b59e77ff63daad1253cb7bb6 has one new portable pure control:1PASS/31.85pytest seconds/69.597wrapper, exact MAIN/checkout/countries/private roots, UID65534/network isolation, three identities/profiles, zero denials/raw-getter calls. The test executes the actual wave wrapper and pure helper/current cards/country formatter, inventing only the native getter payload; its pandas parquet leaf spy is below unchanged sanctioned path resolution and requires the exact private2021 wave path, rejects the source tree, and checks literal full frame/dtypes/attrs/raw conservation. All original wave executable AST is recovered by removing only the new callback and restoring its write keyword; adapter/config/maps/oldtests remain exactd47. Evidence: reports/1687-bfa-wave-writer-correction-20261110. Corrected normal Country/data-CI acceptance remains UNRUN pending ROOT's finite continuation review; no current-source rows or tests were replayed beyond this single pure case.

ROOT's subsequent precise normal-source grant executed the unchanged failed node ONCE on617ebee7:1PASS/183.52pytest seconds/225.053wrapper. All2018 own-card/602/Seed/requested-wave,2014empty and2021nonempty/requested-wave assertions passed via normal Country/sample APIs, with the same10authenticated source roles and existing private caches subject ONLY to ordinary hash/coverage gates. Exact30original/private raw seals and10sidecars passed pre/post;3MAIN/UID65534/private identities,6activeprofiles,0denials, sourceclean/bindingssame. Normal hash invalidation rebuilt2021sample/country sample from4declared2021sample inputs and the input tables;2014/2018wave sample seals were unchanged. This is explained by the existing whole wave-directory build-input hash (country.py1554–1567), not a freshness override. Corrected10getter/10StataReader.read events plus preserved firstfailed14/14 give24/24 cumulative call events; no new source/acquisition/field or policy. Quantity warnings and values remain unchanged. Evidence: reports/1687-bfa-source21-normal-corrected-20261110. Its first metadata-only closure summary wrongly expected all sample-cache bytes unchanged; that assertion/source is preserved and corrected using the already-saved inventories, with no API/payload replay. This finite normal-node acceptance is separate from the original guarded1FAIL, pure1PASS, historical native-adapter acceptance and eventual complete data-CI. No original discarded publicCI failing assertion is claimed recovered.

### Exact a292 unit-CI wrapper fixture correction (2026-11-10; ledger first)

Public unit42831553220/job129882623816 on a292cee408d984f985839938e2c582ec79fbae75 has exactly1FAIL/14531PASS/5673SKIP/4XFAIL: test_driver_reads_only_own_numeric_source_and_writes_after_validation. The fixture compiles only the actual wrapper's final call; the accepted placement repair introduced preceding _write_wave FunctionDef, so the fixture raises NameError before the production builder. This is not a product, package-version, scientific-input or downstream data failure. Full-data run remains independently owned/live.

Read prior-art/STANDING and existing own1483/writer ledger; authenticated prior own3385-line country carry and current dated1483/status source passages retain the household-s16b vs parcel-s16a, own2021 code6 catch-all, and inactive/UNRUN vs accepted-wiring qualifications. No nutrient/parcel/coverage hold changes. Independent original-node reproduction on EXACTa292: actor25277 exit1/16.0316wrapper,1FAIL/8.33pytest, same _write_wave NameError at wrapper22. All3private MAIN/source/root identity profiles active0denials/configdata0/sourceclean. No raw/provider/API/native/DVC/census or live branch mutation.

Root approves only compiling the ACTUAL wrapper _write_wave FunctionDef with its actual final call, with the existing invented to_parquet/write leaf. No fabricated callback alias, product/config change, altered source/map/quantity/validation/writer assertion or new science. Preserve original failure and all prior current/native evidence. Freeze before corrected affected1; no wholemodule/suite/native replay. Root owns public head/CI replacement; independent train stays untouched.

Corrected runtime3b4cdeee52f0da6c81d160ac5be375372ed78a3d compiles actual _write_wave FunctionDef + actual finalCall in the same inert namespace and positively checks write keyword names that definition. All old source/map/getter/validation/frame/writes assertions remain byte-exact; reversing only fixture assembly recovers the ENTIRE original test file/AST. Library subtree and all production/config bytes remain exacta292. ONE affected node actor15329 CLOSED0:1PASS/0FAIL/ERROR/SKIP,7.36pytest/15.583961wrapper. Baseline25277 remains1FAIL/NameError separately; neither is a fullsuite/current-data verdict. Same guard byte-exact/fresh private namespace/head/selection bindings; both runs have3exact MAIN/package/cwd/root identities, all3profiles active through finish,0denials/privateconfigdata0/sourceclean. No observed global import-fixture defect and no changes to it. No native/API/DVC/census/dependency/GitHub action. Reports/1687-a292-bfa-driver-fixture-20261110 holds exact public-failure trace, frozen source inverse proof, baseline/corrected XML/logs/receipts. Root/integration lead own public-head and independent train decisions.

# Prior-Art Ledger — #1483 Burkina Faso post-header adapter

Status: grounded source-only candidate; implementation and controls follow this
ledger-first commit. Verification is UNRUN. Search tier: rg + git (no exposed
GitNexus). Initial HEAD4381d701ffb06ab447a37c66362119d1c1e23009 and exact
checkout/branch asserted. Tracked source is clean; the two pre-existing
untracked channel shadows are untouched. Absolute dispatch channels only.

## §1 Task, restated

Prepare an own2021-22 household s16b adapter and the smallest reviewable
serving-wiring candidate for #1483's first independent Tier B objective.
The authenticated own header now supplies the 22 input and three 22-unit
cards; it is metadata, not native row evidence. No imports, tests/collection,
providers, runtime setup or source-record reads in this worker. Preserve2018
exactly; freeze finite synthetic controls for separate dispatcher execution.

## §2 Existing machinery

| source / symbol | existing coverage / decision |
|---|---|
| `2018-19/_/plot_inputs.py:_input_labels,_seed_crop_labels,_unit_labels,_finish_plot_inputs` | #1123/#1119/#1188 tests; reference for wide nullable payload, usage gate and pre-write collision refusal; module has top-level IO, cannot import as a pure helper |
| `_/burkina_faso.py:i` and `local_tools.py:format_id` | own2021 plot wrapper/current YAML formatter; reuse by injection, retain pre-walk native namespace |
| `_/categorical_mapping.org:harmonize_input,harmonize_seed_crop,u` | reuse exact label keys only; no 2018 numeric-tail lookup for2021 |
| `local_tools.py:get_categorical_mapping,get_dataframe,to_parquet` | reuse existing Org resolver and sanctioned driver leaves; pure controls replace only driver leaves |
| `.coder/ledger/1123-burkina-input-identity.md` | native slots must not collide even with equal payloads; count-only refusal |
| `.coder/ledger/1188-burkina-input-purchase-routing.md` | ROOT's wide column representation: independent nullable flags/amounts/units, no false/zero/u fill |
| `.coder/ledger/1119-burkina-seed-crop-labels.md` | crop names meet existing vocabulary, no household-to-parcel allocation |
| closed v3 source5f3533d0706c1fc497fe9d10b8497b0d45955109 ledger/plan and canonical evidence | reuse H2021 binding and old key/gate observations; no old documents transplanted, no diagnostic replay |

New pure numeric adapter is justified because the current2018 driver executes
IO at import and uses2018 decoded labels/maps. Its unknown-to-NA code mapper
`burkina_faso._map_codes` cannot preserve unknown slot/unit identities. Do not
refactor2018 just to share a few pandas operations.

## §3 Definitions & conventions in force

Inherit `STANDING.md` §§3–4, CLAUDE's Canonical Schema, Grain Collapse,
Panel ID and Data Access contracts, and `lsms_library/data_info.yml`'s input
axes. Personally read FULL3385-line `_/CONTENTS.org`, including TODO/WAITING.
Relevant idiosyncrasies: s16b is household × input, no parcel axis; vague is a
sample split. 2021 i is grappe+menage.zfill(3), before the one framework walk.
CONTRADICTION retained: generic `ehcvm_i` prose says new EHCVM features “MUST
use this helper”, while own2021 plot wrapper says it “must build ... i with
the SAME formatter” as sample, country i. Follow own-wave source, not that
generic2018 instruction. 2021 code21 is rubber/teak plants, not2018 groundnut.

Header JSON SHA25661045df1365ad170216e088ef02794a5d8aa950746907ba77fc0369eeb912f52
binds md5ed8625abcf1d679669d7d231cf77f58c. q02=1Oui/2Non plus prior closed
source diagnosis establish use gate; q03a/b used amount/unit; q05/07a/b gifts;
q08 purchases; q09b its own unit. q09a literally names both quantity and total
value. Do not assign that double to Quantity_purchased or a monetary field.

## §4 Invariants & assumptions

No scientific-default, weight, strata, nutrient-grade, exclusion, parcel,
season, population, anonymous-route or2018 change. Preserve native codes,
unknowns, quantities, masks, row order and typed attrs; refuse missing native
identity, unknown/missing gate, malformed numeric values, duplicate native or
canonical keys. Missing acquisition flags remain NA; unknown nonmissing flags
must refuse, not silently become NA. Decode each unit from its own card;
unknown codes stay source-qualified. Native codes remain payload columns.
All CONTENTS bytes must survive as a prefix. No native file, sidecar mutation,
shared write, package import or test execution. No claim of observed counts,
cold/warm/API acceptance, metadata fingerprints or whole-issue completion.

## §5 Reuse decision

- Reuse current country i/format_id via explicit parameters and existing
  categorical resolver for current country maps and newly transcribed own cards.
- Extend only source-settled identities: groundnut spelling `arichide` ->
  existing Arachide; existing crop-generic splits; rubber/teak plants retain
  their literal combined source identity instead of inventing one crop.
- New bounded pure adapter/driver are necessary for provider-free controls and
  own numeric cards; no shared helper or2018 refactor.
- Keep proposed wiring inactive pending q09a carrier decision. Adapter may
  return unambiguous used/gift quantities, purchase flag/unit and verbatim
  q09a with Quantity_purchased NA plus explicit unresolved warning. This is
  reviewable code, not permission to activate an unapproved carrier.

## §6 Exact remaining question

1483-Q09A is appended once to the absolute QUESTIONS_FROM_AGENT channel:
may own2021 q09a be Quantity_purchased, or should activation retain its native
name with an explicit unresolved warning and an NA canonical purchased amount?
#1188 settles representation but not this own2021 field's dual wording. No raw
resupply is requested. Gate/identity/used semantics need no new ruling.
All other #1483 objectives and #1573/#1442/#1148/#1437/#1436/#1482/#1485 holds
remain. NPK/DAP existing mappings are not a new grade endorsement.

### Phase 3 — verification

UNRUN, including pure controls. Dispatcher must execute a finite frozen
selection only after code+tests freeze, with exact cwd/source/configuration,
private false credentials and BLAS/build1, -n2 --dist=loadfile. This worker
will perform only standard-library source closure/diff checks.

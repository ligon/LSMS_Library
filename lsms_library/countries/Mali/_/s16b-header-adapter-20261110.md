# Mali #1483: own-header adapter and bounded serving patch design

Base: `4381d701ffb06ab447a37c66362119d1c1e23009`. Implementation is
`s16b_header_adapter.py`; controls are
`tests/test_mali_1483_header_adapter_20261110.py`. Ledger:
`.coder/ledger/1483-mali-post-header-adapter-20261110.md`.

This is a **nonactivated implementation candidate**, not a serving fix or a
new inventory. No wave declaration, Make target, country schema, categorical
map, EAC-I driver/finalizer, framework or installed CLI changed. The CLOSED
v3 packet's bindings/residuals are reused; the new deliverable implements
the mechanical transport and executable synthetic controls after header supply.

## Native semantics now established

Each own authenticated HEADER.json supplies all native fields (19 in 2018,
17 in 2021), the 20/22 input menu, and separately attached q02/q05/q08 flags
and q03b/q07b/q09b unit cards. Constants retain both source/header hashes and
the full source inventory. Own flags are 1 Oui/2 Non. Each own unit card is
1 Gramme/2 Kilogramme/3 Tonne/4 Litre/5 Charrette. Equal menus do not mean a
record's three unit answers agree.

`s16b_rows_for_wave(raw, wave, household_id=...)` accepts numeric source
frames and the **existing** `mali.i` (2018) or `mali.ehcvm21_i` (2021).
It produces a review frame indexed `(t,i,s16bq01)` without an ID walk.
Every original column remains recoverable, in original row order, with its
input dtype, amount and missingness. The source slot also stays in the
payload. Duplicate/missing/malformed native keys refuse before selection or
summation; native uniqueness is not assumed from zero returned header rows.
Unknown card codes keep their raw values and explicit `*_unlisted` masks;
their decoded aliases are NA, distinguishable from an unanswered source.

| Alias | Own source | Meaning |
|---|---|---|
| Input_native | q01 | literal own input-type label, no harmonization |
| Used | q02 | type-level native use answer, not an EAC-I group answer |
| Quantity / u | q03a / q03b | used amount / own unit |
| Received_as_gift | q05 | gift/donation answer |
| Quantity_received_as_gift / Unit_received_as_gift | q07a / q07b | gift amount / own unit |
| Purchased | q08 | purchase answer |
| Quantity_purchased / Unit_purchased | q09a / q09b | purchase amount / own unit |
| s16bq09c (raw name retained) | q09c | reported total purchase amount; no new cost/price definition |

The four independent acquisition carrier names reuse Burkina GH1188's
country-only precedent; the source meanings come from Mali's OWN cards.
The 2021 gift quantity is native float32, unlike its used/purchased double
fields; the adapter preserves that distinction, not a numeric dtype cast.
All source fields, including 2018 area/respondent and 2021 donor text, stay.

The adapter preserves native 99/999/9999/99999, zero and negative amounts.
That is not approval of their scientific meaning: no own header declares
those amount codes missing, and row evidence has not been read. Neither
EAC-I masks nor Niger's quantity sentinel transfer. `s16b_use_classes` gives
an exhaustive yes/no/unanswered/unlisted answer partition without filtering.

2018 code19=tubers and20=other seeds; 2021 code19 is literal
`Semences d'arichide`,20=tubers,21=hevea/teck,22=other seeds. No numeric menu
is transplanted. DAP catch-all and NPK/Formule unique remain literal; no
pure-DAP, N grade, kg conversion or seed catch-all crop is inferred.

## Concrete activation proposal for the existing public table

Target is the EXISTING `Country('Mali').plot_inputs()` table, with its unchanged
country-native index `(t,i,plot_id,input,crop)`. Required scalar columns are
`Quantity: float`, `u: str`, `Purchased: bool`, `Quantity_purchased: float`,
`Improved: bool`, and `Applied: bool` in `Mali/_/data_scheme.yml`. `v` remains
an API-time sample join. `Feature` retains its current crop->j alignment and
unit/season/observation sentinels. No new table or global index is proposed.

**Membership proposal is now exact, rather than a policy hold:** native
`s16bq02 == 1`, and ONLY that predicate. This follows the own Oui card, #1483's
explicit unserved-APPLIED objective, existing EHCVM drivers, and Mali's existing
applied-input table. `s16b_applied_candidate` implements this selection on the
review operand. It has no quantity-positive, amount-present, purchase/gift,
unit-present, recognized-input or EAC-I content predicate. A Yes with no used
amount survives; a No/blank/unlisted answer does not become application from
another amount. These classes remain in `s16b_rows_for_wave` with explicit
answer masks. The earlier question about serving every raw answer is withdrawn
as an eligibility blocker after ROOT's steering clarification.

Mali's GH1118 group-answer/zero ruling is preserved exactly for EAC-I. It does
not change this per-type source question. At the proposed EHCVM projection,
`Applied <- q02` is therefore True on every eligible type row, while EAC-I's
Applied remains its own group-row answer. `Improved` remains NA: s16b has no
such answer. Its existing REQUIRED declaration stays, so an EHCVM-only strict
read can report/fail the all-missing Improved field; no optionality waiver is
proposed to hide it.

**Payload:** `Quantity/u <- q03a/b`; `Purchased/Quantity_purchased <- q08/q09a`.
The four optional country-only fields are exactly GH1188's
`Unit_purchased <- q09b`, `Received_as_gift <- q05`,
`Quantity_received_as_gift <- q07a`, `Unit_received_as_gift <- q07b`.
They require no guessed purchase meaning: Mali's OWN header settles these
bindings. Purchase amount q09c, acquisition source q04, donor q06/text,
respondent q00, area fields and vague are withheld from this minimal public
payload, not erased from the raw review operand. q09c is a reported total,
not a unit price or cost of used inputs; adding a new monetary API is outside
the applied-input fix. No source value is substituted for Improved.

**Household carrier:** every new row gets existing `plot_id='Farm-level'`.
Mali's already accepted GH1476/GH1437 household records and the canonical
EHCVM-input marker settle this structural carrier. No parcel join, allocation
or area denominator follows. `i` remains bare in 2018 and EHCVM21_-qualified
in 2021 before the normal exactly-once framework panel map.

**Concrete label proposal, still not activated:** use the following
wave-local exact-label extension, retaining literal Input_native/q01 in the
review operand. This is a proposed country projection, not a claim that these
new curation rows already exist. Classifications use existing Mali input/crop
spellings; broad native seed categories remain broad rather than a guessed
species. The source code is shown only to locate its OWN card.

| Own slot | Proposed input | Proposed crop |
|---|---|---|
| 1 animal wastes | Manure | (not crop-specific) |
| 2 household wastes | Other organic | (not crop-specific) |
| 3 Uree | Urea | (not crop-specific) |
| 4 Phosphates | Engrais inorganiques - Phosphates (literal) | (not crop-specific) |
| 5 NPK/Formule unique | NPK | (not crop-specific) |
| 6 DAP et autres engrais inorganiques | DAP et autres engrais inorganiques (literal) | (not crop-specific) |
| 7 pesticides / 8 herbicides / 9 fungicides / 10 other phyto | Pesticide / Herbicide / Fungicide / Other phytosanitary respectively | (not crop-specific) |
| 11 millet / 12 sorghum / 13 maize / 14 rice | Seed | Mil / Sorgho / Maïs / Riz respectively |
| 15 other cereal seeds | Seed | Autre céréale (existing #1123 broad-category precedent) |
| 16 cotton / 17 sesame / 18 beans-cowpea | Seed | Coton / Sésame / Haricots secs (Mali's existing fold) |
| 2018 19; 2021 20 tubers | Seed | Autre tubercule (existing #1123 broad-category precedent) |
| 2018 20; 2021 22 other seeds | Seed | Autres semences (literal broad category) |
| 2021 19 | Seed | Semences d'arichide (literal retained; no spelling/species inference) |
| 2021 21 | Seed | Plants d'hévéa, de teck (literal compound category; never split) |

All 20/22 proposed `(input,crop)` pairs are distinct within their wave, so the
current public key can retain native-slot identity WITHOUT migrating EAC-I's
index. An observed repeated native household-slot key already refuses; an
unlisted input, unlisted unit, unexpected map collision or missing native ID
must refuse the PUBLIC write, not drop a source row or silently give it an
existing identity. The raw review adapter still retains unlisted answers.
A future curated fold must retain injectivity or explicitly add input_obs;
no current coordinate migration is needed by this proposal.

For missing used units use the already accepted Mali GH1118 `U_UNKNOWN`
representation, only after distinguishing absence from an unlisted code.
Known units retain their OWN literal labels. Ordinary public `u` mapping can
respell `Kilogramme -> Kg` through Mali's existing table without changing any
amount. Acquisition units stay paired independent payload columns, with NA
preserved and no use-unit substitution. No Tonne relabel, kg factor or density
is proposed. Existing NPK/default consumers remain unchanged; no grade is
asserted. **The explicit reserved #1483/#1445 question is code6's classification
for nutrient consumers.** The proposed literal carrier makes no pure-DAP/0.18
claim; adopting a pure-DAP alias requires the separately reserved decision.
Phosphates likewise is not silently renamed TSP. This proposal changes no
nutrient mapping or fraction.

## Exact follow-on source edits and effects

After review of that proposed projection (not executed here):

1. Add own `2018-19/_/plot_inputs.py` and `2021-22/_/plot_inputs.py` drivers,
   reading exactly their literal `../Data/s16b_me_mli2018.dta` and
   `../Data/s16b_me_mli2021.dta` with the sanctioned numeric getter. Each calls
   the adapter with existing wave household helper and projects the exact
   predicate/columns above, never `plot_inputs_finalize`.
2. Add only the above exact-label mapping rows at Mali wave/country level;
   add `plot_inputs: !make` to both own wave declarations. Add the four
   optional acquisition fields to Mali's country scheme, retaining every
   existing required field. No q09c, input_obs or shared schema migration.
3. Add the two new wave parquet prerequisites to the existing country VAR
   rule. Its current declaration-driven concatenator already refuses missing
   declared outputs. Guard native and public keys before the sanctioned writer.
4. Preserve both EAC-I driver/finalizer bytes and all old assertions. Public
   EAC-I payloads/keys/quantities remain exact under projection to their old
   columns; cross-wave concat can add NA-valued acquisition columns. Full old
   schema equality must not be claimed for an ACTIVATED all-wave frame with
   new columns. In the delivered inactive candidate even that change is absent.

New native public rows would equal the number of own q02=1 records if all key,
map and unit guards pass; the header gives no such observed count. No reducer,
unit sum or amount conversion occurs. q03a/b and eligible q08/q09a/b and
q05/q07a/b are retained one-for-one. Rejected answer classes remain fully
recoverable from the unfiltered operand. The inverse/raw route is exact
`review[original_source_columns].reset_index(drop=True)`; t/i decoding is an
addition, not an irreversible replacement. Keep the full review operand before
selection in any later bounded native proof; native input bytes need not be
copied into Git or a public cache.

No wave driver, declaration, schema/map/Make edit or public write is delivered
in this phase: the concrete projection remains a nonactivated design, while
native transport and its exact applied predicate are implemented. Next gate
is the finite frozen pure selection below and ROOT review of this explicit
projection, especially the reserved DAP classification boundary. Native-source
correspondence and Default/Make/Feature/warm are later separately scoped gates.
Source-only work establishes neither actual key uniqueness nor cache footprint.
All other named reserved issues, parcel s16a and historical #587/#594 stay held.

## Frozen pure verification gate — UNRUN

Only coordinator execution is authorized by a later runtime release. This
worker has neither imported dependencies nor collected/run tests. The runtime
profile says `source-only; no LSMS/runtime mount/import` and supplies no
verified interpreter; it must not be replaced or guessed by this worker.

From the exact candidate checkout after authenticating the code/test SHA and
prepared MAIN interpreter, use the coordinator's private false-credential
bindings/data root and source/config identity, BLAS/build pins1, and **only**:

```sh
env PYTHONPATH="$CHECKOUT" \
  LSMS_COUNTRIES_ROOT="$CHECKOUT/lsms_library/countries" \
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 \
  LSMS_BUILD_WORKERS=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  "$MAIN" -m pytest -p xdist.plugin --noconftest -n 2 --dist=loadfile \
  -o addopts='' -o cache_dir="$PRIVATE/pytest-cache" -q \
  tests/test_mali_1483_header_adapter_20261110.py \
  > "$PRIVATE/pure-controls.log" 2>&1
```

`CHECKOUT` must be cwd. These are command parameters, not environment bootstrap
instructions. No other suite/native/source/API/Make selection is implied.
The tests load this pure module and AST-extract current `mali.i`,
`mali.ehcvm21_i`, and `local_tools.format_id` with only `pd`/format_id/prefix
globals. They never import the provider-capable LSMS or Mali module. AST import
and call allowlists and provider/writer/socket refusals guard the selected
closure; arbitrary caller-supplied household callbacks are outside that proof.
Independent expected frames cover full raw transport, dtypes/masks/exact
nonmissing values/typed attrs, source menus/unknowns, independent units/flags,
empty frames, namespace, and missing/repeated/malformed-key refusals.
All expected results are controls to execute, not observed successes.

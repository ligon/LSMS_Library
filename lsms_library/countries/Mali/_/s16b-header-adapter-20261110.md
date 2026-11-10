# Mali #1483: own s16b implementation candidate

Base `4381d701ffb06ab447a37c66362119d1c1e23009`; country-only source changes.
ROOT explicitly authorized the actual private Tier B wiring candidate and
accepted native q02=1. Code/tests are frozen before verification. **All tests,
native sources, producers and API routes remain UNRUN by this worker.**
This delivers source for review, not empirical or merge acceptance.

## Existing table, exact predicates and carriers

The existing `Country('Mali').plot_inputs()` key remains
`(t,i,plot_id,input,crop)`. Its required scalar columns remain Quantity, u,
Purchased, Quantity_purchased, Improved and Applied, with their existing types.
Two own wave `plot_inputs: !make` declarations and numeric-source drivers now
read only their own s16b via the sanctioned getter. The existing declaration-
driven country concatenator and Make pattern are reused; the country target
adds exactly the two EHCVM prerequisites. Their shared pure helper is an
explicit Make prerequisite too. No EAC-I driver/finalizer is edited.

**Only membership predicate: own numeric `s16bq02 == 1` (Oui).** It follows
#1483's APPLIED scope, Mali's applied-input table and EHCVM precedent. No
amount-positive, amount-present, unit-present, recognized-label, gift/purchase
or EAC-I content gate is added. Yes with a missing/zero/negative used amount
stays a row. No/blank/unlisted use answers are not applications from their
other amounts. `s16b_rows_for_wave` preserves every native answer separately;
`s16b_use_classes` exposes yes/no/unanswered/unlisted masks. The initial
question about public inclusion of all raw answers was withdrawn as a blocker
after ROOT's clarification; it is not a scientific disagreement.

EHCVM rows get structural `plot_id='Farm-level'` under the existing canonical
and same-country GH1476/GH1437 carrier. This names no parcel and supplies no
area denominator. Existing mali.i supplies bare2018 IDs, mali.ehcvm21_i the
EHCVM21_ raw2021 namespace; normal framework panel map and sample v join remain.
No ID walk, v join or household-to-parcel allocation occurs in these drivers.

## Own-source payload and identity

| Public field | Own source | Treatment |
|---|---|---|
| Quantity / u | q03a / q03b | used amount / own literal unit |
| Purchased / Quantity_purchased | q08 / q09a | purchase answer / amount |
| Unit_purchased | q09b | independent purchase unit, optional |
| Received_as_gift | q05 | independent gift answer, optional |
| Quantity_received_as_gift / Unit_received_as_gift | q07a / q07b | gift amount / own unit, optional |
| Applied | q02 | True on selected EHCVM type rows; EAC-I keeps group-only meaning |
| Improved | no s16b source | NA, preserving the REQUIRED declaration and its warnings |
| Input_native / input_code_native | q01 own card / raw code | optional literal identity payload |

Every one of the known20/22 source slots has a distinct `(input,crop)` pair,
so the existing public key preserves its identity without migrating EAC-I's
index. Retained native label/code also makes the source menu explicit.
Specific seed crops reuse Mali's existing labels. Other cereals/tubers reuse
existing broad `Autre céréale` / `Autre tubercule` labels (#1123). Other seeds
stay `Autres semences`. 2021 code19 stays literal `Semences d'arichide`,
code21 `Plants d'hévéa, de teck`; neither is assigned a new species or split.
2018 code19 is tubers and20 other seeds; no numeric menu is transplanted.

**Code6 stays full `DAP et autres engrais inorganiques`, never pure DAP.**
Existing production nitrogen matching is exact after normalization; this
literal catch-all does not acquire DAP's0.18. Phosphates also stays its full
native label, never inferred TSP. NPK uses the existing generic NPK label;
no grade/fraction/consumer changes. The reserved #1483/#1445 DAP classification
remains unruled; literal transport preserves the input without deciding it.
The frozen pure controls exercise the actual extracted nutrient matcher.

Missing used units get existing `Unknown` under Mali GH1118. Unlisted
identities/units/acquisition flags on selected rows refuse the public write,
never masquerade as missing/False or disappear. Raw unknown values and masks
remain in the unfiltered adapter. Known units use each field's OWN attached
card; no purchase/gift unit substitutes for u. Country's existing u mapping
can respell Kilogramme to Kg, without scaling; acquisition labels stay paired
native payloads. No Tonne relabel, kg factor, density or amount sentinel mask.

q09c, q04, q06/text, q00, 2018 area fields and vague are withheld from the
minimal public payload and retained exactly in the raw adapter. q09c is a
reported total purchase amount, not a price or used-input cost. q00 is a
respondent, not a line ID; area supplies no parcel ID. No new monetary API,
season/recall interpretation or nutrient derivation is introduced.

## Effects, inverse route and limits

The raw adapter preserves source columns/order/dtypes/missingness/typed attrs,
including2021 gift float32, all numeric quantities, zeros and unlabelled large
amounts. The public helper converts its three numeric amount carriers to
float64 for the declared API while retaining their exact native numeric values;
flags are nullable boolean, units/labels nullable string, native code Int64.
Native missing/repeated/malformed household-slot keys refuse; public label/key
collisions refuse. These guards do not claim actual source uniqueness.

New public rows would equal own q02=1 records if the guards pass. Header-only
metadata establishes no observed count/multiplicity/eligibility conservation.
The inverse raw route is
`review[original_source_columns].reset_index(drop=True)` before selection;
all excluded answers and withheld fields remain there. No raw bytes enter Git.

Both EAC-I producer/finalizer bytes and old required assertions are unchanged.
Cross-wave concat can add NA-valued optional columns on EAC-I rows; exact
old-column/key/value/dtype/attrs comparison is a later runtime gate, not a new
measurement. Full activated all-wave schema equality to the old frame is not
claimed. Config/module additions are country build inputs; a country re-warm
is expected, but no fingerprint census or cache operation was run. The old
sampling/season/coverage, sale/state/strata/Tonne and historical #587/#594
qualifications remain untouched.

## Finite pure verification gate — UNRUN

ROOT owns execution using prepared MAIN, exact candidate cwd/PYTHONPATH and
LSMS_COUNTRIES_ROOT plus private false-credential/data bindings. This worker's
runtime profile remains source-only and names no verified interpreter; no
runtime was mounted, replaced or guessed. Freeze code/test hashes first, then
from CHECKOUT execute ONLY:

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

Controls load the pure adapter and AST-extract CURRENT i/ehcvm21_i/format_id,
source prefix, the two real driver build functions and nutrient matching.
No LSMS/Mali module, Country, native parser or producer is imported/executed.
Only invented frames feed driver getter spies; writer spies capture frames.
Independent full expected frames cover both adapters, public keys/amounts/
units/masks/dtypes/typed attrs, own full menu tails and broad identities,
unknown/refusal/empty cases. AST import/call allowlists, socket/provider/writer
refusals bound the tested closure; arbitrary caller-injected callbacks are
outside that proof. No source paths or private credentials are test operands.

Subsequent native correspondence, country compiler, Default/Make/Feature/warm,
strict diagnostics and full CI require separately scoped ROOT receipts. The
existing Improved all-missing required-column warning remains an expected
scientific-data limitation, not permission to weaken the schema. Private
publication and file presence do not establish any runtime pass.

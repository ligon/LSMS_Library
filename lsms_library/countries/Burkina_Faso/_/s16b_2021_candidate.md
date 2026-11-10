# Own2021 s16b serving candidate for #1483

New real adapter, normal wave wrapper, own cards and portable pure controls
are frozen for the dispatcher. **All pure/runtime verification is UNRUN.**
No native values, LSMS package, producer or API was executed in this worker.
This is a private source implementation candidate, not an accepted runtime fix.

## Own authority and exact binding

Initial header SHA256
`61045df1365ad170216e088ef02794a5d8aa950746907ba77fc0369eeb912f52`
binds source MD5 `ed8625abcf1d679669d7d231cf77f58c`: complete19-name inventory,
22 input labels, three separately attached22-unit cards and own Oui/Non cards.
The separately authorized q09c supplement SHA256
`8656adfce39210322208af352622866b77f828b8091fe2130800d5ade92f14c9`
binds the SAME source and labels q09c **"16B.09c. Montant"**. Both headers
returned zero records. They do not establish observed multiplicities, native
missingness, counts, fielded CAPI or a new scientific/default policy.

q09a's exact wording remains: “16B.09a. Quelles sont la quantité et la valeur
totale de %rostertitle% achetées?” q09b is “16B.09b. Unité”. Together with
q09c's explicit Montant, this own-source split binds q09a to the quantity
and q09c to the amount, following the already accepted SAME-country #1188
wide-column contract. This is a structural reading of the own labels, **not**
a guess from double/int32 types or a transplanted2018 question. The initial
withheld-purchase candidate at30b9cc177 is superseded; its source-only check
receipts and original wording remain historical. No further Q09A decision is
needed for this binding. q09c is retained under its native name; no canonical
monetary field, valuation, unit price or conversion is added.

Historical #1123 source observations (70,994 rows,12,223 applied rows, unique
native household/input key) are reused as historical evidence, not remeasured
or inferred from the zero-row headers. q01/q02 variable labels are absent in
the header; q02's own1Oui/2Non card and the independently closed own2021 gate
diagnosis bind use. No2018 or sibling tail meanings are borrowed.

## Concrete serving wiring and membership

The new normal `2021-22/_/plot_inputs.py` uses existing native-script
registration. It passes sanctioned `get_dataframe`, `get_categorical_mapping`
and `to_parquet` leaves plus current `burkina_faso.i` and `format_id` to the
country-local driver. No compiler, framework, global schema or Makefile edit.
The source file is exactly `../Data/s16b_me_bfa2021.dta`, read numerically once.
This is the panel file; no Cross_Section operand is introduced.

Existing public table: `plot_inputs`, Country index `(t,i,input,crop,u)`.
Required fields remain `Quantity: float`, `Purchased: bool`,
`Quantity_purchased: float`. Existing optional fields remain `Unit_purchased`,
`Received_as_gift`, `Quantity_received_as_gift`, `Unit_received_as_gift`.
The separate `raw_s16b_2021` projection preserves all admitted No/blank/unknown
answers without decoding or an application gate. Fourteen native admitted
fields are also added as optional raw columns on the public applied result. The inverse
route is `result.reset_index()[RAW_FIELDS]`: original selected answers/codes,
order, dtypes and masks, not a household-to-parcel allocation.

Exact public membership predicate: own numeric `s16bq02 == 1`. Only explicit
2Non is excluded, counted in native attrs. Blank/unknown q02 refuses the
candidate, not a default Non. Amount sign/positivity, gift and purchase
responses never gate an applied row. Negative, zero and missing quantities
survive; unknown nonmissing acquisition flags refuse rather than become NA.
Missing flags remain nullable NA. No usage/acquisition field fills another.
Required native key `(grappe,menage,s16bq01)` must be nonmissing/unique.
Duplicate native keys (even equal payloads) and mapped-key collisions refuse
before writing. Missing IDs and malformed/fractional codes refuse; unknown
integral input/unit codes retain source-qualified identity plus the raw code.

| Output | Own source | Exact role |
|---|---|---|
| native `i` | current country `i(grappe,menage)` | e.g. invented `(2,126)` -> `2126`, not2018 `20126`; framework walks once later |
| `Quantity`, `u` | q03a, own q03b card | used amount/unit, no conversion |
| `input`, `crop` | own q01 card, existing exact-label maps | no numeric2018 tails; `arichide` extends approved Arachide identity; rubber/teak stays its literal combined identity |
| `Received_as_gift` | own q05 | independent nullable flag |
| `Quantity_received_as_gift`, `Unit_received_as_gift` | q07a, own q07b card | independently unit-tagged gifts |
| `Purchased` | own q08 | independent nullable flag |
| `Quantity_purchased`, `Unit_purchased` | q09a, own q09b card | independently unit-tagged purchases, never q09c Montant |
| fourteen native columns | admitted original source answers | includes source input/unit codes, q02, vague and q09c; no canonical money derivation |

The22-unit dictionaries include explicitly sized bags and local containers;
labels remain exact, with only existing exact country u-map keys applied.
This selects no mass conversion or nutrient grade. Own code6 remains the FULL
`DAP et autres engrais inorganiques` catch-all on the input axis, even though
the legacy2018 map calls it DAP. ROOT explicitly prohibits extending that
pure-DAP/0.18 treatment to the new wave; no existing2018 mapping is changed.
The current exact nitrogen normalizer cannot match this full label to DAP. The combined
rubber/teak slot is present in the table and remains distinct; no single crop
or new physical Seed classification is invented for it. q04/q06 supplier
questions and their free text are outside this used/acquired-quantity adapter.

## Effects, preserved scope and limits

This private source now wires own2021 applied s16b through the existing table;
its execution remains UNRUN. The first independent #1483 Tier B objective has
an implementation candidate. It adds no s16a parcel applications, CS coverage,
allocation, weight, strata, season, default or scientific-grade change.
The old scheme phrase “no parcel-level input data” is too broad for all
agriculture (the dated #1483 note names s16a questions); this adapter relies
only on s16b's household grain and does not rewrite that separate question.
Generic `ehcvm_i` “MUST use” prose contradicts the own2021 plot wrapper's
same-as-sample instruction; the own2021 country i contract is operative here.

Every2018 builder/map/assertion is byte-preserved. A combined output necessarily
has new raw columns, NA for2018; its old keys, seven payload columns, values,
units and masks must remain exact. The retained old2021-empty assertion in
`test_burkina_faso_plot_inputs_identity.py` would be superseded by this new
coverage. It is NOT edited within this ownership scope; an activation review
must explicitly update that expectation. Do not claim it would pass unchanged.
The literal new controls independently pin old seven-column semantics at the
new source grain; no native2018 or combined API equality is claimed here.

Full typed attrs are preserved on the adapter return. This does not establish
Country/Feature transport: different wave attrs can disagree at concat. Cache
hash movement is expected from country py/org/scheme and wave py additions,
but has NOT been measured. No cache stamp, schema bump or corpus census.
The provider-free adapter/driver controls do not establish actual loader,
parquet, cold/warm, ID-walk, sample join or API transport behavior.

#1483's parcel, DAP and other-country objectives remain. Preserve #1573 strata,
#1442 money, #1148 completion, #1437 CAPI, #1436 crop, #1482 unit basis and
#1485 anonymous routing. #587/#594 is already done; no matrix/diagnostic replay.

## Frozen finite verification handoff

Selection: **only** `tests/test_burkina_faso_1483_header_adapter_20261110.py`.
No native data is needed. Dispatcher supplies its verified MAIN interpreter
and private false credentials/data/cache/config prefix; the worker profile
names no runtime interpreter and none is invented here.

From the exact candidate checkout, after dispatcher source/configuration and
MAIN identity assertions, use the separately prepared `PROJECT_PYTHON`:

```sh
PYTHONPATH="$PWD" LSMS_COUNTRIES_ROOT="$PWD/lsms_library/countries" \
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 \
PYTHONDONTWRITEBYTECODE=1 "$PROJECT_PYTHON" -m pytest --noconftest \
  -n 2 --dist=loadfile -q -p no:cacheprovider \
  tests/test_burkina_faso_1483_header_adapter_20261110.py \
  > "$PRIVATE_VERIFICATION_DIR/1483-pure.log" 2>&1
```

Private prefix/false credential bindings are required dispatcher preconditions.
Actual current country i/format_id definitions are extracted from committed
source into a pandas-only namespace; the actual wrapper's final call executes
with injected leaves. No package import/provider/raw read runs in these tests.
Provider imports and dependency AST are guarded. Literal independent expected
frames pin ordered keys/dtypes/masks/raw answers/typed attrs. Additional
controls cover all22 slots, all22 units in EACH unit role, changed independent
cards, q09a versus q09c/gift/use values, contradictory/missing flags, explicit
No versus missing/unknown eligibility, malformed codes/amounts, native/mapped
equal-payload collisions, writer-after-validation, typed empty return and
missing fields/cards. No PASS count is claimed before actual receipts.

Next gate: this frozen pure selection. Native/API/storage, ID-walk/attrs and
old-baseline review require a finite separate dispatcher plan afterward.
No native/header/record resupply, broad suite, Make or provider execution is
requested by this candidate handoff.

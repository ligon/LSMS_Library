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

## Existing serving contract and the exact activation boundary

Existing EHCVM drivers (Niger/Burkina) gate **public inputs** on `q02==1`.
They do not serve No/unknown rows or gifts/purchases as application. Mali's
EAC-I `plot_inputs_finalize` uses a different content predicate and its
explicit GH1118 decision keeps recorded zero slots and GROUP use answers.
Those are different source questions. Its sum/first-unit/drop-unmapped
reducer cannot be used for s16b. The canonical `Farm-level` marker does settle
the household carrier: it denotes a structural household record, not a plot
or a missing parcel. It does not settle source eligibility or crop mapping.

**Exact decision question, recorded in the dispatch channel:** apply the
ordinary EHCVM applied-only gate to Mali, including a No/unanswered use flag
with reported amounts or gift/purchase-only rows, or preserve these answers
through a separately specified public carrier? There is no source conflict
about the meaning of q02. The gap is the operative policy for those classes;
no actual frequency is claimed. No new source read is needed to run pure
controls. Inclusion of all rows in this review operand is NOT a public
eligibility decision.

Mali's current public `(t,i,plot_id,input,crop)` index does not itself retain
q01. A public mapping must keep source slots injective or retain their native
observation coordinate. Changing the country index would also change EAC-I's
interface, contrary to this dispatch's preservation scope. This candidate
therefore stops before selecting an index migration. The adapter's key is
explicitly an INPUT-grain operand, not a new canonical index.

## Concrete minimal follow-on patch, after a ruling

1. Add **two small numeric-source drivers**, at
   `2018-19/_/plot_inputs.py` and `2021-22/_/plot_inputs.py`. Each reads exactly
   its own literal `../Data/s16b_me_mli{2018,2021}.dta` via `get_dataframe` with
   `convert_categoricals=False`, then calls this adapter with the existing
   wave formatter. Neither imports or calls `plot_inputs_finalize`.
2. Project the explicitly ruled eligibility class from the review operand.
   Provide accounting for every rejected native class; unlisted flags/units
   require an explicit refusal or documented carrier, never a map-to-NA drop.
   An applied-only projection must not erase an applied row with all amounts
   missing. No/unknown inclusion must not be inferred from a gift or purchase.
3. Extend only own country mapping rows where authority dictates an exact
   label. Retain q01 and literal Input_native. Require injectivity on the
   existing declared key before writing; if identity cannot fit without a
   coordinate change, stop for that explicit migration. In particular code6
   may not silently enter DAP's 0.18. No source-backed crop is fabricated for
   another-cereal/tuber/other-seed/hevea-teck catch-all.
4. Set the ruled household records' plot_id to existing `Farm-level`;
   preserve used-unit `u` and independent gift/purchase-unit columns. Declare
   the four GH1188 acquisition fields optional in the Mali scheme while
   preserving existing scalar requirements. Decide `Used` versus `Applied`
   per-type carrier explicitly beside EAC-I's group-only interpretation;
   retain q09c under its raw name until an optional amount carrier is named.
5. Add `plot_inputs: !make` to the two own wave declarations and add the two
   exact wave parquet prerequisites to the existing country VAR Make rule.
   Its declaration-driven concatenator already refuses missing declared wave
   outputs. Keep both EAC-I producers, outputs and assertions intact.
6. Verify source/declared/public identity, masks, values, dtype, typed attrs
   and gate accounting in private roots; then Default/Make/Feature/warm.
   Any scheme/index/map changes require a separately reviewed full-country
   hash footprint. No numeric cache bill is claimed for this inactive helper.

Steps 1–6 are a bounded patch DESIGN, not executed changes or approval to
read sources. Parcel s16a, DAP policy, sampling/weights, all other named
reserved decisions and historical #587/#594 remain outside this candidate.

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

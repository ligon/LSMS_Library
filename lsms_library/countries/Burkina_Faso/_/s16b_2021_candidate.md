# Own2021 s16b: concrete source-only candidate for #1483

This is new executable adapter/driver code and an unapplied serving patch,
not a repeat of the closed v3 inventory. All controls and runtime verification
are **UNRUN**. The source-only profile admits no imports/collection, providers,
native rows, Make, cache or API execution in this worker.

The header is authenticated by SHA256
`61045df1365ad170216e088ef02794a5d8aa950746907ba77fc0369eeb912f52`.
Its source MD5 is `ed8625abcf1d679669d7d231cf77f58c`, matching the v3
binding. Complete 19-name inventory, 22 input labels, three separately
attached 22-unit cards and three own flag cards are supplied metadata.
The 70,994 rows / 12,223 applied rows / unique household-input key are
historical #1123 observations reused from the closed source diagnosis, not
new observations from this zero-returned-row header.

## Concrete serving proposal

Apply `s16b_2021_activation.patch` only after the dispatcher verifies the frozen
candidate and accepts its bounded used-quantity profile. It adds the normal
`2021-22/_/plot_inputs.py` script and optional raw columns to the **existing**
country `plot_inputs` table. The current native script declaration machinery
discovers that script; no shared registry, country compiler or Makefile change
is proposed. The wrapper passes current `burkina_faso.i` and `format_id` to
the candidate's explicit driver leaves `get_dataframe`,
`get_categorical_mapping` and `to_parquet`.

Current country index: `(t,i,input,crop,u)`. Required payload columns:
`Quantity: float`, `Purchased: bool`, `Quantity_purchased: float`.
Four existing optional fields: `Unit_purchased`, `Received_as_gift`,
`Quantity_received_as_gift`, `Unit_received_as_gift`.
The patch declares the thirteen native admitted columns optional; this is the
inverse route from an adapter result to its selected source answers, with raw
codes and original masks/dtypes. No respondent records are committed.

The exact applicable membership predicate is numeric own `s16bq02 == 1`
(own card `1 Oui`, `2 Non`, independently reused #1123 use-gate diagnosis).
Only explicit `2` is excluded and counted. Blank/unknown q02 refuses the
candidate; it is not silently treated as Non. Required native identity is
`(grappe,menage,s16bq01)`; null or duplicate native keys refuse, including
equal-payload repeats. No quantity sign/positivity or acquisition response
gates a used row. Missing amounts, explicit zero and negative amounts survive
unchanged. Unknown input/unit codes survive as source-qualified identities
alongside their numeric source columns. Fractional/malformed code values
refuse rather than truncate. An output-key collision refuses before writing.

| Column / axis | Own binding | Effect / qualification |
|---|---|---|
| `i` | current country `i(grappe,menage)` | native panel namespace; e.g. invented `(2,126)` -> `2126`, not 2018's `20126`; framework walks once later |
| `Quantity`, `u` | q03a, q03b's own card | used amount in its native unit; no scaling/conversion |
| `input`, `crop` | q01's own 22-slot card, existing exact-label maps | no bare 2018 tail codes; literal combined rubber/teak identity stays distinct; `arichide` maps to existing groundnut identity `Arachide` |
| `Received_as_gift` | q05 own Oui/Non | nullable; unknown nonmissing flags refuse |
| `Quantity_received_as_gift`, `Unit_received_as_gift` | q07a, q07b own card | independent from usage and purchase; no u fill |
| `Purchased`, `Unit_purchased` | q08 own Oui/Non, q09b own card | independent nullable purchase flag/unit |
| `Quantity_purchased` | withheld, nullable Float64 NA | q09a not assigned quantity or value semantics; explicit warning |
| native `s16bq09a` | verbatim source answer | wording preserved below; no source-NA claim or inferred quantity |
| `vague` | retained native payload | sample split, not repeated measurement/season |

Exact q09a label: “16B.09a. Quelles sont la quantité et la valeur totale de
%rostertitle% achetées?” Its q09b is “16B.09b. Unité”. q09c appears in the
inventory but its metadata was not supplied. Neither a guessed q09c meaning
nor 2018's resolved q09a quantity binding settles this wording.

The required-column contract is presence, not an instruction to invent a
number: country `_assert_built_required_columns` checks column presence;
`test_gh1188_burkina_input_acquisition_fields.py` explicitly preserves wholly
missing acquisition answers as nullable fields. Thus the **used/gift profile
does not need a new scientific-default ruling**. `Quantity_purchased` can be
present and unavailable, with the raw answer retained and an explicit warning.
An all-NA required purchased quantity can also trigger the existing Site B
warning. That warning is not suppressed. The original Q09A question remains
only for a future full purchase binding, not household application membership.
The proposed raw-column carrier is concrete here for ROOT review; no shared
schema/metadata policy is introduced.

## Scope and effects

Today this checkout remains unactivated: no normal 2021 plot_inputs script,
YAML block or existing scheme/map edit was made. The country helper and cards
are build inputs, so even an inactive addition is expected to move Burkina
hashes; the amount of hash movement has **not** been measured. A source-only
check is not cold/warm/cache/API verification.

On later activation, the first Tier B objective gains own2021 **applied** s16b
household-input rows and used quantities in their recorded units. Native row
order and per-slot identities are preserved. It adds no s16a parcel
applications, no Cross_Section source, no allocation or weights. `v` still
joins from sample. Generic `ehcvm_i` MUST-use prose conflicts with the own2021
plot wrapper and country notes; the latter determine the own2021 namespace.

All existing2018 source, maps and assertions remain exact. A combined public
table after activation would have extra raw columns, NA for2018; its old keys,
seven payload columns, values and masks must remain exact. The existing old
assertion that2021 input requests are empty is deliberately **not edited** in
this ownership scope. Activation would supersede that coverage baseline and
needs a separately reviewed expectation update; do not claim the retained
old empty-wave assertion would pass an activated implementation.

The adapter preserves full typed attrs on its return and refuses an already
walked source. This does not prove Country/Feature transport: attrs from
different waves may disagree during concat. The native warning is a build
warning, not a newly implemented warm-cache warning replay. The raw q09a
column and the existing all-NA-required-column report remain visible; composed
metadata and warm reporting require independent verification before activation.

NPK and DAP use existing label-map identities only. Their current mappings are
not a new grade endorsement. #1483 parcel integration and DAP residuals,
#1573 strata, #1442 money, #1148 completion, #1437 CAPI, #1436 crop,
#1482 unit basis and #1485 anonymous routing remain untouched. #587/#594 is
already done; no matrix or historical diagnostic is rerun.

## Frozen finite verification handoff

Selection: **only** `tests/test_burkina_faso_1483_header_adapter_20261110.py`.
No native data is needed. The dispatcher supplies its verified MAIN interpreter
and private false credentials/data/cache/config prefix; no interpreter is
declared in the worker's runtime profile and none is invented here.

From the exact candidate checkout, after the dispatcher asserts MAIN and
source/config identities, use its prepared `PROJECT_PYTHON`:

```sh
PYTHONPATH="$PWD" LSMS_COUNTRIES_ROOT="$PWD/lsms_library/countries" \
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 \
PYTHONDONTWRITEBYTECODE=1 "$PROJECT_PYTHON" -m pytest --noconftest \
  -n 2 --dist=loadfile -q -p no:cacheprovider \
  tests/test_burkina_faso_1483_header_adapter_20261110.py \
  > "$PRIVATE_VERIFICATION_DIR/1483-pure.log" 2>&1
```

`PRIVATE_VERIFICATION_DIR` and false credential bindings are dispatcher-owned,
required preconditions, not permission to write a shared cache. No LSMS package
import occurs in this test file: actual current country `i` and `format_id`
definitions are extracted from committed source into a pandas-only namespace.
The real new candidate module and driver execute against invented frames and
injected leaves. The tests assert provider imports are unreachable, inspect
allowed module dependencies, and require exact source/map/read/write roles.

Controls include a complete independently specified expected frame, explicit
ordered keys/dtypes/masks/raw answers/typed attrs, all22 slots, each of22 units
in each of3 own unit roles, independently changed card sentinels, q09a refusal
to fill a quantity, flags that disagree, no/blank/unknown eligibility, malformed
codes and amounts, native and canonical equal-payload collisions, no writer
after refusal, all-nonuse typed empty return, missing columns/cards, and the
unwalked native namespace. No success count is claimed before a real receipt.

Next gate is this frozen pure selection. Only after its actual receipts and
ROOT's carrier/coverage review should a finite separate activation/native/API
plan be admitted. No source-row, package import, native builder, Make,
Country/Feature, census, cache or broad-suite execution is included here.

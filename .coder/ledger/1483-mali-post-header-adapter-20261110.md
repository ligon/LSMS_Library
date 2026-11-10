# Prior-Art Ledger — Mali #1483 post-header adapter

**Search tier:** git/ripgrep floor; no GitNexus tool exposed. Source base
`4381d701ffb06ab447a37c66362119d1c1e23009`. Initial tracked tree and index
clean; the two untracked dispatch channel stubs are preserved. AGENTS.md and
CLAUDE.md compare byte-identical. Inherits STANDING.md §§2–5. This ledger is
committed before implementation. No project/dependency import or test run.

## §1 Task, restated

Prepare own 2018-19/2021-22 household s16b adapters from the freshly supplied
authenticated header JSON, plus portable invented-frame controls and a minimal
serving-wiring design. EAC-I producers and their finalizer stay byte-identical.
If activation lacks an operative eligibility/carrier decision, deliver a
nonactivated adapter preserving native rows rather than selecting that policy.
The CLOSED v3 packet is reused for field bindings and residuals, not repeated
as implementation or code verification.

## §2 Existing machinery

| Machinery | Source anchor | Existing coverage | Decision |
|---|---|---|---|
| Household formatting | `Mali/_/mali.py:i`, `ehcvm21_i`; `local_tools.py:format_id` | `test_gh1494_mali_eaci_namespace.py`; namespace ledger | reuse actual functions through required injection; pure controls extract exact source definitions |
| EAC-I input finalizer | `Mali/_/mali.py:plot_inputs_finalize` | `test_mali_plot_inputs_crop_marker.py`; #1118 ledger | do not call: sums amounts, first unit, drops unmapped input, EAC-I sentinels |
| EHCVM application gate | `Burkina_Faso/2018-19/_/plot_inputs.py`; `Niger/2021-22/_/plot_inputs.py` | #1123/#1188 ledgers and tests | existing `q02==1` precedent; not authority to add No/unknown rows to Mali public inputs |
| Separate acquisition carriers | Burkina #1188 ledger and driver | `test_gh1188_burkina_input_acquisition_fields.py` | reuse names/bindings of four country-only optional gift/purchase fields; do not substitute used unit |
| Native slot identity | canonical `input_obs`; Niger `_ecvma_input_observations` | #1123 identity tests | retain raw `s16bq01` key at adapter stage; refuse repeated/missing native keys, do not invent row ordinals |
| Country serving/Make | `Mali/_/{data_scheme.yml,plot_inputs.py,Makefile}` | #1287 compiler ledger | leave inactive; concrete later changes enumerated in country design |
| Shared row/attrs handling | STANDING.md; `country.py`, `feature.py` | shared tests | unchanged; adapter copies raw payload/dtypes and typed attrs, no public-API claim |

## §3 Definitions & conventions in force

- STANDING.md §3 / CLAUDE / `lsms_library/data_info.yml` govern schema, raw-once
  IDs, `Farm-level`, `input_obs`, and source access. No new global schema.
- Personally read all 3,744 lines of Mali CONTENTS, including dated caveats.
  Relevant idiosyncrasy: EAC-I has plot/group-use questions; s16b is household
  input-type grain. 2018 area fields supply no parcel key; q00 is respondent.
  The exact old claim “EHCVM waves ... have no agriculture-input block” in
  the scheme contradicts the dated #1445 note identifying both unwired files.
  Preserve historical notes; append only a concise new finding.
- H18 SHA256 `701c60fe2b2e988d116da23d92d8cc5e260592a4d292ecfb34e7e0c870ee5eb5`;
  H21 `3f467ae181bb4d3f1d537c535ce2cf94d8cf8bc8cfe1ded5067adcf6577aca35`.
  Own q02/q05/q08 cards are 1 Oui / 2 Non; each of q03b/q07b/q09b has
  1 Gramme / 2 Kilogramme / 3 Tonne / 4 Litre / 5 Charrette. Keep associations.
- Source tails differ: 2018 19=tubers,20=other seeds; 2021 literal
  19=“Semences d'arichide”,20=tubers,21=hevea/teck,22=other seeds. No borrowed
  code menu, inferred crop, NPK grade, DAP fraction or conversion.
- Native q03a used, q07a gift, q09a purchase, q09c purchase amount remain
  distinct. Header-only results establish no observed keys/counts/sentinels.

## §4 Invariants & assumptions

Inherit STANDING.md §4. Source-only profile overrides skill suggestions to
import/run/build/acquire. No package/dependency imports, collection/tests,
native/header/row/PDF/workbook reads, API/Make/producers, mounts, network,
DVC/shared-cache writes, CLI changes, agents or GitHub writes. Standard-library
metadata/source inspection and private evidence writes only.

Owned changes: Mali/**, this ledger, exactly
`tests/test_mali_1483_header_adapter_20261110.py`. Preserve all unrelated bytes,
old assertions and country note prefix. No source-sidecar edits. No formatter
default: callers must supply the existing wave-qualified production helper.
Refuse malformed/missing/repeated household-slot keys before formatting; raw
unknown codes and all raw fields survive; decoding distinguishes unanswered
from unlisted. Raw amounts preserve their input dtype and masks, including
0, negatives, 99/9999; the adapter asserts no scientific sentinel rule.

## §5 Reuse decision

New pure adapter justified: Mali's EAC-I finalizer is destructive for this
source, and importing another country's driver executes providers and carries
its own codebook/gate. Bind only own header dictionaries, reuse production
household formatting via explicit injection, preserve native slot at input
grain. No applied-only filter, public classification, crop assignment,
plot marker, unit substitution, reducer or writer inside the adapter.
Provide explicit yes/no/unanswered/unlisted use masks for the activation
review. This is mechanical source interpretation, not a new scientific model.

## §6 Exact activation question

Recorded once in the EXISTING absolute QUESTIONS channel: sibling EHCVM
`q02==1` versus Mali's EAC-I-only group/zero decision does not settle treatment
of No/unanswered s16b with amounts, gift/purchase-only rows or placeholders.
Should Mali inherit the applied-only gate for those classes, or carry native
answers separately? Four acquisition fields have a country-only precedent;
q09c monetary naming and a key retaining native slot still require a concrete
serving declaration. DAP catch-all classification remains the separate #1483/
#1445 decision. No additional headers/raw reads are requested to write controls.

### Phase 3 — verification

UNRUN. Freeze source, controls and finite coordinator command before execution.
No test counts, runtime identity, source-row conservation or cache-footprint
measurement are claimed. Static ledger review and final source proof pending.

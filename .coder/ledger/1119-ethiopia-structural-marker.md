# Ethiopia #1119 structural crop marker

## §1 Scope and source

At public `edc9b080c86c9f51fc5a17773681e189aef1c505`, implement only the
human-ruled structural crop marker: `Whole Plot` → `(not crop-specific)`.
Root approved the source proposal and this narrow emitted-row implementation.
No seed/pesticide eligibility, raw-source, retention, reducer or unit change.
Search tier: Git/ripgrep/AST; no index refresh or GitNexus is available.

## §2 Existing machinery

Reuse `WHOLE_PLOT_SENTINEL` and `plot_inputs_for_wave` in Ethiopia/_/ethiopia.py.
Only fertilizer/organic branches emit that constant; seed/pesticide branches
map source crop codes and retain their current unresolved-label exclusions.
All five native colmaps/drivers and country compiler stay unchanged.
Tests: `test_gh1118_ethiopia_plot_input_units`,
`test_1184_ethiopia_seed_purchase_sum`, and organic/NPS tests in
`test_867_nitrogen_content`. Current #1118 and #1184 ledgers bind these contracts.
Uganda merged #1350 and Malawi #1323 supply vocabulary/provenance precedents;
no other-country eligibility, zero-code or source-observation rule is adopted.

## §3 Definitions in force

Inherit STANDING.md §§2–4, CLAUDE.md and canonical data_info.yml comments250–265.
GH1119 ruling6368100317: structural noncrop is `(not crop-specific)`; missing
crop-specific is `unknown_crop`; recorded unmapped passes through. This unit
changes only already-emitted structural rows. `plot_id=Farm-level` is separate.
Full holder/household/parcel/field identity and unique holder/crop purchase
attachment are country authority. GH1357 option1 keeps missing-household rows
excluded; GH1183 purchase retention remains separate.

## §4 Invariants

No existing function AST or native driver/colmap changes. Only the constant's
value and corresponding documentation change. All non-j keys, order,
multiplicity, payload values, nullable types/masks and attrs remain exact.
Keep Applied-only observations, explicit zeros, missing purchase sums and
unknown-unit rule; do not fill unresolved seed/pesticide crops or change joins.
Preserve original country notes as history; add dated marker migration.

## §5 Reuse decision and verification plan

Extend the existing constant; no new parser/classifier or shared-schema change.
Invented controls exercise actual builder with known/unmapped/missing crop rows,
use-only fertilizer/organic rows, duplicates, multiple fields and exact payload
comparison against a separately loaded baseline module. Reuse strict EA/mask/
NumPy/typed-attrs precedent. AST review proves all functions/assignments except
this literal remain unchanged and all five drivers byte-identical.
Prepare finite actual Country/Feature/source accounting from compatible CLOSED
operands where possible; no raw/API build until root reviews that scope.

## §6 Human questions

None for this implementation: ruling and root scope are explicit. Historical
unresolved-crop exclusions and #1183 retention are not reopened. Scientific
counterexamples outside this emitted-row boundary go to existing issues/root.

## Verification status

Ledger-only commit26ff6516 preceded product8eee878c. Source AST proof changes
only WHOLE_PLOT_SENTINEL; all functions and five native drivers remain exact.
Initial focused run:28passed/5failed because invented expected j inferred pandas
StringDtype(na_value=nan), while actual explicitly uses nullable string. Corrected
only expected j cast; failed-five-only continuation:5passed. Thus33 distinct
cases passed across two runs, not a clean33 invocation. Original failure logs
remain. Controls preserve recorded crop-specific Whole Plot labels, unresolved
seed/pesticide exclusion, first-duplicate reducer and #1357 missing-household
exclusion. Source audit and diff whitespace checks pass. Mounted interpreter,
checkout/countries/data/config identity and core34/thread1 bounds are recorded.
No raw/API/Make actor authorized or executed by this task.

Closed operand plan:15 source-stage numeric frames totaling208878417bytes match
current15sidecars/MD5 and all unchanged drivers. All60 current country helper/class
ASTs match closed1120source9f3ff012; five sample drivers and updated_ids.json are
byte-identical, enabling conditional reuse of closed273360byte pre-finalized
sample after current identity/coverage verification and explicit private stamps.
Sample map.py and country sample.py are absent at both heads; failed proposed
source-read checks were corrected without data access. Actual admission remains
pending root review of saved-operand plan; no baseline finalized frame substitutes
for raw inputs or current public expectation.


## Qualified actual result and public extraction

Completed dispatch b6e9e49702364b2c9b61a3c00623e9eb uses source f180929e.
All 20 matching-stage native/compiler/Country/Feature marker predicates pass:
180,900 rows, 69,024 structural changes, 111,876 nonstructural rows unchanged.
Both typed Feature compositions, candidate strict Country and fresh-process warm
pass. All 14 transport failures and original/import/accessor failures remain
failed. Saved numeric source/sample operands and private source-compatible
stamps mean composed verification, not a clean raw cold build or fresh sample,
cache or scientific acceptance. Root accepted sealed independent review
bd2d229b; the report is under the country recovery path named above. Public
extraction starts from 1302cec2, preserves the #1364 canonical schema and all
other work; no test or data replay. Refs #1119 only; no umbrella auto-close.

Late terminal publication: task completed/native 0, supervisor and Slurm 74.
Git publication is complete at 6c3d8033, while ordinary evidence publication
has 120 size-cap errors; directly authenticated shared witnesses remain durable.
The independent review has an immutable publication addendum, with no replay.

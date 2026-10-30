# Nigeria #1118 retained plot-input units

## §1 Task
Diagnose normal current Country/Feature plot_inputs at 10ad99d62a5e, then
normalize only retained missing units under #1041. No row eligibility or
channel aggregation decision is authorized.

## §2 Existing machinery
Search tier: git/ripgrep (GitNexus not available). Nigeria/_/nigeria.py
assemble_plot_inputs is called five times by the country plot_inputs.py builder.
_pick_channel and fert_rows_long_typed collapse channels BEFORE assembly;
test_gh1047_nigeria_unit_codes covers recorded no-quantity units and the old NA
policy. crop_production_for_wave already fills U_UNKNOWN. Niger.fill_missing_u
uses nullable string then fillna; importing another country's module is unnecessary.
local_tools.map_index and Country._finalize_result (#1251) normalize index units,
not Nigeria's column u. Malawi #1334 is a current no-patch precedent, not Nigeria
evidence. Reuse transformations.U_UNKNOWN, seed_kg and nitrogen_kg unchanged.

## §3 Definitions
Inherit STANDING.md, CLAUDE.md and data_info.yml. Live #1118 (read 2026-10-30)
requires missing/blank units to become Unknown, preserving eligibility and all
other values/keys/attrs. CONTENTS.org says "Country() serves them u = NA, not
Unknown" but subsequently says this "contradicts the #1041 ruling". Apply the
established ruling only to retained units; retain the contradiction as history.

## §4 Invariants
Retain no-channel/tree-crop records, zero and missing quantities, recorded units
including units on quantity-missing channels. Fill AFTER channel collapse, not
before: a blank channel must not become a second unit affecting the reducer.
Preserve all identities, payloads, derived seed/nitrogen kilograms and dtypes.
Normal Feature comparison needs explicit canonical axes and all-null-column
alignment. Historical counts are not acceptance constants.

## §5 Reuse decisions
Extend only assemble_plot_inputs' existing final fill if current APIs demonstrate
residuals. Reuse canonical missing spellings and U_UNKNOWN, no new reducer or
factor. Verify exact EA dtype/mask and nonmissing NumPy values with calibrated
tiny/nextafter negative controls. Fresh private L2 and MD5/SHA/size-verified
private copies of finite declared existing L1 sources; network denied at build.
Country module edits invalidate more than this table; measure hashes explicitly.

## §6 Open questions
Source-row eligibility and mixed-channel quantity aggregation remain for root's
independent scientific review, not this patch. No public mutation. Runtime gaps
or unavailable cached sources will be reported as gaps rather than source absence.

## §7 Verification outcome
OK anchored on §2–§5: current cached-source normal APIs reproduce 96,620 rows
and a Country-only 5,546-null-unit gap (Feature already fills them). Candidate
changes exactly those cells; all are retained Seed rows with missing Quantity.
The 91,074 already-populated units, including 90 with no quantity, stay exact.
All other payloads, index identities/dtypes/masks and Country attrs are exact;
Feature values are unchanged and only its promoted-null count changes to zero.
Explicit canonical alignment removes season/input_obs, demotes u and renames
j to crop; no all-null columns needed restoration for this full-country frame.
seed_kg and nitrogen_kg are exact before/after and across aligned APIs. Fourteen
strict comparator negative controls reject tiny, nextafter, dtype/mask/axis/attrs
mutations. Original new tests: 15 failures / 12 controls passed; candidate
selection: 60 passed / 5 data-dependent deselected (normal APIs checked separately).
Cold baseline 167 s, hash-triggered rebuild 182 s, warm 17 s with unchanged
parquet mtimes. The code edit changes all 24 measured Nigeria country hashes.
No REINVENTION; the old quantity-conditioned exception is deliberately superseded
by §3, while eligibility and reducers stay unchanged. Independent review remains
coordinator-owned. Evidence is in this dispatch's evidence/REPORT.md and bound
private operands under campaign/private-witnesses/a46aa180f03d45c9a092d72cbe69799a.

## §8 Additive retained-eligibility successor (2026-10-30)
Base a345ca139efa7e2600c07e99bbaaed39e9b64cfc remains frozen in the original
worker result. This private successor addresses Mali's concrete red-team
blocker: unconditional sentinel fill can turn an otherwise all-payload-NA row
into a nonempty row before Country._finalize_result's post-dtype
`dropna(how='all')` (country.py:4163). This is eligibility preservation under
existing policy, not a new source-row exclusion rule.

Reuse the assembler's exact returned payload: Quantity,u,Purchased,
Quantity_purchased,Improved (nigeria.py:1560); exclude keys and ignored extra
columns. Capture baseline any-notna before unit normalization; False and zero
are observed, as are literal blank/'nan'/'<NA>' unit strings. Fill only a
missing unit on that existing eligible payload. Leave genuinely hollow
records hollow for the existing finalizer. Preserve sort/keep-first dedup
and its position: do not remove hollow records early, since that could expose
a later duplicate which the baseline kept-first rule suppresses.

Existing27 new controls remain as a corrected successor: quantity=None cases
must carry a genuinely observed payload (Purchased=False), rather than
accidentally test hollow-row rescue. Add synthetic actual-finalizer controls
for true-null hollows, False/zero observations, literal string markers,
ignored extra fields and first-duplicate order. Follow existing
`tests/test_1109_malawi_input_quantity.py` sample-cache precedent to exercise
the real Country finalizer with invented metadata/IO leaves only, no source
or sample build. No guessed quantity-only screen or changed reducer.

No raw/API/Make/corpus replay. The original worker's96,620-row proof is retained
history; this successor may compare authenticated saved operands only if their
exact owned paths and manifest are clear. Root/Mali own independent review;
no public writes, new environment or dependency changes.

Successor verification (tested product/test head adc6d132266afb3055d5de2954ad9f8b852f3d00):
- OK (§8):74 targeted synthetic tests pass,5 requires_s3 deselected;14 added
  cases exercise the actual existing finalizer with invented sample metadata
  and readers that refuse source/cache IO. Truly hollow native records remain
  hollow, False/zero/string markers retain eligibility, ignored columns do not
  rescue rows, and a hollow first duplicate is not removed before keep-first.
  Original27 cases now mark quantity=None retained fixtures as Purchased=False.
- OK (§8): exactly the owned before-country/after-country operands matched the
  original manifest SHA/size and regular paths before decoding. All96,620
  measured retained payloads are nonhollow. Actual original-worker and successor
  assemblers are strict-equal on all five saved-wave payload groups. The saved
  before/after frames still differ only by5,546 units, and90 recorded-unit /
  missing-quantity records remain exact. Comparator reuses authenticated
  original exact.py (EA dtype/type/mask plus exact nonmissing NumPy values),
  not pandas frame check_exact alone.
- LIMIT (§8): this is an incremental saved-finalized-payload proof, not a new
  native, Country/Feature, Make, cache-rebuild or fresh/warm cohort. The original
  worker's60-pass proof,167/182s cold/rebuild and17s warm-mtime observations
  retain their scope. No raw payload or API rebuild was replayed. Source-row
  eligibility and seven mixed-channel cases remain open. Independent Mali
  review governs whether the soleB1 hold is closed; root handles integration.

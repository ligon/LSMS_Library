# Prior-Art Ledger — GH #824 sold-unit consumer

Baseline 64dd533394e27bd9c0d4ea7bada042a9a43b6eca / tree
e993ba448f3a38c66ec304a18ba74c66620b3c64. Search tier: git and ripgrep;
GitNexus tools unavailable. Inherits STANDING.md and CLAUDE.md.

## §1 Task

Implement the approved sold-unit denominator in median_price_valuation only.
Owner ruling https://github.com/ligon/LSMS_Library/issues/824#issuecomment-6367367289
supersedes canonical prose forbidding harvest fallback / excluding disagreements.
No country wiring, window, factor construction, grain or estimator change.

## §2 Existing machinery

- transformations.py:7307 median_price_valuation: preserve all ladder, weights,
  thresholds, eligibility, grouping, target quantity and result order. Tests:
  test_median_price_valuation.py. Only production caller is food_expenditures
  valuation at :2920, which supplies explicit kg_qty and stays authoritative.
- :4265 _kg_factor_series_with_declared composes :4195 _declared_kg_factors
  with :3611 _kg_factor_series; preserve #1093/#931 precedence, per-country
  maps, explicit-empty override, volume switch, sentinel guard and parser.
  Tests test_gh1093_declared_unit_consumers.py, test_crop_kg_factor.py,
  test_gh919_country_qualified_units.py.
- :3835 _level_or_column supplies positional units/country identity;
  :3849 _valid_factor rejects nonpositive/nonfinite factors;
  :3668 _screen_reported_factors implements existing reported-factor screen.
  Reuse its precedence and screen with SOLD unit context, never harvest
  KgFactor, no new threshold/median/calibration. Valid own reported factors
  may resolve a missing label (harvest_kg_factors precedent).
- #849 ledger explains reported kg per ONE native unit, never a total.
  #824 Mali/Burkina/Uganda ledgers are prior wiring work, not consumer code;
  #1093 is already merged. No broad private ancestry copied.

## §3 Definitions

Canonical schema is data_info.yml (STANDING §3); Unit_sold is NA-bearing.
Only actual NA or absent column licenses harvest fallback, not 'Unknown',
'Manquant', empty strings or failed conversion. Sentinel labels remain
unconvertible by declarations; a valid own reported factor can resolve them.
Known label pairs exclude the existing unit sentinels; compare stripped,
case-folded labels, not raw numeric codes. Disagreement is a quality flag,
not evidence that either answer is true and not an exclusion.
Mali CONTENTS read first including TODO/WAITING/LOGBOOK: rural 2017 Lourd
one third versus Leger two thirds; Leger yes-sale amounts remain NA. EACI
field and tree sale blocks have distinct unit fields. Historical tree rule
"quantity sold per month x months_harvested" is explicitly superseded by
"Quantity_sold and Value_sold are served as recorded in both EAC-I waves".
Harvest remains monthly quantity times harvest months; sale window is separate.
Retain quantity sentinels and TREE999999-only value mask, no new999998 or
field masks. EHCVM has harvest blocks; unwired is not absence.

## §4 Invariants

STANDING §4 applies. Input values, dtypes, axes (including unused levels/codes)
and recursively typed attrs must not change. Work on a separate conversion
view, retaining country identity and metadata. Explicit kg_qty bypasses all
unit work including invalid unit_kg declarations. No harvest factor use.
Disclosures count ALL input rows, including zero/missing sale quantities:
factor-source counts partition rows; actual resolved fallback is distinct
from attempted fallback, and paired-label disagreement has its own denominator.
Explicit-quantity mode reports bypass, not fictitious factor-source counts.
Preserve original output columns/index and attach only documented diagnostics.

## §5 Decisions and finite verification plan

Extend only denominator selection; reuse existing helpers above. New compact
sold-factor helper is needed to adapt a per-row Unit_sold to the existing u
resolver without overwriting input axes. No Country/config loader.

Serial n0 tests, each timeout --kill-after=5s 600s, all threads1:
T01 new focused sold-unit tests in test_median_price_valuation.py;
T02 existing median, #1093, crop-factor, weighted/country-unit tests;
N01 same corrected numerical cases against public baseline extracted to a
private node-local package, expecting assertion failures rather than imports.
Preserve every attempt separately, JUnit and exact exit receipts. Include
strict EA payload/mask and recursively typed attrs checks, calibrated with
nextafter and tiny changes. No full suite, no builds/acquisition.
A01 consumer-only saved Mali frame computation after ordinary evidence and
source/config compatibility authentication; bound 300s (whole actual bound
1200s). Copy only named closed operands, private permissions, hash receipts.
No cold/warm/compiler claims. Cache/hash impact measured narrowly or explicitly
unmeasured. Root independently reviews before integration.

## §6 Open questions

No semantic approval pending. Asked coordinator for absolute accepted Mali
review/operand paths. Missing-unit sentinel strings do not license fallback;
NA is the existing sold-unit contract. Existing reported-factor screen remains
applicable to supplied factors; this is not evidence of survey factor wiring.

## Phase 3

- `_sold_kg_factors` — OK (§2–§4): reuses declared resolver, level/column
  helpers, finite-positive validation and existing reported screen in SOLD
  context. NA-only fallback and sentinel refusal pinned; no new loader/parser.
- `median_price_valuation` — OK (§4): only denominator construction and attrs
  disclosure change. Explicit kg_qty bypass, weighted ladder, target quantity,
  grouping and duplicate index preservation pass. No existing baseline repinned.
- Docs — OK (§3): only superseded canonical consumer instruction and Mali's
  no-consumer sentence change; ruling cited, scientific ambiguity retained.
- Tests — 198 pass / 3 deliberately deselected at final T03, serial n0.
  N01 exact public consumer function gives four literal numerical assertion
  failures; resolver/median dependencies authenticated unchanged. Strict EA,
  NumPy payload/mask and typed-attrs controls reject nextafter/tiny mutations.
- A02 — both accepted current-byte native prepared RETURN operands authenticated
  and newly copied; 281/282 producing/source bindings match, sole difference
  is Mali CONTENTS documentation. Canonical schema differs only in the consumer
  note. 35,060 rows conserved exactly; per-wave price pools 78→119 and83→148.
  Known positive-sale label disagreements135/755 and105/576. Native stage,
  not API; no factor wiring or scientific preference demonstrated. A01 failed
  calibration dtype assumption retained, corrected to actual floating dtype.
- Cache hash movement unmeasured. No cold/warm, Make or corpus claim.
- Qualification: T02 existing composition tests warmed Ethiopia/Panama config
  before their CONTENTS review; no microdata/source decoder used. Their live
  differing quintal definitions were then read and retained, not adjudicated.
  T01/T02 inherited one CPU/threads1 but lacked a per-process AS cap; final T03,
  N01 and A01/A02 explicitly cap AS8GiB after ordinary import and deny network
  through unshare-rn. Source discovery filename searches were terminated after
  the coordinator supplied exact ordinary-evidence paths.

Evidence: dispatch a77082eecf4640799963c321a0126da2/evidence/REPORT.md,
T01/T02/T03, N01, A01/A02, compatibility and closed-operand export receipts.
Independent scientific/source review belongs to root and remains pending.

## Public extraction

Extracted reviewed product/test bytes onto public 373af66f0390b59a919b32db1b42cd8086ee6beb. Independent Mali
review51343bc266329cab66cbe168be7a412f8824b61406fb66c84bbe3f30ec8f443f
clears the ruled consumer with retained actual-stage/custody/resource limits.
Only the narrow canonical Unit_sold note patch is applied, preserving merged
1352 optional plot declarations. No private operator ancestors, data, new
test/API run or cache-hash claim is included. Umbrella824 stays open.

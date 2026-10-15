SCOPE DEVIATIONS

None. Draft only; no GitHub writes, source changes, survey builds, scans, API
calls or scientific recalculations. Read current Uganda CONTENTS including
LOGBOOK/WAITING/TODO before issue/source review. Source reviewed is assets
head515864c99, full-tree-equivalent to merged development3914d8804.

Recommendation: keep #962 open only under the narrowed title/body supplied.
Its original Uganda failure is complete, including the previously pending
nutrition decision. The prior plan's original reason to defer closure no
longer applies. There is still substantive work explicitly requested in the
original issue: corpus-wide source-backed missing-unit vocabulary coverage.
The current tests do not implement that requirement. A second concrete
contract/documentation contradiction is measured in the accepted residuals.
If the coordinator chooses to treat #962 as only the original Uganda defect,
it should be closed rather than held for unrelated science/source questions;
the genuinely unfinished coverage/contract work would need its own tracked
scope. The supplied draft retains it here to honor the original requested net,
not simply to preserve a prior keep-open decision.

Named idiosyncrasy: Uganda API kg factors are runtime estimates; its curated
conversion_to_kgs.json joins to Kgs in the wide reader but canonicalization
drops that column. It is an answer key, not the API factor input. Source-specific
missing-unit handling also distinguishes money-only Value from reported
quantity under Unknown before aggregation. Crop and food units have different
recorded off-table treatment; changing the food deletion adds rows and is not
a sentinel spelling patch.

Contradiction quoted rather than dismissed: food-acquired/units/SKILL.md and
test_u_code_leak.py say accepted code residuals are excluded from the kg
aggregate (“only the kg aggregate excludes them”). Uganda CONTENTS:850 says
these residuals are “not a claim ... that the kg inference excludes them.”
Both the September validation and fresh #967 accounting measure all24 observed
residual-code rows receiving the existing inference. The draft requests
contract/documentation reconciliation, not an unapproved numerical change.

Evidence anchors

- Saved original issue: control/ci-completion/issue962-before.json, original
  “Fix” section requests an every-served-unit vocabulary regression guard.
- .coder/ledger/962-uganda-food-units.md §§3–5 and dated release disposition:
  distinct codes preserved; no exclusion added; nutrition hold superseded.
- Uganda/_/CONTENTS.org:794–890: runtime-factor idiosyncrasy, 131 source labels,
  eight residual codes/24 rows, three unobserved item-only codes, per-source
  Unknown/Value, and historical/current off-table-drop count differences.
- transformations.py:3472–3495: actual null/spelling mask; :2133 onward applies
  it to food inference. No new all-country vocabulary declaration was added.
- tests/test_962_unit_sentinel.py: source classification, negative/cancelling
  quantities, real count control, catalog residual identities, and price modes.
- tests/test_food_kg_inference.py:388–424: known sentinel/NaN priced controls.
- tests/test_u_code_leak.py:94–119: four known-clean countries, numeric-prefix
  leak checks, unavailable countries skipped/continued; not full semantic
  vocabulary coverage. Existing accepted-residual comment contradicts Uganda.
- uganda967.org: fresh incremental arithmetic and now-executed53 baseline/
  127-focused checks; uganda-nutrition-history.org: exact older #850 attribution.

Deduplication and limits

Read-only search of saved release-all-issues-live.json found #223/#832 unit
labeling work closed, #850 closed inference work, and separate #976 unacquired
raw-sentinel sweep/#865 crop quantity sentinel/#777 Panama Value quantity
sentinel. None supplies this corpus-wide missing-unit vocabulary guard.
#1000 is the separately filed FCT-note follow-up. No live GitHub refresh was
performed for this draft; the coordinator should retain its normal concurrent
edit check before applying. No current corpus was rescanned, so the draft
explicitly does not assert another observed sentinel leak or an all-country
clean result. Accepted undecodable codes are not automatically missing units.

The old whole-issue text should not remain as current description: it includes
resolved7359-row claims, obsolete hold language and unverified per-item
magnitudes. GitHub history retains it; the replacement records the corrected
history, concrete remaining acceptance criteria and limits without microdata.

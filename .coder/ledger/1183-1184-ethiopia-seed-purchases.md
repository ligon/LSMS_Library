# Ethiopia #1183 / #1184 — seed-purchase source semantics

## §1 Task and reconciliation
Read live issues and comments: both remain open, zero comments. Public development
is d65e8b13683409ab0005f1a5590208cd7d94ae62. Merged #1118 normalized final units
only; its ledger and current CONTENTS explicitly preserve both purchase defects.
Diagnose questionnaire routing before changing missing purchase flags or row
eligibility. Keep the all-missing holder/crop sum a separate measurement.

## §2 Prior art
Search floor: Git/ripgrep, current country notes and prior #1118 ledger.
Reuse `plot_inputs_for_wave`, unchanged native drivers, `_yesno_bool`, the existing
unique-plot purchase join, and normal `get_dataframe` / `to_parquet` plus existing
hash invalidation. Relevant skills: prior-art-ledger and pp-ph. The skill audit
passes; its Poetry wrapper created an empty local environment, which was removed.
All substantive validation uses the existing approved mounted Python.
Inherit STANDING.md §§2–4; do not create another reader, decoder or build framework.

## §3 Definitions and country idiosyncrasies
CONTENTS: §3 is field grain, §4 is plot/crop grain, §5 is purchase grain. W1 §5
has field/crop keys; W2–W5 §5 has holder/crop keys and attaches only where that
pair maps to exactly one planted plot. Never fan out a holder amount across plots.
Applied-only organic/pesticide observations and the #1041/#1118 unit rule stay.
Panel eras and PP/PH source/reference periods stay. The existing notes say both
purchase boundaries need source-routing diagnosis: that caveat is evidence, not
permission to call absent answers No or delete purchase-only rows.

## §4 Invariants and measurements
Preserve reported Yes, No and zero, source/crop labels, keys, row multiplicities,
unique-plot attachment, all unrelated numeric payloads/dtypes/null masks and attrs.
Use explicit exact nonmissing NumPy or ExtensionArray predicates (GH1206), not
pandas frame equality alone. Private cached inputs, one CPU, 8 GiB, thread one,
exact package/config roots, no acquisition/providers/shared-cache mutation.
Initial scope was two cached section5 sources and their cached post-planting
questionnaires. Validation extended only to the five unchanged plot-input drivers
and normal sample dependencies: 20 already-cached raw objects, 128,365,797 bytes.

## §5 Reuse/extension decision
Source interpretation: reuse recorded questionnaire instructions and source flags;
do not infer routing from missing-value counts. Extend only the existing named
holder/crop sum with `sum(min_count=1)`, retaining explicit zeros and known flags.
No decoder/retention change is bundled merely because the two issues interact.

## §6 Remaining question
Missing truth value is already settled by the same-country `_yesno_bool_col`
and the #1250/#1253 precedent: missing is not No. A later seed-call repair can
reuse that decoder and zero only an explicitly false purchase flag. The remaining
#1183 question is retention of source crop/seed-roster records whose entire
purchase payload becomes unknown, not permission to invent a truth value.
The existing issue records 371 W1 and, with both boundary changes, three W2
purchase-only rows. Those are attributed earlier diagnostic counts, not a new
counterfactual cohort here. Raw-key attribution and a source-specific retention
rule remain; root should update existing #1183 rather than file a duplicate.

### Source diagnosis and chosen implementation boundary
The cached W2 post-planting form (MD5 5ce83f65b3ec26bee1e03e4708816ebd),
section 5 Q3, routes explicit No to Q13. Q5 asks total purchased/credit quantity;
the zero-if-nothing instruction is on Q10 up-front payment, not Q5. This was
inspected as literal text-drawing strings in local PDF streams 56 and 58, not a
full PDF/CMap/page reconstruction. Official WB dictionary entries independently
confirm Q3 and Q5 meanings:
https://microdata.worldbank.org/catalog/2247/variable/F113/V5189?name=pp_s5q03
https://microdata.worldbank.org/catalog/2247/variable/F113/V5192?name=pp_s5q05_a

Two normal cached section5 reads find 287 explicit W2 Yes rows with both amount
components missing, versus 49 explicit Yes rows reporting both zero. These are
different source states; a grouping-only missing-preserving sum is justified for
#1184. The decoder/caller and row-retention predicates remain exact. Fresh paired
source checks reproduce 265 post-caller groups and 187 attached changes.

#1183 remains separate: all 4,433 / 90 missing flags have missing purchase
quantity components, but one W2 missing flag carries a positive purchase value
and financing answer; 20 W1 missing flags carry a financing answer. Missing Q3
cannot universally mean a documented No. Source evidence does not settle what to
do with the 371 purchase-only W1 records once their fabricated False/zero payload
is removed. No decoder, flag, eligibility or row deletion is implemented.

## §7 Precedent search and active red-team
- **Reuse:** same-country `_yesno_bool_col` (ethiopia.py:665) already preserves
  missing answers with explicitly false-filled mask conditions and documents why
  broader `_yesno_bool` is different. The #1252/#1253/#1256 ledger §3 states
  missing is not No; Uganda #1250 separately zeroes only a positive No condition.
- **Reuse for sum, not eligibility:** Malawi `_seed_purchase_block` uses
  `sum(min_count=1)` at source and household/crop grain, but its purchase amount
  is value-backed and unreported groups are dropped. That source-specific filter
  is not authority to drop an Ethiopian explicit-Yes row.
- **Retention precedent, applicability limited:** Uganda's seed block explicitly
  keeps a recorded crop label even when amounts/flags are absent; Tanzania also
  retains its seed-roster records. They refute a universal numeric-only retention
  rule. The Uganda peer actively challenged transfer to every Ethiopia wave:
  W1's used-seed roster differs from W2–W5's §4 planting records, including
  perennial crops. Their rules do not settle blanket Ethiopian retention.
- **Different instruments:** Tanzania derives some purchase flags from amounts
  paid/quantities, while Burkina #1188 explicitly preserves missing purchase/gift
  fields on unchanged native rows. Neither authorizes inferring a missing ESS
  purchase flag or its crop-roster eligibility.
The peer supports the narrow #1184 change and recommends raw-key attribution of
the #1183 purchase-only class before choosing a W1-specific preservation rule.

## §8 Current results and retained failures
Product/test commit 36a0a52b4 changes only the named sum and adds ten regressions.
Candidate selection: 24 unique passes (10 new + 14 retained #1118), no skips.
Initial baseline selection: two expected all-missing-sum failures, seven healthy
passes and one authored direct-join alias-fixture KeyError. The fixture now uses
the actual W1 field_id/parcel_id spellings; original log/JUnit remain unchanged.
No clean baseline ten-case invocation is invented.

Five native drivers ran once with the same full inputs passed to exact d65 and
candidate constructors: 180,900 rows on each. Only W2 has changes: 187 Seed
Quantity_purchased cells zero→missing, all Purchased=True and retained. W1/W3/W4/W5
have zero changes. Keys, order, every other cell, dtypes, missing masks and attrs
match by explicit ExtensionArray/nonmissing NumPy predicates, calibrated with
tiny/nextafter negative controls. The 265 all-missing holder/crop groups all have
an explicit Yes under the unchanged decoder/caller. Unique-plot attachment stays.

Normal private empty-L2 baseline and candidate API cold runs each serve 180,900
rows and a 25,914-row sample. W2 seed quantity-purchased zeros decrease
6,541→6,354 and missing amounts increase 21,256→21,443; purchase flags stay exact.
Country/Feature/requested-W2 comparisons pass through the documented u-column
promotion and season-sentinel oracle, without normalizing actual frames.
Fresh-process warm checks pass on both heads with exact 32-entry private data
snapshots. A saved-operands-only comparison verifies the complete sample and all
other keys/cells/order/dtypes/masks/attrs; the same 187 cells appear through three
interfaces, not three new populations. No provider, shared mutation, Make-backend,
other-table scientific validation or #1183 decoder/retention acceptance is claimed.

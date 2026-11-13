# Malawi #1298: source-observation identity and exact numeric verification

## §1 Task
Preserve every retained Module G source observation across four Malawi waves,
with public Country/Feature alignment, and verify numerical conservation with
explicit nonmissing-array equality rather than pandas nullable tolerances.

## §2 Existing machinery (git/ripgrep floor)
- `Malawi/_/malawi.py:_seed_source_reference`: country-relative declared-sidecar
  reference; never a download or raw-byte fallback for constructed identity.
- `_seed_block`, `_seed_crop_identity`, `_plot_ids`, `_is_unknown_plot`: retain
  original G payload and native crop identity, distinguish unknown plots.
- `_seed_purchase_block`, `assemble_plot_inputs`: existing H value gates and
  arithmetic; attach only to one known native-code/same-half G observation.
- Four `Malawi/{wave}/_/plot_inputs.py` drivers capture positions before any
  selection; 2010 removes full-raw repeats without adding a unique field first.
- Standard `get_dataframe`, Country finalizer/sample v-join and Feature canonical
  alignment are reused, not replaced. Tests: `test_gh1298_malawi_bounded_observations`,
  non-S3 `test_gh637_malawi_tail`, canonical `TestPlotTableDeclarations` and
  `TestPlotInputsHarmonize`.

## §3 Definitions
Inherit STANDING.md, CLAUDE.md, canonical data_info.yml and Malawi CONTENTS.
Settled #1119/#1120: missing G crop is unknown_crop; recorded unmapped identities
pass through, including fractional codes and zero. D's structural crop absence
is (not crop-specific). Missing/partial plot keys are unknown_plot.

## §4 Invariants
Positions precede the nine 2010 full-record repeats. All unknown/all-NA G siblings
count against purchase uniqueness. H ownership does not establish unit agreement.
Module D remains unresolved_observation with its previous reducers. No new
eligibility, conversion, quantity-use or nitrogen rule is introduced.
The finalizer still excludes 185 all-payload-NA 2013 records, including the 184
blank crop/plot records; source identity must not manufacture payload to retain them.

## §5 Reuse decisions
Reuse the existing seed helpers, source-qualified input_obs axis and canonical
legacy sentinel. Extend their production callers to all six G sources. Reuse
normal APIs and independent raw H group arithmetic for validation. The exact
numeric verifier is analysis-only: separate dtype/null-mask/index/order checks
plus NumPy equality of original nonmissing values, with no floating coercion or
tolerance. No reader-admission framework or compiler change is needed.

## §6 Remaining decisions
#1190 purchase-unit representation and #1174 reported-use/eligibility/Module D
uncertainty remain separate unresolved work. Neither blocks representing G
observations correctly. Publication and issue disposition require maintainer review.

## §7 G payload boundary
Each uniquely identified G observation carries its own Quantity, unit, Improved
and missingness: no G sum, first selection or projected-payload deduplication.
This seed-only boundary does not change D reductions or H arithmetic.

## §8 Verification
All six sources/four waves pass explicit numerical source-to-wave-to-Country
checks, independently recomputed H ownership/arithmetic, nonseed conservation,
and cold/warm Country/Feature comparisons. Known-crop quantities reconcile
exactly at the previous grain; only zero-total old groups disappear under the
unchanged eligibility rule. Feature adds the documented canonical_alignment
record for unknown_season, in addition to preserving Country attrs.
Calibration rejects tiny-delta and nextafter changes on nullable Float64 and
NumPy float64, plus dtype/mask/index/order/attrs mutations. Targeted tests at the
clean public extraction: 50 passed, 17 requires_s3 deselected.
These are new ordinary cached-source measurements. The comparison baseline is
0f28c7ce (partial 2013), not public d65. Product/config/test bytes match the
reviewed implementation; whole private and public trees/history do not.

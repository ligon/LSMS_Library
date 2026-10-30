# Prior-Art Ledger — GH #1150 plot geovariables

Search tier: git + ripgrep floor; no GitNexus tool used. Inherits STANDING.md.

## §1 Task
Option A: attach held WB plot covariates to Ethiopia field rows, disclose
provenance, use canonical optional declarations, and declare Nigeria's existing
PlotSlope consistently. No acquisition, coordinate reconstruction or #1183 work.

## §2 Existing machinery
- Ethiopia/_/ethiopia.py:plot_features_for_wave: field constructor with holder-aware
  parcel join; tests/test_ethiopia_shipped_factors.py covers area conversion.
- Five Ethiopia wave plot_features.py drivers supply native colmaps. Extend them.
- Nigeria/_/nigeria.py:plot_features_for_wave already emits degree PlotSlope; reuse.
- lsms_library/derivations.py: registry, Derivation column, API attrs, input accessor;
  tests/test_derivations.py. Reuse for percent-to-degree conversion.
- Country finalization and Feature composition: reuse; declaration-concat tests
  in test_gh1287_ethiopia_plot_features_declarations.py cover existing assembly.
- No arctan/percent-slope conversion or #1150 implementation found in supplied refs.

## §3 Definitions
STANDING.md §3 and canonical data_info.yml govern column names and optionality.
Ethiopia CONTENTS "Source granularity" and "Household id emitted as i": fields,
full holder/household/parcel/field join, W2/W3 output household_id2.
Nigeria CONTENTS plot_features: lasting attributes are PP only; W5 renumbered.
Supplied authenticated ISSUE-1150 approves default degrees = atan(percent/100).

## §4 Invariants
STANDING.md §4; preserve base rows, order, payload, masks, dtypes and attrs.
Never admit geo-only rows or reduce duplicate geo keys. No confidential coordinates.
Missing matches stay missing; distance tail stays intact. W5 genuine v03 only.
Raw W1-W3 units must be inspected before wiring; similar names are not evidence.

## §5 Decisions
Extend existing field constructor with optional enrichment; reuse sanctioned IO.
New named slope conversion only because no existing conversion was found; reuse
existing derivation registry rather than add a registry or implicit conversion.
Unit-named alternative preserves percent but fragments the existing PlotSlope API;
prefer registered degrees if source labels confirm percent for each wired wave.

## §6 Residuals
W1-W3 raw geovariable blobs absent in checked shared L1; alternate held location
requested through dispatch channel. No scientific inference of their units.
Independent product/source review is pending root review before public integration.

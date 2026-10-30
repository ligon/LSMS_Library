# Prior-Art Ledger — GH #1150 plot geovariables

Search tier: git + ripgrep floor; inherits STANDING.md. Independent unit review
used a separate read-only agent and coordinator's independent source reviewer.

## §1 Task
Option A: attach WB plot covariates to Ethiopia W1-W5 fields, declare optional
canonical columns/provenance, and declare Nigeria's unchanged existing slope
in the same native PERCENT unit under the final coordinator disposition.

## §2 Existing machinery
- Ethiopia/_/ethiopia.py:plot_features_for_wave: field constructor with holder-aware
  parcel join; tests/test_ethiopia_shipped_factors.py covers area conversion.
- Five Ethiopia wave plot_features.py drivers supply native colmaps. Extended.
- Nigeria/_/nigeria.py:plot_features_for_wave emits unconverted PlotSlope;
  preserve arithmetic. Older degree docstrings are contradicted and corrected.
- lsms_library/derivations.py: existing registry, input accessor, row disclosure;
  read and tested, but no new derivation is needed for native-value passthrough.
- Country finalization and Feature composition reused; existing declaration
  concatenator tests cover assembly. No new registry or independent builder.
- No #1150 implementation found in supplied refs/ledgers. #1127 acquisition
  already merged; historical fix ref not adopted. #1183 untouched.

## §3 Definitions
STANDING.md §3 and canonical data_info.yml govern names/optionality.
Ethiopia CONTENTS: fields, full holder/household/parcel/field native join;
W2/W3 output household_id2 differs from join household_id. Nigeria CONTENTS:
lasting attributes PP-only, W5 questionnaire renumbered, W5 no geovar source.
All five Ethiopia source labels independently confirm percent/metres/km.
Nigeria W4 held label confirms percent. W1-W3 official dictionaries
catalog1002/F3,1952/F19,2734/F98 confirm percent for the current drivers'
srtmslp_nga; W4 official catalog3557/F96 independently corroborates its label.
Final coordinator ruling clears canonical native-percent PlotSlope for both.

## §4 Invariants
STANDING.md §4: preserve base rows, keys/order, payload/masks/dtypes/attrs.
No geo-only plots, duplicate-key reduction, coordinate reconstruction, clipping,
winsorization, or soil/PCA/aggregate columns. Distances keep their long tail.
No percent reconstruction by inverting rejected degree outputs.
W1-W3 CSV dictionary evidence is not new measured-byte export provenance.

## §5 Reuse decisions
Extend existing constructor with a small full-key alignment/attachment helper;
reuse get_dataframe, to_parquet, Country/Feature and canonical optionality.
Pass original numeric slope percent without conversion or derivation marker.
Text distance NA/blanks become missing; numeric distance never round-trips text.
No Derivation entry: unlike the rejected angular conversion, serving WB native
values introduces no library numerical construction.

## §6 Residuals and verification
- Final native-percent decision supersedes the initial issue's uncited Nigeria
  degree premise. Rejected candidate and failed attempts remain private evidence.
- Exact original payload/native-key/cold-warm evidence was obtained before this
  correction. Final changed attachment values are verified directly against
  closed ORIGINAL source frames, not inverse angular outputs; no broad native
  replay after coordinator steering. Final full cold/warm rebuild is not claimed.
- Original Nigeria W1-W3 CSV export receipts are not held; official dictionary
  evidence plus current wiring is the stated source basis, approved by root.
- Independent public-integration review remains pending.

### Phase 3
OK (§2): sanctioned IO and existing assembly reused; no duplicate registry.
OK (§4): five-wave 132,694-row original payload conservation; source-key joins
and exact NumPy/ExtensionArray negative calibrations exercised.
CONTRADICTION RESOLVED (§3): Nigeria degree comments contradicted W4 measured
label and four official dictionaries; final common unit is native percent.

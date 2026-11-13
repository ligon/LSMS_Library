# Uganda1198 implementation boundary

Sue, 2026-10-22. Reviewed design head
989fbf3d98439be802d1f9cebd8ab5197c56b002; runtime/config/schema/tests remain f8.

Root read the complete DESIGN/PROTOTYPE/replay/validation/READY reports and the
actual prototype, downstream, controls, exact comparator, alignment and attrs
checker sources. The private prototype establishes a useful observation-grain
oracle. Its synthetic comparator and synthetic yield areas are not real-source
validation or an end-to-end candidate Country/Feature implementation.

## Approved observation contract

Implement the opaque string `production_obs` level after `season`. Preserve
reported native IDs in 2011/2013/2015, actual condition slots in 2018/2019 and
explicitly constructed release-row locators in 2009/2010. Keep every eligible
observation, including equal payloads, missing factors and sale-only reports.
Future missing ID values retain explicit constructed fallback identities;
duplicate native IDs/slots disambiguate every occurrence rather than selecting
a first member. Missing declared ID columns raise existing CropColmapError.
Use existing format_id, eligibility/decoding, normalization, finalization,
id_walk, sentinel alignment and conversion/reduction machinery.

Append the same canonical axis for Feature and declare the honest sentinel
`unresolved_observation` for countries whose served rows do not resolve it.
This sentinel does not assert one native observation. Preserve existing sample
joins and the separate 403 null-key exclusions; do not turn the undeclared-axis
control's additional null-v loss into a new source policy.

## Loader identity and metadata revision

The first implementation will not introduce the proposed new
`attrs['production_observation']` contract. Document the coordinate's reported,
slot, missing-ID and constructed meanings in the country notes and canonical
Single Index documentation. Keep existing attrs intact at matched stages;
existing canonical_alignment records may acquire the explicitly declared new
sentinel entry.

This revision avoids an untested promise in the private prototype: it manually
sets combined by-wave attrs after concat. Actual country aggregation uses
plain pd.concat (country.py:5075 and Uganda/_/crop_production.py), and Feature
captures only registered carriers (feature.py:113-159,1081). Different per-wave
provenance would otherwise disappear. A new generic attrs carrier is separate
work, not necessary to preserve the source observations.

Use existing local_tools.source_fingerprint (local_tools.py:2954) for the
constructed locator's source identity instead of implementing another hash
scheme. Revise the private raw-MD5 token proposal to
`row:<source-fingerprint>:<zero-based-physical-row-ordinal>` and its explicit
missing-ID/collision variants. State that a dvc fingerprint includes the source
name and SHA256 of the declared sidecar; it is not the raw source MD5.
It is stable only for the same declared release and physical row order.

The actual wave loader must compute that identity from the SAME literal source
path it reads and pass explicit source references to the pure helper. Do not
infer a file identity merely from a wave label for arbitrary input frames.
Capture physical row positions before filtering, sorting, joins or melt.
A nonempty source needing a constructed locator cannot use an unverifiable
`missing:` fingerprint. No source download or new fingerprint exclusion is
authorized. Synthetic helper tests supply explicit synthetic source identities
and must not claim those frames came from real release files.

## Production and test scope

Allowed: Uganda/_/uganda.py identity helper/CROP_COLMAPS and removal of the
lossy final reducer; the seven actual wave crop_production.py callers needed
to pass source references; Uganda's crop declaration; global data_info.yml
canonical crop axis/sentinel/documentation; dated Uganda CONTENTS; necessary
index/audit comments in the existing country crop script; focused tests,
ledger and safe reports. Inspect every helper caller before changing the
internal signature. Existing synthetic inputs may gain required native IDs or
explicit source references while retaining their old behavioral assertions.

Retain the existing country concatenation route and grain-audit transport.
No new framework flag/carrier/reducer, sample/ID rule, eligibility screen,
unit table, total layer, factor averaging, fixture/golden refresh or dependency.
GH849's twelve factor mappings remain a separate change after the carrier.
Keep the existing 2015 sale-value correction and reported-factor screens.

Before builds, measure actual 549 API / 39 framework fingerprints and API-wave
hash scope on the patch. The predicted 28 Uganda hashes is not a measurement.
Use new private held-source/current-cache gates and ordinary rebuilds; never
stamp the old collapsed cache as if its native detail survived.

Acceptance includes meaningful parent-failing ID/slot/equal/partial/sale-only
controls; genuine source-cold native and public builds; actual all/subset and
unsupported2005-06 routes; complete paired payload/axes/codes/dtypes/masks/order
and typed attrs at zero tolerance; actual Feature with a gated real comparator;
fresh-process warm cache receipts; scanner accounting; existing harvest/median
behavior and real plot/parcel area/yield controls using gated held prerequisites.
Report source or area gaps before dependent probes. No whole-corpus build.

The measured 133761 prototype observations and +2091kg current-wiring scenario
are source oracles to reconcile, not numbers to force into the implementation.
Final independent review is required before a PR. Human merge remains final.
The frozen design/evidence roots must remain untouched.

Sue

# Uganda GH1198: production observation design for engineering review

Sue; baseline f8d236694c26fe9e709c5548f02c70f74ea72c33.
Ledger-first commit: 4332afede. Design and private prototype only.
No production/config/schema/test or public-axis change is implemented.
The measured projection is an acceptance oracle, not a mandated future public
row count or a production fix. GH849 factor wiring remains separate.

## Recommended coordinate

Add one crop-specific opaque **string** index level, `production_obs`, after
`season`. Native grain becomes `(t,i,plot_id,j,u,condition,season,production_obs)`;
Country supplies `v` through its existing sample join. Feature's canonical
crop index appends this same level. Reuse the existing declared-index,
`_normalize_dataframe_index`, `_finalize_result`, id_walk and
`_harmonize_country_frame` / `_align_to_canonical_levels` hooks. The prototype
needs no framework flag, new reducer or new conversion layer.

The coordinate identifies an observation within the existing full key, not a
persistent entity across files, waves or countries. Its source namespace is
country/wave/module-season, recorded with relative file path and DVC MD5 in
`attrs['production_observation']` by wave. Proposed tokens are:

- 2011-12: `reported:<productionID>` in A, `reported:<production2ID>` in B.
  2013-14/2015-16 use `Production_ID` and `Production2_ID` respectively.
  Values use the existing format_id representation; these are reported IDs.
- 2018-19/2019-20: `slot:1` / `slot:2`, meaning the actual mapped condition
  slot, not a calendar season or an invented interview. Current full keys
  plus these slots uniquely identify every eligible observation.
- 2009-10/2010-11: `row:<source-MD5>:<zero-based-physical-row-ordinal>`.
  This is explicitly CONSTRUCTED, tied to held release bytes and row order,
  not an instrument ID. Do not count within filtered groups: attach the
  physical position before eligibility, sorting, joins or concatenation.

For a future missing reported ID use
`missing-id:row:<source-MD5>:<ordinal>`. For repeated IDs/slots on the full key,
retain **all** occurrences, including equal payloads, appending the release-row
locator to **every** colliding token. Never keep a first member by policy.
An identical `(source file, physical row, slot)` locator appearing twice in the
projection is a loud internal uniqueness error, not a deletion rule.
Missing/duplicate-ID status belongs in provenance, alongside the original ID.
A declared reported-ID source column that is absent must raise the existing
CropColmapError through _require; missing-column failure is not a missing-value
fallback. Explicitly unkeyed wave colmaps declare no reported ID.
Reported IDs remain stable under reordering of the same records; constructed
coordinates are stable only for identical release bytes and physical order.
A revised MD5 changes constructed coordinates even if the records look similar.
No panel relationship or persistent identity is inferred from them.

Countries without this declared axis receive the proposed Feature sentinel
`unresolved_observation`. It means their served row does not resolve this
coordinate; it may conceal one or several source observations. It does not
mean one reported observation, a sum, or a particular native slot. Prefixes
keep actual Uganda coordinates distinct from that reserved token. Existing
constant sentinel alignment works in the synthetic comparator control.
No second design is needed to make the existing machinery work.

## Source-grain census

The table covers every existing eligible native observation, before factor
screening. Existing null-key losses are shown separately and remain excluded
in this private projection; the phase does not authorize their repair.

| Wave | Baseline keys | Native observations | Recovered | Multi-row groups | Equal/missing-factor groups | Partial-factor groups | Existing null-key exclusions |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2009-10 | 22408 | 22549 | 141 | 136 | 136 | 136 | 360 |
| 2010-11 | 20970 | 21051 | 81 | 81 | 40 | 0 | 0 |
| 2011-12 | 20152 | 20200 | 48 | 47 | 28 | 14 | 43 |
| 2013-14 | 17585 | 17646 | 61 | 61 | 41 | 18 | 0 |
| 2015-16 | 19576 | 19639 | 63 | 63 | 40 | 11 | 0 |
| 2018-19 | 15616 | 15630 | 14 | 14 | 8 | 5 | 0 |
| 2019-20 | 17024 | 17046 | 22 | 22 | 10 | 5 | 0 |

Total: 134,164 source-eligible observations, 403 separate existing null-key
exclusions (360/43), and **133,761** retained native observations versus
133,331 baseline keys: **430** additional records across **424** old-key groups.
The requested five waves'115 raw factor-conflict groups are only a subset.
2018's six already-wired conflicts are included, without extending GH637 policy.

Across all14 held AGSEC5 source frames, **zero full-row repeats** occur over
all distributed columns, including in2009/2010. There is no source-backed
redundancy to remove. There are33 groups with equal complete *served helper
payloads* (17/5/8/3 in2009/2011/2013/2015), which are different from full-source
row duplicates. They remain separate observations. All eligible reported
production IDs in2011/2013/2015 are present; the proposed extended keys have
zero duplicates. Future missing/duplicate cases are tested synthetically.

## Country and Feature behavior

The original helper's exact pre-reducer code is reused privately through an
AST extraction; only its final lossy sum/first block is bypassed. The prototype
separately applies the existing null-key exclusions and attaches coordinates.
The original module, colmaps, eligibility, quantities, units, sales correction,
field decoders and all production files remain unchanged.99 independent
source-field pairing checks cover quantities, own factors, sale quantity/value,
sale unit/condition and harvest month; intercropped remains the unchanged
season-specific AGSEC4 lookup in that reused helper.

The coordinate is appended using the original MultiIndex levels/codes.
A strict first attempt caught set_index reconstructing unused i-level metadata;
that stop remains preserved rather than weakening equality. Complete row
payload/dtypes/masks/order/axes/attrs compare at zero tolerance. Full public and
Feature attrs equal the baseline attrs plus the explicitly proposed provenance
dictionary, recursively by exact type, record fields and values.

Existing normalization, finalizer and proposed Feature declaration each retain
all133,761 observations. The current undeclared-extra route drops the
coordinate, collapses428 nonnull-key source records and deletes928 null-v
records, returning132,405. The current baseline Feature route retains133,331;
it already has926 null-v rows, and two recovered observations share those
unplaced sample keys. This is a demonstration of the undeclared-axis hazard,
not permission to change IDs, sample eligibility or null-key policy. Declared
alignment retains those rows without changing the join.

The comparator is explicitly **synthetic**, not a claimed healthy Niger cache
or a cross-country source build. It proves constant unresolved-coordinate
alignment preserves all its payload/dtypes/attrs. Production approval should
require a physically gated, current/full-coverage real comparator and normal
Country/Feature execution after the actual patch.

## Conversion and analyst reduction

Whole frames were evaluated using existing harvest_kg_factors, harvest_kg,
_survey_median_factors and yield_kg, with explicit exact scalar/array/frame
oracles. Quantity is multiplied by its own screened factor while the
observation coordinate still exists; only then does existing harvest_kg reduce
at `(t,i,plot_id,j)`. No average/weighted/blanked factor, reported-total layer,
unit relabel or new eligibility screen is introduced.

| Scenario | Input observations | Whole-frame harvested kg |
|---|---:|---:|
| Current baseline |133331|29093322.407894|
| Native observations; current factor wiring |133761|29095413.407894|
| Native observations; separate849 12-field measurement |133761|42026694.992980|

Current-wiring preservation changes kg by+2091 in2018 only; this includes
whole-frame median effects. The older collapsed12-field projection was
42035114.892980kg. The observation-preserving12-field measurement is
8419.900000kg lower, measured from complete transformed frames, **not** from
subtracting10710.9 or treating the112 complete conflicts as the whole dataset.
These are private scenarios, not accepted physical production totals.

The current transform counts one usable screened report per retained eligible
observation, unweighted by Quantity or household survey weight; min_reports=5.
The independent fine/coarse pool count/median oracle is exact. Keeping native
observations under current factor wiring changes7 fine report counts and one
fine median, two coarse report counts and zero coarse medians, crossing zero
support thresholds. The factor-wiring scenario separately changes coverage
and support; its692 fine and133 coarse threshold crossings versus baseline
must not be attributed solely to the grain fix.

Actual yield_kg was exercised with a clearly synthetic, fixed2ha area for every
observed plot. Its whole-frame result equals grouped actual harvest divided by
that area at zero tolerance in all three scenarios. No real source areas or
real Uganda yields are claimed. Before production, gate current/full-coverage
plot_features from held sources, run the three saved projections through
existing parcel and plot routes, independently reconcile area joins and masks,
and report area denominator coverage and per-parcel yield changes. That is the
precise remaining source-backed yield test, with no core yield-policy change.

## Country facts and dated contradictions

Full f8 Uganda CONTENTS (3604 lines, including TODO/WAITING/LOGBOOK) was carried
only after exact SHA256 verification:
774d5bc13c35ca5a6d9543766648f88dbc1efe508d73aa388d5ac6e81146256c.
A/B are modules, not calendar order:2015 A refers to second season2014 and B
first season2015. Value vocabularies resolve the2013 swapped unit/condition
labels.2018 mixes legacy names with WB slot suffixes; A has no genuine harvest
unit.2019 slots1/2 are condition observations. Sales use their own unit/state;
2015's Value_sold x100 restoration is separate and unchanged.

CONTENTS:2435 calls the factor movement "small and in the right direction";
the frozen849 full measurement was+12.94Mkg, so that expectation is not an
invariant. CONTENTS:1469 says harvest_kg "does not yet consume KgFactor" in a
dated before/after section; current transformations already consume screened
reports. The helper:1573 calls its sum/first operation "just de-duplication of
repeated identical source rows"; the all-record census finds distinct native
IDs/slots and no full-source-row repeats. CONTENTS:1674's older commensurability
claim and:2945's later missing-level diagnosis are distinct historical claims;
the latter explicitly recommends retaining a record ordinal. Do not erase
either history or declare the document stale wholesale.

Proposed future country note: name the native production ID fields, actual
slots and constructed-release ordinals; record the430 recovered observations,
zero full-source-row repeats,33 equal-helper-payload groups, and separate
403 null-key exclusions. State the Feature undeclared-axis hazard and the
existing2015 sales correction without changing either policy. No CONTENTS
edit is authorized or made in this phase.

## Minimal future boundary and migration

Proposed allowlist, pending root approval:
Uganda helper/CROP_COLMAPS for native identity and the uniqueness guard before
return/write; country crop declaration/comments; global data_info.yml Single
Index documentation, crop canonical index and unresolved sentinel; dated Uganda
CONTENTS; focused source/semantic/Feature tests; task ledger and safe reports.
No new framework flag, reducer, ID normalizer, sample rule, unit/factor table,
physical-unit curation, fixture/golden refresh or conversion-layer rewrite.
Keep GH849's twelve kg_factor entries separately reviewable after the carrier.

Future tests must reject the old sum/first for distinctIDs/slots even with equal
factors, sale-only and partially reported records; preserve equal payloads;
cover missing/repeated IDs and source reorder/release stability; retain original
axis metadata; prove ordinary source-cold/native/country/public/Feature and
fresh-process warm full values/dtypes/masks/attrs; check the declared-axis
absence sentinel and actual healthy real comparator; preserve unsupported
wave behavior (including the unsupported2005-06 crop subset) and all unrelated
payloads/ID/universe rules. Test actual builder
and public boundaries, not a second copy of the proposed implementation.

Static dependency prediction: editing uganda.py and data_scheme.yml will move
all28 Uganda country table hashes and their224 API-wave cells because both
files are unconditional inputs. This is **not** a measured candidate scope:
there is no runtime patch. Global canonical metadata is read after cache load,
so adding the axis changes other crop Feature shapes even if other country
cache hashes stay unchanged. Reusing existing hooks predicts no framework-code
fingerprint move; measure the actual549/39 and per-wave inventory on the final
patch before building, rather than assert zero cost here. Rebuild affected
Uganda prerequisites normally from held sources; never restamp the old
133331-row cache as if its lost native detail survived. No corpus rebuild was
performed or is proposed merely to design the coordinate.

Root should approve or revise this concrete coordinate/provenance/sentinel
contract before production/schema work. All engineering/source uncertainties
are stated here; no new question about settled factor screens or2015 policy
is needed.

Sue

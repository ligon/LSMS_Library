# Malawi #1118: current retained missing-unit census

## §1 Task
Measure current Country/Feature plot_inputs units on the four supported Malawi
waves at public d96a8152. Repair only a demonstrated residual covered by #1041.

## §2 Existing machinery (git/ripgrep floor; no GitNexus available)
Reuse local_tools.map_index and Country._finalize_result (#1251), which already
normalize string/pd.NA units. Reuse ordinary cached-source Country and Feature
builds, _seed_block/assemble_plot_inputs and the six original G source identities
(#1323). Tests test_gh1234_map_index_unit and test_gh1298_malawi_bounded_observations
pin the respective boundaries. Mali's post-reduction fill and Tanzania/Ethiopia
#1118 tests are precedents, not authority to change Malawi reducers.

## §3 Definitions
Inherit STANDING.md, CLAUDE.md, data_info.yml and Malawi CONTENTS.org, especially
“Module G source-observation identity”. Live #1118 explicitly excludes historical
counts as current evidence and leaves plain float NaN for country investigation.
Unknown is U_UNKNOWN, never Other (Specify), a minted label or a dropped record.

## §4 Invariants
Preserve all source observations, nonblank units, payloads, ownership, eligibility,
other keys, dtypes and attrs. G is observation-preserving; D retains its reducers.
#1190's XS H09/H40 transport, H10 purchase value and H38 free-receipt diagnosis
is recorded, not permission to change the current purchase representation.
No unsupported 2004 plot_inputs request, provider acquisition or shared cache writes.

## §5 Reuse
Prefer authenticated saved operands if producer/config/source bindings match;
otherwise build from exact declared, MD5-verified cached blobs in private storage.
Use explicit NumPy nonmissing equality plus extension-array dtype/null/axis/attrs
checks, calibrated against tiny and nextafter changes. No production change until
there is an actual served residual. No blanket shared NaN conversion.

## §6 Open decisions
Purchase measurement representation (#1190) and use/eligibility (#1174) remain
human-review issues. They are not decided by this unit census.

## §7 Verification outcome
OK (§2–§5): fresh cached-source Country/Feature calls on all four waves serve
179,917 rows, 240 Unknown units and no null, float-NaN, blank or stringified
missing unit. No production patch is warranted. Saved #1190 operands were rejected
as current API evidence because country.py differed; current builds have their own
source MD5/SHA256, config/code and private-witness bindings. Fifty target tests pass
(17 requires_s3 deselected). The report preserves the initial Feature all-null-column
comparison failure and Arrow receipt-count failure; neither is a production fix.
No REINVENTION or contract CONTRADICTION: #1251 is reused, #1323/#1190 boundaries
remain exact. Documentation-only changes add no cache invalidation cost.

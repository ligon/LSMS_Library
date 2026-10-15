# Declared-corpus validation findings

Source: 7fa05179c8519f44ea329ba6387f5d6d6441a8ea. Run completed: 38 countries,
549 declared requests, 530 success, zero empty, and 19 documented errors.
The original private root is quiescent and preserved; REPORT.md records final
hash/manifest coverage, elapsed time and artifact identities.
Raw statuses remain in each country's results.jsonl; classifications below do
not remove failed or empty requests from the denominator.

## Armenia — known unavailable source (2 errors)

household_roster and derived household_characteristics fail on missing
1996/Data/INDSECA.dta after the normal local/DVC/World Bank fallback. This is
consistent with data_scheme.yml `data_available: false` and CONTENTS:
"Armenia ships exactly one wave, =1996= ... and ships no microdata".
The relevant idiosyncrasy is a documentation-only AHBS 1996 configuration;
its 25% diary subsample is documented but no microdata is installed. The
18 later ILCS catalog waves mentioned by CLAUDE are acquisition backlog,
not declared/built data in this run. No new contradiction found. The failures
remain explicit unavailable entries, not successful builds.

## Albania — expected cluster warning observed

The 2003 cluster build warns that 711 source rows with null v were dropped.
CONTENTS explicitly documents this exact count and says 2003 has 449 clusters,
not 450. The survey-specific distinction is that cluster identity survives
moves while the current district can change; the 2004 reducer anchors Region
on the original 2002 PSU map. Administrative 995/999 household codes are not
clusters. This warning is documented behavior. All 11 Albania declarations
succeeded; every final country parquet has its expected hash and complete
declared-wave manifest, and all 46 shared/private cache contents agree.

## Cambodia — old wave cache differs; country output agrees

All 12 declared requests succeeded. The shared 2019-20 wave plot_features cache
has 3,005 rows and Area sum 8,420,961.88; rebuilt wave cache has the same rows and
Area sum 9,207.8693902. The country-level cached frame is identical by content
fingerprint to its shared counterpart (served plot_features has 2,951 rows).
This is consistent with the existing #879 cambodia.plot_features hook: native
SQUARE METERS/HECTARES/ACRE are converted to hectares; RAI and KONG have no
asserted factor and stay missing. AreaUnit preserves the reported unit, not the
served unit. Shared wave stored hash ca55f5a5... differs from current 60044d80...;
the rebuilt wave footer matches its current expected hash. The old shared wave
is observational evidence, not the reference truth for release.

Documentation contradiction, quoted rather than dismissed: CONTENTS's overview
says "Cambodia is not yet accessible via the =Country()= API" and claims no
data_scheme, whereas its later DONE data_scheme section says "Cambodia is
integrated into the =Country()= API" and lists the live declarations. The run
confirms the latter mechanically. The live TODO nutrition/FCT-data-gap remains
unresolved and nutrition is not declared. Proposed documentation follow-up:
reconcile the overview and obsolete no-data-scheme bullet, and record the
load-bearing #879 hectare/AreaUnit-provenance distinction in CONTENTS. No frozen
source file was changed by this validation.

## CotedIvoire — two digest changes are row order only

All 21 declared requests succeeded. The 2018-19 and country household_roster
parquets have different order-sensitive content hashes versus the shared cache.
Both have exactly equal sorted DataFrames (including dtypes), and identical
multisets of row hashes. No value, index-key or multiplicity change was found.
Evidence: order-comparison.json. The named EHCVM idiosyncrasy is that each
household belongs to one vague and identity is grappe/menage, without a vague
level; raw Section 1 supplies the 61,116-member roster. CONTENTS's existing
older-wave contradiction is preserved: "same households followed across 4
waves" versus the quoted rotating half-panel whose retained households are
"interviewed a second time". This validation does not adjudicate that historical
panel design and no panel crosswalk was added.

## Burkina Faso — expected release changes

All 24 declared requests succeeded from the empty private L2. Changes versus
shared cache are confined to food_acquired (2014 wave, 2018-19 wave and country),
with identical row counts and numeric sums. This matches the independently
reviewed missing-quantity and crosswalk repair; 2018-19's distinct label repair
is not merely a row-order change. The load-bearing idiosyncrasy is four separate
seven-day recalls in 2014, retaining visit, with money-only passages 2-4. The
reviewed candidate comparison (countries/burkina/.../burkina-cold-comparison.json)
already pins exactly 449,033 zero-to-NA quantities and unchanged non-quantity
values after the four label replacements. The full-corpus API reproduces the
same 765,816 food rows and unchanged aggregate values.

## Liberia — old wave area cache, unchanged country output

All 10 declared requests succeeded. As with Cambodia, the shared wave-only
plot_features cache contains the values before the existing #879 hectare conversion: 2,064 rows
on both sides, Area sum 22,914.5 -> 8,544.589559. The country parquet content
fingerprint is unchanged. The source mapping converts acres/hectares and keeps
unquantified lot(s)/other areas missing, with native AreaUnit as provenance.
No new release-specific served-data change is demonstrated by this old cache.

Named idiosyncrasy from CONTENTS: Liberia's NHFS is forest-proximate, not a
national household survey. The quoted universe is "All EAs within 2.5 kilometers
of forests except for the EAs from the urban part of the Montserrado county."
The existing housing-unavailable caveat remains: the instrument has no dwelling
characteristics module. No contradiction with this run was found. As in Cambodia,
the #879 area-unit distinction belongs in future CONTENTS maintenance; frozen
source remains untouched.

## GhanaLSS — documented label repair and older wave cache differences

All 22 declared requests succeeded. Ten changed cache files are completely
accounted for by exact DataFrame comparisons (`check_exact=True`, including
dtypes, after sorting): four plot_features wave parquets equal the existing
#879 hook applied to the old shared frames; two early crop_production wave
parquets differ only by the additional, entirely non-null `plot_id='Farm-level'`
column; four community_prices/nonfood_expenditures wave/country files differ
only by the documented #926 C1-to-ASCII transcription in j and Description.
The two new early-wave livestock caches had no shared counterpart. Neither
new cache presence nor an old raw wave cache is treated as a scientific truth
comparison. Evidence: known-transform-comparison.json.

Named idiosyncrasies: GLSS1/GLSS2 Section 9 resolves households/crops but has
no plot roster, so Farm-level marks an unresolved structural axis. The 2016-17
format-118 file declares UTF-8 while carrying cp1252 punctuation bytes; the
CONTENTS entry explicitly warns that the older-format reader rule does not
protect re-derivation. The ASCII replacement is the recorded curation choice.
The exact comparison confirms that numerical fields are unchanged by it.

Documentation contradictions are retained, not waved away: the same file says
"plot_labor is NOT wired" and later "plot_labor: hired and exchange labour",
and "STILL UNWIRED: Part D Q43-44" precedes "Part D Q43-44 is now wired".
These are candidates for documentation reconciliation, not new data failures.
The open GLSS5 household-universe wording and GLSS7 agriculture-subsample
questions are unrelated and remain unresolved. No frozen source edits.

The same exact hook comparison also accounts for the entire Cambodia and
Liberia wave plot_features differences described above, with no remaining
value or dtype differences after applying their existing conversion hooks.

## Mali — three roster differences are row order only

All 24 declared requests succeeded. The two EHCVM wave household_roster
caches and the country roster have equal sorted DataFrames with exact
comparison, equal dtypes and equal row-hash multisets. Their order-sensitive
digests differ solely because row order changed (order-comparison-mali.json).
Named idiosyncrasy: Mali combines two EAC-I agriculture surveys (2014-15 and
2017-18) with two EHCVM surveys (2018-19 and 2021-22); only the latter follow
the grappe/menage single-vague EHCVM identity convention. The documentation
itself flags the tension: the Overview's "Mali EHCVM ... Four waves are
configured" is followed by "Only 2018-19 and 2021-22 are EHCVM". This run
does not collapse those survey designs or alter their interpretation.

## Nepal — known unavailable source, loud errors (11 requests)

All 11 declared requests failed explicitly, after the normal source lookup and
fallback paths; the raw exceptions and tracebacks remain in results.jsonl.
The documentation says "no source data files have been DVC-tracked or
downloaded" and explains the load-bearing acquisition distinction: Nepal's
NLSS data is licensed through the NSO portal, not hosted directly by the
World Bank. No access workaround was attempted. The docs' statement that
"all features return empty DataFrames" contradicts the observed current
behavior: these requests raise FileNotFoundError or RuntimeError. Record that
as a documentation/API behavior mismatch, not as evidence that data exists.

The file also records (without resolving) a design wording tension: "There
is no rotating panel or refreshment sample" can be read as no panel, whereas
NLSS-II and III each contain a separately sampled panel component. These
unavailable-source failures cannot settle panel weights or survey design.

## Niger — reviewed repairs reproduced from empty L2

All 23 declarations succeeded, including panel_ids and the four derived
tables. The ten principal outputs have exactly the reviewed candidate's
shapes, per-wave row counts and index-uniqueness results; recorded quantity
and expenditure sums are compared in niger-reviewed-comparison.json. The
existing assets duplicate index remains visible and is not called resolved.
The 33 changed intermediate/country caches versus shared data are consistent
with the reviewed repairs, rather than a reason to treat the older shared
cache as a baseline to preserve.

Named idiosyncrasies: ECVMA2 EXTENSION=2 is a separate split-off household;
sale replication is by harvest LINE. Missing-unit Unknown and an all-nines
numeric don't-know code are different phenomena. Literal sentinel widths
vary by wave and column; legitimate unadjudicated 999 inputs remain served.
CONTENTS explicitly corrects the old "eleven Mil rows" description: the
eleven harvest-sentinel rows span four crops. The candidate reproduces the
reviewed 46,376 crop rows and 42,426 input rows; no broader scientific closure
of the remaining seed-slot collisions or orphan identifiers is asserted.

## Peru — four implemented successes, six declared but unwired failures

The census retained all ten declarations. cluster_features (364 rows), sample
(3,623), household_roster (19,285), and household_characteristics (3,621)
succeeded. Education, food_acquired, interview_date and the three derived food
outputs raise RuntimeError. CONTENTS explicitly says these source tables
are "declared ... even though no wave" implements them, and the registry
explains why they remain visible to coverage audits. These six errors are
existing wiring gaps with acquired sources, not missing-credential failures.
Earlier 1985/1990/1991 data also remain unwired; only 1994 contributes rows.

Named idiosyncrasies: SEGMENTO is a nationwide sampling segment, now padded
to three digits in both sample and clusters, while HOGAR is a fraction rather
than a household ordinal. The export weight is all ones and cannot represent
the documented design weights. The two changed cluster caches and two new
sample caches agree with that reviewed scope. CONTENTS's legacy unpadded-v
discussion is explicitly followed by its dated RESOLVED padding decision;
it is historical context, not a justification to undo the new joinable keys.

## Panama — reviewed native value-unit repair reproduced

All five declarations succeeded. Food has 570,942 rows, matching the reviewed
candidate. Both 1997 wave and country caches have 412 fewer rows than shared
cache, exactly the already-disclosed amountless-row removal. The 1997 expense
sum stays 356,079.56 (floating summation differs by 5.82e-11); quantity and
derived price totals change as expected when code-77 quantities are removed.
Named idiosyncrasy: lowercase native `value` is the survey's code-77 monetary
unit with no physical quantity, while capital `Value` is the separate library
Q=E convention. The existing source-backed regression distinguishes the two
and was independently reviewed with an absolute-cent tolerance (`rel=0`).
This census supplies an independent integrated cold reproduction; it does not
declare the existing missing-unit scientific residual resolved.

## Nigeria — W5 cluster addition and sold-unit column reproduced

All 24 declarations succeeded. cluster_features has 7,070 rows over ten
quarters, with exactly 911 in each W5 round; the 2024Q1 community price table
has 27,318 rows. crop_production has 62,844 x 10, with 6,916 non-null
Unit_sold entries. These match the independently reviewed release evidence.
Only the two new W5 cluster wave caches, the country cluster cache and the
country crop cache differ from shared state. Exact per-cell comparison of
all previous columns/rounds is in nigeria-change-comparison.json.

Named idiosyncrasies: EA=0 marks movers and requires household-specific
cluster keys (518 actual EAs plus 393 mover keys per W5 round); W1/W2's
sold-unit field is asked separately from harvest units, while W3-W5 sales
lack a plot link and remain missing here. The documented v01/v02 distinction
is preserved: the acquired v01 lacks the geovars added to v02. Thus the older
claim "ships NO household geovariables ... by source, not by omission" does
not settle the current acquisition task, retained as #978. The existing
deduplication and non-first-buyer sale losses are also not fixed by this run.

## Tajikistan — old country cache held native area, cold output is hectares

All ten declarations succeeded. The only cache-content difference is the
country plot_features Area: 3,974 rows, total 88,238 -> 882.38. Every field
is exactly equal after dividing only the old Area by 100, as the existing
#879 wave script does. The rebuilt country table now agrees with the wave
cache, whose content was already identical in shared and private roots.
This is a material old-cache difference; it is not dismissed because the
source conversion predates this release work. Evidence is retained in
tajikistan-area-comparison.json and both footer snapshots.

Named script-level idiosyncrasy (not yet in CONTENTS): the owned/rented-in/
rented-out subsections restart plotcode, requiring tenure-tagged plot IDs;
native sotka is 1/100 hectare, and AreaUnit is provenance. CONTENTS was read,
including its sampling caveat that the 1999 extra Dangara/Varzob sample was
intended for exclusion rather than reweighting; this validation does not
settle that separate population question. A concrete documentation follow-up
is needed: data_scheme.yml still says "Area is in sotka (1/100 ha)", while
the wave script says "Area ... converted to HECTARES" and the canonical
schema agrees with the script. Preserve that contradiction and add the
plot/subsection and unit conventions to CONTENTS in follow-up work. Frozen
source remains unchanged.

## Uganda — reviewed food-chain candidate reproduced; scientific hold remains

All twelve cache entries listed in the country lane's candidate
manifest match exactly in shape, columns, index names, dtypes and content
fingerprint: all eight wave food caches, country food, FCT, sample and
nutrition. Food is 355,571 x 3, FCT 15 x 113, nutrition 23,777 x 15 with raw
cache hash 12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a.
Evidence: uganda-reviewed-comparison.json. Served API digests are recorded
separately, since finalization adds v and other transformations to raw cache
frames; they are not compared to raw-cache manifests as though interchangeable.

Named idiosyncrasy: curated conversion_to_kgs is discarded during
canonicalization, leaving the runtime factor ladder responsible for served
mass. CONTENTS explicitly says "Do not regenerate the baseline until the
direction is signed off". The cold reproduction confirms the reviewed #967
candidate and does not resolve the older historical nutrition direction.
Neither the historical fixture nor the proposed patch was changed or applied.
All 28 declarations subsequently completed successfully. The final snapshot
contains 91 files with no shared counterpart and eight changed entries: the
four changed food waves plus country food, and two crop waves plus country
crop. Crop is 133,331 rows, +1,422 in 2018-19 and +1,303 in 2019-20, exactly
the second-condition-slot additions recorded under #848. Its Quantity,
Quantity_sold and Value_sold totals agree with that recorded cold after state.
Named idiosyncrasy: the second condition slot overwhelmingly reports dried
crop against the fresh first slot, and identical unit/condition collisions
still sum; the documented grain question remains open. Other shared Uganda
entries are unchanged. The final hashless siblings are earnings and fct,
both also missing wave manifests; the actual country read policy is stale.

Final-state caveat: nutrition.py writes both
var/fct.parquet and var/nutrition.parquet, and the API stamps only its requested
output. Thus nutrition(), called after fct(), removes the already-verified
FCT hash and wave manifest. The FCT values still match the reviewed candidate
exactly. CONTENTS describes the same sibling/prerequisite rewrite mechanism
for income/earnings under #788; its older "trust once, then stamp" statement
is no longer the L2-country read policy after #809, which rebuilds a hashless
country artifact. Do not manually restamp this artifact or claim every final
country cache is fresh. Final metadata was preserved unchanged, including
the likewise hashless earnings sibling. Source correction was not authorized
in this validation task, and the coordinator has been notified.

## Togo — two roster differences are row order only

All 22 declarations succeeded. The wave and country household_roster caches
have identical sorted DataFrames under exact comparison, including dtypes,
and identical row-hash multisets. Only ordering changes their fingerprints
(order-comparison-togo.json). Named idiosyncrasy: each EHCVM grappe is in one
vague, so household identity is grappe/menage without a vague level; the
instrument asks two six-month eligibility questions, not a continuous months
duration. The documented cp1252 roster-label correction is preserved by the
exact comparison. No new data difference is inferred from these hashes.

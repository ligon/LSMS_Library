# Independent Region-market dependency review

**Verdict: confirmed pre-existing geography dependency gaps.** This is a real
country exclusion from the requested Feature variant, not an absent cluster ID,
not a successful Region fallback, and not evidence that all source geography is
unavailable. No implementation or release approval is implied.

Inspected frozen source `7fa05179c8519f44ea329ba6387f5d6d6441a8ea`, the saved
corpus aggregates, and a complete-line snapshot of the active Feature log at
2026-10-15 06:54:16 UTC. Used only Git object reads, text/JSON parsing and YAML/AST
inspection. No lsms_library imports, API calls, cache reads/writes, builds, Git
mutations, or external writes. `inspect_evidence.py` reproduces this review's
source and aggregate checks; `evidence.json` records hashes and selected warnings.

## Observation and causal route

The audit records a warning for **each of GhanaSPS and Panama**, for **each of
food_expenditures, food_prices and food_quantities**, with `market='Region'`:

> Failed to load [feature] for [country]: RuntimeError: cluster_features returned no data for any wave.

The snapshot also contains the same warning for GhanaSPS
`household_characteristics(market='Region')`. This is seven observed warnings,
not a final full-scan count. The harness's `expected=True` classification does
not establish that the exclusion is acceptable.

The ordinary corpus has 13/13 successful GhanaSPS pairs and 5/5 successful Panama
pairs. Their successful sample frames are respectively **15,452 × 5** and
**18,353 × 3**, both with **zero null v**, and neither has a Region column.
This does not assert that every food household joins sample: GhanaSPS has a
separate documented 29-household/690-food-row source mismatch.

Source route at the frozen ref:

1. `country.py:2797` `_add_market_index` prefers the requested column from
   sample; it does not interpret `strata` as Region. GhanaSPS wave 3 maps a source
   `region` into `strata`, not into the requested `Region` column.
2. Unfilled m values reach `_market_lookup` at `country.py:2886`. The presence of
   v allows this fallback attempt; it does not supply geography.
3. `_market_lookup` (`country.py:2548`) calls each Wave's cluster_features.
   Both country schemes and all six wave data_info files lack that declaration.
   `Wave.grab_data` (`country.py:1498`) warns and returns an empty frame for an
   undeclared table. With no nonempty wave frame, the lookup raises the recorded
   RuntimeError (`country.py:2593`).
4. `feature.py:1056` catches that per-country exception, warns, and adds no frame
   for that country. This is distinct from Aggregate-label Contract B.

## Historical attribution

Independently recomputed the runtime agent's hashes from Git object contents at
v0.14.0 (`1528fa937bcf87f146b3af9ebfdbe18b8f912750`), pre-train
`452302adcfdbf3af538ef06375df1ac6c5aaa8a0`, and frozen candidate `7fa05179…`.
The following are identical at all three refs:

| Input | Hash |
| --- | --- |
| `_market_lookup`, SHA256 of AST source segment | `28badc458588033448a756d887b473b9a556347e0d904309208adac02a789e1c` |
| `_add_market_index`, same method | `06abd186d9338a8ab91c042da8d944ec50cfaa973643421b89e1dc0a6423bc74` |
| entire feature.py Git blob | `acb72acd21d4538dc569c685ae47ded53e209563` |
| GhanaSPS country scheme Git blob | `2851131b2861815c5f5ed1374b85a609030777e0` |
| Panama country scheme Git blob | `76cf7430a49a97b697c9206fe87eced9dbb91d1e` |

All six wave data_info blobs are also identical; their exact hashes and sample
mappings are in evidence.json. Thus the missing dependency and fallback code
predate this release. Historical environments/data were not executed, so this
does not claim old snapshots had identical returned row counts or warning logs.

## Country contracts, contradictions and existing issues

Read each CONTENTS's prose and TODO/WAITING/LOGBOOK search before inspection.
Both use a TODO/WAITING declaration; neither has a LOGBOOK drawer or an actual
WAITING heading. Their remaining TODOs are not treated as completed work.

**GhanaSPS:** CONTENTS:406–474 records the relevant idiosyncrasy: id3 alone is
the wave-1 EA; a district composite splits one EA. Waves 2/3 need the
right-anchored `FPrimary[-6:-3]` because split-counter width varies. The later
waves' missing weights and wave-2 missing geography are separate source gaps.
CONTENTS:49 says, **"For now, all of 2013-14 has Ghana set as its region."**
That legacy claim contradicts a functioning current Region-market fallback:
the observed API route fails for all three waves, rather than supplying Ghana
for wave 2. Preserve and qualify this claim; do not silently assume it stale or
turn the country name into an invented market. The source wave-3 `region`
already present as strata further prevents an overclaim that the source has no
geography in any wave.

Live issue **#140 remains OPEN** and explicitly includes adding cluster_features
in its P1 plan. **#110 was closed as superseded by #140**, as its closure comment
says, not because the Region gap was fixed. Closed #579 concerned sample/cluster
identity and its former no-ID hypothesis is explicitly refuted by CONTENTS and
the later #140 comment. Record this new measured consequence under #140; no new
GhanaSPS issue is needed.

**Panama:** CONTENTS:106–128 distinguishes ENV-97 planning-region domains from
fieldwork collection regions. Its quoted English rendering says their
**"geographic and physical boundaries do not coincide with the Collection
Regions created ad hoc for fieldwork."** Indigenous and hard-to-access areas
form separate domains; a province/domain/collection-region shortcut is not
justified. The current sample maps upm in each wave (1997 INFORM, 2003 E03BASE,
2008 02hogar), so this limitation is not missing cluster identity. No CONTENTS
promise of a functioning current Region table was found. The unresolved
790,813-versus-790,858 Census-2000 count discrepancy in CONTENTS:470–475 remains
untouched and unrelated. `panama.py:65` retains an unreferenced regional_data
helper; that does not register a live geography table.

Read-only GitHub searches for Region/Panama, cluster_features/Panama and
market/Panama found no existing issue specifically tracking this live dependency
gap. Closed #573 added sample and canonical food, not Region. Open #218 is the
legacy-food modernization umbrella; closed #141 is old nutrition's baked-in m
failure, and open #942 is cross-wave v semantics. None is evidence this gap was
fixed or an exact substitute issue. Search results are saved in the parent
control/countries folder; no issue was filed. A dedicated Panama geography issue
would be reasonable if the coordinator decides to track it separately, citing
#573/#218 only as context.

## Disposition and limits

The paired appended notes and release sentence in this folder are ready for
coordinator-owned documentation. They explicitly retain the gaps. This review
does not choose a Region definition, recover geography, rename strata, change
weights, or establish present raw source completeness. The full Feature scan is
still active. Uganda's scientific baseline hold and all release/cache gates
remain unchanged.

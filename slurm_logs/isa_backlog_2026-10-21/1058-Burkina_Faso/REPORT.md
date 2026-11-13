# Burkina Faso #1058: parcel coordinates

The 2021 parcel source records latitude and longitude, but the existing
helper and wave colmap omitted them. The patch exposes those two fields
through the existing retained-source helper. It preserves all 19,784 public
rows and the original six-column payloads, order, keys, nulls, dtypes and
logical attributes. Independent review is pending; this is an unmerged
implementation, not a claim of production resolution.

Ledger-first commit: `b5ced85c571bc4d5b430371d1f92ace336c24a3f`.
Runtime/tests commit: `7eee549b15cacbe1e86b60a925b0899dc85d49e6`.
Both were published by the ordinary commit hook. The final evidence commit
is a documentation descendant; its full SHA is recorded in the private
freeze proof and review handoff.

## Boundary and source authority

The only executable helper addition independently assigns optional
`latitude` and `longitude` mappings using positional nullable-float arrays
on the existing retained source rows. This avoids alignment with the
source's noncontiguous index. The 2021 colmap binds only
`s16aq46__latitude` and `s16aq46__longitude`; their held variable labels
identify parcel GPS latitude and longitude. The country declares the
already canonical fields as optional float columns. A declared absent
source column raises; an absent optional mapping leaves the wave unchanged.

There is no new eligibility, flag gate, interpolation, screening, household
formatter, panel mapping, merge, source, universe, area or unit rule.
`s16aq47` remains GPS **area**, distinct from coordinates. The complete
helper module AST is identical to baseline after removal of the new loop
and docstring change; the wave AST is identical after removal of the two
bindings. The parsed country scheme is identical after removing the two
optional declarations. Canonical schema and other country files are exact.

## Survey and key layers

The full country CONTENTS was reread, including its live tasks. Burkina's
2014 EMC differs from EHCVM, including four food recalls. The 2021 survey
is a half-panel restricted to original enumeration areas, without an
automatic top-up to the old sample size. The 2018 parcel script passes
`ehcvm_i`; 2021 deliberately passes `_old_i`/country `i`. These wave-specific
formatters are preserved.

The former helper/scheme assertion "EHCVM s16a has no decimal-degree parcel
GPS" contradicts the dated CONTENTS correction and held 2021 labels. Those
comments are corrected. The concatenator description "Cached parquets
store pre-transformation data" describes a different layer from the
default framework route: `load_from_waves` applies the existing `id_walk`
before the country write, and the raw-country frame carries
`id_converted=True`. Native script and walked country keys are therefore
compared separately, using the tested mapper without normalization changes.

The survey description also contains a French enumeration-area split
description beside the English "randomly divided each enumeration area
into two equal groups", and planned 2021 counts summing to 3,730 beside
3,510. This task does not adjudicate those survey-design statements.

## Held-source and served accounting

| Layer | 2018-19 | 2021-22 |
|---|---:|---:|
| Source rows | 12,441 | 8,467 |
| Retained native parcel rows | 12,441 | 7,350 |
| Public rows | 12,434 | 7,350 |
| Existing empty-payload exclusion | 7 | 0 |
| Native/walked duplicate keys | 0 | 0 |
| Public sample orphans | 0 | 0 |

The 2021 source has 1,117 placeholders with both native parcel components
missing. The unchanged parcel mask excludes them. All 4,180 reported pairs
are retained, neither axis has a partial pair in this distribution, and
3,170 retained parcels lack both coordinates. No position is inferred for
the ten GPS-measured parcels that have area but no coordinates. Retained
measurement flags are 4,190 Yes, 3,151 No and nine missing; the flag does not
control the new fields.

An independent source-key projection reproduces the native and public
coordinates exactly. Zero native or walked collisions, null public keys or
sample orphans were observed. The raw-country table retains 19,791 rows;
the existing finalizer excludes seven 2018 rows missing all six old
payloads, giving 19,784 public rows. Private full-frame invariance preserves
this behavior without adding permanent tests that pin that separate policy.
The 2018 native wave stays six-column; country aggregation adds missing
optional coordinate columns to it. The unsupported 2014 subset remains
empty with its prior structure and attributes.

The earlier immutable diagnosis read the full Mali, Niger and Senegal
CONTENTS files before their held 2021 s16a metadata. Respectively 9,924,
5,729 and 6,112 raw rows contain no q46 or other coordinate header/label;
GPS-related fields describe measurement flags and area. These are negative
findings for the held distributions, not an assertion about unheld releases.
No other country's implementation changes.

## Verification

Preparation physically copied 15 held L1 blobs into each private root,
verified their current sidecar MD5 plus SHA, modes and mtimes, and asserted
distinct inodes and single links. Six Senegal comparator parquets had
current hashes and complete declared wave coverage before copying. Both
successful cold roots started with zero Burkina parquets; normal builds
also rebuilt supporting sample tables. There was no acquisition or manual
stamping. The final candidate used a newly prepared physical root.

Each baseline/candidate cold process and separate fresh warm process
checks 13 complete frames: two countries' public plot/sample frames,
Feature, three Burkina wave subsets, raw-country, and both native
helper/wave pairs. The candidate's original columns, keys, row/column order,
axes, values, nulls, dtypes and logical attrs equal baseline exactly. Both
warm processes use new Country objects and preserve every cache byte,
mode and mtime. Senegal's whole frames and all six parquet receipts remain
exact. Feature returns 33,608 rows, exactly 19,784 Burkina plus 13,824 Senegal.

All 15 new test outcomes and 21 existing controls are listed in validation.
Candidate: **36 passed, zero failed/errors/skips**. Before runtime edits,
the new file against ledger-only baseline gave **12 failed, three passed,
zero errors/skips**; the failures detect the omission and missing-source
contract. Tests exercise actual helper and wave bindings, noncontiguous
source indices, independent missing axes, both missing, zero/signed values,
a non-Yes flag, missing declared columns, q47/axis/key fault rejection,
typed declarations and actual held wave/public source projections.

The API audit passes, including no untracked hashed build inputs. The
existing Feature scanner phases 1 and 2 yield the identical full unmodified
finding multiset: **13 warnings, zero failures/errors/skips**. It contains
nine runtime warnings (two NullRead, six QuantityImplausible, one population
heterogeneity), two scheme/index diagnostics and two constant-AreaUnit
diagnostics. The two countries' existing API-added `v` level accounts for
the scheme/index diagnostics. There is no plot_features phase-2 kwarg grid,
so this is zero kwarg cases, not extra coverage. Across the complete API
probe sequence, each baseline/candidate cold/warm run emits six NullRead,
eight QuantityImplausible and one population-heterogeneity warning; their
full stage/class/message multisets are exact. Raw warning examples remain
private.

The actual full inventory measures **549 country hashes and 39 framework
fingerprints**. Exactly **24 Burkina** hashes move because the country
helper/scheme are shared country build inputs. The other **525 hashes and
all 39 framework fingerprints are exact**. This is normal cold coverage of
plot_features and supporting sample, not a rebuilt all-table Burkina cohort.

## Isolation, preserved failures and review readiness

Runs assert exact cwd, package/config/data realpaths and use the mounted
image Python, `unshare -rn`, active socket guards, numerical/build/Make
threads one and one loadfile test worker. Two guard probes reject network;
actual outbound attempts and in-tree parquets are zero. The only checkout
metadata exception is root's prepared, Git-ignored `countries/.dvc/tmp`
symlink to the exact private owner-only target; DVC site state is private.
No mount namespace, runtime patch or source/cache bypass was used.

An initial preparation gate counted an empty import-created directory as
a file and stopped. An attempted dependent command also stopped at that
gate before any API call. The first candidate cold build then completed,
but a private proof compared the six-column 2018 native wave against the
eight-column country union. Its output/logs are preserved; the corrected
proof compares that wave's native columns and separately checks missing
country coordinates. A fresh physical candidate root supplies the final
cold/warm proof. These were harness distinctions, not product failures.

Four immutable tracked trees, all their file contents/modes/heads/clean
states, and the old #1058 diagnosis plus #1123 writer/review evidence remain
exact. Neither those roots nor shared caches were mutated. Private source,
full frame, warning and freeze receipts are under the separate `1058-fix`
control root. No raw IDs or coordinates are committed.

Suggested #1058 readiness update: the Burkina implementation is published
and awaiting independent review, with preserved rows/payloads and the
source/Feature/hash evidence above; the requested held Mali/Niger/Senegal
checks found no coordinate variables. The dispatcher can narrow the issue
during PR readiness using explicit reviewed-but-unmerged status. There is
no newly required source authority decision within this bounded mapping.
Separate survey/key/area policies remain unchanged. No GitHub write was
made by this lane.

Sue

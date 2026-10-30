# Prior-art ledger — Uganda #689 parish-grain disclosure

## §1 Task

Finish the remaining source-facing disclosure requested in live #689 without
changing survey values, keys, reducer policy or a scientific definition.
Public base is d65e8b13683409ab0005f1a5590208cd7d94ae62. Live #689 is open,
zero comments. CONTENTS already explains parish grain and honest conflict nulls;
the data_scheme cluster_features block currently has no explanation. One
CONTENTS overview sentence incorrectly describes the library's key change as
a sampling-frame change. Correct that inference using the existing sampling
record, whose documented PSU remains enumeration-area.

## §2 Existing machinery and search floor

Git/ripgrep floor. Reuse STANDING.md §§2–5 and CLAUDE.md core API/cache rules.
`Uganda/_/CONTENTS.org:45` records key columns; its projection section at1095
explains reduce_to_agreed, conflict NA and household Rural. `sampling.yml:287`
onward records enumeration-area PSUs separately from the library's parish v.
`data_scheme.yml:19` declares the actual index and columns; do not change them.
`uganda.cluster_features` uses the existing tested reduce_to_agreed contract;
tests/test_uganda_v_grain_invariants.py covers the shared sample/cluster keys.
No new extraction, inferred EA, reducer or metadata vocabulary is needed.

## §3 Definitions in force

Harmonize the interface, not the data. A shared `v` axis does not imply a shared
survey geographic grain. For Uganda from2013-14 it is a parish proxy, not the
survey's EA sampling unit; later names are district/parish composites. Early
origin-EA keys and2009 GPS bins have their own caveats. A parish may span mixed
urban/rural EAs; conflict NA is honest and must not be replaced by a first row,
majority or mean. Household Rural belongs to sample(). Cite CONTENTS and the
existing sampling.yml rather than repeat counts as new measurements.

## §4 Invariants and limits

Only explanatory comments and prose change. Parsed YAML, scripts, registry,
index/column declarations, sampling design metadata and returned frames remain
the same. No data read or numerical test is necessary for comment-only changes.
Fingerprint impact must be disclosed: raw data_scheme.yml text is hashed at
Country._table_cache_hash; the YAML has no wave hash entry. Measure a finite
metadata-only before/after census if useful; do not rebuild unrelated tables.
Do not claim the EA cannot be recovered from every other source: that separate
source-search question has not been completed here. Keep all TODO/WAITING and
LOGBOOK caveats, including2019 weight/refresh tension and #829 scale question.

## §5 Reuse decision

Reuse existing documentation and reducer policy. Extend only user-facing
disclosure in the requested schema block and correct the misleading overview
sentence. No new tests that mirror comments; verify semantic YAML equivalence
and diff cleanliness. No public writes; root owns publication.

## §6 Other reconciled scopes

#1297 now has an evidence-backed draft issue update distinguishing95 complete
candidates with incomplete siblings from historical89 eligible rows;2011 plots
remain unresolved. #647 remains a question with zero comments and no verdict.
Its questionnaire/manual sidecars exist but neither object was found in checked
local L1 layouts. The public catalog confirms both documents; the web PDF reader
refuses the octet-stream response. No harvest inclusion/exclusion rule is guessed,
and source counts or labels alone are not promoted to instrument authority.

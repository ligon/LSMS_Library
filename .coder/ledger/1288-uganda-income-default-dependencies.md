# #1288: Uganda income cache dependencies

## Existing authority and reproduced defect

See the #1062 Uganda sibling-writes ledger and the country CONTENTS
requested-output note. The default builder reads the current full raw country
earnings and enterprise-income artifacts. Do not change materialization,
arithmetic, weights, id walks, routing, receipt semantics or live sibling policy.

Two independent source-free public API witnesses at pinned development
or the separate #1796 Make-literal candidate show a two-wave income cache
stays warm under DVC/default when only one sibling's sidecar changes.
One income cache was authored by default and has no Make receipt. The
other was Make-authored and a subsequent explicit Make call can detect
receipt drift. A Make-receipt-only validation cannot fix both witnesses.

## Design

Add an opt-in top-level Cache Dependencies mapping in data_scheme.yml,
separate from Data Scheme columns. Uganda income declares its two known
L2 country inputs. Preserve the exact ordinary hash for a table without
outgoing dependencies. An opted-in table folds named full-country
dependent hashes in stable order with a versioned domain marker.
Refuse malformed declarations, repeated list entries, undeclared or
non-parquet runtime-derived/JSON table names, cycles, and unavailable
declared hashes. No persistent digest memo or scanner/path inference.
Use one per-call memo for transitive dependencies. This is not a new
on-disk manifest, generic Make availability policy or concurrent producer
atomicity change. The explicit assume_cache_fresh override is unchanged.

## Verification

Build the actual Country public synthetic Uganda route with two waves and
distinct private sidecars. Default-owned and Make-authored outputs must
rebuild in default when a sibling source changes, with the correct payload,
new selected hash and full coverage. Check alternating subsets, unrelated
source drift, malformed DAGs, old hashless/legacy semantics and preservation
of untouched hashes. Compare both pinned dev and #1796 composition.
Measure one-time Uganda scheme hash movement separately from later
input-sensitive hashes. No real input stage unless bounded exact custody
and execution are feasible. No merge or whole-issue closure.

## Acceptance

Both source-free API author cases fail with stale values on development:
default authored/no receipt and Make authored/with receipt. With the patch,
the same-instance default subset rebuilds to the new income, retains full
coverage, and the rotated subset and all-wave calls use the warm result.
Subsequent wages-sidecar drift moves only earnings and declaring income.
A valid assume_cache_fresh still bypasses all dependency digests.

97 focused controls pass, three real-data tests deselected. The new explicit
assumed-fresh control passes separately. All 20 source-free combined #1796
checks pass on a composed tree. An independent review found and resolved
the missing empty graph in the GH1066 non-Country test fixture, with its
full suite included in the 97. No public value or merge change.

The 38-country (2,879-entry) metadata census moves exactly 28 Uganda L2
country fingerprints once, since the existing hash includes whole scheme
text. No wave or other-country fingerprint moves. The new digest
contribution itself is attached only to declared owners. Ordinary rules for
direct GNU Make timestamps and races within an active default build are
unchanged and not certified by this repair.

Real default Country rebuilt cold from 29 held, exact sidecars in a private
offline projected run. All 14,740 rows and typed attributes match the
pre-change public reference. The fresh warm call changes no bytes or mtimes,
reads no new native data and confirms that the result needs no Make receipt.

# GH1051D — select only validated live window identities

Search tier: git/ripgrep/AST floor. Own isolated currenta578 plus ordinarymerge
7b288fc3ad2611ab8fe086fced0cc8c002d06bf5. Inherit original D ledger/ROOT B+C1
decision and STANDING §§2–4. No country-specific data/science/CONTENTS scope.

## §1 Task

Close the remaining selection gap: a matching window published after the first
snapshot can be retained by the correct R1 prune logic, but has not been route
validated by that selection. It must not suppress that caller. Select only an
identity both validated in the snapshot and still live after owner/options
transitions. Keep later publications for the next call's validation.

## §2 Prior art and exact touchpoint

local_tools._fast_window_select:527–568 takes entries, validates routes outside
state.lock, preserves later live publications/strong references, then mistakenly
selects from every live entry. _fast_window_publish already validates publication
metadata; that earlier validation does not qualify a post-snapshot entry for the
current selection after its route may change. _fast_same_route/_fast_scope,
_FastProbeOutcome, reset/expiry/owner/options and last-reference lifetimes are
existing authority; reuse them, not another client/lock/provider abstraction.
Original D114 and R1 23PASS/map remain historical and immutable; original R1FAIL
was a different ownership transition. The independently owned new parent control
is manager1590 job41050528, not run or asserted observed by this author.

## §3 Definitions

ROOT-selected internal5second non-sliding/max16 advisory B+C1 contract remains.
Snapshot-validity is object identity, not tuple equality or matching scope alone.
Still-live means the same identity remains in state.windows after existing
pruning and owner/options invalidation. Scope matching stays exactly unchanged.
No revalidation or SDK/config/credential getter under the lock.

## §4 Invariants

Preserve every original product node except _fast_window_select's final membership
condition/comment. Keep entries,valid,released and owner strong references through
mutex exit; no new finalizers/close/IO/sleep under lock. Late entries are retained,
not discarded as unvalidated. Normal warmers/readers, retry/layout/progress/error
rules, A/B/C/E, transient source-memo policy and all original tests stay exact.
No new exclusion/schema/user knob/network/auth or actual raw/API/cache work.

## §5 Implementation and finite controls

Extend one existing condition: require `any(entry is v for v in valid)` before
its unchanged scope match while iterating the still-live state list. Existing
max16 bounds this cheap identity scan. No new helper or SDK interaction.

New deterministic real-helper interleaving: validate a nonmatching old snapshot;
during its outside-lock route callback publish a matching late entry through the
real publisher, then optionally invalidate its route before selection. Current
caller must use its own backend, no suppression, and late identity must survive.
Next caller validates it: invalid route pruned/no suppression; healthy retained
route selected. Strict metadata leaves only; no real network/files/provider data.
Parent expected failure is source-supported, not claimed observed until the
independent actor reports it. Keep assertions unchanged after freeze.

One meaningful affected cohort: new2parameterizations plus selected existing
R1 publication retention, four select expiry/invalid-route/owner/options lifetime
cases, two concurrent owner/options eviction cases, non-sliding expiry/legacy
memo and config/owner scope controls. No full23/114 or other old bulk replay.
Freeze source/tests/selection first; MAIN/cwd/PYTHONPATH/countries realidentity,
privateemptyconfig/data, falsecreds/no optional DIAG exports, unshare-rn,
thread/build1 and2loadfile workers. Preserve any failure/no automaticretry.
One corrected final existing metadata census vs original closedE SHA66187750…,
header73842347…; no baseline replay, exclusions or actual source/builds.

## §6 Remaining gates

ROOT source/independent review/configured CI/publication. No user/scientific
choice is needed for this ordinary membership correction. Report parent control
and candidate results separately; no self-independent claim or universal-outage/
SDK-request/latency promise. Final ledger-only child conserves tested nonledger.

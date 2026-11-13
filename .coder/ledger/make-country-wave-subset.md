# Prior-Art Ledger — direct Make country wave subsets

Search tier: git + ripgrep floor; GitNexus unavailable. Inherits STANDING.md.

## §1 Task
Restrict the direct country-builder Make result to requested waves, without
changing builders, failure policy, cache writes, or scientific transformations.
Public base d65e8b13683409ab0005f1a5590208cd7d94ae62; private 7076b5916 is
attributed evidence only. PR1312 compiler work remains separate.

## §2 Existing machinery
- Country._restrict_to_waves (country.py): tested GH891/892 row selection,
  full-self.waves pass-through including null t, no-t pass-through, attrs copy.
- Country._aggregate_wave_data: direct non-DVC country builder currently omits
  that restriction; other cache routes already use it.
- _has_country_builder and load_from_waves: GH1039 no-builder routing tests.
- run_make_target strict_country_make: GH1288 terminal recipe failure tests.
- Tanzania livestock wave scripts and compiler: reuse unchanged; two LF_SEC_02
  sidecars suffice; no sample join for livestock.

## §3 Definitions
STANDING.md §3/4, CLAUDE.md and data_info.yml remain authoritative.
Tanzania CONTENTS.org: 2019-20 Extended and 2020-21 Refresh are disjoint.
Full-country requests are pass-through, per _restrict_to_waves docstring.

## §4 Invariants
No unconditional filtering of every API return. Restrict after successful
country target read and before finalization. Preserve None failure, strict Make
failure and unknown-wave validation. Preserve attrs, order, dtype and null masks.
Exact nullable floats require nonmissing NumPy exact checks (GH1206).
Changing _aggregate_wave_data conservatively invalidates build fingerprints
beyond Make users; measure metadata only. No freshness-policy change.

## §5 Reuse decision
Reuse _restrict_to_waves; add direct-route tests using GH1039's fake country.
Reuse existing GH1039, GH1288 and GH891 suites as route/failure regression nets.
No formulas, reducers, weights, eligibility or compiler retirement.

## §6 Open questions
None blocking authorized scope. Root retains independent review/publication.

## Phase 3
- OK (§2–§5): direct branch reuses _restrict_to_waves after the None gate;
  no duplicate filtering algorithm, builder change or failure-policy change.
- OK (§4): nine new direct-route cases plus updated GH1039 deferred assertion;
  baseline corrected new tests 2 fail / 6 pass before the ninth missing-output
  case was added. Patched broad selection 195 pass / 2 known oracle-contract
  failures / 3 credential skips; corrected affected selection 10 pass. Union:
  197 distinct passing cases, 3 skips. Original failures retained in evidence.
- OK (§3–§4): actual private fresh-L2 Tanzania baseline Make returns 6,555 rows
  for 2019-20; patched returns 1,212 (2020-21 5,343; absent supported wave zero).
  Full output remains 6,555, native 6,562; seven all-null rows are removed by
  the unchanged finalizer. 26 cross-revision/backend comparisons exact in keys,
  order, dtype, null mask, attrs and nonmissing NumPy numeric payload.
- OK (§4): metadata-only sample moves all five fingerprints/country hashes;
  conservatively expect broad invalidation, not just Tanzania/Make. Repeated
  Make work and successful-output freshness remain outside this repair.
- No contradiction or reinvention in the product change. Test oracle correction:
  attrs describe the selected frame, so expected attrs must come from finalizing
  the selected native rows, not slicing already-finalized full-frame metadata.
- Root retains independent review, public publication and PR1312 integration.


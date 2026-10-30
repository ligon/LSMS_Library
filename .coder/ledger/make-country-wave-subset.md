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
Pending scoped baseline, patched synthetic and actual cached-source checks.

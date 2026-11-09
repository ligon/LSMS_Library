# Prior-Art Ledger — GH #1135 Uganda historical measurement configuration

Inherits `.coder/ledger/STANDING.md` (§0). Search tier: ripgrep + own Git +
stdlib text/AST only. Source base: `79f5979cf8022e64928c5150950dfb00456994da`.
This is a source-only port, not runtime or scientific acceptance of GH #1135.

## §1 Task, restated

Add only `panel.cluster_id_measurement` to Uganda's existing 2009-10,
2010-11, 2013-14 and 2018-19 boundary records. Reuse dated facts supplied in
the frozen `COUNTRY-INPUT.json` asset (reviewed source
`f1cf7d06f30367cd1e34a4b3db4012f951b2e497`), reconciled with this checkout's
`lsms_library/countries/Uganda/_/sampling.yml` and full `_/CONTENTS.org`.
Keep all existing configuration bytes, including EA/design disclosures,
oversamples, notes and provenance. Only that configuration and this ledger
are owned. First commit is this ledger alone; configuration and final ledger
updates have separate commits.

## §2 Existing machinery

| Existing source | Role | Verification status / decision |
|---|---|---|
| `Uganda/_/sampling.yml:112` onward (under `lsms_library/countries/`) | Existing boundary declarations and dated N/D evidence | Reuse; own bytes match the exact base and frozen input |
| `Uganda/_/CONTENTS.org:1–6072` | Country design and implementation chronology, including TODO/WAITING/LOGBOOK | Personally read in full before configuration edits; reuse qualifications |
| Frozen `reviewed-sampling.py.txt:208–260` | Count definitions and allowed measurement keys | Read as plain source only; reuse approved contract |
| Same source, `_check_measurement:717` | Integer, provenance, applicability and coherence rules | Static source anchor only; never imported/executed |
| Same source, `_size_fragment:1220` | Consumer distinguishes measured / not applicable / not measured | Static source anchor only; shared implementation/tests belong to coordinator |
| Frozen `APPROVED-SCHEMA.md:1–9` | Root-approved current contract, including Nigeria/Tanzania corrections | Binding; no other-country edits |

The reviewed source names `tests/test_sampling.py`; no test was run or
collected and no current test coverage is asserted. The exact base deliberately
lacks the new schema. Source text and byte proofs are the only allowed checks.

## §3 Definitions & conventions in force

- See `STANDING.md §3`, `CLAUDE.md` “Site C” and “sample() and Cluster
  Identity”, and canonical `lsms_library/data_info.yml` for standing schema
  authority. This task edits none of them.
- Frozen `reviewed-sampling.py.txt:208–237`: N is linked households with `v`
  on both sides in the historical served sample; D counts all changes,
  including movers. K counts clean one-to-one changes of real earlier IDs;
  synthetic bins and changes of kind use `not_applicable`.
- Frozen `APPROVED-SCHEMA.md:4`: unknown counts are omitted, never zero or
  `not_applicable`. No supplied record has a settled net-of-movers count:
  omit that field and `mover_rule`. Each measurement has its own provenance.
- Own historical counts to port, not remeasure:

  | Boundary ending | N | D | K |
  |---|---:|---:|---|
  | 2009-10 | 2601 | 191 | not applicable |
  | 2010-11 | 2640 | 433 | not applicable |
  | 2013-14 | 1717 | 1717 | not applicable |
  | 2018-19 | 2973 | 2973 | not applicable |

  First two are the 2026-10-22 post-#1133 measurements, excluding the six
  and seven then-missing `v` values from their respective denominators.
  Last two are the 2026-10-19 measurements. The supplied 2026-10-23 notes
  also record 65/212 and 576/788 one-to-one pairs; these stay qualified
  prose, not K, and their original historical locators are retained.

## §4 Invariants & assumptions

- `STANDING.md §4` and actual `AGENTS.md`/`CLAUDE.md` apply within the user's
  stricter source-only authorization. No package imports, YAML parser,
  pytest/collection, raw/header/DTA reads, Country/Feature/getters, builds,
  DVC/Make, network/acquisition, cache/rewarm, weights or scientific changes.
  Prepared runtime was already bootstrapped; task checks use its Python's
  standard library only. No native-home/state/transcript/observer-body reads.
- Relevant idiosyncrasy: `CONTENTS.org:69–79` says “From 2013-14 =v= is a
  parish, not the unit the survey sampled.” `sample.ea` is the design PSU of
  origin, distinct from location `v`; 2009-10 additionally mixes EAs and
  synthetic GPS bins (`CONTENTS.org:301–421`, `487–576`). Preserve both.
- Historical contradiction already resolved in the current source:
  `CONTENTS.org:1706–1713` quotes the old note “A parish is not an EA, so
  no crosswalk can exist” and limits that claim to served parish `v`, while
  the survey's `ea` pairs with earlier `comm`. The current 2013-14 note
  includes this correction; never restore the older complete panel block.
- Later membership/singleton facts qualify old counts: `CONTENTS.org:328–331`
  says “the last is now 2,510 / 2,540” (2011-12), and `:5002–5028` describes
  cover extensions. These are not a license to invent updated N/D values
  for the four assigned boundaries. Every new note must say historical.
- Existing design tension is preserved: `CONTENTS.org:1088–1097` contrasts
  “one-third of the original sample is refreshed each wave” with the DDI's
  “This same sample was used for the UNPS 2015/16 (Wave 5)”. Existing
  2019-20 panel-weight TODO and #829 WAITING remain untouched.
- The existing header “DERIVED QUANTITIES DO NOT BELONG IN THIS FILE”
  remains byte-exact. Frozen reviewed source `:68–74` explicitly authorizes
  the GH #1135 boundary-measurement exception; design PSU/strata counts are
  still excluded. No newly discovered unrecorded country quirk or blocking
  contradictory authority is claimed.
- Remove only the four new measurement blocks to reproduce exact original
  configuration bytes. All other tracked mode/type/blob identities must equal
  base, except this new ledger. Do not normalize old text or change defaults.

## §5 Reuse decision

| Quantity / behavior | Decision | Reason |
|---|---|---|
| Four historical N/D measurements | reuse | Supplied reviewed facts agree with dated own boundary notes |
| Applicability and own provenance | reuse | Approved typed contract; distinguish unknown from not applicable |
| Configuration | extend | Insert four optional nested records; no existing byte changes |
| Runtime/schema/tests | reuse later, outside this worker | Coordinator owns shared implementation; no local execution authorized |

## §6 Open questions for the coordinator

No blocking question. Coordinator acceptance and runtime integration remain
pending by design. The ordinary absolute dispatch steering channel is checked
at checkpoints; no human approval or new agent is requested.

### Phase 3 — verification

UNFINISHED: ledger-grounding checkpoint only. Exact base/config/document/input
bindings checked with stdlib text and Git. Configuration insertion, count-text
checks, removable-block proof, nonowned Git object proof and publication remain
pending. No numerical measurement or runtime/test result is asserted.

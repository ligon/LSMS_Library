# Prior-Art Ledger — GH #1135 Ethiopia measurement config

Inherits [.coder/ledger/STANDING.md](STANDING.md). Source-only port at base
`79f5979cf8022e64928c5150950dfb00456994da`; this ledger is not numerical or
runtime acceptance. Search tier: Git + ripgrep + stdlib source/AST inspection.
No package, parser, Country, Feature, raw-data, Make, DVC or test execution.

## §1 Task, restated

Add only `panel.cluster_id_measurement` to Ethiopia's existing `2013-14`
sampling boundary, lifting the already recorded 2026-10-19 measurement:
`linked_households: 3776`, `households_changed: 3776`,
`clusters_renumbered: 333`. Preserve every other config byte, including all
current notes, locators and nonmeasurement metadata. Only this new ledger and
`lsms_library/countries/Ethiopia/_/sampling.yml` are owned tracked paths.
Initial ledger, config, and final ledger updates require separate commits.

## §2 Existing machinery and source evidence

| Source / machinery | Relevant fact | Verification status / decision |
|---|---|---|
| `lsms_library/countries/Ethiopia/_/CONTENTS.org:1` through `:4652` | Personally read the entire file, including TODOs and dated checkpoints, before config edit. | Source text only; preserve entire file. |
| Ethiopia `_/CONTENTS.org:368` ("Cluster (PSU) identifier"), especially `:385` | Dated 2026-10-19 sample measurement: 3,776 linked households, none keeps `v`, 333 cluster pairs form a complete bijection. | Reuse historical facts, not a new/current measurement. |
| Ethiopia `_/sampling.yml:17` through `:32` at base | Existing W2 panel note records the same counts, `ea_id` versus `ea_id2`, and 100 new large-town EAs with no W1 antecedent. Own panel provenance dates the measurement to 2026-10-19. | Extend only by a new nested measurement block. |
| Ethiopia `_/CONTENTS.org:1735`, `:3854` | Later cover expansion: "W2 adds" / "25 links with 3776 unchanged". | Broader later linkage is not this dated measurement's denominator; no recomputation. |
| Frozen `APPROVED-SCHEMA.md:1` through `:9` in the supplied `ISA_CONTEXT_ROOT` | Approved country contract, including the binding Nigeria/Tanzania corrections and byte preservation. | Reuse contract; no other country config is owned. |
| Frozen `COUNTRY-INPUT.json`, sole Ethiopia `2013-14` row | Literal old counts and own locators. `panel_only_equal` is true; surrounding blank-line difference explains the full-entry comparison's false flag. | Reconcile with own base bytes, never replace the old file wholesale. |
| Frozen `reviewed-sampling.py.txt`, `_MEASUREMENT_COUNTS`, `_MEASUREMENT_FIELDS`, `_check_measurement` | Optional typed measurement with independent provenance, unknown versus not-applicable, canonical counts and coherence. | Read-only source; never imported or executed. |
| Base `lsms_library/sampling.py:201`, `:584`, `:653` | Existing frozen panel fields and loader do not yet support the new measurement field. | Coordinator owns schema integration; deliberate interim mismatch, no repair here. |
| `tests/test_sampling.py:1`, `:218` | Existing source declares config/vocabulary coverage. | Read as source only; no execution/collection and no new test. |

## §3 Definitions and conventions in force

- Inherit `STANDING.md` §3, `CLAUDE.md` "Site C" and "sample() and Cluster
  Identity", and canonical declarations in `lsms_library/data_info.yml`;
  this port changes none of those definitions.
- Frozen reviewed source defines N as linked households with `v` on both
  sides, D as those changing `v` counting all causes, and K as clean
  one-to-one renumbered earlier-wave IDs, not a design PSU total.
- The measurement has its own `source_type: derived-from-data`,
  `confidence: high`, and nonempty dated locator. These qualify the historical
  evidence, not a documentary claim about sampling design or fresh accuracy.
- Unknown net-of-movers count is omitted, with no `mover_rule`. Nothing in
  the assigned evidence is designated `not_applicable`; do not manufacture
  either zero or not-applicable from an unknown.

## §4 Invariants and country caveats

- Inherit `STANDING.md` §4 and the source-only dispatch restrictions. No
  scientific/default/sample/v/weight/mover policy changes; no runtime gates.
- Relevant idiosyncrasy: W1 `ea_id` and W2 `ea_id2` are disjoint code spaces
  for the same EAs. The 333 pairs, measured historically, form a bijection.
  W2 adds an independent large-town cohort with no W1 antecedent. Panel II
  starts at 2018-19; no record is added to that boundary or to W3/W5.
- Relevant chronological qualification: later `CONTENTS.org:3854` says
  "W2 adds" and `:3855` says "25 links with 3776 unchanged". Retain the
  2026-10-19 counts as historical observations, not current sample coverage.
- Existing documented prose tension is preserved and reported: Overview
  says "rural and small-town areas in waves 1-3" (`CONTENTS.org:330`), while
  its own later reconciliation says "small-town-only restriction applies to
  *wave 1 alone*" (`:895`). Neither statement changes this boundary's dated
  measurement. No blocking authority contradiction or unrecorded quirk found.
- Every original config byte must survive removal of only the inserted
  measurement block. All nonowned tracked mode/type/blob identities must
  remain equal to base. No documentation, source, schema or test edits.

## §5 Reuse decision

| Quantity / behavior | Decision | Reason |
|---|---|---|
| N=3776, D=3776, K=333 | reuse | Explicit assigned historical counts agree with own committed note and country documentation. |
| Measurement mapping | extend | Add one approved optional mapping under existing W2 panel; shared implementation belongs to coordinator. |
| Net-of-movers count and rule | omit unknown | No settled historical movement classification; do not infer a net count. |
| not_applicable | omit | No assigned count is marked inapplicable. |
| Runtime/scientific validation | unrun by instruction | This stage admits text, stdlib AST and own Git proofs only. |

## §6 Open questions for the coordinator

No blocking question for the assigned source-only port. Runtime/schema
integration and any fresh remeasurement remain separate coordinator gates.
Normal dispatch steering is read at checkpoints; no GitHub or source-remote
publication is authorized.

### Phase 3 — verification

UNFINISHED. The initial ledger precedes config mutation. Planned checks:
literal counts/provenance against approved assets, text-only field/coherence
review, exact config byte restoration after block removal, nonowned Git
object identity, and separate commit/path scope. No runtime acceptance claim.

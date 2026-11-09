# Prior-Art Ledger — GH #1135 Tanzania measurement configuration

**Status:** grounded; config port and final static verification pending.
**Source:** `79f5979cf8022e64928c5150950dfb00456994da`.
**Search tier:** ripgrep + own Git + stdlib text/AST only. No GitNexus,
Make audit, project import, parser, test collection or task runtime.
Inherits [STANDING.md](STANDING.md) §§2–4 and the current `CLAUDE.md`.
The frozen context copy of `shared-prior-art-ledger-SKILL.md` was read.
The user authorizes this headless port, ledger first and separately committed;
its human-review pause and runtime test instructions do not apply here.

## §1 Task, restated

Add only `panel.cluster_id_measurement` at the already-declared Tanzania
2012-13, 2014-15 and 2019-20 boundaries. These are historical measurements
ported from the ROOT-approved frozen contract and `COUNTRY-INPUT.json`, not
new observations or present-day population claims. Preserve every existing
configuration byte. The only other owned tracked path is this new ledger.
The coordinator owns the shared schema implementation and tests: base 79f
intentionally does not yet accept the measurement field.

## §2 Existing machinery and evidence

| Source / machinery | Locator at base or frozen source | Reuse decision / coverage |
|---|---|---|
| Existing country boundary notes | `lsms_library/countries/Tanzania/_/sampling.yml:34`, `:55`, `:74` | Reuse committed historical N/D and qualifications; insert blocks only |
| `SamplingRecord`, `_check_panel`, `sampling_records` | `lsms_library/sampling.py:320`, `:584`, `:653` | Existing loader; leave unchanged; `tests/test_sampling.py` names `TestThePanelDeclaration` / `TestConfigRoundTrip` (text inspected, UNRUN) |
| `audit_cluster_identity` | `lsms_library/sampling.py:931` | Existing boundary reporting; leave unchanged; `TestTheAudit` / `TestTheWarning` exist (text inspected, UNRUN) |
| Reviewed measurement definition / checker | frozen `reviewed-sampling.py.txt:208`, `:718` | Read-only source supplied for reviewed commit `f1cf7d06f30367cd1e34a4b3db4012f951b2e497`; never imported/executed |
| Frozen port corrections | `APPROVED-SCHEMA.md` and Tanzania `COUNTRY-INPUT.json`, three rows | Authoritative scope and literal counts; override obsolete reviewed commentary, not current config |
| Round-3 renumbering | `Tanzania/_/CONTENTS.org:2098`–2139 | Dated 2026-10-19 raw tracking-line vs API-household distinction |
| Geocode layouts / current split-off qualification | `CONTENTS.org:2182` scheme table; `:2478` onward GH #1441 | Preserve current 2019-20 300-pair / 130-branching-ID note |
| Older cluster-key ledger | `.coder/ledger/323-tanzania-key.md` §§3–4, 6, 9 | Historical context only; newer CONTENTS qualifications remain binding |

Fully read `CONTENTS.org` (all 3,156 lines, including LOGBOOK/WAITING/TODO)
before configuration edits. Read actual `AGENTS.md`; `CLAUDE.md` is byte
identical. Also read STANDING, ledger README/template, add-feature and its
sample sub-skill, cross-country-features, multi-round-waves and Tanzania
panel-design guidance. A static skill-description inventory substitutes for
`make api-audit`, explicitly prohibited by this dispatch.

## §3 Definitions and conventions in force

Per STANDING §3 and `CLAUDE.md` “Site C” / “sample() and Cluster Identity”,
`v` is a sampling-cluster identifier. The panel record describes a boundary
against the preceding logical wave, not the physical folder. Four logical
rounds share `2008-15/`; 2014-15 pools Extended and Refresh panels, tracked
separately in 2019-20 and 2020-21. UPD4 back-casts split-off descendants into
earlier rounds (`CONTENTS.org:65` onward); UPHI lines are not interviewed
households. WAITING #114 remains live (`:1167` onward).

The frozen measurement definition (`reviewed-sampling.py.txt:208` onward)
defines N as linked households with `v` on BOTH sides, D as their all-causes
`v` changes, and K as strict one-to-one earlier-ID renumberings, not physical
PSUs. Unknown counts are omitted; `not_applicable` means the quantity does
not describe the boundary. Counts need independent provenance. No supplied
record has a settled net-of-movers count or mover rule.

Recorded contradiction, not a license to revise authority: CONTENTS:2120
says the round-3 changes are “not movers”; it quotes the old DESIGN §2
interpretation, “panel-stable; 20% are /movers/”, then states: “A mover does
not take its whole cluster with it; a bijection of whole clusters is a
renumbering.” The port preserves this distinction and records no net count.
No new unrecorded country quirk has been established by this source-only read.

## §4 Invariants and assumptions

Inherit STANDING §4; no data IO is needed or authorized in this task.
Task-specific hard limits:

- Strip ONLY the three inserted measurement blocks and recover the exact
  base `sampling.yml` bytes; all other tracked mode/type/blob identities
  equal base except this ledger.
- Preserve all notes, locators, source files, design/EA/oversample fields;
  current 2019-20 prose says 300 pairs / 130 branching IDs, with its own
  historical 310 / 131 qualification. Never restore the older file.
- Insert only literal nonnegative plain-decimal counts. Known K <= D <= N;
  no denominator based on households missing `v`; no design PSU counts.
- Historical provenance stays historical. Frozen JSON's text clues and raw
  file references do not authenticate a new measurement or fresh linkage.
- No net-of-movers count and no mover_rule. K is not applicable in 2019-20;
  do not substitute an incidental one-to-one oracle count for that verdict.
- No shared schema/tests/source-code/CONTENTS/AGENTS/other-country changes,
  package import, Country/Feature/getter/build, parser/header/DTA/capture,
  pytest/collection, Make, DVC, network/acquisition/provider/cache/weights,
  science/default changes, new agents, GitHub write or source-repo push.

## §5 Reuse decisions

| Boundary | Decision | Literal historical record / qualification |
|---|---|---|
| 2010-11 -> 2012-13 | extend metadata, reuse counts | N=2904, D=591, K=82; derived-from-data, high measurement confidence; 2026-10-19 API count in committed note and CONTENTS; raw 1565/7082/83 are a different layer |
| 2012-13 -> 2014-15 | extend metadata, reuse counts | N=521, D=108, K=35; derived-from-data, medium; N/D dated 2026-10-19, strict-ID oracle K dated 2026-10-23 from reviewed input; refresh linkage unverified |
| 2014-15 -> 2019-20 | extend metadata, reuse counts | N=849, D=849; K not_applicable; derived-from-data, high; historical 2026-10-19 count; layout change plus many-to-many movement; preserve current 300/130 note |
| Net-of-movers counts | reuse unknown state | Omit counts and mover_rule at all three boundaries |
| Runtime/schema implementation | reuse coordinator-owned port | No worker implementation or execution; integration acceptance pending |

All three locators will name own-country committed notes and original dated
measurement provenance. The 2014-15 locator will additionally retain the exact
historical strict_k oracle path from reviewed COUNTRY-INPUT. That oracle is
not independently opened, rerun or claimed current. No new estimator or
policy is introduced.

## §6 Open questions and remaining gates

No blocking ambiguity in the supplied contract. Blocking questions, if any,
go only to the absolute shared QUESTIONS_FROM_AGENT channel; no human prompt
is needed for already-authorized work. The absolute MESSAGES_TO_AGENT channel
was checked at initialization and after country/contract reads.

Remaining: ledger-only first commit; config-only commit; static source-byte
and Git-object proof; separate final-ledger commit; publication to private
results.git; final five canonical evidence files. No runtime or numerical
acceptance, full GH #1135 closure or coordinator acceptance is claimed.

### Phase 3 — verification

UNFINISHED. Static anchored verification will replace this paragraph after
the config port. Runtime/parser/tests and scientific verification remain
UNRUN by explicit dispatch instruction.

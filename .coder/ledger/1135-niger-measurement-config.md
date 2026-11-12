# Prior-Art Ledger -- GH #1135 Niger measurement configuration

**Status:** source-only configuration port statically verified; coordinator
integration/runtime/scientific acceptance remains outside this task.
**Source:** `79f5979cf8022e64928c5150950dfb00456994da`.
**Search tier:** ripgrep + own Git + standard-library text/AST checks. No
GitNexus, package imports, survey reads, parsers, tests or runtime calls.
Inherits [.coder/ledger/STANDING.md](STANDING.md), especially sections 3-5.

## §1 Task, restated

Add only `panel.cluster_id_measurement` at Niger's existing `2018-19`
boundary, carrying the supplied historical GH #1135 record. Preserve every
existing byte of `lsms_library/countries/Niger/_/sampling.yml` outside the
inserted block. The only other owned tracked path is this new ledger. Initial
ledger, configuration and final ledger commits are separate. This does not
implement the shared schema or claim runtime/numerical acceptance or issue
closure.

## §2 Existing machinery and evidence

| Source | Role | Verification / reuse |
|---|---|---|
| `lsms_library/countries/Niger/_/sampling.yml:58` | Existing independent/redefined ECVMA -> EHCVM boundary and its provenance | Reuse the facts; preserve all original text |
| `lsms_library/countries/Niger/_/CONTENTS.org:1893` | Dated 2026-10-23 GH #1247 boundary evidence, including no cross-programme links and overlapping serials | Personally read all 3,521 lines, including TODO/WAITING declaration and all historical qualifications, before config edit |
| Frozen `COUNTRY-INPUT.json`, sole Niger / 2018-19 row | Reviewed source `f1cf7d06f30367cd1e34a4b3db4012f951b2e497`: `linked_households: 0`, `not_applicable: [clusters_renumbered]` | Reuse literals and historical own-country provenance; current config/document hashes match supplied pins |
| Frozen `APPROVED-SCHEMA.md` | Coordinator-approved source-only port contract, including Nigeria/Tanzania corrections | Binding; do not port other countries or old complete files |
| Frozen `reviewed-sampling.py.txt:208`, `:717`, `:1220` | Count definitions, permitted fields/coherence and warning formatting | Read-only source text/stdlib AST, never imported or executed; implementation belongs to coordinator |
| `CLAUDE.md`, Site C and sample/cluster identity sections | Boundary declarations and the `sample` ownership of `v` | Context only; no sample, weight, ID, default or warning-policy edits |
| `.claude/skills/add-feature/sample/SKILL.md`, parent add-feature and cross-country-features skill | Sampling and assembly conventions | Read; runtime/data/test workflows explicitly outside this dispatch |

Test status: **UNRUN by task scope**, including pytest collection and schema
validation. Base 79f deliberately lacks acceptance of the new block; the frozen
shared implementation is not installed or executed here. The prepared
`project-runtime.json` reports setup complete; no task package import repeats
the bootstrap identity check.

## §3 Definitions and conventions in force

- Per `STANDING.md` section 3 and `CLAUDE.md`, `v` is the sampling-cluster
  identifier; a boundary concerns the wave and its predecessor.
- Frozen reviewed source lines 208-250 defines three count states: present
  integer, absent/unknown, and explicitly `not_applicable`. Unknown is never
  inferred as zero. `linked_households` is the linked-with-v denominator;
  `clusters_renumbered` counts clean one-to-one old-ID renumberings, not EAs
  coincidentally sharing an integer.
- Frozen approved contract: plain-decimal nonnegative integers; independent
  nonempty `source_type`, `confidence`, `locator`; at least one count;
  coherence where known. No settled net-of-movers count is supplied, so omit
  that count and `mover_rule`.
- Niger's own `CONTENTS.org:1895` describes "two survey programmes, each a
  two-wave panel". Its `:1922` says "No crosswalk exists" and `:1924` says
  "Zero shared =i= is by construction". The historical zero rests on the
  recorded lack of cross-programme linkage, not the EHCVM `E_` namespace.

## §4 Invariants, qualifications and contradictions

- Inherit `STANDING.md` section 4 and `CLAUDE.md` invariants; the explicit
  source-only dispatch further prohibits all survey/runtime/test/network/DVC/
  Make/cache/acquisition operations. Use only ordinary source text, stdlib AST
  and own Git proofs. No subagents, shared schema/test/code/document edits.
- Source config SHA256:
  `aeec2b8b9ff54f173bbf4c8a356761942f0b843cd169b8a769059cf2dda470b4`.
  Entire country document SHA256:
  `d50a9232013d4dc0187408fc64acb5fbf6232ca8d09ed050d97156337a4fd644`.
- Relevant idiosyncrasy: both programmes use cluster serials; 267 integers
  recur across unrelated EAs. The existing `independent` / `redefined`
  declaration is retained. A renumbered-cluster count does not describe that
  boundary; changed-household and net-of-movers counts remain unknown.
- Reviewed/current wording difference: the old supplied boundary says the
  `redefined` gloss "was widened to cover" the "same unit, independent draw,
  overlapping serials" pattern; current 79f says it is "the closest
  warning-bearing value in the frozen vocabulary" and its gloss "fits
  loosely". Report this difference; preserve current text, do not choose or
  correct vocabulary policy. No contradiction blocks the assigned literals.
- The country file itself has a separate seasonal-wording tension: Sampling
  Design says "each grappe is visited in exactly one =vague=", while the
  2018-19 within-wave paragraph says both seasonal visits cover "half of every
  EA". Neither is presumed stale or adjudicated by this measurement port.
- No genuine unrecorded country quirk was discovered. `CONTENTS.org` is not
  owned and is not edited. All historical facts retain the 2026-10-23 date and
  development `9a0ed5f3c` source qualification; no current-data claim.

## §5 Reuse decision

| Item | Decision | Reason |
|---|---|---|
| N = 0 and K not applicable | Reuse | Exact assigned historical literals; no recomputation |
| D, net count, mover rule | Reuse omission | Unknown in the supplied record; no inference from N |
| Measurement note/provenance | Extend locally | Add explicit historical/date/source-only qualification inside the new block |
| Existing panel/design/EA/oversample/source metadata | Reuse verbatim | Subtracting only the new block must recover exact original bytes |
| Shared schema, warning format, tests | Reuse coordinator ownership | Read frozen reference only; no implementation in this worker |

## §6 Open questions for the coordinator

None blocks this bounded port. Shared schema integration and runtime acceptance
remain the coordinator's gates. The no-network/source-only scope supersedes
generic skill instructions to inspect data or run tests; there is no pending
human approval step.

### Phase 3 -- verification

- **OK (sections 2-5):** config commit
  `b0effb9aba6787dad0d96a64b36241eaaca519ca` adds exactly one 18-line
  `2018-19.panel.cluster_id_measurement` block. The only numeric field is the
  supplied plain-decimal `linked_households: 0`; `clusters_renumbered` is in
  `not_applicable`. D, net count and mover rule are omitted, without inferring
  them from the zero denominator.
- **OK (sections 2-4):** the record carries its own `derived-from-data` /
  `high` provenance, exact historical date/development/reviewed-source pins,
  own country locators and explicit no-remeasurement qualification. No fresh
  current-population claim is made. The current-versus-reviewed gloss
  difference and the separate seasonal wording tension remain reported.
- **OK (section 4):** removing only the new block recovers all 7,653 base
  configuration bytes and SHA256
  `aeec2b8b9ff54f173bbf4c8a356761942f0b843cd169b8a769059cf2dda470b4`.
  New config SHA256:
  `d1a9ebaa751e693dae0ebe68005bb829deb45b4e49eb1ce39d28a31084efcbe9`.
- **OK (sections 1, 4):** 19,247 nonowned leaf Git entries compare exact to
  base in path/mode/type/object. Only this ledger and Niger sampling.yml
  differ. Config mode/type remain `100644 blob`; `git diff --check` passes.
  Initial commit `0774bdaa8cfc721478ce3560c43e65a1fd6842cd` is ledger only;
  the next commit is config only. This verification update is ledger only.
- **OK (section 5):** no new runtime symbol or computation; the literal port
  reuses the assigned historical record. No reinvention or blocking authority
  contradiction was found.
- **UNRUN (sections 2, 4, 6):** all package imports, YAML/library parser calls,
  pytest/collection, actual Country/Feature/getter/build/census/data/header/
  capture/API checks, DVC/Make/network/acquisition/provider/cache/rewarm,
  weights and numerical/scientific acceptance. These are prohibited here,
  not failed or implicitly passed. Base79f's deliberate schema mismatch is
  left for the coordinator; whole GH #1135 is not closed.

Durable review evidence is the dispatch evidence directory's `report.md`,
`RESULT.json`, `verification-plan.json`, `ledger-checkpoint.md` and
`source-byte-proof.json`; the final report records final HEAD, repeat tree
proof and private publication after this commit. No evidence is stored in
nonowned tracked paths.

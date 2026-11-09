# Prior-Art Ledger — GH #1135 Nigeria measurement config

**Status:** source-only config port statically verified; coordinator acceptance pending.
**Base:** `79f5979cf8022e64928c5150950dfb00456994da`.
**Search tier:** source text, ripgrep and own Git; prepared Python stdlib only.
Inherits [STANDING.md](STANDING.md). No package imports or runtime checks.

## §1 Task, restated

Add only `panel.cluster_id_measurement` to Nigeria's existing `2018Q3` and
`2023Q3` sampling boundaries. Lift the already committed 2026-11-04 readings,
with their own dated provenance, into the frozen GH #1135 schema. This is a
source-only metadata port, not a new measurement or full issue closure. Only
this ledger and `lsms_library/countries/Nigeria/_/sampling.yml` are owned.

## §2 Existing machinery

| Source / symbol | Locator at base | Role / existing checks | Decision |
|---|---|---|---|
| Nigeria boundary notes | `sampling.yml:100-117,131-147` | GH #1448 notes give N1454/D54/K1 and N4736/D267, dated 2026-11-04 | reuse facts; extend with optional blocks only |
| Crosswalk population qualification | `CONTENTS.org:798-931`, especially 911-921 | GH #1264 links interviewed households using own-round covers; GH #1448 distinguishes links from cover overlaps | reuse definition |
| Composite cluster identity | `CONTENTS.org:985-1061` | `v=state/lga/ea`; `ea=0` singletons; whole-EA LGA changes | reuse, no identity changes |
| Existing schema reader | `lsms_library/sampling.py:201,584,653` | `_PANEL_FIELDS`, `_check_panel`, `sampling_records`; `tests/test_sampling.py` is the existing test surface | unchanged; base does not accept new field yet |
| Existing Nigeria checks | `tests/test_gh1448_nigeria_sampling_notes.py:1-115` | text says notes count the committed crosswalk; includes source-dependent sample comparison | read only; not collected or run |
| Reviewed schema text | `$ISA_CONTEXT_ROOT/reviewed-sampling.py.txt:219-274,717-845` | optional counts, independent provenance, coherence and missing-state contract | read only, never imported/executed |
| Frozen port authority | `$ISA_CONTEXT_ROOT/APPROVED-SCHEMA.md` and `COUNTRY-INPUT.json`, both assigned rows | Nigeria-specific correction overrides obsolete old literals | reuse exactly |

The full 4,142-line `CONTENTS.org` was personally read, including TODO/WAITING
material, before any config edit. Root `AGENTS.md` and byte-identical
`CLAUDE.md`, repo STANDING/README/template, shared prior-art-ledger skill, and
add-feature/sample and cross-country guidance were read. The general
add-feature skill was also read as sample's prerequisite. Runtime procedures
and Make audits in those general instructions are superseded for this task
by the explicit source-only authorization. No agents are dispatched.

## §3 Definitions & conventions in force

Per STANDING §3 and CLAUDE's Site C / sample sections, household continuity
and cluster-id continuity are separate. Per the frozen schema, N counts linked
households with `v` on both sides; D counts all causes of changed `v`, including
movers. K counts clean one-to-one earlier cluster-id renumberings among linked
households, excluding synthetic ids. K is not the design's EA count.

Unknown means omission. `not_applicable` means the quantity does not describe
the boundary. Neither is zero. A net-of-movers count requires a settled
`mover_rule`; neither Nigerian boundary has one in the approved contract.
Counts are nonnegative plain-decimal integers with K <= D <= N where known.
Every new block carries its own `source_type`, `confidence`, and `locator`.

Relevant idiosyncrasy: whole-EA LGA recoding changes the composite `v` even
without a household move; PP and PH use distinct own-round covers. The notes
explicitly warn (`CONTENTS.org:746-748`): "=ea=0= is a source sentinel, not a
complete census of dwelling moves; a nonzero EA can accompany a movement
answer too." No unrecorded country quirk was discovered; CONTENTS is not owned.

## §4 Invariants & assumptions

Inherit STANDING §4; this task performs no data IO or weight changes.

- Preserve all original sampling bytes, including notes, design/oversample/EA
  metadata and existing locators. Removing only the two insertions must
  reproduce the exact base bytes. All other tracked mode/type/blob identities
  must equal base, except this new ledger.
- Use 2018Q3 N1454/D54/K1 and 2023Q3 N4736/D267; omit 2023Q3 K, both net
  counts and mover rules. Do not infer K from 12 cover EAs or 10 linked EAs.
- Record historical provenance; no fresh-data or current-accuracy claim.
- Source-only checks are text/stdlib/own Git. No YAML/schema execution,
  package import, pytest/collection, data/header read, Country/Feature, Make,
  DVC, network, acquisition, cache mutation, numerical policy or default work.
- Shared schema and tests belong to the coordinator. The deliberate interim
  mismatch with the base reader must remain explicit and untested here.

Contradictions are findings, not permission to rewrite documentation:

- Frozen old input says `linked_households: 1493`, `households_changed: 79`
  (2018Q3) and `5067`, `343`, `clusters_renumbered: 11` (2023Q3). Base notes
  instead say "1,400 of 1,454 linked households" / "of the 54 that change"
  and "4,469 of 4,736 linked households" / "of the 267 that change".
  CONTENTS §GH #1448 explains: counts "used to be hhid overlaps of covers";
  "The 12 EAs (112 households) are a fact of the two covers; two of them
  hold no linked household." The frozen correction resolves N/D, and leaves
  2023Q3 K unknown. Old marker bounds are not transported to this population.
- Sample skill's generic Nigeria row says "`ea` | Consistent across waves";
  the country's own rule is "=ea= ALONE IS NOT THE CLUSTER ID" and requires
  composite state/LGA/EA. No wiring or skill correction is authorized here.

## §5 Reuse decision

| Quantity / behavior | Decision | Reason |
|---|---|---|
| 2018Q3 N/D/K | reuse | literal dated base notes and binding frozen correction |
| 2023Q3 N/D | reuse | literal dated base notes; old overlaps superseded |
| 2023Q3 K | omit, unknown | cover/linked EA counts do not establish schema K |
| net-of-movers, both boundaries | omit, unknown | no settled rule/count for this population |
| schema, API, warning formatter, tests | unchanged | coordinator-owned; no new implementation |

## §6 Open questions for the coordinator

No blocking question for this bounded port. Shared schema integration and
runtime acceptance remain coordinator gates, not outcomes claimed here.

### Phase 3 — verification

Source proof at config commit `faaa6442132f2ed096dc18096ca9422f4136262c`
passed using the prepared interpreter with stdlib text/AST/Git only. The
reviewable verifier is `verify-source.py` under the evidence directory; it
does not import or execute the frozen schema or any project module.

- **OK (anchored on §2/§3/§5):** exactly two blocks, at 2018Q3 and 2023Q3,
  with N1454/D54/K1 and N4736/D267, respectively. Literal count spellings,
  field names, independent provenance and dated qualifiers agree with the
  frozen contract. Known counts satisfy coherence; no computation from data.
- **OK (anchored on §3/§4):** both net counts and mover rules omitted;
  2023Q3 K omitted as unknown, never zero or `not_applicable`. The old
  1493/79, 5067/343/K11 records and conditional bounds are not restored.
- **OK (anchored on §4):** removing only the 2,316 inserted bytes (32 lines)
  reproduces all 7,885 original bytes, SHA256
  `ba9ec16d22cf23db9037f4c1227b55164716d70825a998ab38842d182a48f26f`.
  The 10,201-byte candidate is SHA256
  `a61e2d0c69b7e158f1c1f40b9527390c56fd90cbe2fb64383516dda5e2073a37`.
- **OK (anchored on §4):** all 19,247 nonowned tracked entries retain exact
  base mode/type/object identity. Config mode/type is unchanged; ledger is
  new. `git diff --check` passes; tracked worktree is clean at config head.
- **OK (anchored on §4/§5):** no new algorithm or shared behavior: no
  reinvention. The existing base reader's field set lacks this block; the
  reviewed text admits it. This deliberate interim integration limit is
  reported, not repaired or runtime-tested by this worker.

Initial ledger-only commit is `26df7fe0dca6b72a9b011a5a0dbedcc83100b2ca`;
config-only commit is `faaa6442132f2ed096dc18096ca9422f4136262c`. This final
ledger update is separate. The verifier repeats the byte/object/commit checks
after this commit and records the exact final head in `source-byte-proof.json`.

Canonical durable evidence: `report.md`, `RESULT.json`,
`verification-plan.json`, `ledger-checkpoint.md`, `source-byte-proof.json` in
`SUCODER_DISPATCH_EVIDENCE_DIR`. All five existed as UNFINISHED before the
long country read. Checkpoints preserved pending publication honestly.

**UNRUN by explicit task scope:** package imports, YAML/schema validation,
pytest/collection, actual Country/Feature/getter/API, data/header/DTA,
Make/api-audit, DVC/network/acquisition/provider/cache/rewarm and numerical
or weighting checks. Skills were enumerated by source frontmatter only.
No runtime, scientific or full GH #1135 closure is claimed.

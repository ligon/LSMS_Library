# Ethiopia next queue: historical derived cells and ESPS-5 documentation

**Status:** source-only queue preparation completed; independent ROOT review
and any later runtime/acquisition are separate. Base
`4381d701ffb06ab447a37c66362119d1c1e23009`.
**Search tier:** git + ripgrep + primary read-only GitHub tools. No GitNexus
tool is exposed; use AGENTS.md's named source/diff substitute. No subagents.

## §1 Task, restated

Reconcile Ethiopia's two historical `household_characteristics` cells in
#587, then the 2021-22 documentation claim in #650. The full 145-issue
snapshot supersedes the earlier capped worklist. This is queue preparation:
reuse accepted work, explain current source and name finite residuals, without
new Country/Feature/native/PDF operations or scientific interpretations.

## §2 Existing machinery

| Machinery | Source | Existing verification (read, not run here) | Decision |
|---|---|---|---|
| Derived roster dispatch | `lsms_library/country.py`, `_ROSTER_DERIVED` and roster branch of `__getattr__` | `tests/test_roster_residence_filter.py` | reuse |
| Residence coalesce | `lsms_library/transformations.py:644`, `roster_to_characteristics` | `TestD1PerRowCoalesce`, same test file; #594 named by #587 maintainer comment | reuse |
| Wave roster definitions | `Ethiopia/{wave}/_/data_info.yml` | committed YAML, source inspection only in this task | reuse |
| Documentation identity | `Ethiopia/2021-22/Documentation/SOURCE.org` and BID `.dvc` | later country notes record #1443 restoration and citation rechecks | reuse |
| Historical qualified compiler receipts | `.coder/ledger/1287-ethiopia-*-runtime-checkpoint.md` | dated, qualified receipt prose in country CONTENTS | reuse; no replay |

## §3 Definitions and grounding

Inherit `.coder/ledger/STANDING.md` §§3-4, `CLAUDE.md` (symlink to AGENTS.md)
and `lsms_library/data_info.yml`; no global edits authorized. Derived tables
are surfaced by their source and must not be registered in country schemes.
Personally read all 4,700 lines of Ethiopia `_/CONTENTS.org`, including all
TODO/WAITING/LOGBOOK material present, in bounded chunks after the first large
output truncated. Whole-file SHA256:
`876ef317914876868c4533f55b7a7d6c764c9538b173ced9dddbfdb5688b6f3f`.
Own top-level is `/local/job41079836/mirrors/dispatch_adb2417ad2864c8f92b80b2442b24413`;
initial HEAD matches the base exactly.

Relevant idiosyncrasies: Ethiopia's PP/PH features generally reuse a wave label
when reading one round; its household roster is the HH module, not two copies
of an agriculture roster. MonthsAway (W1-W3) and WeeksAway (W4-W5) coexist
after country concatenation. Agricultural holder, household, parcel and field
keys are distinct; multiple holders reuse parcel numbers. Preserve the exact
recorded tension: PH form p6 asks about the "last completed agricultural
season", while manual p81 asks for "expected Meher harvest if harvesting is
incomplete". Neither licenses a new default. Older no-documentation claims
must be quoted beside later holding evidence, never silently overwritten.

## §4 Invariants and authorization

The latest dispatch limits ownership to Ethiopia, uniquely named
`tests/test_next_queue_ethiopia_*.py`, and this ledger. Keep the full old
country-note prefix. No data/runtime mount/import, collection/tests, raw/header
or new PDF parsing, providers/acquisition, Make producer, cache change,
corpus/audit sweep or public/source Git write. Static source metadata audit
is the narrowly allowed skill-discovery operation. Five private artifacts
were initialized UNFINISHED, mode 0600, regular with nlink1. The absolute
steering channels are authoritative; local lookalikes remain untouched.

## §5 Reuse decisions

Reuse #594's existing residence machinery and its controls; do not invent an
Ethiopia-only workaround for a historical shared defect. Reuse committed BID
identity/restoration notes before proposing document acquisition. Extend only
country documentation if a supported finding is genuinely missing. Preserve
#1570/#833/#1457/#1485 holds and completed #851/#1570/compiler evidence.

## §6 Disposition and finite continuation

- #587: exact original CSV rows at `23794a51829751202ca61a5bdcb0009bb0b26c76`
  name `2018-19` and `2021-22`, both dropped/declared. Full fresh issue and
  its one comment identify merged #594
  (`68b9425287de5bb45e6f982d4ccab0f1680e6bef`). Both roster declarations are
  HH `sect1` sources with native `household_id/individual_id`, `s1q06` weeks
  and logical-wave `t`. W1-W3 contribute `hh_s1q05` months. The old column
  if/elif selected an all-missing month column on W4/W5. The current 82-line
  residence-resolution block is byte-identical to fix `85bdf4a57`, SHA256
  `f14201d1ff17f421fc39bee8d9787389272bfc01fbb9b595a0b171bf06d6670d`.
  #594 records 6,768 / 4,946 restored households and continued filtering,
  not fallback; these are historical receipts, not a new run or eligibility
  oracle. Current committed coverage rows carry those same historical counts.
  The nine #1287 compilers do not build this YAML roster or runtime derivation.
  No current country defect remains demonstrated for these two cells.
- #650: restored BID sidecar is MD5 `4949ee8c911e8512458f64c5f57d5d3e`,
  1,058,117 bytes, catalog 6161/resource 78630. Restoration `11ed1b811`
  is merged through #1506 (`df8faa75f`); full fresh closed #1443 and both
  comments confirm holding and citation rechecks. Current W5 tracked tree
  has only that document sidecar. SOURCE says held data release v03_M;
  the old v01 licence citation example does not supersede it. W5 household/
  agriculture questionnaire, DDI, manual and report identities are not
  established by these admitted sources; none has a tracked W5 sidecar.
  The restoration commit explicitly excludes questionnaires (.xls/.xlsx),
  manual and report. W4 questionnaires cannot substitute for W5.
- Next bounded unit proposed once in the absolute channel: ROOT admission
  for one catalog-6161 Related Materials metadata inventory, confined to W5
  household and agriculture PP/crop-cut/livestock and PH instrument roles;
  capture exact IDs/names/formats/language/version, reject W4 and ambiguous
  matches, and stop before downloads or parsing. Then propose exact named
  restoration/read operands separately. No acquisition/parse is authorized
  or needed to finish this preparation. Shared #650 automation stays ROOT's.
- Fresh full issue/all-comment reads: #1570 (0), #833 (2), #1457 (1),
  #1485 (0); #587 (1), #650 (0). All six open issues have no assignee.
  Two read-only open-PR searches (`Ethiopia`, `650`) return no results;
  that bounded search is not universal ownership proof. No held scientific
  choice, already-tested alternative or anonymous default is reopened.

## Phase 3 — verification

- **OK (§2/§3):** no duplicated implementation; historical #594 machinery and
  controls reused as source evidence. No test execution claimed. Historical
  PR's unrelated single full-suite failure remains historical failure.
- **OK (§3/§4):** append-only country documentation commit `7bb0eded3` adds
  64 lines; all 298,522 original bytes / 4,700 lines are an exact prefix.
  No executable/schema/map/weight/eligibility or other-country edits.
- **OK (§4):** inspected `make api-audit` completed, exit 0, using a private
  wrapper with `/usr/bin/python3.12 -I -B -S`: stdlib AST/text/Git metadata
  only, 15 skills listed, no undocumented kwargs or untracked build inputs.
  This is not LSMS runtime, tests, collection or a Feature/corpus/data audit.
  All 15 full frontmatter descriptions are retained in private evidence.
- **OK (§4/§6):** no runtime mount/import, test, native/header/row/PDF/workbook
  parse, provider/acquisition/cache/producer operation, source/public push,
  nested agent or GitHub mutation. Read-only GitHub bodies/comments and
  existing ordinary reports are the only external-source research.
- One attempted evidence receipt save exceeded argv size and failed before
  process creation; the successful replacement saves public metadata/body/
  comments and omits the oversized diff, reviewed instead in local Git.
  No science stage failed or retried. Initial oversized read outputs were
  replaced by bounded complete country/AGENTS reads.

Final private artifacts record the source conservation/commit/ref proof and
exact residual drafts. Independent review/integration/CI, later metadata or
document restoration and any runtime verification remain with ROOT.

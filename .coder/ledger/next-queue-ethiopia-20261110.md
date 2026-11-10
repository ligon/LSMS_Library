# Ethiopia next queue: historical derived cells and ESPS-5 documentation

**Status:** grounded, source-only diagnosis in progress. Base
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

## §6 Open questions and next checks

Fresh #587 comment explicitly names Ethiopia 2018-19/2021-22 and says #594
fixed deletion; reconcile the merge and exact source inputs before deciding
whether any residual exists. #650 remains open with zero comments and no
assignee: distinguish restored BID from genuinely absent W5 questionnaires.
Any new finite document/native read needs separate ROOT admission. No human
approval prompt is attached; record precise residuals once in the absolute
QUESTIONS_FROM_AGENT channel and continue independent work.

## Phase 3 — verification

Pending final source reconciliation. No executed tests, runtime or data
acceptance is claimed. Ledger surfaced before implementation; the dispatch
already authorizes bounded source work, so the skill's review step does not
create an additional permission wait.

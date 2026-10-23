# Prior-Art Ledger - post-v0.16.0 LSMS-ISA backlog

Search tier: ripgrep + git and live GitHub through `gh`; no memory or
GitNexus MCP is available in this session. Inherits `STANDING.md`.

## §1 Task, restated

Resolve existing bugs affecting Burkina_Faso, Ethiopia, Malawi, Mali,
Niger, Nigeria, Tanzania and Uganda, starting at development
`f8d236694c26fe9e709c5548f02c70f74ea72c33` after published v0.16.0.
Inventory the whole open backlog, identify actual ISA residuals, reconcile
merged fixes, and produce independently reviewed PRs for human merge.
EthiopiaRHS and Tanzania_Kegera are distinct surveys outside this scope.

## §2 Existing machinery

| Machinery | Source | Decision and evidence |
|-----------|--------|-----------------------|
| Backlog lifecycle | `.claude/skills/backlog-workflow/SKILL.md:48` | Reuse inventory, reconciliation, diagnosis, isolated fix, audit, independent review and PR phases. |
| Existing orchestration sketch | `bench/feature_audit/backlog.workflow.js:1` | Reuse phase/schema concepts; adapt to available collaboration tools, not a second workflow implementation. |
| Deterministic regression net | `bench/feature_audit/scan.py:366` | Reuse country sanity, assembly conservation and kwarg checks; covered by `tests/test_feature_audit*.py`. |
| Canonical schema and read/build machinery | `STANDING.md` sections 2-4; `lsms_library/data_info.yml` | Reuse existing Country/Wave/Feature and sanctioned readers/writers; do not invent parallel harmonization. |
| Per-country evidence | `lsms_library/countries/{C}/_/CONTENTS.org` | Read first, including TODO/WAITING/LOGBOOK, then source variables and issue comments. |
| Skill discovery | `scripts/api_surface_audit.py:128` | Reuse `make api-audit`; all 15 hand-authored skills are discoverable. |

## §3 Definitions and conventions in force

Follow `STANDING.md` section 3 and `AGENTS.md`.
The backlog skill defines A as assembly/index bugs, B as source/wiring or
silent-loss bugs needing diagnosis, C as enhancements/design, and D as
acquisition/rebuild work requiring a human disposition.
An incidental country mention is not evidence that an issue is in scope.
A partial resolution requires a drafted exact residual, not just a comment.

## §4 Invariants and assumptions

Follow `STANDING.md` section 4: sanctioned I/O, read-only country config at
runtime, cache content provenance, preserved index grain and attrs.
Do not erase Tanzania UPHI back-casting, EHCVM grappe identity, or recorded
country-specific survey distinctions to make an audit green.
Do not infer a missing module from absent config: questionnaire evidence is
required before a closing negative verdict.
Each worker edits only its own worktree, pinned to the stated base.
Verification asserts exact package/config identity and uses private writable
L2 caches, pinned numerical threads and bounded loadfile pytest workers.
No raw microdata or credentials belong in tracked reports.
Do not change dependency pins, baselines, locks, or weaken existing tests.
Freeze a fix tree before independent verification; reviewers must not edit it.
The scrum master controls GitHub writes; no PR merges are authorized here.

## §5 Reuse decision

Reuse the existing backlog and Feature audit machinery.
Extend country configs/scripts only after a concrete diagnosis and a
per-fix ledger identifies existing helpers and relevant tests.
No new estimator, unit assumption, aggregation policy or survey redesign is
part of this campaign without an explicit maintainer decision.

## §6 Open questions for the human

None blocks inventory, reconciliation or documented bug fixes.
Scientific choices, capability negatives and data-acquisition cases will
receive precise residuals and an evidence-backed disposition.

## Phase 3 verification

Pending implementation and independent reviews; inventory is not a claim
that any bug is fixed.

Sue

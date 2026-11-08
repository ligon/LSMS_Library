# Prior-Art Ledger — 1423 backlog runbook revision

**Search tier used:** ripgrep + Git floor. No index refresh or network calls.

## §1 Task, restated

Revise the eleven backlog-workflow files from PR1423 on the exact supplied
ordinary merge `e9f577292770c4934cf2687293dbd3a289ee0993`. Treat the seven
helpers as operational code. Author hermetic contract tests, but execute only
stdlib AST/JSON parsing and `bash -n` here. Root owns functional verification,
integration, GitHub actions and acceptance. No country task or data access.

## §2 Existing machinery

| machinery | source anchor at starting tree | coverage found | decision |
|---|---|---|---|
| backlog reconciliation / partial residual | `.claude/skills/backlog-workflow/SKILL.md`, Phases / Non-negotiables | prose; no helper tests found by call-site search | retain, qualify execution by task grant |
| sweep admission / review loop | `.claude/skills/backlog-workflow/savio-sweep.md` §§2–8 | independent static review supplied | extend with immutable inputs and evidence |
| `burst.sh`, `submit.sh` | `sweep/`, source setup and Slurm isolation | no helper tests found | replace shared-cache defaults, bind full SHA |
| `prep_branch.sh`, `open_pr.sh` | `sweep/`, coordinator Git/PR lane | no helper tests found | extend with expected SHA checks and fail-closed publication |
| `merge_on_green.sh` | `sweep/`, merge gate | review finding 1 | explicit reviewed manifest; coordinator-required if thread proof unavailable |
| `make_briefs.py`, templates | `sweep/`, body/comment clipping and source policy | review finding 5 | complete paginated snapshot, freshness evidence, task-specific grants |
| `save_report.py` | `sweep/`, transcript extraction | no helper tests found | replace with explicit supported artifact copy |

Call-site search found the seven helpers referenced only in the owned runbook,
templates and their own help text under `.claude`, `tests`, `.coder/ledger`.
Risk is HIGH for merge/publication, source/runtime isolation and Slurm exit
handling; there is no library API edit. No survey modules were read.

## §3 Definitions & conventions in force

- Task access limits and required fixes: dispatch assignment; these override
  source-build examples, source acquisition claims and evidence suppression.
- Source identity and owned paths: supplied `context/SOURCE-PROOF.json`;
  actual development `fdec032efd4b98ed4b99883c9586f5ef07b7bf3e`, PR author
  `580c1fe4f8d28a5807ad9c2e15bd1acd39b4ed4d`, zero owned/incoming overlap.
- Existing observations are historical: `context/review-1423.md` (full read).
  Owned-child FD refusal cause remains UNLOCALIZED; upstream PR81/641d8 is
  bounded terminal-proof correction, not a diagnosis of the old failure.
- Repository conventions: `STANDING.md` §§3–4 and `CLAUDE.md` (symlink to
  `AGENTS.md`), especially no DVC CLI, allocation-aware concurrency,
  LSMS_BUILD_WORKERS=1 / BLAS pins, complete CONTENTS/log/TODO and contradiction
  rules when country work is actually assigned. Canonical schema in
  `lsms_library/data_info.yml` is cited, not accessed for this operational task.
- Shared `working-with-github/SKILL.md`: no closing-keyword token next to
  an issue reference in commit messages; use actual author/adapter identity.
- Supported evidence / steering interface: absolute dispatch-supplied paths,
  final text for in-process agents; no native transcript/state reads.

## §4 Invariants & assumptions

- Full approved head AND base SHAs; base branch development. Missing, unknown,
  duplicated or unsupported checks/reviews/thread evidence cannot authorize a merge.
- New clones must detach at the approved source and assert exact HEAD equality.
- Private empty config/data/L1/state/cache/tmp/MPL before any LSMS import;
  no inherited credentials/acquisition overrides or writable shared caches.
  This wrapper is not universal I/O confinement. Source access needs its own plan.
- HOME and CODEX_HOME unchanged. Explicit coordinator exclusion and allocation
  cpus-per-task. Free-account default; billed use needs separate user authority.
- Complete issue bodies/comments and update/pagination evidence; reread current
  complete rulings before action, including linked authority not in the snapshot.
- Freeze bytes until worker AND verifier terminate; atomic rename is insufficient.
- Allowed edits: eleven owned paths, this ledger, narrowly scoped helper tests.
  Never execute changed helpers, tests, providers or data calls in this author phase.

## §5 Reuse decision

| behavior | decision | reason |
|---|---|---|
| reconcile/fix/test/review/PR and residual drafts | reuse | useful existing workflow |
| Slurm env isolation and true exit propagation | extend | keep intent, explicit allowlist and status preservation |
| approved merge/check contract | extend | observed-head/blacklist gate cannot represent reviewed approval |
| report transport | new explicit artifact copy | native JSONL extraction violates supported access boundary |
| hermetic regression controls | new stdlib unittest drafts | no existing helper contract tests found; subprocess mocks only in later verifier |

## §6 Open questions for the coordinator

No authoring blocker. Existing `gh pr view` rollup does not prove complete review
threads. Unless a trustworthy pinned paginated query is established, the merge
helper must return coordinator-required without issuing a merge. Functional
behavior and independent/root acceptance remain unverified here.

### Phase 3 — verification

Pending author-phase anchored static review and frozen test-plan hashes.

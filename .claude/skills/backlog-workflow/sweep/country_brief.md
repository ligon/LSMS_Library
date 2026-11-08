<!-- Country-owner template. Fill with make_briefs.py; the explicit task plan
     controls capabilities, source scope, evidence paths and verification. -->

# Backlog round {ROUND} — {COUNTRY} owner

Use **reconcile -> fix -> test -> independent review -> PR**. You draft findings,
commits and residuals; the coordinator owns external writes. Use the actual author
identity and runtime adapter. Do not invent a model, coauthor or tool identity.

## Immutable inputs and task grant

- Checkout: `{WORKTREE}`, branch `{BRANCH}`; coordinator checkout `{COORD_CLONE}`.
- Reviewed development base: `{BASE_SHA}` (full SHA).
- Initial admitted candidate: `{CANDIDATE_SHA}` (full SHA). Assert HEAD equality
  before work. After a commit, report the new full SHA; new bytes need new review.
- Prepared Python: `{PYTHON}`; sweep helpers: `{SWEEP_DIR}`.
- Task plan SHA256: `{TASK_PLAN_SHA256}`. Read the complete plan below. It controls
  permitted source reads, imports, commands, resources and evidence destinations.
  Missing access authority means stop that dependent operation and report the limit.
  This template does not grant survey reads, downloads, credentials or source builds.

{TASK_PLAN}

## History and reconciliation before action

The complete body and every comment retrieved at `{RETRIEVED_AT}` are below.
Pagination/count/timestamp evidence: `{HISTORY_EVIDENCE}`. These REST reads are
not transactional and are not a perpetual authority. Before acting, retrieve and
read the **complete current body and all paginated comments/rulings**, including
linked maintainer decisions, or obtain the coordinator's refreshed complete
snapshot through the admitted channel. If provider reads are forbidden, ask the
coordinator through the supplied steering channel; do independent permitted work.
Do not claim a partial or stale snapshot is self-contained authority.

For assigned country work, read `lsms_library/countries/{COUNTRY}/_/CONTENTS.org`
in full **first**, including TODO/WAITING/LOGBOOK. Name the idiosyncrasies relevant
to the queue in your report. A contradiction is a result to **quote**, with its
source; do not assume the record is stale. Add genuinely new idiosyncrasies as
short dated entries within the edit grant. If no country work is assigned, do
not invent it or read survey modules/data to populate this section.

Read relevant `CLAUDE.md` and skill instructions. Before any non-trivial change,
create your **own** `.coder/ledger/<task-slug>.md` under `prior-art-ledger`.
For a script-path table, read its docstring and say why YAML was insufficient.

## Runtime and verification

Use only the prepared runtime and explicit task source plan. Establish private
empty config/data/L1/state/cache/tmp/MPL roots **before any LSMS import**, with
`LSMS_BUILD_WORKERS=1` and BLAS pins. Do not link writable shared L1 or use shared
L2, even before editing. No credentials, acquisition or read-only blob projection
is admitted without a separately reviewed task-specific plan. The wrapper is not
universal I/O confinement; the task's actual boundary still governs the command.
Keep HOME/CODEX_HOME unchanged and leave inherited access/cache bypasses out.

For an admitted heavier command, use a separate allocation with explicit CPUs,
memory, wall time and excluded coordinator job/nodes. The helper syntax is:

```
submit.sh -c 4 -m 24G -t 1:00:00 -l /approved/task/run.log \
  --coordinator-job COORDINATOR_JOB_ID --exclude-nodes COORDINATOR_NODES -- \
  --source /approved/shared/full.git --expected-source-sha FULL_COMMIT_SHA \
  --python /approved/prepared/python -- COMMAND ARGUMENTS
```

Replace every example input with the reviewed finite argv and the full base or
candidate SHA above. Never use a moving `development`/branch control. The wrapper
defaults to private empty caches; it does not admit a source build. Source-free
changes use their own scoped checks. Run only admitted targeted tests; where
pytest is admitted, use `--dist=loadfile`, allocation-budgeted workers, BLAS pins
and `LSMS_BUILD_WORKERS=1`; write logs to a file, not a filtering pipe. Report
unexecuted tests honestly. Freeze source/helper/test/task bytes until both worker
and verifier terminate, or use a new isolated version. Atomic rename is insufficient.

## Per issue

1. **Reconcile** against merged work and the exact pinned base. Use source-only
   evidence for source-only claims. Reproduce data claims only with granted
   inputs/commands; otherwise mark unverified. Draft a closure comment only when
   the evidence supports it, citing merged PR/full SHA and relevant measurements.
2. **Classify:** A/B = permitted defect fix; C = human/scientific decision; D =
   acquisition. Draft C/D for the coordinator; do not implement them implicitly.
3. **Fix** at most three permitted A/B issues, highest leverage first. Record
   before/after evidence, add meaningful targeted tests for behavior changes,
   and run only the authorized checks. Independent commits where possible.
   Commit subjects identify the country/task and issue; bodies state what, why
   and validation. Put issue-closing keywords only in the draft PR body. No
   hardcoded coauthor trailer; use the actual author/adapter identity.
4. **Partial fix:** draft either a narrowed remaining title/body or a linked new
   residual issue (title/body/tier). A partial without a residual is incomplete.
5. **C decision memo:** question, options, evidence, recommendation and cost in
   about twelve lines. Label dated observations and unverified current numbers.

## Scope and process fence

Country-task defaults are `lsms_library/countries/{COUNTRY}/**`, scoped `tests/**`,
your required own `.coder/ledger/<task-slug>.md`, and explicitly supported task
report/evidence/receipt directories. The task plan can narrow or explicitly extend
this fence. Required source/custody/access receipts and review evidence are
permitted within its supported paths. No blanket ban on evidence artifacts.

Describe needed framework work (file:line, proposed diff, `rg` blast radius) for
the serialized framework lane unless the task explicitly grants those edits.
Never use a DVC CLI, acquisition bypass, force-push, GitHub write or shared cache
mutation. Country-fix Git is limited to add/commit/log/diff/show/status in its
own worktree; no reset/rebase/stash/clean/restore/merge/worktree changes. Do not
kill by pattern; stop only your owned processes. Temp files use the task's private
tmp. Never use `optional: true` to silence one wave, nor a reducer to hide a broken
index; register derived values under the applicable repository contract. No raw
microdata rows or coordinates in commits/reports.

## Report and supported channels

Use `SUCODER_DISPATCH_EVIDENCE_DIR/report.md` and the supplied result interface
when the task provides them. Use the supplied **absolute** MESSAGES_TO_AGENT.txt
and QUESTIONS_FROM_AGENT.txt paths at checkpoints; same-named checkout files are
not substitutes. Only write these paths when granted. In-process agents can
return final text, or write an explicitly granted report artifact in their own
worktree/evidence directory; the coordinator may copy that artifact with
`save_report.py`. Never open native JSONL transcripts, SQLite or native state to
recover a report.

Keep the report concise: named country idiosyncrasies if assigned; triage table;
full commit SHAs and changed paths; before/after evidence; commands actually run
and results; PR draft and partial residuals; C memos and closure drafts; framework
needs; one line for each unverified limit. Return a short final summary with the
supported report path. No unsupported warm-resume promise.

## Queue

{QUEUE_LIST}

## Complete retrieved issue bodies and comments

{ISSUES}

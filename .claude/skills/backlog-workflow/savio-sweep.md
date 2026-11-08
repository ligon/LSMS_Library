# Savio backlog sweep — reviewed operational runbook

The loop is **reconcile -> fix -> test -> independent review -> PR**. Report
commands, results, full input/commit SHAs, residual work and unverified limits.
Create the required own prior-art ledger and keep source/access/custody receipts
and review evidence at the task's supported paths. Concise evidence must still
prove the claim. Task instructions override these examples and determine access.

Historical context only: field notes from 2026-11-03 describe one coordinator,
eight in-process country owners and a fresh reviewer per fix, with twelve PRs
opened in about four hours and eleven merged on maintainer authority. Those
counts, timings and installed-tool observations are not current runtime facts
or resource estimates. This revision changes operational helpers and templates;
it makes no measured claim about library cache fingerprints or scientific data.

## 1. Roles and admission

| Role | Responsibility |
|---|---|
| Coordinator | pin inputs, reconcile backlog, write task grants, assign isolated checkouts, collect evidence, route reviews and own authorized external actions |
| Country owner | only assigned country work; read full CONTENTS/log/TODO first, reconcile, fix a bounded queue, draft PR and partial residuals |
| Independent reviewer | examine frozen base/candidate and admitted verification, report collateral effects and limits |
| Serialized framework lane | framework changes described by country owners unless explicitly granted to them |
| Separate burst allocation | finite admitted heavier commands, explicit resources and coordinator exclusion |

In-process agents return final text or write explicitly granted artifacts.
Dispatched workers use the evidence/result/steering channels supplied by their
adapter. Neither topology implies a universal ban on evidence outside a worktree.
Use the actual author and runtime adapter identity; never a template's model name.

Before each task, record:

1. Full reviewed development **base SHA** and full **candidate/source SHA**;
   expected branch `development`; source repository and committed object identity.
   Mirrors and mutable branches can lag or move. Do not silently refresh frozen
   inputs on a heartbeat. A new base/candidate requires new review.
2. A **full committed shared Git repository** reachable from the compute node,
   with the pinned source object retained. A worktree snapshot or dirty tree is
   not the supported dispatch source. Do not assume post-commit hooks published
   it; verify publication to the explicitly authorized private destination.
3. Exact task text, owned paths, own ledger, supported evidence and steering paths,
   runtime/version identity, finite commands, CPU/memory/wall time and coordinator
   job/nodes to exclude. Freeze helper/task/test/source bytes before verification.
4. Separate **task-specific access plan**: which source/metadata/data is admitted,
   which operations/imports are allowed, and how missing inputs stop. Source-free
   tasks do source-free checks. These helpers grant no survey data, provider,
   credential, acquisition or arbitrary blob projection capability.
5. Tool/runtime admission: prepared interpreter and reviewed executable PATH;
   no dependency repair, shared tool update or credential copying by default.
   Required receipts are evidence, not an optional bookkeeping style.

For assigned country work, the brief must say to read its `_/CONTENTS.org` in
full **first**, including TODO/WAITING/LOGBOOK, and **name** the relevant
idiosyncrasies. Quote contradictions as findings; do not discard prior authority
as stale. Record genuine newly discovered idiosyncrasies within the edit grant.
No country assignment means no invented country investigation.

## 2. Reconcile and make complete briefs

Reconcile open issues with already merged work before fixing. A default-branch
release can delay issue closure after a development merge; a PR closing phrase
is not proof that the current claim is resolved. Check complete current issue
history, merged work and task-appropriate evidence at the pinned base. Classify
A/B defect fixes, C human/scientific decisions, D acquisition. Escalate C/D.

`make_briefs.py` accepts `--queue`, `--out`, `--base-sha`, `--candidate-sha`,
`--sweep-dir`, `--python`, `--coord-clone`, `--worktree-root`, `--task-plan`, and
optional `--repo` / `--round`. The queue maps each country to ordered issue
numbers. Both SHAs must be full forty-character lowercase Git SHAs. The task
plan is required and embedded without granting broader access.

The generator uses `gh api` REST issue reads before and after **all** comments
(`--paginate --slurp`, 100 per page). It retains the complete bodies and every
comment, including early rulings, and refuses inconsistent counts, duplicate
comment IDs or changed before/after issue metadata. A sibling `.issues.json`
records repository, full SHAs, retrieval start/end, task-plan hash, issue and
comment bytes, endpoints/argv, page sizes, counts and the metadata equality check.
If this existing gh interface cannot produce that representation, stop; never
fall back to a truncated issue view. No fixed character/comment limits.

REST reads are not transactional. Before action the owner/reviewer must retrieve
and read complete **current** body/comments and linked maintainer rulings through
an admitted channel, or obtain a refreshed coordinator snapshot. A freshness
receipt is evidence of a retrieval, not permission to ignore later rulings.
If provider access is forbidden, use the supplied steering channel while doing
independent permitted work. Do not call a stale/partial brief self-contained.

Country edit defaults are its config tree, targeted tests, required own
`.coder/ledger/<task>.md`, and explicitly supported report/evidence/receipt paths.
The task grant can narrow or explicitly extend these paths. Route ungranted
framework changes with file:line, proposed change and `rg` blast radius.

## 3. Private runtime and burst submission

Every `burst.sh` run starts with **private empty** config, data, L1, state,
cache, tmp and Matplotlib roots. There is no warm/shared-L2 mode and no shared-L1
symlink. A clean allowlist removes inherited credentials, acquisition flags,
cache/config roots and Python overrides before clone/import/command. It sets
`LSMS_CONFIG_DIR`, XDG roots, `LSMS_DATA_DIR`, `DVC_SITE_CACHE_DIR`, tmp/MPL and
issue-log locations; `LSMS_SKIP_AUTH=1`, `LSMS_BUILD_WORKERS=1`, BLAS/NumExpr/BLIS
thread counts 1, and `PYTHONNOUSERSITE=1`. HOME and CODEX_HOME keep their values.
No native auth/state/transcript is inspected. PATH remains an explicit operator
input; pin/review its executables as part of admission.

The shared source must already be admitted as committed code/metadata without
raw blobs or real credentials. The wrapper is **not universal I/O confinement**:
a child can address other filesystem paths or providers. Clearing environment
credentials and creating private caches does not prove absence of ambient file
credentials or network access. A separate reviewed task-specific plan must admit
and enforce any source access or read-only finite blob projection. Missing inputs
do not authorize retrieval. No broad credential/acquisition option is provided.

Use a prepared interpreter; the helper does not mount, install or update tools.
It clones the explicitly supplied shared Git directory, detaches at the full
expected source SHA, and **asserts HEAD equality and detached state** before the
command. `PYTHONPATH` and countries root point at that clone. An optional
`--lsms-import-check` asserts the package realpath under it only when the task
admits LSMS import; source-free tasks omit it. Separate base/candidate runs get
separate roots and the **full reviewed SHAs**, never a moving branch name.

Example form only; replace all placeholders with reviewed finite inputs:

```bash
sweep/submit.sh -c 4 -m 24G -t 1:00:00 -l /approved/task/run.log \
  --coordinator-job COORDINATOR_JOB_ID --exclude-nodes COORDINATOR_NODES -- \
  --source /approved/shared/source.git --expected-source-sha FULL_SOURCE_SHA \
  --python /approved/prepared/python -- COMMAND ARGUMENTS
```

`submit.sh` starts srun with an environment allowlist, stripping inherited
SLURM and node-local state. It requires coordinator job/nodes and passes the
node exclusion to Slurm; `burst.sh` independently refuses that job ID. It uses
allocation `SLURM_CPUS_PER_TASK`, never physical-node core counts. Default:
4 CPUs, 24G, 1 hour, free/pre-emptible `co_carleton` / `savio4_htc` /
`carleton_htc4_normal`. These are configurable **examples**, not resource
estimates. Non-free accounts require existing user authority and the explicit
`--billed-account-authorized` acknowledgement. Do not infer billing authority
from urgency. Logged submission preserves srun's exit code; a logging failure
also fails the command. Unknown submissions are not blindly retried.

Only admitted targeted checks run. For pytest tasks use allocation-budgeted
workers, `--dist=loadfile`, build workers 1 and all BLAS pins, and log to a file.
No blanket source rebuild, broad `-k`, DVC CLI, shared cache mutation, acquisition
bypass or pattern kill. Stop only owned processes. Report unrun checks truthfully.

## 4. Fix, report, independent review

For A/B, fix a bounded queue with independent commits when practical. Preserve
useful before/after evidence, test the behavior with granted inputs, and obtain
independent review of the **resulting** full SHA. A source-only change does not
require source-data builds. C memos can be brief: question, options, evidence,
recommendation and cost. Label dated observations; never present an old number
as freshly measured. A partial fix **must** draft a narrowed remaining issue or
a linked new residual (title/body/tier). The coordinator reconciles the open
queue, rather than merely commenting and leaving a misleading original issue.

Create your own prior-art ledger before non-trivial work. Keep required
source/custody/access/review receipts; do not suppress them. Use actual commit
author identity. Avoid issue-closing tokens in commit messages; put intended
closing language in a PR draft only when the complete issue is resolved.

Save reports by explicit supported transport:

```bash
sweep/save_report.py --report-artifact /approved/task/final.txt --out /approved/reports/REPORT.md
sweep/save_report.py --dispatch-evidence-dir /supplied/evidence --out /approved/reports/REPORT.md
```

The second form reads **only `report.md`**. Copy explicit UTF-8 `.md`/`.txt`
artifacts, not native JSONL transcripts, SQLite, homes, auth/config or session
state, even to extract assistant text. Empty, unsupported/native or symlinked
paths fail. In-process final text can be materialized by the coordinator in an
approved task artifact. Dispatched workers use the exact supplied absolute
`SUCODER_DISPATCH_EVIDENCE_DIR`, `SUCODER_DISPATCH_TASK_RESULT` and mutable steering
paths; similarly named checkout files do not replace them. Evidence outside the
worktree is permitted when the task/adapter supports it.

A review reads the complete current issue/rulings and diff, receives the author's
repro **command**, and runs only the admitted finite base/candidate checks. Report
APPROVE / CHANGES / REJECT, collateral effects and what was checked, PR must-state
items and every unverified limit. CHANGES produce a new commit and new reviewed
full SHA, not an invisible rewrite. Freeze input bytes until **both workers and
verifiers terminate**, or create a new isolated version. Atomic replacement does
not permit editing while a verifier is using the old version or path.

## 5. Coordinator branch and PR helpers

`prep_branch.sh <branch> <reviewed-development-sha> <full-commit-sha>...` uses
already admitted local objects and checks the exact remote development SHA.
It does not fetch a new base. Set an explicit private `SWEEP_PR_DIR`. It creates
a worktree at the reviewed base and cherry-picks with `-x`, disabling hooks.
Conflicts stop for coordinator review. Preserve prior authority and semantic
conflicts in CONTENTS/log/TODO; appending notes or dropping another branch's
caveat is not a general resolution rule. Re-review the resulting full SHA.

`open_pr.sh <branch> <reviewed-head-sha> <reviewed-base-sha> <title> <body-file>`
requires explicit publication authority. It checks the local head/branch and
base ancestry, the exact remote development base, pushes without force, then
verifies exact remote head and base again **before** PR creation. A failed push
stops; errors are never filtered or masked. The remote can change after these
observations; PR creation is not a merge authorization. The body describes final
behavior, relevant evidence/risks, residuals and unverified checks. Claim cache
impact only within the code/config scope actually established.

## 6. Merge approval gate — coordinator required

`merge_on_green.sh <reviewed-approvals.json>` requires an explicit manifest:

```json
{
  "schema_version": 1,
  "repository": "ligon/LSMS_Library",
  "approvals": [{
    "pr": 1423,
    "head_sha": "580c1fe4f8d28a5807ad9c2e15bd1acd39b4ed4d",
    "base_branch": "development",
    "base_sha": "fdec032efd4b98ed4b99883c9586f5ef07b7bf3e",
    "required_success_checks": ["EXACT_REVIEWED_CHECK_NAME"],
    "permitted_skips": []
  }]
}
```

Those historical SHAs demonstrate the **schema**, not approval to merge this PR.
The coordinator supplies actual approved PR/head/base/check inputs. Each approval
requires at least one explicit successful check name; skips are explicit and
cannot include a required-success check. Duplicate JSON keys/PRs/checks, missing
required checks and unsupported check representations fail closed. Only completed
CheckRun SUCCESS is accepted, except SKIPPED for an explicitly permitted name.
All other conclusions, unknown/pending states, draft/closed PRs, head/base drift,
non-APPROVED reviewDecision, and non-MERGEABLE/non-CLEAN state require coordinator
action. Legacy StatusContext is deliberately unsupported, never silently green.

**This helper currently issues no merge.** The admitted `gh pr view` observation
does not prove complete resolved review threads or absence of unresolved reviews.
A trustworthy pinned complete paginated thread/review query has not been verified
for this helper, so even a green observation returns **exit 3, needs coordinator**.
It prints only an **unexecuted proposed argv** using `--merge --match-head-commit`
with the **approved** full head SHA, never a newly observed head. A coordinator
must obtain complete current review/thread proof, revalidate head AND base and
handle the remaining race under explicit authority. Do not treat a green rollup
or `reviewDecision` alone as that proof. No force/squash/rebase/delete policy is
changed; there is no automatic merge, moving-head poll or silent relaxation.

A base move after one PR invalidates approvals for the old base. Reconcile shared
file/semantic conflicts, rerun the admitted combined checks in isolated versions,
and obtain fresh approval for new head/base pairs. Never carry authorization
across drift, or auto-resolve a semantic conflict to keep a batch moving.

## 7. Historical dispatch observations and bounded readmission

The 2026-11-03 dispatch attempts recorded source-preparation/submit delays,
credential-file permissions, read-only L1 projection requirements, site-cache
placement and an owned-child FD refusal. These are observations under particular
versions/jobs. Old 1.7 GB / 7–25 minute figures are **not universal** sizing or
latency assumptions. The supported timeout bounded both prep clone and sbatch
in the reviewed implementation; size it for an isolated exact-version pilot.

The old `cannot inspect owned child file descriptors (errno 13)` failure (owned
git PID 2018875, start 91955401, job 41018752) remains **UNLOCALIZED**. A zombie/
exiting-child hypothesis and toy experiments did not establish its cause or
restoration. Upstream sucoder PR81, commit
`641d8fdbab6e67a29b214d16399ffc5af6e29b0c`, is a **bounded terminal-proof
correction**, not proof of this old error. This task does not inspect historical
native/process state or adopt a runtime change on that basis.

For readmission, the coordinator should prepare an **isolated exact-version**
runtime with source/version receipts, a full committed shared source repository,
explicit immutable task/source/resources/evidence inputs and excluded coordinator
job/nodes. Review a dry-run first, then a bounded synthetic control/pilot using
only the task's admitted inputs. Never globally upgrade a shared installation
or execute moving upstream main to test a diagnosis. No unsupported warm-resume
promise. Profile/data admission is separate: a source-free task needs no survey
access; a data task without admitted sources reports its verification limit.

For dispatch **exit 3 / submission_unknown**, preserve the supplied submission
receipt and reconcile the exact attempt/job with the coordinator through the
supported interface. **NO blind retry**: a submission may already exist. The
coordinator establishes its fate before authorizing any new submission. Do not
open native homes/state/transcripts or copy real credentials to investigate.

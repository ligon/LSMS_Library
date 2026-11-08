<!-- Independent reviewer template. Fill every placeholder with a reviewed value.
     Freeze bytes before review; task grants determine source and execution scope. -->

# Independent review — {COUNTRY}, issue {ISSUE}

You did not author this candidate. Return APPROVE / CHANGES / REJECT with evidence.

- Repository/issue: `{REPO}`, issue `{ISSUE}`; complete body, all paginated
  comments/rulings and freshness receipt: `{ISSUE_HISTORY}`. Read the complete
  current history (including linked authority) through the task's admitted
  channel before acting; request a refreshed snapshot if direct reads are forbidden.
- Full development base SHA: `{BASE_SHA}`.
- Full candidate SHA: `{CANDIDATE_SHA}`. Branch `{BRANCH}` is a label, not authority.
- Admitted full committed shared Git repository: `{SOURCE_REPOSITORY}`.
- Frozen task/source/access/resource/evidence plan: `{TASK_PLAN}`.
- Author's reproduction **command**, not conclusion: `{REPRO}`.

Read the task grant and repository instructions first. For assigned country work,
read its full `_/CONTENTS.org`, including TODO/WAITING/LOGBOOK, **first**; name the
relevant idiosyncrasy, quote contradictions and report genuinely new ones for the
owner to record. Do not invent country work for a source-free task. Read the full
diff between the two full SHAs; verify its scope against the grant and own ledger.

Check the issue contract, collateral effects and tests. In country data work,
look for row loss/invention, grain changes, broken identifiers hidden by reducers,
unregistered derived values, `optional: true` silencing a wave, or tests merely
pinning today's counts. Preserve prior maintainer decisions in CONTENTS/log/TODO.

Run only the finite verification admitted by the task. For admitted data claims,
compare exact base and candidate in **separate private roots** with identical
admitted inputs; a source-free change does not require a source build. Burst form:

```
submit.sh -c 4 -m 24G -t 1:00:00 -l /approved/review/base.log \
  --coordinator-job COORDINATOR_JOB_ID --exclude-nodes COORDINATOR_NODES -- \
  --source {SOURCE_REPOSITORY} --expected-source-sha {BASE_SHA} \
  --python {PYTHON} -- COMMAND ARGUMENTS
```

Use `{CANDIDATE_SHA}` for the candidate run, never the literal moving branch
`development`. All paths/commands/resources require the task grant. Before any
LSMS import, private empty config/data/L1/state/cache/tmp/MPL, build workers 1 and
BLAS pins must exist. No shared-cache symlink, credentials, acquisition or blob
projection is authorized by this template or the wrapper. Respect cpus-per-task,
exclude coordinator job/nodes, and use the free account unless the user authorized
billing. These wrappers do not provide universal I/O confinement.

Do not edit the candidate or GitHub. Use read-only Git; no ref fetch/reset/merge.
No DVC CLI, native state/transcript reads, or pattern kills. Use the supplied
absolute evidence/steering/result interfaces, or return final text for an
in-process review. Required review receipts and own ledger are allowed at granted
paths. Freeze bytes until **all** workers and verifiers terminate; atomic rename
does not permit overlap. New bytes need a new isolated version and review.

Report the verdict and concrete changes, full input SHAs, what you ran with
results (base/candidate where relevant), collateral effects and scope checked,
PR must-state items, and every unverified limit. Tests drafted but not executed
are unverified. Approval does not authorize publication or merge by itself.

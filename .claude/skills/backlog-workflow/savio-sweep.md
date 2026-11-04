# Running a multi-country backlog sweep on Savio (runbook)

Field notes from the 2026-11-03 LSMS-ISA sweep: one coordinator, eight in-process country
agents, a fresh red-team reviewer per fix. Round 1 opened 12 PRs in about four hours (six code
fixes, six documentation-only). Every code fix was reviewed independently, and 11 of the 12 were
merged to `development` the same day on maintainer authorization. It replaced a campaign that had
stalled under its own bookkeeping, so the first rule is the one that made the difference:

> **The loop is reconcile -> fix -> test -> red-team -> PR, and the evidence standard is plain
> engineering:** the commands run, the numbers measured, the tests and their results, and one
> line for anything not verified. No hash manifests, custody records, "release"/"freeze"/
> "authority" documents or private witness files.

The scripts and brief templates referred to below live in `sweep/` next to this file.

## 1. Topology

| role | where | does |
|---|---|---|
| coordinator | the sucoder session, its own clone (`/local/job<ID>/mirrors/LSMS_Library`) | triage, briefs, red-team dispatch, cherry-picks, PRs, merges, the tracker and decision digest |
| country owner (one per country) | in-process `Agent` (general-purpose), own worktree `/local/job<ID>/worktrees/<C>` on `isa/<c>-<date>` | reads `CONTENTS.org`, reconciles its queue, fixes at most ~3 issues, drafts PR bodies and decision memos |
| reviewer (one per fix) | fresh in-process `Agent`, no worktree of its own | re-runs base vs candidate in separate cold burst jobs, returns APPROVE / CHANGES / REJECT |
| framework lane (one, serialized) | own worktree | the fixes that need `lsms_library/*.py`; country agents only describe them |
| burst jobs | separate Slurm allocations via `sweep/submit.sh` | every cold build and every test run heavier than a few minutes |

Commits made in any worktree are published to the shared mirror by the post-commit hook. That
is how burst jobs and reviewers see a branch before it reaches GitHub.

## 2. Setup checklist (coordinator, once per sweep)

1. **Base on GitHub `development`, not on the mirror's.** The mirror can lag: on 2026-11-03 it
   was 115 merges behind. Fetch into a separate ref, then fast-forward:
   `git fetch https://github.com/ligon/LSMS_Library.git development:refs/remotes/github/development`,
   then `git merge --ff-only`, then `git push origin development` (updates the mirror; the
   working tree there must have no tracked changes).
2. **GitHub remote over HTTPS** (outbound port 22 is blocked on Savio compute nodes):
   `git remote add github https://github.com/ligon/LSMS_Library.git` and
   `git config --local credential.https://github.com.helper '!gh auth git-credential'`.
   Worktrees share remotes and config.
3. `bin/savio_venv.sh mount` in the coordinator clone. Agents use its `.venv/bin/python` with
   `PYTHONPATH` = their worktree.
4. **A shared sweep directory on Lustre** (e.g. `/global/scratch/.../burst/<sweep>/`). Copy `sweep/`
   into it, because burst jobs on other nodes cannot see the coordinator's `/local`. Keep
   `TRACKER.org`, `DECISIONS.org`, `tasks/`, `reports/<C>/` and `prs/` there.
5. **Worktrees:** `git worktree add /local/job<ID>/worktrees/<C> -b isa/<c>-<date> <base-sha>`,
   one per country. Pass the base SHA explicitly; the coordinator clone may be on a notes branch.
6. **Reconcile before dispatching.** `Closes #N` fires only on the `development -> master` release
   merge, so issues fixed since the last release are still open. Cross merged PR bodies
   (`Closes|Fixes|Resolves|Addresses #N`) with the open list. Then pre-sort: `DECISION:` -> C,
   "acquire / new wave" -> D, the rest -> a short ordered queue per country.

## 3. Briefs

- `sweep/make_briefs.py` fills `sweep/country_brief.md` per country from a `queue.json` and embeds
  the issue bodies and last two comments, so a brief is self-contained. The Agent prompt itself is
  short: "your assignment is in <brief>, read it in full first", plus the five rules that matter
  most, restated (worktree and scope fence, `CONTENTS.org` first, data policy, the never-list,
  no ceremony).
- The brief requires the agent to **name** the country idiosyncrasies that bear on its queue. It also
  says that a contradiction with `CONTENTS.org` is a result to quote, that a C issue gets a memo of at
  most 12 lines (question, options, numbers, recommendation, cost), and that a partial fix drafts its
  residual.
- **Scope fence:** country agents edit only `lsms_library/countries/<C>/**` and `tests/**`. A
  framework change is described with file:line, a sketch and the `rg` blast radius, then routed
  to the framework lane. Grant one-file extensions explicitly, e.g. a canonical `data_info.yml`
  declaration.
- **Report = the agent's final message.** Subagents cannot write report files outside their
  worktree. Save it with `sweep/save_report.py <output_file> reports/<C>/REPORT.md`, which never
  prints the transcript into the coordinator's context.

## 4. Data policy and compute (put these in every brief)

- **Before editing**, reads may use the shared warm cache: the config equals `development`'s, so
  this is an ordinary read.
- **After any edit, never read through the shared cache.** Other sessions read it, and a branch
  build would overwrite a `var/` parquet. Use a private root
  (`LSMS_DATA_DIR=/local/job<ID>/data/<C>`, with `dvc-cache` symlinked to the shared L1) or a burst
  job with `--cold`.
- **Locally:** at most 2 cores and about 10 minutes per command, BLAS pinned,
  `LSMS_BUILD_WORKERS=1`. Anything heavier: commit, then
  `sweep/submit.sh -c 4 -m 24G -t 1:00:00 -l <log> -- --cold <branch> <cmd...>`.
  Use branch `development` for the base control.
- Watch the coordinator's own cgroup (`/sys/fs/cgroup/.../job_<ID>/cpu.stat`), not the load
  average: on a shared node the load average is mostly co-tenants.

## 5. The review loop

1. **Agent reports.** Save the report. Add memos to `DECISIONS.org` and commits to `TRACKER.org`.
2. **`sweep/prep_branch.sh fix/<n>-<country>-<slug>-<date> <commits>`.** This makes a fresh
   branch from GitHub `development` and cherry-picks the author's commits with `-x`. Commits
   appended to the same `CONTENTS.org` usually conflict when picked alone. Keep only this branch's
   entries, then `git cherry-pick --continue`.
3. **Reviewer.** Fill `sweep/reviewer_brief.md`: the issue, the branch, and the author's repro
   **command**, but not the author's conclusions. The reviewer must run base and candidate in
   separate cold burst jobs and report collateral damage, or say "none found" and what it checked.
4. **CHANGES go back to the author** (`SendMessage`, which resumes the agent with its context), as
   ONE new commit, never a rewrite of a published commit. Cherry-pick it onto the PR branch.
   Re-review if it adds a data path; check the diff yourself if it is text only.
5. **`sweep/open_pr.sh <branch> <title> <body>`.** The body carries the reviewer's must-state items:
   served-value changes, warnings, the cache cost, and what was not verified. Use `Refs`/`Addresses`
   unless the issue is fully resolved.
6. **Documentation-only commits** (`CONTENTS.org` facts) get one PR per country. The coordinator
   reviews them and spot-checks a number or two warm.

## 6. Merging (only on explicit maintainer authorization)

- `sweep/merge_on_green.sh <prs>` makes merge commits pinned to the reviewed head
  (`--match-head-commit`). It never squashes, rebases, force-pushes or deletes branches, and it
  hands back anything failing or conflicting. It works as a Claude Code `Monitor` command.
- **Pairs that touch the same country files** (a docs PR and a fix PR, two fixes): merge one, then
  merge `development` into the other's branch, resolve append-only conflicts, re-test the merged
  branch in a burst job, and push. A semantic conflict stays open, with a stated reason.
- **After a batch lands,** run the targeted tests of every merged fix together on the new tip in one
  cold burst job, because no one tested the combination. Then sync the mirror (`git push origin
  development`).

## 7. Failures already paid for (do not rediscover them)

| failure | fix |
|---|---|
| `srun` inside the coordinator's job ran as a step of its own 12-core slice | `submit.sh` strips `SLURM_*` and node-local paths; `burst.sh` refuses the coordinator's job id |
| burst job inherited the coordinator's `TMPDIR` / private `LSMS_DATA_DIR` | `submit.sh` unsets them |
| an agent ran `pkill -f "python food_acquired.py"`, which could kill other agents' builds | brief: never kill by pattern; stop only your own PIDs |
| broad `pytest -k` / `test_derivations` started cold multi-country builds locally | brief: targeted test files only |
| subagents wrote temp files into the coordinator's scratchpad (shared, so names collide) | brief: temp files under the agent's private data dir |
| report files refused by the harness | report = final message; `save_report.py` |
| editing a script while agents execute it | replace it atomically (write `x.new`, then `mv`) |

## 8. `sucoder dispatch` (one node per worker), status 2026-11-03

It suits this shape of work (separate allocations, a private `results.git` per worker, a file
channel for steering), but it did not run with the installed version. In order:

1. The default 30 s timeout is too short for the 1.7 GB source snapshot: use `--timeout 1800`.
   Each submit still costs 7–25 minutes, mostly that snapshot.
2. The reader credential must be a private 0600 file: pass a private copy.
3. The L1 projection requires blobs with no write bits, and the shared L1 is 0644: point
   `--shared-l1` at a read-only copy of just the countries' blobs.
4. DVC's site-cache SQLite in `/var/tmp` is "misplaced" and the worker is killed: add
   `DVC_SITE_CACHE_DIR` to the profile's `local_directories`.
5. "cannot inspect owned child file descriptors (errno 13)": a zombie or exiting child's
   `/proc/<pid>/fd` is unreadable, and the supervisor refused. This is fixed upstream (sucoder #79,
   PR 81, 2026-10-30), but the installed `sucoder` predated it. Upgrade it, or run from a checkout of
   upstream `main`, before trying again.

The previous campaign's profiles gave workers **no data access** (`LSMS_SKIP_AUTH=1`, no reader).
A worker that cannot read survey data cannot verify a fix, and that is how the custody ceremony
grew.

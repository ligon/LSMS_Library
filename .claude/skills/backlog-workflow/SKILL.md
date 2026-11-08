---
name: backlog-workflow
description: Use this skill to systematically work down the GitHub issue backlog with multi-agent orchestration — triage and classify open issues, reconcile against already-merged work, prioritise by leverage, dispatch isolated fix-agents that verify against the Feature() audit harness, red-team before filing, and open PRs for human merge. Read before running a backlog sweep or designing one. Builds on the `bench/feature_audit/` audit harness.
---

# Backlog workflow (resolve the GitHub backlog)

The companion to `bench/feature_audit/` (the audit harness *finds* bugs; this
*resolves the backlog*). Same machinery — triage → red-team → worktree-isolated
fix → verify → PR — pointed at the existing open issues instead of fresh scan
findings. **Opt-in / multi-agent: only run when the user explicitly asks.**

## When to use
- "work down the backlog", "tackle the open issues", "what can we auto-fix".
- After an audit run filed a batch of issues and you want them resolved, not just filed.

## The shape of the LSMS backlog (why this works)
Most of the backlog is *the same kind of work the audit harness already
verifies*. Classify into tiers before dispatching:

| Tier | What | Auto-tractable? |
|------|------|-----------------|
| **A — cross-country assembly / index** | collapse, unnamed-index, `index_info` registration (e.g. #496 is often the *root* that closes several) | **yes** — the harness (`scan.py`) is the direct regression net |
| **B — silent data-loss / wiring** | wrong source column (the cross-country-features §1 trap), wrong id-mapping, `.first()`-collapse row loss | yes, but **diagnose-then-fix** (a read-only diagnosis agent first) |
| **C — enhancements** | new features, harmonisation, modernisation | no — design-then-human-decide |
| **D — data acquisition / rebuilds** | needs WB microdata, country bootstraps, geovars | no — escalate to a human |

Auto-fix Tiers A (+ B on request). **Summarise C/D for the human; never auto-fix
them.**

## Phases
1. **Inventory & classify** — one agent per issue (or clustered): `type / effort /
   tractability / dependencies / dedup-against-recent-PRs`. Output a structured
   backlog table.
2. **Reconcile FIRST** (cheap win) — cross-reference open issues against merged
   PRs and recent fixes; **close the already-resolved** (e.g. an audit re-derived
   a bug already fixed) and comment the partially-addressed. Group by shared root
   cause — one root fix can close a batch.
3. **Prioritise** — rank by leverage (root-cause fixes that close multiple),
   severity (silent data loss = top), tractability. Emit a dependency-ordered
   dispatch plan (fix roots before symptoms).
4. **Fix pipeline** (Tier A, then B) — per issue/cluster: **isolated worktree** →
   implement → **verify** (task-admitted repro + targeted checks; the audit
   harness only when its source/runtime scope is granted) → **adversarial review** (does it fix it
   *and* not break the 24 healthy countries / lose data?) → open PR. `pipeline()`,
   worktree-per-fix.
5. **Escalate** Tiers C/D — a structured report for the human. Not auto-fixed.

## Non-negotiables (each was learned the hard way)
- **Worktree isolation per fix.** Parallel fix-agents must each get their own git
  worktree (`isolation: 'worktree'`). Editing one shared checkout in parallel
  corrupts it.
- **Hard no-git / read-only guardrail on every agent prompt.** A diagnosis agent
  once ran `git reset` and switched the main checkout's branch, wiping the working
  tree. Forbid `checkout/switch/reset/stash/clean/branch/restore/merge` and any
  edit to tracked files; read by absolute path; run repros from `/tmp`. (Fix-agents
  edit only inside their own worktree.)
- **Real verification with explicit access.** Match the reproduction to the
  defect and the task grant. Source-free changes do not require survey builds.
  Data claims need admitted sources and commands; unavailable inputs mean an
  explicit unverified limit, not implicit acquisition authority. Use private
  empty config/data/L1/state/cache/tmp/MPL before import and never a writable
  shared L1 link or shared L2 default. A separately reviewed task-specific plan
  must admit/enforce any read-only blob projection. These wrappers are not
  universal I/O confinement. Cache semantics and fingerprint scope depend on
  the actual code/config changed; consult current repository guidance rather
  than relying on historical warm-cache or `LSMS_NO_CACHE` claims. Run an audit
  harness only when its country/source/runtime scope is explicitly admitted.
- **Red-team before PR.** A fix-agent's patch gets an independent skeptic: does it
  resolve the issue, *and* leave the healthy countries / data intact?
- **PRs for human merge — never auto-merge.** Open a PR per fixed issue; a human
  reviews and merges.
- **Dedup / reconcile before fixing.** Check merged PRs first; don't re-fix what
  #520 (or any recent merge) already closed. Use a stable `audit-key` fingerprint
  (feature|country|kwargs|check + a pattern hash — the bare fingerprint is too
  coarse for `runtime_warning`-class findings and causes false dedup-skips).

- **An open issue must describe exactly the remaining work (2026-10-14).** The
  default branch is `master`; routine PRs merge into `development`, so `Closes
  #N` fires only at the next `development -> master` release merge, and a
  partial PR (written honestly as "Addresses #N") leaves the issue open with its
  *original* text. After a few of those the open list no longer says what is
  left -- #813/#945/#832/#953 each sat behind two or three merged PRs with no
  reader able to tell. So every PR that resolves PART of an issue does one of
  two things, in the same PR, never a third:
  - **narrow** -- edit the issue's title/body to the residual, with a
    "Resolved so far: PR #N (sha)" header, when the residual is the same kind
    of work; or
  - **split** -- `Closes #N` and open a residual issue, linked both ways, when
    the residual is a different kind of work (a judgement call, a design
    decision, another country).
  Fix-agents cannot write to GitHub, so a fix brief must require the agent to
  DRAFT the residual (title + body + tier) in its report, and the dispatcher
  files it. In the triage record, a `dedup_status: partial` entry MUST carry
  `residual_title` / `residual_body`; a partial without a drafted residual is
  an incomplete record. Reconcile = close the resolved, file the residuals,
  narrow the rest -- not "comment the partially-addressed" and move on.

## Gotchas seen in practice
- **Stale caches can masquerade as bugs.** Historical `cluster_features`
  observations are not proof of current freshness or fingerprint scope. Record
  exact code/config/cache inputs under the task's access plan; compare isolated
  base/candidate when the claim needs it. Do not clear or rebuild shared caches.
- **The Workflow runtime delivers `args` as a JSON *string*.** Parse it
  (`typeof args === 'string' ? JSON.parse(args) : args`) or `args.x` silently
  resolves to `String.prototype.x`. Pass bulk data (the issue/cluster list) via a
  file an agent reads, not a giant inline-`args` blob.
- **Not every issue is a bug.** Triage honestly — a large share classify as
  by-design / already-fixed / enhancement. The win is often *closing* and
  *reconciling*, not patching.

## Running a multi-country sweep on Savio
The reviewed operational recipe is in [savio-sweep.md](savio-sweep.md), with
helpers and brief templates in `sweep/`. Read it before a sweep: complete current
issue/ruling history, full reviewed base/candidate SHAs, task-specific access and
evidence grants, private empty roots before import, allocation-aware submission,
independent review and partial residual drafts. The merge helper takes an explicit
approval manifest but currently returns coordinator-required because complete
review-thread proof is unavailable; it issues no merge. Historical dispatch
observations are qualified and readmission uses an isolated pinned dry-run/pilot.

## Reuse
- `bench/feature_audit/scan.py` — the regression net (re-run after a fix to confirm
  no new collapse / loss across countries).
- `bench/feature_audit/audit.workflow.js` — the triage → red-team → file pattern to
  adapt (swap "scan findings" for "open issues").
- `.claude/skills/cross-country-features/SKILL.md` — the silent-corruption traps and
  collapse semantics most Tier-A/B fixes turn on.
- `scrum-master-hpc` — subagent dispatch, worktree, DVC-lock hygiene.

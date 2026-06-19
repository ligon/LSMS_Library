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
   implement → **verify** (cold repro from `/tmp` + targeted tests + re-run
   `scan.py` as the regression net) → **adversarial review** (does it fix it
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
- **Real verification, not "looks right."** Cold build from `/tmp`
  (`LSMS_NO_CACHE=1`), targeted tests, and the audit harness re-run as the
  regression net. For config-only edits, point the live package at the worktree
  with `LSMS_COUNTRIES_ROOT=<wt>/lsms_library/countries` (no fresh venv needed);
  for framework-code edits the `.pth` pins to the main checkout, so verify from
  `cwd=worktree` or build a venv in the worktree.
- **Red-team before PR.** A fix-agent's patch gets an independent skeptic: does it
  resolve the issue, *and* leave the healthy countries / data intact?
- **PRs for human merge — never auto-merge.** Open a PR per fixed issue; a human
  reviews and merges.
- **Dedup / reconcile before fixing.** Check merged PRs first; don't re-fix what
  #520 (or any recent merge) already closed. Use a stable `audit-key` fingerprint
  (feature|country|kwargs|check + a pattern hash — the bare fingerprint is too
  coarse for `runtime_warning`-class findings and causes false dedup-skips).

## Gotchas seen in practice
- **Stale local cache masquerades as a bug.** `cluster_features` "duplicates" were
  a stale `var/` parquet; the build was already correct and CI (which rebuilds)
  was green. Always confirm a failure reproduces on a **cold** build before
  "fixing" it. The v0.8.0 cache hash versions inputs + per-country scripts but
  **not framework transform code**, so a code-only harmonisation change can leave
  hash-valid stale parquets (see #522).
- **The Workflow runtime delivers `args` as a JSON *string*.** Parse it
  (`typeof args === 'string' ? JSON.parse(args) : args`) or `args.x` silently
  resolves to `String.prototype.x`. Pass bulk data (the issue/cluster list) via a
  file an agent reads, not a giant inline-`args` blob.
- **Not every issue is a bug.** Triage honestly — a large share classify as
  by-design / already-fixed / enhancement. The win is often *closing* and
  *reconciling*, not patching.

## Reuse
- `bench/feature_audit/scan.py` — the regression net (re-run after a fix to confirm
  no new collapse / loss across countries).
- `bench/feature_audit/audit.workflow.js` — the triage → red-team → file pattern to
  adapt (swap "scan findings" for "open issues").
- `.claude/skills/cross-country-features/SKILL.md` — the silent-corruption traps and
  collapse semantics most Tier-A/B fixes turn on.
- `scrum-master-hpc` — subagent dispatch, worktree, DVC-lock hygiene.

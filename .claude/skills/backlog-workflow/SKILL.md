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
- **Real verification, not "looks right" — but use the cache intelligently.**
  The cost trap (observed 2026-06-19): a blanket `LSMS_NO_CACHE=1` cold build
  rebuilds every country from `.dta` source, and since `LSMS_NO_CACHE` does
  **not write** the cache, N parallel agents each pay that from-source cost
  independently — no shared warming. Match the repro to *where the defect lives*:
  - **Assembly-time / post-read defects** (cross-country index collapse,
    unnamed-index, `_join_v_from_sample` 100%-NaN-`v`, kinship/spelling, the
    `groupby().first()` duplicate-collapse) re-run on *every*
    `Feature()`/`Country()` call **on top of** the per-country parquets — they
    never touch the cache. A **warm** call reproduces them identically in
    seconds; `LSMS_NO_CACHE` buys nothing here. (Most Tier-A and several Tier-B
    issues are this class.)
  - **Config edits** (a country's `data_info.yml` / `data_scheme.yml` / `.org`):
    the v0.8.0 content hash auto-invalidates *just that table for that country*
    on the next normal read, leaving the other ~39 warm — so edit, then repro
    **warm**; the touched table rebuilds itself, no global flag. (Or point the
    live package at a worktree config with
    `LSMS_COUNTRIES_ROOT=<wt>/lsms_library/countries`, no fresh venv.)
  - **Framework-code edits to the per-country *build* path** (not assembly) are
    the one case the hash does **not** cover, so a warm parquet shadows the
    change — here you genuinely need a rebuild, but scope it with
    `lsms-library cache clear --country X` (one country) instead of the global
    `LSMS_NO_CACHE`. The `.pth` pins imports to the main checkout, so verify from
    `cwd=worktree` or a worktree venv. Same caveat for **script-path wave
    parquets** (Nigeria PP/PH, Tanzania multi-round), where `LSMS_NO_CACHE` is
    "soft" (still read from disk) — clear the country instead.
  - Then re-run `scan.py` as the cross-country regression net (itself a warm
    read once the suspect tables are built once and shared).
- **Red-team before PR.** A fix-agent's patch gets an independent skeptic: does it
  resolve the issue, *and* leave the healthy countries / data intact?
- **PRs for human merge — never auto-merge.** Open a PR per fixed issue; a human
  reviews and merges.
- **Dedup / reconcile before fixing.** Check merged PRs first; don't re-fix what
  #520 (or any recent merge) already closed. Use a stable `audit-key` fingerprint
  (feature|country|kwargs|check + a pattern hash — the bare fingerprint is too
  coarse for `runtime_warning`-class findings and causes false dedup-skips).

## Gotchas seen in practice
- **Stale local cache masquerades as a bug — but the v0.8.0 hash mostly handles
  it now.** `cluster_features` "duplicates" were once a stale `var/` parquet (the
  build was already correct; CI, which rebuilds, was green). The v0.8.0 content
  hash now auto-invalidates on changes to inputs + per-country scripts, so a warm
  read of a **config-covered** table is trustworthy — you do *not* need a blanket
  cold build to trust it. The residual blind spot is **framework transform code**,
  which the hash does NOT version (only the manual `LSMS_CACHE_SCHEMA` token) — a
  code-only harmonisation change can leave hash-valid stale parquets (see #522).
  So: trust warm for config/assembly defects; force a *scoped* rebuild
  (`cache clear --country X`) only when validating a framework change to a
  per-country build path.
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

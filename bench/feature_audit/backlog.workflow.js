export const meta = {
  name: 'backlog-tackle',
  description: 'Triage the GitHub backlog, reconcile against merged work, propose worktree-verified fixes for Tier A+B, escalate the rest',
  whenToUse: 'Working down the open-issue backlog (see .claude/skills/backlog-workflow). Opt-in / multi-agent.',
  phases: [
    { title: 'Triage',   detail: 'one agent per issue: tier / tractability / dedup / priority' },
    { title: 'Fix',      detail: 'Tier A+B auto-tractable: implement + verify in an isolated worktree (dry-run returns the diff)' },
    { title: 'Report',   detail: 'write the backlog report; return a compact summary' },
  ],
}

// ---- inputs (args arrives as a JSON STRING — parse it) --------------------
const A = (typeof args === 'string')
  ? (() => { try { return JSON.parse(args) } catch (e) { return {} } })()
  : (args && typeof args === 'object' ? args : {})
const REPO    = A.repo || '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library'
const PY      = A.py   || `${REPO}/.venv/bin/python`
const RESULTS = A.resultsDir || `${REPO}/bench/feature_audit/results/2026-06-18`
const ISSUES  = A.issuesFile || `${RESULTS}/backlog_issues.json`
const DO      = !!A.file                          // false = DRY RUN (propose only; no close/PR)
const SKILL_BL = `${REPO}/.claude/skills/backlog-workflow/SKILL.md`
const SKILL_CC = `${REPO}/.claude/skills/cross-country-features/SKILL.md`

log(`backlog: issues=${ISSUES}; mode=${DO ? 'LIVE (close/PR)' : 'dry-run (propose only)'}`)

const GUARD = `GUARDRAIL — shared repo with concurrent work. Do NOT run git that changes state
(checkout/switch/reset/stash/clean/branch/restore/merge) on the MAIN checkout, do NOT edit tracked
files outside any worktree you are explicitly given, do NOT \`cd\` into the main repo to run code.
Read by absolute path; run repros from /tmp with ${PY}.`

const ISSUES_SCHEMA = {
  type: 'object', required: ['issues'],
  properties: { issues: { type: 'array', items: { type: 'object' } } },
}
const loaded = await agent(
  `Run: cat ${ISSUES}\nIt is a JSON array of GitHub issue objects {number,title,labels,body}. ` +
  `Return it verbatim as {"issues": <that exact array>} — every element, no edits or summarizing. ` +
  `If missing/empty return {"issues": []}.`,
  { label: 'load-issues', phase: 'Triage', schema: ISSUES_SCHEMA }
)
const issues = (loaded && loaded.issues) || []
log(`loaded ${issues.length} open issues`)
if (!issues.length) return { triaged: 0, note: 'no issues loaded' }

// ---- schemas --------------------------------------------------------------
const TRIAGE_SCHEMA = {
  type: 'object',
  required: ['number', 'tier', 'tractability', 'dedup_status', 'priority', 'summary'],
  properties: {
    number:       { type: 'integer' },
    tier:         { type: 'string', enum: ['A', 'B', 'C', 'D'] },
    type:         { type: 'string', description: 'bug / enhancement / data / umbrella / meta' },
    tractability: { type: 'string', enum: ['auto', 'diagnose-then-fix', 'human'] },
    dedup_status: { type: 'string', enum: ['open', 'resolved-in-development', 'partial', 'duplicate'] },
    resolved_by:  { type: 'string', description: 'PR/issue # that resolves or supersedes it, or ""' },
    root_cluster: { type: 'string', description: 'shared root cause key, if any (e.g. "index_info-registration")' },
    priority:     { type: 'integer', description: '1 (highest) .. 5' },
    approach:     { type: 'string', description: 'one-line fix direction (for auto/diagnose tiers)' },
    summary:      { type: 'string', description: 'what the issue is + why this classification, with path:line' },
  },
}
const FIX_SCHEMA = {
  type: 'object',
  required: ['attempted', 'verified', 'diff', 'verification'],
  properties: {
    attempted:    { type: 'boolean' },
    verified:     { type: 'boolean', description: 'did the cold repro + targeted scan/test confirm the fix and no regression?' },
    diff:         { type: 'string', description: 'unified git diff of the proposed change (worktree)' },
    files:        { type: 'array', items: { type: 'string' } },
    verification: { type: 'string', description: 'exact commands run and their key outputs (before/after numbers)' },
    regression:   { type: 'string', description: 'what you checked to confirm healthy countries/data are intact' },
    pr_title:     { type: 'string' },
    notes:        { type: 'string', description: 'caveats; if not attempted, why' },
  },
}

// ---- prompts --------------------------------------------------------------
const triagePrompt = (iss) => `${GUARD}

Triage GitHub issue #${iss.number} of the LSMS_Library repo for a backlog sweep.
Title: ${iss.title}
Labels: ${(iss.labels || []).join(', ') || '(none)'}
Body (excerpt): ${(iss.body || '').slice(0, 900)}

Read the FULL issue (\`gh issue view ${iss.number}\`), the code it concerns, and check whether it is
already fixed: \`gh pr list --state merged --search "<keywords>"\` and inspect
\`git show origin/development:<file>\` for the relevant code. The audit just merged #520 (cross-country
index collapse: market= drops v/adds m; reduced-index modal exclusion) and #521 (the audit harness) into
development. Context skills: ${SKILL_BL}, ${SKILL_CC}.

Classify:
- tier: A=cross-country assembly/index (harness-verifiable) · B=silent data-loss/wiring · C=enhancement · D=data-acquisition/rebuild.
- tractability: auto (clear fix, harness/test verifiable) · diagnose-then-fix · human.
- dedup_status: open · resolved-in-development (give resolved_by PR#) · partial · duplicate.
- root_cluster: a shared-root-cause key when several issues share one (e.g. #496 index_info registration likely roots several collapses).
- priority 1..5 by leverage (root fixes that close several) × severity (silent data loss = high) × tractability.
Every claim cites path:line or an issue/PR number.`

const fixPrompt = (iss, t) => `${GUARD}
You are in an ISOLATED GIT WORKTREE (your cwd). Edit ONLY files inside this worktree. Do NOT push,
do NOT open a PR, do NOT touch the main checkout — this is a DRY-RUN proposal.

Implement the fix for issue #${iss.number}: ${iss.title}
Triage approach: ${t.approach}
Root cluster: ${t.root_cluster || '(none)'}

1. Read the issue (\`gh issue view ${iss.number}\`) and the code. Implement the minimal correct fix IN THIS WORKTREE.
2. VERIFY for real:
   - cold repro of the issue from /tmp (\`cd /tmp && LSMS_NO_CACHE=1 ${PY} -c "..."\`) — show before is broken;
   - confirm your fix resolves it (run with this worktree's code: for config edits set
     LSMS_COUNTRIES_ROOT=<this-worktree>/lsms_library/countries; for framework-code edits run with cwd=<this-worktree>);
   - regression: re-run the relevant table-structure test and/or \`${REPO}/bench/feature_audit/scan.py --features <f> --countries <a few> -j 4\` to confirm healthy countries/data are intact (no NEW collapse/loss).
3. Return the unified \`git diff\` of your worktree change, the files, the exact verification commands + key
   before/after numbers, and what regression you checked. Set verified=true ONLY if the cold repro + a
   regression check both passed. If the fix needs more than a focused change, set attempted=false and explain.`

// ---- Phase 1+2: triage every issue, then fix the auto-tractable Tier A/B ---
const results = await pipeline(
  issues,
  (iss) => agent(triagePrompt(iss), { label: `triage:#${iss.number}`, phase: 'Triage', schema: TRIAGE_SCHEMA })
             .then((t) => ({ iss, t })),
  ({ iss, t }) => {
    if (!t) return { iss, t: null, action: 'triage-error' }
    const fixable = (t.tier === 'A' || t.tier === 'B')
      && t.tractability === 'auto'
      && t.dedup_status === 'open'
    if (!fixable) {
      const action = t.dedup_status !== 'open' ? 'reconcile'
        : (t.tier === 'C' || t.tier === 'D') ? 'escalate'
        : 'needs-diagnosis'
      return { iss, t, action }
    }
    return agent(fixPrompt(iss, t), { label: `fix:#${iss.number}`, phase: 'Fix', schema: FIX_SCHEMA, isolation: 'worktree' })
      .then((f) => ({ iss, t, action: 'fix', fix: f }))
  }
)

const ok = results.filter(Boolean)
const tally = {}
for (const r of ok) tally[r.action] = (tally[r.action] || 0) + 1
const fixed = ok.filter((r) => r.action === 'fix' && r.fix && r.fix.verified)
const reconcile = ok.filter((r) => r.action === 'reconcile')
log(`triage actions: ${JSON.stringify(tally)} | verified fix proposals: ${fixed.length} | reconcile(close): ${reconcile.length}`)

// ---- Phase 3: write the backlog report (compact return) -------------------
const report = {
  generated_for: 'open backlog',
  mode: DO ? 'live' : 'dry-run',
  triage: ok.map((r) => ({
    number: r.iss.number, title: r.iss.title, action: r.action,
    tier: r.t && r.t.tier, tractability: r.t && r.t.tractability,
    dedup: r.t && r.t.dedup_status, resolved_by: r.t && r.t.resolved_by,
    root_cluster: r.t && r.t.root_cluster, priority: r.t && r.t.priority,
    summary: r.t && r.t.summary,
  })),
  reconcile_close: reconcile.map((r) => ({ number: r.iss.number, resolved_by: r.t.resolved_by, why: r.t.summary })),
  fix_proposals: fixed.map((r) => ({
    number: r.iss.number, title: r.iss.title, tier: r.t.tier, files: r.fix.files,
    verification: r.fix.verification, regression: r.fix.regression, pr_title: r.fix.pr_title, diff: r.fix.diff,
  })),
  escalate: ok.filter((r) => r.action === 'escalate' || r.action === 'needs-diagnosis')
    .map((r) => ({ number: r.iss.number, title: r.iss.title, tier: r.t && r.t.tier, action: r.action, summary: r.t && r.t.summary })),
}

const writer = await agent(
  `Write this JSON to ${RESULTS}/backlog_report.json (pretty), and a readable markdown digest to ` +
  `${RESULTS}/backlog_report.md grouped by action (reconcile-close, fix-proposals with their diffs, ` +
  `needs-diagnosis, escalate), each with number/title/tier/priority and the one-line summary. Use Bash/Write ` +
  `only under ${RESULTS} (gitignored); run NO git command. Report the two paths and the counts.\n\nJSON:\n` +
  JSON.stringify(report).slice(0, 180000),
  { label: 'write-report', phase: 'Report' }
)

return {
  open_issues: issues.length,
  actions: tally,
  verified_fix_proposals: fixed.map((r) => `#${r.iss.number} ${r.iss.title}`),
  reconcile_close: reconcile.map((r) => `#${r.iss.number} (by ${r.t.resolved_by || '?'})`),
  report: `${RESULTS}/backlog_report.md`,
  writer_note: writer,
}

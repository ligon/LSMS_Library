export const meta = {
  name: 'feature-audit-triage',
  description: 'Triage + red-team cross-country Feature() audit clusters; file confirmed issues to GitHub',
  whenToUse: 'After scan.py + cluster.py have produced clusters.json for a Feature() audit run.',
  phases: [
    { title: 'Triage',   detail: 'one agent per cluster: real-bug / by-design / false-positive + repro' },
    { title: 'Red-team', detail: 'multi-lens skeptics try to REFUTE each candidate bug' },
    { title: 'File',     detail: 'memo + would_file.jsonl; gh issue create (dedup) only if file=true' },
  ],
}

// ---- inputs --------------------------------------------------------------
// Only small SCALARS travel via `args` (a large inline-JSON args blob does not
// survive the scriptPath plumbing reliably).  The cluster list itself is loaded
// from a file ON DISK by a loader agent — the JS sandbox can't read files, but
// agents can.  Write the slice you want triaged to `slice` before launching.
// args = { file: bool, repo: str, py: str, resultsDir: str, slice: str }
const doFile    = !!(args && args.file)                       // DRY-RUN unless explicitly true
const REPO      = (args && args.repo) || '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library'
const PY        = (args && args.py)   || `${REPO}/.venv/bin/python`
const RESULTS   = (args && args.resultsDir) || `${REPO}/bench/feature_audit/results/2026-06-18`
const SLICE     = (args && args.slice) || `${RESULTS}/clusters_slice.json`
const SKILL     = `${REPO}/.claude/skills/cross-country-features/SKILL.md`

log(`args keys: ${args ? Object.keys(args).join(',') : '(none)'}; loading clusters from ${SLICE}`)

const CLUSTERS_SCHEMA = {
  type: 'object',
  required: ['clusters'],
  properties: { clusters: { type: 'array', items: { type: 'object' } } },
}
const loaded = await agent(
  `Run \`cat ${SLICE}\`. It is a JSON array of audit "cluster" objects. Return it verbatim as ` +
  `{"clusters": <that exact array>} — no edits, additions, reordering, or summarizing. ` +
  `If the file is missing or empty, return {"clusters": []}.`,
  { label: 'load-clusters', phase: 'Triage', schema: CLUSTERS_SCHEMA }
)
const clusters = (loaded && loaded.clusters) || []
if (!clusters.length) {
  log('no clusters loaded — nothing to triage')
  return { total_clusters: 0, confirmed: 0, items: [] }
}

// ---- structured-output schemas -------------------------------------------
const TRIAGE_SCHEMA = {
  type: 'object',
  required: ['classification', 'confidence', 'rationale'],
  properties: {
    classification: { type: 'string', enum: ['real-bug', 'by-design', 'false-positive', 'harness-artifact'] },
    confidence:     { type: 'string', enum: ['low', 'medium', 'high'] },
    rationale:      { type: 'string', description: 'why, citing path:line and exact values/numbers' },
    repro:          { type: 'string', description: 'minimal command that reproduces it, or "" if none' },
    repro_confirmed:{ type: 'boolean', description: 'did you actually run the repro and observe the symptom?' },
    affected:       { type: 'string', description: 'which features/countries genuinely affected' },
    suggested_fix:  { type: 'string', description: 'one-line fix direction if real-bug, else ""' },
  },
}

const VERDICT_SCHEMA = {
  type: 'object',
  required: ['refuted', 'reason'],
  properties: {
    refuted: { type: 'boolean', description: 'true if THIS lens shows the bug is not real / is expected' },
    reason:  { type: 'string', description: 'evidence for the verdict, with numbers / path:line' },
  },
}

// ---- prompt builders ------------------------------------------------------
const GUARDRAIL = `HARD GUARDRAIL — this is a SHARED working tree with concurrent work in flight:
  • NEVER run git that changes state: no checkout / switch / reset / stash / clean / branch / restore / merge.
    (A prior run switched the branch and removed files from the working tree — do NOT repeat this.)
  • NEVER edit, create, move, or delete a TRACKED file. Investigation is strictly READ-ONLY.
  • Do NOT \`cd\` into the repo to run code. Read files by ABSOLUTE path; run every repro from /tmp
    (\`cd /tmp && <py> -c "…"\`) so the import resolves the installed package, not the cwd.`

const ctx = (c) => `${GUARDRAIL}

Cluster ${c.cluster_id} from a cross-country Feature() audit of the LSMS_Library repo.
  repo: ${REPO}   python: ${PY} (use this exact interpreter)
  check: ${c.check}   severity-class: ${c.severity}   status: ${c.status}   occurrences: ${c.count}
  normalized pattern: ${c.pattern}
  example detail: ${c.example_detail}
  example kwargs: ${JSON.stringify(c.example_kwargs || {})}
  affected features: ${(c.features || []).join(', ') || '(cross-country)'}
  affected countries (${(c.countries || []).length}): ${(c.countries || []).slice(0, 12).join(', ')}${(c.countries||[]).length>12?' …':''}
  raw records: grep this cluster's occurrences in ${RESULTS}/results.jsonl by check="${c.check}".

Severity classes: A=loud (build error/empty); B=cross-country assembly defect (index collapse/unnamed level,
missing canonical level, row loss vs sum-of-parts); C=silent-semantic (NaN blow-up, reaggregation drift, etc.).`

const triagePrompt = (c) => `${ctx(c)}

You are triaging this cluster. Read what you need (don't trust the wired column or a config comment — verify):
  - ${SKILL}  (cross-country-features: silent-corruption traps, collapse semantics, redteam discipline)
  - ${REPO}/CLAUDE.md  (Feature kwargs/units/labels/market contracts, no-microdata countries, derived tables)
  - ${REPO}/lsms_library/feature.py, country.py, diagnostics.py as relevant
  - the country config under ${REPO}/lsms_library/countries/<C>/_/ for an affected country, if structural

If a repro is cheap, RUN IT from a neutral CWD (the .pth pins lsms_library to this checkout):
  cd /tmp && LSMS_NO_CACHE=1 ${PY} -c "import lsms_library as ll; df=ll.Feature('<feature>')(<args>); print(df.index.names, len(df))"

Classify the cluster:
  real-bug         — a genuine defect in the framework or a country's wiring.
  by-design        — expected per a documented contract (e.g. units 'no silent fallback' NaN; cluster-level
                     tables have no household 'i' index; no-microdata countries Nepal/Armenia failing to load).
  false-positive   — the scanner's invariant is wrong here.
  harness-artifact — the scan itself induced it (e.g. it passed a deprecated arg form).

Every claim carries a number or a path:line. Set repro_confirmed only if you actually ran the repro.`

const LENSES = [
  { key: 'cold-repro',  ask: `Reproduce COLD from /tmp with LSMS_NO_CACHE=1 using ${PY}. If the symptom vanishes cold, it was a stale-cache artifact -> refuted. If you cannot reproduce it at all, refuted.` },
  { key: 'by-design',   ask: `Check the documented contract (${REPO}/CLAUDE.md, ${SKILL}, the country CONTENTS.org). If the behavior is expected-by-design (units no-fallback NaN, by-design collapse/dup per GH#323/#501, no-microdata country, Mover sentinel), it is NOT a bug -> refuted.` },
  { key: 'source-truth',ask: `Verify against source truth: read the actual Stata/SPSS variable label and cross-validate values for an affected country. If the data/labels show the finding is a misdiagnosis, refuted.` },
]

const redteamPrompt = (c, t, lens) => `${ctx(c)}

A triage agent classified this as a REAL BUG with ${t.confidence} confidence:
  rationale: ${t.rationale}
  repro: ${t.repro || '(none given)'}  (confirmed=${t.repro_confirmed})

Your job is ADVERSARIAL: try to REFUTE it through one specific lens. Default to refuted=true if you are
uncertain — the bar to confirm a bug is high. Re-derive the key facts independently (don't just re-read the
triage note). Lens — ${lens.key}: ${lens.ask}

CAVEAT (from the audit skill): do NOT refute a cross-country COLLAPSE or row-loss finding on a hand-wave —
only refute it if you can PROVE the dropped/collapsed rows are identical/benign. An unproven dismissal of a
collapse should come back refuted=false.`

// ---- Phase 3 + 4: triage each cluster, red-team the real-bugs -------------
log(`triaging ${clusters.length} clusters (file=${doFile ? 'LIVE gh' : 'dry-run'})`)

const triaged = await pipeline(
  clusters,
  (c) => agent(triagePrompt(c), { label: `triage:${c.cluster_id}`, phase: 'Triage', schema: TRIAGE_SCHEMA })
           .then((t) => ({ cluster: c, triage: t })),
  ({ cluster, triage }) => {
    if (!triage || triage.classification !== 'real-bug') {
      return { cluster, triage, verdicts: [], confirmed: false }
    }
    return parallel(LENSES.map((lens) => () =>
      agent(redteamPrompt(cluster, triage, lens),
            { label: `redteam:${cluster.cluster_id}:${lens.key}`, phase: 'Red-team', schema: VERDICT_SCHEMA })
    )).then((verdicts) => {
      const refutes = verdicts.filter(Boolean).filter((v) => v.refuted).length
      // confirmed only if a MAJORITY of lenses fail to refute (i.e. < 2 of 3 refute)
      return { cluster, triage, verdicts, confirmed: refutes < 2 }
    })
  }
)

const ok = triaged.filter(Boolean)
const confirmed = ok.filter((t) => t.confirmed)
const byClass = {}
for (const t of ok) { const k = t.triage ? t.triage.classification : 'null'; byClass[k] = (byClass[k] || 0) + 1 }
log(`triage: ${JSON.stringify(byClass)} | red-team confirmed ${confirmed.length}`)

// ---- Phase 5: file the survivors -----------------------------------------
const payload = confirmed.map((t) => ({
  cluster_id: t.cluster.cluster_id,
  check: t.cluster.check,
  severity: t.cluster.severity,
  features: t.cluster.features,
  countries: t.cluster.countries,
  count: t.cluster.count,
  fingerprint: t.cluster.example_fingerprint,
  rationale: t.triage.rationale,
  repro: t.triage.repro,
  suggested_fix: t.triage.suggested_fix,
  detail: t.cluster.example_detail,
}))

const filePrompt = `${GUARDRAIL}
(You MAY write new files under ${RESULTS} — it is gitignored — and run \`gh\`; that does not violate the
guardrail. You must NOT run any git command or touch any tracked file.)

You are the filing step of the Feature() audit. ${confirmed.length} clusters survived
red-team as confirmed issues. Here is the payload (JSON):

${JSON.stringify(payload, null, 2)}

Do ALL of this with Bash/Write, using repo ${REPO}:
1. Write a human memo ${RESULTS}/audit_memo.md: one section per confirmed issue, grouped by feature, each with
   severity, affected features/countries, the repro, the rationale (numbers/path:line), and suggested fix.
2. Write ${RESULTS}/confirmed_issues.jsonl: one JSON line per issue from the payload.
3. For EACH issue build a GitHub issue body containing a line "audit-key: <fingerprint>" plus title
   "[feature-audit] <feature> / <scope>: <symptom>" and labels feature-audit + the severity class label
   (B->assembly-defect, A->build-error, C->silent-corruption) + auto-filed.
4. DEDUP before creating: gh issue list --state all --search "<fingerprint>"  — skip any that already exist
   (open OR closed; a closed one means a human already triaged it).
${doFile
  ? '5. file=TRUE: actually run `gh issue create` for the non-duplicates. Report the URLs.'
  : '5. file=FALSE (DRY RUN): do NOT run `gh issue create`. Instead write the would-be issues to '
    + `${RESULTS}/would_file.jsonl (title, labels, body, fingerprint, dedup_status). Report the counts.`}

Report: memo path, confirmed_issues count, and (filed URLs | would_file count + how many were dedup-skipped).`

const filer = confirmed.length
  ? await agent(filePrompt, { label: doFile ? 'file:gh' : 'file:dry-run', phase: 'File' })
  : 'no confirmed issues to file'

return {
  total_clusters: clusters.length,
  triage_breakdown: byClass,
  confirmed: confirmed.length,
  filed_mode: doFile ? 'gh-live' : 'dry-run',
  items: payload,
  filer_report: filer,
}

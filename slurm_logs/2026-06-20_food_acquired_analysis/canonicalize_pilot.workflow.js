export const meta = {
  name: 'food-acquired-canonicalize-pilot',
  description: 'Canonicalize+register food_acquired for Guatemala & Serbia (Phase 3 of #218): implement in worktree -> 3-lens adversarial review -> loop on refute -> leave pushed green branch for human PR',
  phases: [
    { title: 'Implement' },
    { title: 'Review' },
  ],
}

const REPO = '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library'
const PY = REPO + '/.venv/bin/python'
const BRIEF = REPO + '/slurm_logs/2026-06-20_food_acquired_analysis/BRIEF_pilot.md'
const FIXSPEC = REPO + '/slurm_logs/2026-06-20_food_acquired_analysis/BRIEF_guatemala_round3.md'
const BASE = 'development'
const MAX_ROUNDS = 2

// Round-3 re-run: Guatemala ONLY. Serbia already landed (PR #577). Maintainer
// decision on PR #578: record the ACTUAL RECALLED ACQUISITION (last-15-days
// window), NOT the usual/typical-month (p12a05/p12a09a) and NOT annualized via
// meses. Round-2's monthly basis was wrong for this intent; FIXSPEC encodes the
// uniform 15-day actual-recall build (purchased p12a06a/p12a06d, obtained
// p12a10a, s split via p12a11*, i/j fix) + the required CONTENTS.org note.
const COUNTRIES = [
  {
    name: 'Guatemala', lc: 'guatemala', wave: '2000',
    src: '2000/Data/ECV13G12.DTA',
    hint: 'i=hogar(household), j=item(food via food_items.org 2000 col). ACTUAL 15-DAY RECALL: purchased Quantity=p12a06a*p12a06c*cnlib, Expenditure=p12a06d (NOT monthly p12a05); obtained Quantity=p12a10a (15-day, NOT monthly p12a09a), Expenditure=NaN; s split via p12a11* into produced/inkind/other. Do NOT use p12a05/p12a09a/p12a04/p12a08. Add the CONTENTS.org recall-structure note. See FIXSPEC.',
  },
]

const IMPL_SCHEMA = {
  type: 'object',
  required: ['green', 'branch', 'summary', 'scope_deviations'],
  properties: {
    green: { type: 'boolean', description: 'true ONLY if every acceptance-bar item passed with a real verification run' },
    branch: { type: 'string', description: 'feature branch name, pushed to origin' },
    commit: { type: 'string' },
    worktree: { type: 'string' },
    summary: { type: 'string' },
    food_acquired_index: { type: 'array', items: { type: 'string' } },
    food_acquired_cols: { type: 'array', items: { type: 'string' } },
    n_rows: { type: 'integer' },
    n_i: { type: 'integer' }, n_j: { type: 'integer' },
    sane: { type: 'boolean' },
    derived_shapes: { type: 'string', description: 'shapes of food_expenditures/prices/quantities' },
    reconciliation: { type: 'string', description: 'canonical-vs-raw-.dta totals + distinct i/j vs source, with numbers' },
    scope_deviations: { type: 'array', items: { type: 'string' }, description: 'files touched outside countries/{country}/ ; [] preferred' },
  },
}

const VERDICT_SCHEMA = {
  type: 'object',
  required: ['lens', 'refuted', 'reason'],
  properties: {
    lens: { type: 'string' },
    refuted: { type: 'boolean', description: 'true = this build FAILS the lens (default to true if you cannot positively confirm)' },
    reason: { type: 'string' },
    evidence: { type: 'string', description: 'concrete command output / numbers' },
  },
}

function implPrompt(c, notes, round) {
  return `Canonicalize and register the \`food_acquired\` feature for **${c.name}** in the LSMS Library (Phase 3 of GH #218), then verify it builds green and push the branch. You are in a fresh git worktree.

FIRST, bring the worktree to the right base (idempotent):
    git -C "$(pwd)" fetch origin ${BASE} 2>&1 | tail -2
    git -C "$(pwd)" reset --hard origin/${BASE}
    git -C "$(pwd)" checkout -B feat/218-${c.lc}-food-acquired-canonical
    git -C "$(pwd)" log -1 --oneline

READ THE FULL SPEC (absolute paths, read BOTH now): ${BRIEF}
AND the round-3 build spec (AUTHORITATIVE, supersedes ALL prior period handling):
${FIXSPEC}
Together they contain the canonical target, the Cambodia #561 pattern, the
per-country specifics, the acceptance bar, the EXACT verification recipe
(LSMS_COUNTRIES_ROOT matters — your config edits are invisible without it),
the stop-list, and the required report format. FOLLOW THEM — especially the
ACTUAL 15-DAY RECALL build in the round-3 spec: purchased Quantity=p12a06a*
p12a06c*cnlib / Expenditure=p12a06d; obtained Quantity=p12a10a (15-day, NOT the
monthly p12a09a) / Expenditure=NaN; s split via p12a11*. Do NOT use the usual-
month (p12a05/p12a09a) or months-acquired (p12a04/p12a08) variables. Reconcile
purchased Expenditure against Σ p12a06d (=2,952,285), NOT p12a05. Add the
required CONTENTS.org recall-structure note.

Country specifics: source ${c.src}; ${c.hint}

${round > 1 ? 'PRIOR ROUND FEEDBACK (fix these):\n' + notes + '\n' : ''}
Deliverable (commit all to the feature branch):
- rewrite ${c.wave}/_/food_acquired.py -> canonical (t,i,j,u,s); FIX the i/j swap
- rewrite _/food_acquired.py -> simple wave concat (Cambodia pattern)
- add the food_acquired: block to _/data_scheme.yml (index (t,i,j,u,s), materialize: make)
- DELETE _/food_prices_quantities_and_expenditures.py

Verify with the BRIEF's recipe (PY=${PY}, set LSMS_COUNTRIES_ROOT to YOUR worktree's
lsms_library/countries, LSMS_NO_CACHE=1). All acceptance-bar items must pass.
Then: commit, and \`git -C "$(pwd)" push -u --force-with-lease origin feat/218-${c.lc}-food-acquired-canonical\` (the branch already has a prior round-1 commit on origin that this replaces).
Do NOT open a PR. Do NOT merge. Do NOT touch any other country or any library code.

Set green=true ONLY if you actually ran the verification and every bar item passed
(canonical index, sane, non-empty derived, i/j NOT swapped, source reconciliation).
Report per the BRIEF's mandatory format; populate every schema field.`
}

function reviewPrompt(c, impl, lens) {
  const common = `Adversarially review the canonicalized \`food_acquired\` for **${c.name}** on the pushed branch \`${impl.branch}\` (Phase 3 of GH #218). Your mandate is to REFUTE: find a reason this build is NOT acceptable. Default refuted=true if you cannot positively confirm. Specs: ${BRIEF} AND the AUTHORITATIVE round-3 build spec ${FIXSPEC}. CRITICAL: the intent is the ACTUAL 15-DAY RECALL, so reconcile purchased Expenditure against Σ p12a06d (=2,952,285), NOT the monthly p12a05; obtained uses p12a10a (15-day), not p12a09a. Do NOT refute for "dropping" rows that had no 15-day acquisition — that is correct, not data loss. Implementer's self-report: ${JSON.stringify(impl).slice(0, 1200)}.`
  if (lens === 'coldrepro') {
    return `${common}

LENS = COLD REPRODUCTION + SOURCE RECONCILIATION. You are in a fresh worktree.
    git -C "$(pwd)" fetch origin ${impl.branch} 2>&1 | tail -2
    git -C "$(pwd)" reset --hard origin/${impl.branch}
Then COLD-build from scratch (PY=${PY}, LSMS_COUNTRIES_ROOT=$(pwd)/lsms_library/countries, LSMS_NO_CACHE=1):
- Country('${c.name}').food_acquired(): confirm index has (t,i,j,u,s), cols ⊇ [Quantity,Expenditure], is_this_feature_sane.ok True.
- food_expenditures/prices/quantities are non-empty + canonical.
- Independently load the raw ${c.src} via lsms_library.local_tools.get_dataframe and recompute: purchased-value total, distinct household count, distinct item count. Compare to the canonical food_acquired. Confirm i is the household and j the item (n_i >> n_j).
refuted=true if the cold build is not green, reconciliation is materially off, or i/j look swapped. Put the actual numbers in evidence.`
  }
  if (lens === 'scope') {
    return `${common}

LENS = SCOPE / REGRESSION (no build needed). Run:
    git -C ${REPO} fetch origin ${impl.branch} 2>&1 | tail -1
    git -C ${REPO} diff origin/${BASE}...origin/${impl.branch} --stat
refuted=true if ANY file outside lsms_library/countries/${c.name}/ is touched; or
tests/baselines/lockfiles/pyproject/poetry.lock/any lsms_library/*.py changed; or
the legacy _/food_prices_quantities_and_expenditures.py was NOT deleted; or the
data_scheme.yml food_acquired block is malformed / wrongly registers derived
tables (food_expenditures/prices/quantities must NOT be registered). Evidence = the diff stat.`
  }
  // contract / source-truth
  return `${common}

LENS = CONTRACT / SOURCE-TRUTH (read, don't build). Inspect the branch's wave
script (origin/${impl.branch}:lsms_library/countries/${c.name}/${c.wave}/_/food_acquired.py)
and the raw ${c.src} variable labels (get_dataframe(...).head / Stata labels).
Judge: is i=household / j=item correct per the source variables? Is the s-split
(purchased vs produced) faithful to what the survey actually records? Is the unit
handling sound (no silent 10x scaling; native unit or documented lbs conversion)?
Is produced Expenditure=NaN justified (no obtained value in source)? refuted=true
if the canonicalization misreads the source semantics. Cite the source variables.`
}

async function canonicalize(c) {
  let notes = ''
  for (let round = 1; round <= MAX_ROUNDS; round++) {
    const impl = await agent(implPrompt(c, notes, round), {
      label: `impl:${c.lc}:r${round}`, phase: 'Implement',
      isolation: 'worktree', schema: IMPL_SCHEMA,
    })
    if (!impl) { notes = `Round ${round}: implementer returned no result.`; continue }
    const viol = (impl.scope_deviations || []).filter(s => s && s.toLowerCase() !== 'none')
    if (!impl.green || viol.length) {
      notes = `Round ${round} not green (green=${impl?.green}, scope_deviations=${JSON.stringify(viol)}): ${impl.summary}`
      log(`[${c.name}] round ${round}: impl not green -> ${notes.slice(0,160)}`)
      continue
    }
    const lenses = ['coldrepro', 'scope', 'contract']
    const verdicts = (await parallel(lenses.map(L => () =>
      agent(reviewPrompt(c, impl, L), {
        label: `review:${c.lc}:${L}:r${round}`, phase: 'Review',
        isolation: L === 'coldrepro' ? 'worktree' : undefined,
        schema: VERDICT_SCHEMA,
      })))).filter(Boolean)
    const refutes = verdicts.filter(v => v.refuted)
    log(`[${c.name}] round ${round}: ${refutes.length}/${verdicts.length} lenses refuted`)
    // STRICT gate (lesson from round 1): ANY lens refuting blocks. A single
    // well-evidenced source-truth/losslessness refutation must not be outvoted.
    if (refutes.length === 0) {
      return { country: c.name, green: true, round, branch: impl.branch, commit: impl.commit,
               impl, verdicts }
    }
    notes = `Round ${round} review refuted by ${refutes.length} lens(es): ` +
            refutes.map(r => `[${r.lens}] ${r.reason}`).join(' | ')
  }
  return { country: c.name, green: false, branch: `feat/218-${c.lc}-food-acquired-canonical`,
           note: notes }
}

const results = await parallel(COUNTRIES.map(c => () => canonicalize(c)))
log('=== PILOT DONE ===')
for (const r of results) {
  log(`${r.country}: ${r.green ? 'GREEN branch ' + r.branch : 'NOT GREEN — ' + (r.note || '').slice(0,200)}`)
}
return results

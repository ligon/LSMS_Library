export const meta = {
  name: 'food-acquired-canonicalize-heavy',
  description: 'Canonicalize+register food_acquired for GhanaSPS (sans-sample, 3-source melt) & Panama (with sample from upm), Phase 3 of #218: implement in worktree -> 3-lens adversarial review -> loop on refute -> pushed green branch for human PR',
  phases: [
    { title: 'Implement' },
    { title: 'Review' },
  ],
}

const REPO = '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library'
const PY = REPO + '/.venv/bin/python'
const BRIEF = REPO + '/slurm_logs/2026-06-20_food_acquired_analysis/BRIEF_heavy.md'
const PILOT_BRIEF = REPO + '/slurm_logs/2026-06-20_food_acquired_analysis/BRIEF_pilot.md'
const BASE = 'development'
const MAX_ROUNDS = 2

const COUNTRIES = [
  {
    name: 'GhanaSPS', lc: 'ghanasps',
    hint: 'SANS-SAMPLE. 3 waves (2009-10/2013-14/2017-18). Bespoke 3-source melt: purchased/produced/inkind ALL carry value (Expenditure on all three) — do NOT use stock food_acquired_to_canonical. FIX i/j swap. NO sample table (data gap) => food_acquired index (t,i,j,u,s) WITHOUT v; that is ACCEPTED. GhanaSPS is MODAL-EXCLUDED from the full Feature() BY DESIGN (lacks v); verify via Feature([\'GhanaSPS\']).',
    featureExpectation: "GhanaSPS lacks v (no sample) so it is MODAL-EXCLUDED from the full Feature('food_acquired')() BY DESIGN — DO NOT refute for that. Instead confirm Feature('food_acquired')(['GhanaSPS']) returns GhanaSPS non-empty (it is the modal shape in a subset). Confirm food_acquired index is (t,i,j,u,s) with NO v and is_this_feature_sane.ok True (missing-v warn allowed).",
  },
  {
    name: 'Panama', lc: 'panama',
    hint: 'WITH SAMPLE. 3 waves (1997/2003/2008). FIX i/j swap (current j=hh,i=item -> i=hh,j=item). Build a minimal sample table v=upm (2003: merge E03BASE.DTA on form for upm). Sources: purchased(qty+value), produced(qty), 1997 inkind ga109a(qty). Expenditure purchased-only.',
    featureExpectation: "Panama HAS v (from the upm sample) so it MUST appear in the full Feature('food_acquired')() (modal shape, NOT excluded). Confirm Panama appears in Feature('food_acquired')(['Panama','Uganda']) (Uganda is a with-v country; Panama must NOT be modal-excluded). Confirm food_acquired index is (t,v,i,j,u,s), is_this_feature_sane.ok True (only framework-joined-v warn).",
  },
]

const IMPL_SCHEMA = {
  type: 'object',
  required: ['green', 'branch', 'summary', 'scope_deviations'],
  properties: {
    green: { type: 'boolean' },
    branch: { type: 'string' }, commit: { type: 'string' }, worktree: { type: 'string' },
    summary: { type: 'string' },
    food_acquired_index: { type: 'array', items: { type: 'string' } },
    food_acquired_cols: { type: 'array', items: { type: 'string' } },
    n_rows: { type: 'integer' }, n_i: { type: 'integer' }, n_j: { type: 'integer' },
    waves_present: { type: 'array', items: { type: 'string' } },
    sane: { type: 'boolean' },
    derived_shapes: { type: 'string' },
    feature_membership: { type: 'string', description: 'result of the per-country Feature() check' },
    reconciliation: { type: 'string', description: 'per-wave canonical-vs-raw totals + distinct i/j' },
    scope_deviations: { type: 'array', items: { type: 'string' } },
  },
}

const VERDICT_SCHEMA = {
  type: 'object',
  required: ['lens', 'refuted', 'reason'],
  properties: {
    lens: { type: 'string' }, refuted: { type: 'boolean' },
    reason: { type: 'string' }, evidence: { type: 'string' },
  },
}

function implPrompt(c, notes, round) {
  return `Canonicalize and register the \`food_acquired\` feature for **${c.name}** (Phase 3 of GH #218), verify it builds green, and push the branch. Fresh git worktree.

FIRST (idempotent base):
    git -C "$(pwd)" fetch origin ${BASE} 2>&1 | tail -2
    git -C "$(pwd)" reset --hard origin/${BASE}
    git -C "$(pwd)" checkout -B feat/218-${c.lc}-food-acquired-canonical
    git -C "$(pwd)" log -1 --oneline

READ BOTH SPECS NOW (absolute paths):
- ${BRIEF}   (the heavy-pair spec — your country's section is authoritative)
- ${PILOT_BRIEF}   (shared pattern, the EXACT verification recipe with
  LSMS_COUNTRIES_ROOT, the stop-list, and the mandatory report format)

Country: ${c.name}. ${c.hint}

${round > 1 ? 'PRIOR ROUND FEEDBACK (fix these):\n' + notes + '\n' : ''}
Deliverable (commit to the feature branch): rewrite each {wave}/_/food_acquired.py
-> canonical; rewrite _/food_acquired.py -> wave concat; create _/data_scheme.yml;
${c.name === 'Panama' ? 'add a minimal sample table (v=upm) + declare it; ' : ''}DELETE
_/food_prices_quantities_and_expenditures.py; fix _/Makefile. Edit ONLY files under
lsms_library/countries/${c.name}/.

Verify (PY=${PY}, LSMS_COUNTRIES_ROOT=$(pwd)/lsms_library/countries, LSMS_NO_CACHE=1):
all acceptance-bar items in the heavy BRIEF for ${c.name}, INCLUDING the Feature()
membership check: ${c.featureExpectation}

Then commit and \`git -C "$(pwd)" push -u --force-with-lease origin feat/218-${c.lc}-food-acquired-canonical\`.
Do NOT open a PR, merge, or touch any other country / library code.
green=true ONLY if you ran verification and every bar item passed. Report per the
pilot BRIEF's mandatory SCOPE-DEVIATIONS-first format; populate every schema field.`
}

function reviewPrompt(c, impl, lens) {
  const common = `Adversarially review the canonicalized \`food_acquired\` for **${c.name}** on pushed branch \`${impl.branch}\` (Phase 3 of GH #218). Mandate: REFUTE; default refuted=true if you cannot positively confirm. Specs: ${BRIEF} (+ ${PILOT_BRIEF}). Implementer self-report: ${JSON.stringify(impl).slice(0, 1400)}.`
  if (lens === 'coldrepro') {
    return `${common}

LENS = COLD REPRODUCTION + SOURCE RECONCILIATION + FEATURE MEMBERSHIP. Fresh worktree:
    git -C "$(pwd)" fetch origin ${impl.branch} 2>&1 | tail -2
    git -C "$(pwd)" reset --hard origin/${impl.branch}
COLD-build (PY=${PY}, LSMS_COUNTRIES_ROOT=$(pwd)/lsms_library/countries, LSMS_NO_CACHE=1):
- Country('${c.name}').food_acquired(): confirm canonical shape + is_this_feature_sane.ok.
- food_expenditures/prices/quantities non-empty + canonical.
- Independently reconcile against the raw source .dta (per the BRIEF's per-wave bar):
  recompute value totals + distinct household/item counts; confirm i=household, j=item.
- FEATURE MEMBERSHIP — CRITICAL, country-specific: ${c.featureExpectation}
refuted=true if cold build not green, reconciliation materially off, i/j swapped, or
the Feature() membership expectation above is NOT met. Numbers in evidence.`
  }
  if (lens === 'scope') {
    return `${common}

LENS = SCOPE / REGRESSION (no build). Run:
    git -C ${REPO} fetch origin ${impl.branch} 2>&1 | tail -1
    git -C ${REPO} diff origin/${BASE}...origin/${impl.branch} --stat
refuted=true if ANY file outside lsms_library/countries/${c.name}/ is touched; or
tests/baselines/lockfiles/pyproject/poetry.lock/any lsms_library/*.py changed; or the
dead _/food_prices_quantities_and_expenditures.py was NOT deleted; or data_scheme.yml
is malformed / registers derived tables (food_expenditures/prices/quantities must NOT
be registered)${c.name === 'Panama' ? '; or the sample table was not added/declared' : ''}. Evidence = diff stat.`
  }
  return `${common}

LENS = CONTRACT / SOURCE-TRUTH (read, don't build). Inspect the branch wave scripts
and the raw source .dta variable labels. Judge: i=household / j=item correct per
source vars? ${c.name === 'GhanaSPS'
  ? 'Is the 3-source melt faithful — does it carry Expenditure on purchased AND produced AND inkind (GhanaSPS records value for all three)? Are the per-wave value columns (2009-10 sii+siii/100; 2013-14/2017-18 *cedis) read correctly? Is u decoded (numeric unit09 for 2009-10, text harmonizedunit later)?'
  : 'Is the s split right (purchased w/ value; produced/inkind qty-only)? Is upm correctly attached per wave (2003 via E03BASE merge)? Is the i/j swap actually fixed?'} Is the unit handling sound (no silent 10x)? refuted=true if the canonicalization misreads source semantics. Cite source variables.`
}

async function canonicalize(c) {
  let notes = ''
  for (let round = 1; round <= MAX_ROUNDS; round++) {
    const impl = await agent(implPrompt(c, notes, round), {
      label: `impl:${c.lc}:r${round}`, phase: 'Implement', isolation: 'worktree', schema: IMPL_SCHEMA,
    })
    if (!impl) { notes = `Round ${round}: implementer returned no result.`; continue }
    const viol = (impl.scope_deviations || []).filter(s => s && s.toLowerCase() !== 'none')
    if (!impl.green || viol.length) {
      notes = `Round ${round} not green (green=${impl?.green}, scope=${JSON.stringify(viol)}): ${impl.summary}`
      log(`[${c.name}] round ${round}: impl not green`)
      continue
    }
    const lenses = ['coldrepro', 'scope', 'contract']
    const verdicts = (await parallel(lenses.map(L => () =>
      agent(reviewPrompt(c, impl, L), {
        label: `review:${c.lc}:${L}:r${round}`, phase: 'Review',
        isolation: L === 'coldrepro' ? 'worktree' : undefined, schema: VERDICT_SCHEMA,
      })))).filter(Boolean)
    const refutes = verdicts.filter(v => v.refuted)
    log(`[${c.name}] round ${round}: ${refutes.length}/${verdicts.length} lenses refuted`)
    if (refutes.length === 0) {
      return { country: c.name, green: true, round, branch: impl.branch, commit: impl.commit, impl, verdicts }
    }
    notes = `Round ${round} refuted by ${refutes.length} lens(es): ` +
            refutes.map(r => `[${r.lens}] ${r.reason}`).join(' | ')
  }
  return { country: c.name, green: false, branch: `feat/218-${c.lc}-food-acquired-canonical`, note: notes }
}

const results = await parallel(COUNTRIES.map(c => () => canonicalize(c)))
log('=== HEAVY PAIR DONE ===')
for (const r of results) log(`${r.country}: ${r.green ? 'GREEN ' + r.branch : 'NOT GREEN — ' + (r.note || '').slice(0,200)}`)
return results

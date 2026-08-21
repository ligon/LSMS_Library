export const meta = {
  name: 'food-acquired-review-heavy',
  description: 'Adversarial 3-lens review of the already-pushed GhanaSPS & Panama food_acquired branches (the heavy-pair impl ran but its review was skipped by a scope-gate bug). Cold-repro+reconcile+feature-membership+recall-fidelity, scope, contract. Strict 0-refute = green.',
  phases: [{ title: 'Review' }],
}

const REPO = '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library'
const PY = REPO + '/.venv/bin/python'
const BRIEF = REPO + '/slurm_logs/2026-06-20_food_acquired_analysis/BRIEF_heavy.md'
const PILOT_BRIEF = REPO + '/slurm_logs/2026-06-20_food_acquired_analysis/BRIEF_pilot.md'
const BASE = 'development'

const COUNTRIES = [
  {
    name: 'GhanaSPS', lc: 'ghanasps',
    branch: 'feat/218-ghanasps-food-acquired-canonical',
    featureExpectation: "GhanaSPS lacks v (no sample, by maintainer decision) so it is MODAL-EXCLUDED from the full Feature('food_acquired')() BY DESIGN — DO NOT refute for that. Confirm Feature('food_acquired')(['GhanaSPS']) returns GhanaSPS non-empty. food_acquired index (t,i,j,u,s) with NO v; is_this_feature_sane.ok True.",
    recallNote: "GhanaSPS recall is 'since last visit' (actual inter-visit acquisition) — actual by construction, fine. FOCUS instead on: (a) all THREE sources (purchased/produced/inkind) carry their own Quantity AND Expenditure; (b) the 2009-10 value is cedis + pesewas/100 with the pesewa fraction fillna(0) (an earlier round NaN'd out present-cedi/absent-pesewa rows, dropping ~74% of value) — confirm value reconciliation is now exact on ALL 3 waves; (c) u decoded (2009-10 numeric s11a_f->unit09; later waves text).",
    implSummary: "SANS-SAMPLE 3-source melt; i/j swap fixed (i=hh, j=item); index (i,t,j,u,s) no v; sane.ok True zero warns; n_i 6348 >> n_j 94; 3 waves; s={inkind,produced,purchased}; exact value reconciliation all 3 waves after the cedi+pesewa fillna(0) fix; Feature(['GhanaSPS']) non-empty; commit 2a04ed36.",
  },
  {
    name: 'Panama', lc: 'panama',
    branch: 'feat/218-panama-food-acquired-canonical',
    featureExpectation: "Panama HAS v (from the upm sample built from cover files) so it MUST appear in the full Feature('food_acquired')() (modal shape, NOT excluded). Confirm Panama appears in Feature('food_acquired')(['Panama','Uganda']). food_acquired index (t,v,i,j,u,s), is_this_feature_sane.ok True.",
    recallNote: "RECALL FIDELITY IS THE KEY CHECK for Panama. Verify BOTH sides use the ACTUAL recall window and NO usual/typical-period or double-counted bucket: (a) the non-purchased split uses the 15-day quantity (ga110a/gai10a/s11a10a) split by the source FLAGS (own-prod->produced, gift->inkind, payment+business->other), NOT the lumped monthly bucket; (b) the monthly bucket (ga109a/gai09a/s11a9a) is NOT also summed in (round-1 double-counted 1997). CRITICAL: confirm the PURCHASED side (ga106a/gai06a/s11a6a qty, ga106c/... value) is ALSO an actual-recall figure, not a usual/typical-month estimate — inspect the source variable labels and report the purchased recall period. If purchased uses a typical-period variable, REFUTE.",
    implSummary: "WITH SAMPLE (v=upm from cover files: 1997 INFORM.DTA, 2003 E03BASE.DTA +weight, 2008 02hogar.dta +weight); i/j swap fixed; source-FLAG s-split (produced/inkind/other); 15-day quantity used, monthly bucket deliberately excluded (no double-count); value-only u='Value' convention for some items; added 2003 'Condimentos' to food_items.org; 12 files; commit a0ba3e10.",
  },
]

const VERDICT_SCHEMA = {
  type: 'object',
  required: ['lens', 'refuted', 'reason'],
  properties: {
    lens: { type: 'string' }, refuted: { type: 'boolean' },
    reason: { type: 'string' }, evidence: { type: 'string' },
  },
}

function reviewPrompt(c, lens) {
  const common = `Adversarially review the canonicalized \`food_acquired\` for **${c.name}** on pushed branch \`${c.branch}\` (Phase 3 of GH #218). Mandate: REFUTE; default refuted=true if you cannot positively confirm. Specs: ${BRIEF} (+ ${PILOT_BRIEF}). Implementer self-report: ${c.implSummary}`
  if (lens === 'coldrepro') {
    return `${common}

LENS = COLD REPRODUCTION + SOURCE RECONCILIATION + FEATURE MEMBERSHIP + RECALL FIDELITY. Fresh worktree:
    git -C "$(pwd)" fetch origin ${c.branch} 2>&1 | tail -2
    git -C "$(pwd)" reset --hard origin/${c.branch}
COLD-build (PY=${PY}, LSMS_COUNTRIES_ROOT=$(pwd)/lsms_library/countries, LSMS_NO_CACHE=1):
- Country('${c.name}').food_acquired(): confirm canonical shape + is_this_feature_sane.ok.
- food_expenditures/prices/quantities non-empty + canonical.
- Independently reconcile against the raw source .dta per wave: recompute value totals +
  distinct household/item counts; confirm i=household, j=item (n_i >> n_j).
- RECALL FIDELITY: ${c.recallNote}
- FEATURE MEMBERSHIP: ${c.featureExpectation}
refuted=true if cold build not green, reconciliation materially off, i/j swapped, a
usual/typical-period or double-counted figure is used, or the Feature() expectation is
unmet. Put numbers in evidence.`
  }
  if (lens === 'scope') {
    return `${common}

LENS = SCOPE / REGRESSION (no build; THIS is the authoritative scope check). Run:
    git -C ${REPO} fetch origin ${c.branch} 2>&1 | tail -1
    git -C ${REPO} diff origin/${BASE}...origin/${c.branch} --stat
    git -C ${REPO} diff origin/${BASE}...origin/${c.branch} --name-only | grep -v '^lsms_library/countries/${c.name}/' || true
refuted=true ONLY if a COMMITTED file outside lsms_library/countries/${c.name}/ appears in
the diff; or tests/baselines/lockfiles/pyproject/poetry.lock/any lsms_library/*.py changed;
or the dead _/food_prices_quantities_and_expenditures.py was NOT deleted; or data_scheme.yml
registers derived tables (food_expenditures/prices/quantities must NOT be registered)${c.name === 'Panama' ? '; or the sample table is missing/undeclared' : ''}.
(Ignore uncommitted/untracked working-tree noise — only the committed diff matters.) Evidence = diff.`
  }
  return `${common}

LENS = CONTRACT / SOURCE-TRUTH (read, don't build). Inspect the branch wave scripts +
raw source .dta variable LABELS. Judge: i=household / j=item correct per source vars?
${c.name === 'GhanaSPS'
  ? 'Does the melt carry Expenditure on purchased AND produced AND inkind (GhanaSPS records value for all three)? Are per-wave value columns read correctly (2009-10 cedis + pesewas/100 with pesewa fillna(0); later *cedis)? Is u decoded right?'
  : 'Is the s split driven by the source FLAGS (not the lumped bucket)? Is upm attached per wave (cover-file merges)? Is the i/j swap fixed? '}${c.recallNote} Is unit handling sound (no silent 10x)? refuted=true if the canonicalization misreads source semantics or violates recall fidelity. Cite source variables.`
}

async function reviewCountry(c) {
  const lenses = ['coldrepro', 'scope', 'contract']
  const verdicts = (await parallel(lenses.map(L => () =>
    agent(reviewPrompt(c, L), {
      label: `review:${c.lc}:${L}`, phase: 'Review',
      isolation: L === 'coldrepro' ? 'worktree' : undefined, schema: VERDICT_SCHEMA,
    })))).filter(Boolean)
  const refutes = verdicts.filter(v => v.refuted)
  log(`[${c.name}] ${refutes.length}/${verdicts.length} lenses refuted`)
  return { country: c.name, branch: c.branch, green: refutes.length === 0, verdicts }
}

const results = await parallel(COUNTRIES.map(c => () => reviewCountry(c)))
log('=== HEAVY REVIEW DONE ===')
for (const r of results) log(`${r.country}: ${r.green ? 'GREEN (0 refutes)' : r.verdicts.filter(v=>v.refuted).length + ' refuted'}`)
return results

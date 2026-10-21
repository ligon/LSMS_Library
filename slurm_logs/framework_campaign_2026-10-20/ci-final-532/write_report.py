"""Write aggregate-only reports after final private CI reconciliation."""
import json
from pathlib import Path

ROOT = Path('/local/job40255479/control/framework-campaign/followup-ci-review')


def report(folder):
    verification = json.loads((folder / 'verification.json').read_text())
    run = json.loads((folder / 'run.json').read_text())
    jobs = json.loads((folder / 'jobs.json').read_text())['jobs']
    lines = [
        '#+TITLE: Exact-head remote CI reconciliation',
        '#+AUTHOR: Sue',
        '',
        '* Result',
        f"Run {run['id']} at ={run['head_sha']}= concluded ={run['conclusion']}=.",
        f"Actual data-job checkout: ={verification['actual_checkout_shas'][0]}=.",
        f"Credentialed data job {verification['job_id']} concluded ={verification['job_conclusion']}=.",
        f"Pytest totals: ={json.dumps(verification['pytest_totals'], sort_keys=True)}=.",
        f"Collected items: ={verification['collected_items']}=; suite duration {verification['pytest_seconds']} seconds.",
        '',
        '* Executed nutrition case and credentials',
        f"Nutrition ordered terminal marker: ={verification['nutrition_case']['terminal_marker']}= at zero-based case {verification['nutrition_case']['sorted_case_position_zero_based']} of {verification['nutrition_case']['manifest_case_count']}.",
        f"Unique full ordered terminal groups: {verification['nutrition_case']['ordered_terminal_group_count']}; actual execution and pass: {verification['nutrition_case']['actually_executed_and_passed']}.",
        verification['nutrition_case']['basis'],
        f"Credential gate passed: {verification['credential_gate_passed']}; Uganda build passed: {verification['uganda_build_passed']}.",
        f"Runtime credential failure/skip evidence counts: ={json.dumps(verification['credential_skip_evidence_counts'], sort_keys=True)}=.",
        verification['credential_evidence_limit'],
        'No actual runner BLAS kernel observation is claimed; the earlier numerical explanation is an exact controlled local counterfactual.',
        '',
        '* Comparison with the previous exact-852 failure',
        f"Previous totals: ={json.dumps(verification['previous_852'], sort_keys=True)}=.",
        f"Count differences: ={json.dumps(verification['delta_vs_previous_852'], sort_keys=True)}=; collected difference: {verification['collected_delta_vs_852']}.",
        f"Failed node count: {len(verification['failed_nodeids'])}.",
        'Changed test assets are enumerated in verification.json; no totals from different heads are combined.',
        '',
        '* Job conclusions',
    ]
    for job in jobs:
        lines.append(f"- {job['name']}: ={job['conclusion']}= (job {job['id']}).")
    lines += [
        '',
        '* Reproduction and limits',
        'Read-only GET observation and the final completed job log are retained privately.',
        'The final log was saved once after it became available; raw log contents and survey rows were not printed or copied into this report.',
        'Run =reconcile.py= with the mounted image Python, then =write_report.py= from this private report directory.',
        'A successful workflow_dispatch is exact-head manual evidence; it is not a genuine PR-event data check and does not change repository rules.',
        'The 19 retained source/wiring failures and five tenure audit findings remain separate release-readiness limitations.',
        'No source, fixtures, caches, credentials, repository settings, or GitHub state were mutated by this observation.',
    ]
    (folder / 'REPORT.org').write_text('\n'.join(lines) + '\n')
    return verification


completed = {}
for label, folder in [('4cab', ROOT), ('532', ROOT / 'rate-532')]:
    if (folder / 'verification.json').exists():
        completed[label] = report(folder)

if len(completed) == 2:
    old, new = completed['4cab'], completed['532']
    result = {
        'heads': {key: value['run_head'] for key, value in completed.items()},
        'run_ids': {key: value['run_id'] for key, value in completed.items()},
        'job_conclusions': {key: value['job_conclusion'] for key, value in completed.items()},
        'pytest_totals': {key: value['pytest_totals'] for key, value in completed.items()},
        'count_deltas_532_minus_4cab': {
            key: new['pytest_totals'][key] - old['pytest_totals'][key]
            for key in old['pytest_totals']
        },
        'collected_delta_532_minus_4cab': new['collected_items'][-1] - old['collected_items'][-1],
        'nutrition_executed_and_passed': {
            key: value['nutrition_case']['actually_executed_and_passed']
            for key, value in completed.items()
        },
        'credential_gate_passed': {key: value['credential_gate_passed'] for key, value in completed.items()},
        'separate_readiness_limits': '19 source/wiring errors and five tenure findings retained; no semantic-audit clearance implied',
        'scope': 'Independent exact-head manual runs, kept separate; no PR-check eligibility claim',
    }
    (ROOT / 'two-head-comparison.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
print(json.dumps({'reports_written_for': sorted(completed)}))

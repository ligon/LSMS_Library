"""Aggregate paired frozen-scanner findings without exposing diagnostic rows."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path

root = Path('/local/job40255479/canonical-audit-compare')
results = {}
for label in ['baseline', 'combined']:
    completion = json.loads((root / f'{label}-completion.json').read_text())
    assert completion['returncode'] == 0
    assert completion['controller_unchanged'] and completion['runtime_head_unchanged']
    records = [json.loads(line) for line in (root / f'{label}-raw.jsonl').read_text().splitlines()]
    results[label] = records

def identity(record):
    return json.dumps({k: record.get(k) for k in
                       ['phase', 'feature', 'country', 'kwargs', 'check', 'status', 'severity', 'fingerprint']},
                      sort_keys=True)

def warning_counts(records):
    return Counter(identity(r) for r in records if r['status'] == 'warn')

def describe_counts(counter):
    return [dict(json.loads(key), count=count) for key, count in sorted(counter.items())]

def summarize(records):
    failures = [r for r in records if r['status'] in {'fail', 'error'}]
    return {
        'records': len(records),
        'status_counts': dict(Counter(r['status'] for r in records)),
        'severity_counts': dict(Counter(r.get('severity') or ('skip' if r['status'] == 'skip' else 'none')
                                        for r in records)),
        'by_feature': {feature: {
            'records': sum(r['feature'] == feature for r in records),
            'status_counts': dict(Counter(r['status'] for r in records if r['feature'] == feature)),
        } for feature in sorted({r['feature'] for r in records})},
        'skip_cells': sorted([{'feature': r['feature'], 'reason': r['reason']}
                              for r in records if r['status'] == 'skip'],
                             key=lambda r: (r['feature'], r['reason'])),
        'error_fail_pairs': describe_counts(Counter(identity(r) for r in failures)),
    }

baseline, combined = results['baseline'], results['combined']
warnings_a, warnings_b = map(warning_counts, [baseline, combined])
errors_a = Counter(identity(r) for r in baseline if r['status'] in {'fail', 'error'})
errors_b = Counter(identity(r) for r in combined if r['status'] in {'fail', 'error'})
assembly_checks = {'assembly_build', 'assembly_nonempty', 'index_unnamed_level',
                   'missing_country_level', 'missing_canonical_level', 'country_exclusion',
                   'row_conservation', 'all_nan_column', 'market_adds_m'}

def assembly_counts(records):
    return Counter(json.dumps({**json.loads(identity(r)), 'metrics': r.get('metrics', {})}, sort_keys=True)
                   for r in records if r.get('check') in assembly_checks and r['status'] != 'pass')

assembly_a, assembly_b = map(assembly_counts, [baseline, combined])
completion_a = json.loads((root / 'baseline-completion.json').read_text())
completion_b = json.loads((root / 'combined-completion.json').read_text())
assert completion_a['scanner_sha256'] == completion_b['scanner_sha256']
report = {
    'same_scanner_sha256': completion_a['scanner_sha256'],
    'baseline': summarize(baseline), 'combined': summarize(combined),
    'warning_fingerprint_added_or_increased': describe_counts(warnings_b - warnings_a),
    'warning_fingerprint_removed_or_decreased': describe_counts(warnings_a - warnings_b),
    'new_error_fail_pairs': describe_counts(errors_b - errors_a),
    'removed_error_fail_pairs': describe_counts(errors_a - errors_b),
    'new_or_changed_assembly_findings': describe_counts(assembly_b - assembly_a),
    'removed_or_changed_assembly_findings': describe_counts(assembly_a - assembly_b),
    'scope_skips_identical': summarize(baseline)['skip_cells'] == summarize(combined)['skip_cells'],
    'no_new_assembly_findings': not bool(assembly_b - assembly_a),
    'raw_findings_private': True,
}
(root / 'audit-comparison.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
print(json.dumps({k: report[k] for k in ['scope_skips_identical', 'no_new_assembly_findings',
                                        'new_error_fail_pairs', 'removed_error_fail_pairs']}, indent=2))

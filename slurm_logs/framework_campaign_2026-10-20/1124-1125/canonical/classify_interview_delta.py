"""Classify audit warning delta with independent cold and local cache controls."""
from collections import Counter, defaultdict
import json
from pathlib import Path
import re

import pandas as pd

root = Path('/local/job40255479/canonical-audit-compare')
def norm(message):
    for value in ['/local/job40255479/worktrees/baseline-976fc840d',
                  '/local/job40255479/worktrees/framework-integration',
                  str(root / 'data-baseline'), str(root / 'data-combined'),
                  '/local/job40255479/corpus-baseline-data',
                  '/local/job40255479/corpus-combined-data']:
        message = message.replace(value, '<ROOT>')
    return message

rows = {label: [r for r in map(json.loads, (root / f'{label}-raw.jsonl').read_text().splitlines())
                if r['feature'] == 'interview_date'] for label in ['baseline', 'combined']}
assert all(rows.values())
warnings = {label: Counter(norm(r['detail']) for r in records if r.get('check') == 'runtime_warning')
            for label, records in rows.items()}
lookup = defaultdict(set)
cold = {}
for label in ['baseline', 'combined']:
    cold[label] = {}
    for path in Path(f'/local/job40255479/corpus-{label}-out').glob('*/interview_date-warnings.json'):
        country = path.parent.name
        messages = json.loads(path.read_text())
        cold[label][country] = Counter((r['category'], norm(r['message'])) for r in messages)
        for r in messages:
            lookup[norm(r['message'])].add((country, r['category']))

added = warnings['combined'] - warnings['baseline']
removed = warnings['baseline'] - warnings['combined']
families = defaultdict(lambda: {'count': 0, 'countries': Counter(), 'cold_message_matched': 0})
for message, count in added.items():
    if 'Parsing dates in %d %m %Y' in message:
        family = 'date_format_dayfirst_warning'
    elif 'could not be decoded using utf-8' in message:
        family = 'stata_utf8_fallback_warning'
    elif message.startswith('Cache miss for '):
        family = 'expected_stale_cache_rebuild_notice'
    elif message.startswith('Makefile not found in '):
        family = 'makefile_missing_fallback_warning'
    elif message.startswith('Issue with categorical mapping:'):
        family = 'categorical_mapping_warning'
    else:
        family = 'unclassified'
    item = families[family]
    item['count'] += count
    matches = lookup[message]
    if matches:
        assert len({country for country, _ in matches}) == 1
        item['countries'][next(iter(matches))[0]] += count
        item['cold_message_matched'] += count
    elif family == 'expected_stale_cache_rebuild_notice':
        country = re.match(r'Cache miss for (.*?)/interview_date ', message).group(1)
        item['countries'][country] += count

cache_comparisons = []
paths = {label: {p.parent.parent.name: p for p in (root / f'data-{label}').glob('*/var/interview_date.parquet')}
         for label in ['baseline', 'combined']}
assert paths['baseline'].keys() == paths['combined'].keys()
for country in sorted(paths['baseline']):
    a, b = [pd.read_parquet(paths[label][country]) for label in ['baseline', 'combined']]
    pd.testing.assert_frame_equal(a, b, check_exact=True)
    cache_comparisons.append({'country': country, 'rows': len(a), 'exact_frame_equal': True})

def identity(row):
    return json.dumps({k: row.get(k) for k in ['phase','feature','country','kwargs','check','status','severity']},sort_keys=True)
remaining = {label: Counter(identity(r) for r in rs if r.get('check') != 'runtime_warning') for label,rs in rows.items()}
class_b = {label: Counter(norm(r['detail']) for r in rs if r.get('severity') == 'B') for label,rs in rows.items()}
report = {
    'baseline_records_including_scope_skip': len(rows['baseline']),
    'combined_records_including_scope_skip': len(rows['combined']),
    'added_runtime_warnings': sum(added.values()), 'removed_runtime_warnings': sum(removed.values()),
    'warning_families': {key: {**value, 'countries': dict(value['countries'])} for key,value in families.items()},
    'non_runtime_checks_identical': remaining['baseline'] == remaining['combined'],
    'class_b_warning_messages_identical': class_b['baseline'] == class_b['combined'],
    'independent_cold_warning_messages_and_categories_identical_by_country': cold['baseline'] == cold['combined'],
    'local_audit_l2_country_frames': cache_comparisons,
    'local_audit_l2_country_frames_exactly_equal': True,
    'raw_messages_and_identifiers_private': True,
    'interpretation': 'Candidate rebuilds invalidated frozen-bundle caches; harness captures every repeated emitted warning. These emitted warning counts are real, while exact local cached frames, structural checks and matched cold warning controls are unchanged.',
}
assert 'unclassified' not in families
assert report['non_runtime_checks_identical'] and report['class_b_warning_messages_identical']
assert report['independent_cold_warning_messages_and_categories_identical_by_country']
(root/'interview-delta-classification.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in {'interpretation','local_audit_l2_country_frames'}},indent=2,sort_keys=True))

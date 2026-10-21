"""Summarize synthetic reducer probes, downstream control and hash reach."""
import argparse
import hashlib
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--private-root', type=Path, required=True)
parser.add_argument('--out', type=Path, required=True)
args = parser.parse_args()
root = args.private_root
before = json.loads((root / 'probe-before.json').read_text())
after = json.loads((root / 'probe-after-final.json').read_text())
a = json.loads((root / 'hash-before.json').read_text())
b = json.loads((root / 'hash-after-final.json').read_text())
assert before['downstream'] == after['downstream']
hashes = {}
for tier in ['country', 'wave', 'tagged']:
    assert a[tier].keys() == b[tier].keys()
    changed = [key for key in a[tier] if a[tier][key] != b[tier][key]]
    assert not changed, (tier, changed)
    hashes[tier] = {'total': len(a[tier]), 'changed': len(changed), 'changed_cells': changed}
assert not a['errors'] and not b['errors']
for axis in ['index', 'column']:
    assert before['reducers'][axis]['seed_kg']['rows'] == 1
    assert before['reducers'][axis]['nitrogen_kg']['rows'] == 1
    assert after['reducers'][axis]['seed_kg']['by_country'] == {'A': 1., 'B': 2.}
    assert after['reducers'][axis]['nitrogen_kg']['by_country'] == {'A': .46, 'B': .92}
report = {
    'baseline_runtime_commit': '852ac2bfcd5f798533f56e7efa1f77db80c0138f',
    'probe_before': before, 'probe_after': after,
    'downstream_fertilizer_rate_exactly_unchanged': True,
    'downstream_pooled_country_omission_is_retained_residual': True,
    'cache_hash_reach': hashes,
    'private_hash_evidence_sha256': {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in ['hash-before.json', 'hash-after-final.json']},
    'final_targeted_tests': (root / 'final-targeted-tests.log').read_text().strip().splitlines()[-1],
    'untouched_baseline_negative_control': (root / 'final-baseline-negative.log').read_text().strip().splitlines()[-1],
    'cache_gated_skips': {
        'tests/test_nigeria_sold_unit.py::TestBuiltTable::test_unit_sold_populated_only_in_w1_w2':
            'Private Nigeria crop_production parquet is absent; source guard skips before any data/API read.',
        'tests/test_nigeria_sold_unit.py::TestBuiltTable::test_api_frame_serves_unit_sold_through_finalize':
            'Private Nigeria crop_production parquet is absent; source guard skips before any data/API read.'},
    'synthetic_data_only': True,
}
args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
print(json.dumps({'cache_hash_reach': hashes, 'downstream_unchanged': True,
                  'tests': report['final_targeted_tests'],
                  'negative_control': report['untouched_baseline_negative_control']}, indent=2))

"""Read-only comparison of frozen parquet metadata against the unchanged test manifest."""
import argparse
import importlib.util
import json
from pathlib import Path

import pandas as pd
import lsms_library

parser = argparse.ArgumentParser()
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--base', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
assert Path(lsms_library.__file__).is_relative_to(args.root), lsms_library.__file__
test_path = args.root / 'tests/test_uganda_invariance.py'
spec = importlib.util.spec_from_file_location('invariance_diagnostic', test_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert not list((args.root / 'lsms_library/countries/Uganda').rglob('*.parquet'))
fields = ['shape', 'columns', 'index_names', 'dtypes', 'content_hash']
results = {}
for rel, expected in sorted(module.BASELINE.items()):
    entry = {'fixture': expected}
    for label, data_dir in [('base', args.base), ('candidate', args.candidate)]:
        path = data_dir / 'Uganda' / rel
        if not path.exists():
            entry[label] = {'status': 'absent'}
            continue
        before = path.stat()
        actual = module._fingerprint(pd.read_parquet(path))
        after = path.stat()
        assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
        failed = []
        for field in fields:
            if expected.get(field) is None:
                continue
            left, right = actual[field], expected[field]
            if field == 'dtypes':
                left, right = map(module._canonical_dtypes, (left, right))
            if left != right:
                failed.append(field)
        entry[label] = {'status': 'fail' if failed else 'pass',
                        'different_fields': failed, 'fingerprint': actual}
    if all(entry[key]['status'] != 'absent' for key in ('base', 'candidate')):
        entry['candidate_vs_base_fields'] = [
            field for field in fields
            if entry['base']['fingerprint'][field] != entry['candidate']['fingerprint'][field]]
    results[rel] = entry
summary = {label: {status: sum(entry[label]['status'] == status for entry in results.values())
                   for status in ('pass', 'fail', 'absent')}
           for label in ('base', 'candidate')}
summary['base_failed'] = [rel for rel, entry in results.items() if entry['base']['status'] == 'fail']
summary['candidate_failed'] = [rel for rel, entry in results.items() if entry['candidate']['status'] == 'fail']
summary['new_failure_paths'] = sorted(set(summary['candidate_failed']) - set(summary['base_failed']))
summary['resolved_failure_paths'] = sorted(set(summary['base_failed']) - set(summary['candidate_failed']))
args.output.write_text(json.dumps({'summary': summary, 'entries': results}, indent=2))
print(json.dumps(summary, indent=2))

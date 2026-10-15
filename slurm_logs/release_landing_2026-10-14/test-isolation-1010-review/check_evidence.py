"""Read only source/git objects and saved evidence; never import the library."""
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
SRC = Path('/local/job39919092/release-work/framework/collectonly-env-isolation')
RAW = Path('/local/job39919092/release-work/control/framework/ci-ghana-repeat-build')
PUB = Path('/local/job39919092/release-work/control/framework/1010-publishable')
BASE = 'cf1840e02317bac052b99e1e6c9fa066a8ca45bc'
FIX = '22a841ed8940c618680c5b4363fcb60e455b4a14'
PORT = 'f00f329b37a793ec7ef17970431e5f4f6478dc4d'

def git(*args):
    return subprocess.check_output(['git', '-C', str(SRC), *args]).decode()

def digest(path):
    data = path.read_bytes()
    return {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}

assert git('rev-parse', 'HEAD').strip() == FIX
assert not git('status', '--porcelain').strip()
changed = git('diff', '--name-only', BASE, FIX).splitlines()
assert changed == ['.coder/ledger/ci-ghana-repeat-build.md', 'tests/test_collectonly_no_purge.py']
path = 'tests/test_collectonly_no_purge.py'
old = ast.parse(git('show', f'{BASE}:{path}'))
new = ast.parse(git('show', f'{FIX}:{path}'))
old_functions = {n.name: n for n in old.body if isinstance(n, ast.FunctionDef)}
new_functions = {n.name: n for n in new.body if isinstance(n, ast.FunctionDef)}
for name, node in old_functions.items():
    if name != '_run':
        assert ast.dump(node) == ast.dump(new_functions[name]), name
new_run = new_functions['_run']
extra = [n for n in new_run.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
         and isinstance(n.value.func, ast.Attribute) and n.value.func.attr == 'setenv']
assert len(extra) == 1
assert ast.literal_eval(extra[0].value.args[0]) == 'LSMS_NO_CACHE'
assert ast.literal_eval(extra[0].value.args[1]) == ''
new_run.body.remove(extra[0])
assert ast.dump(old_functions['_run']) == ast.dump(new_run)
assert git('show', f'{PORT}:{path}') == git('show', f'{FIX}:{path}')
for unchanged in ('conftest.py', 'tests/test_currency_denominated_units.py'):
    assert git('show', f'{BASE}:{unchanged}') == git('show', f'{FIX}:{unchanged}')

evidence_hashes = json.loads((PUB/'private-evidence-hashes.json').read_text())
for name, expected in evidence_hashes.items():
    actual = digest(RAW/expected.get('private_snapshot_file', name))
    assert actual == {key: expected[key] for key in ('bytes', 'sha256')}, name
assert '4 failed, 8 passed, 6 deselected' in (RAW/'regression-before.log').read_text()
assert '18 passed' in (RAW/'regression-after.log').read_text()
assert '19 passed, 31 warnings in 112.26s' in (RAW/'exact-selection-after.log').read_text()

expected_hash = 'c5f41b5cd6651509b55c11430793a07b26ba38654c1f7dc043308f0f1bd3c904'
expected_waves = ['1987-88','1988-89','1991-92','1998-99','2005-06','2012-13','2016-17']
comparison = json.loads((PUB/'comparison.json').read_text())
event_checks = {}
for prefix in ('isolated', 'ordered-before', 'ordered-after', 'exact-selection-after'):
    name = prefix+'-events.jsonl'
    expected = evidence_hashes[name]
    records = [json.loads(line) for line in (RAW/expected.get('private_snapshot_file', name)).read_text().splitlines()]
    counts = Counter(row['event'] for row in records)
    snapshots = [row['snapshot'] for row in records if 'snapshot' in row]
    assert all(s['expected'] == s['stored'] == expected_hash for s in snapshots)
    assert all(s['manifest'] == expected_waves for s in snapshots)
    assert len({s['root'] for s in snapshots}) == 1
    calculated_calls = [{k: row[k] for k in ('api','units','seconds','rows') if k in row}
                        for row in records if row['event'] == 'api_end']
    assert calculated_calls == comparison[prefix]['calls']
    assert counts['build_start'] == comparison[prefix]['build_starts']
    assert counts['build_end'] == comparison[prefix]['build_completions']
    if prefix == 'ordered-before':
        assert next(r['snapshot']['NO_CACHE'] for r in records if r['event'] == 'after_preceding_suite') == '1'
        assert not any(r['event'] == 'replay_passed' for r in records)
    else:
        assert counts['build_start'] == 0
        assert records[-1]['snapshot']['NO_CACHE'] is None
    if prefix == 'exact-selection-after':
        calls = [r for r in records if r['event'] == 'test_report' and r['when'] == 'call']
        teardowns = [r for r in records if r['event'] == 'test_report' and r['when'] == 'teardown']
        assert len(calls) == len(teardowns) == 19
        assert all(r['outcome'] == 'passed' for r in calls+teardowns)
        assert all(r['snapshot']['NO_CACHE'] is None for r in teardowns)
        assert calls[-1]['node'].endswith('::test_ghanalss_unitvalue_and_units_modes_are_unaffected')
        assert records[-1]['exitstatus'] == 0
    event_checks[prefix] = {'records': len(records), 'events': dict(counts),
                            'hash_and_manifest_stable': True, 'calls': calculated_calls}

copy = json.loads((PUB/'copy-identity.json').read_text())
assert digest(RAW/'copy-manifest.json')['sha256'] == copy['manifest_sha256']
assert copy['files'] == 88 and copy['bytes'] == 60298616 and copy['writable_L2_links'] == 0
# Do not independently reopen source caches: inspect recorded identity proof only.
out = {'scope': 'read-only saved evidence/source review; no APIs, cache access or survey rerun',
       'verdict': 'APPROVE test isolation change and aggregate evidence; release/scientific holds retained',
       'base': BASE, 'fix': FIX, 'development_port': PORT, 'changed_paths': changed,
       'old_functions_ast_identical_except_one_setenv': True,
       'development_port_test_blob_identical': True,
       'events': event_checks,
       'reviewed_publishable': {p.name: digest(p) for p in sorted(PUB.iterdir()) if p.is_file()},
       'private_hashes_verified': evidence_hashes}
(HERE/'validation.json').write_text(json.dumps(out, indent=2)+'\n')
print(json.dumps({'verdict': out['verdict'], 'changed_paths': changed,
                  'report': out['reviewed_publishable']['REPORT.md'],
                  'private_evidence_verified': len(evidence_hashes),
                  'event_record_counts': {k:v['records'] for k,v in event_checks.items()}}, indent=2))

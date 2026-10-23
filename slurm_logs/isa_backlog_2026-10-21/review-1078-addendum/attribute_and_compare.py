from pathlib import Path
import collections, hashlib, json, xml.etree.ElementTree as ET

P = Path(__file__).resolve().parent
O = P.parent / 'review-1078'
R = Path('/local/job40255479/worktrees/isa-review-1078-unit-attribution')

def receipt(q):
    s = q.stat()
    return {'sha256': hashlib.sha256(q.read_bytes()).hexdigest(),
            'bytes': s.st_size, 'mode': s.st_mode & 0o7777,
            'mtime_ns': s.st_mtime_ns}

xmls = [O / 'unit/junit-private.xml', P / 'unit/junit-private.xml']
before = {str(q): receipt(q) for q in xmls}

def outcomes(q):
    rows = []
    for c in ET.parse(q).getroot().findall('.//testcase'):
        kinds = [k for k in ('failure', 'error', 'skipped') if c.find(k) is not None]
        assert len(kinds) <= 1
        # classname includes the full module and class; name includes parameters.
        # The tuple is lossless for JUnit identity, unlike basename/test-name totals.
        rows.append((c.attrib['classname'], c.attrib['name'], kinds[0] if kinds else 'passed'))
    return collections.Counter(rows)

old, new = map(outcomes, xmls)
assert sum(old.values()) == sum(new.values()) == 9151
removed, added = old - new, new - old
(P / 'node-outcomes-private.json').write_text(json.dumps({
    'original': sorted([list(k) + [v] for k, v in old.items()]),
    'new': sorted([list(k) + [v] for k, v in new.items()]),
    'removed': sorted([list(k) + [v] for k, v in removed.items()]),
    'added': sorted([list(k) + [v] for k, v in added.items()]),
}, indent=2))
assert not removed and not added
canonical = json.dumps(sorted([list(k) + [v] for k, v in old.items()]), separators=(',', ':')).encode()
counts = collections.Counter()
for (_, _, outcome), n in new.items():
    counts[outcome] += n

events = [json.loads(s) for s in (P / 'diagnostic-denies-private.jsonl').read_text().splitlines()]
generic = (P / 'unit/denied-writes-private.log').read_text().splitlines()
assert len(events) == len(generic) == 24
assert not (P / 'diagnostic-recording-errors-private.log').exists()
assert all(e['phase'] == 'call' and e['current_test'] and e['worker'] and e['pid'] and e['ppid'] for e in events)
ts = collections.Counter((e['current_test'], e['operation']) for e in events)
ut = [e for e in events if e['operation'] == 'os.utime']
ul = [e for e in events if e['operation'] == 'os.unlink']
assert len(ut) == 18 and len(ul) == 6
assert len({e['current_test'] for e in ut}) == 9
assert all('test_panel_ids_persistence.py::test_make_never_regenerates_the_crosswalk[' in e['current_test'] for e in ut)
assert all(e['resolved_path'].startswith(str(R / 'lsms_library/countries') + '/') and e['resolved_path'].endswith('/_/panel_ids.py') for e in ut)
assert all(n == 2 for (t, op), n in ts.items() if op == 'os.utime')
expected = {
    'test_retry_succeeds_after_transient_tls_errors': (2, 36),
    'test_non_transient_error_is_not_retried': (1, 50),
    'test_exhausts_attempts_then_reraises': (3, 57),
}
cleanup = []
for name, (n, line) in expected.items():
    current = f'tests/test_s3_fetch_retry.py::{name} (call)'
    matching = [e for e in ul if e['current_test'] == current]
    assert len(matching) == n
    for e in matching:
        assert e['lexical_path'] == e['resolved_path'] == '/tmp/nope.blob'
        assert any(f['filename'] == str(R / 'lsms_library/local_tools.py') and f['function'] == '_get_file_with_retry' and f['lineno'] == 311 for f in e['stack'])
        assert any(f['filename'] == str(R / 'tests/test_s3_fetch_retry.py') and f['function'] == name and f['lineno'] == line for f in e['stack'])
    assert new[('tests.test_s3_fetch_retry', name, 'passed')] == 1
    cleanup.append({'test': f'tests/test_s3_fetch_retry.py::{name}',
                    'denied_cleanup_calls': n, 'test_line': line,
                    'product_cleanup_line': 'lsms_library/local_tools.py:311',
                    'outcome': 'passed'})

for q in (P / 'unit/network-private.log',):
    assert not q.exists() or q.stat().st_size == 0
assert not list((P / 'unit/data').rglob('*.parquet'))
assert {str(q): receipt(q) for q in xmls} == before
(P / 'node-outcome-input-receipts-private.json').write_text(json.dumps(before, indent=2))
result = {
    'status': 'PASS_QUALIFIED_FULL_SELECTION',
    'full_node_identity_outcome_multisets_exact': True,
    'full_node_identities_compared': 9151,
    'canonical_identity_outcome_multiset_sha256': hashlib.sha256(canonical).hexdigest(),
    'outcomes': dict(counts), 'added_or_removed_outcomes': 0,
    'diagnostic_full_selections': 1,
    'new_denied_events': 24, 'fully_attributed_new_denied_events': 24,
    'collection_or_import_denied_events': 0,
    'tracked_utime_denials': 18, 'tracked_utime_failed_cases': 9,
    'fake_FS_best_effort_cleanup_denials': 6,
    'cleanup_controls': cleanup,
    'cleanup_semantics': 'Fake FS creates no target and performs no network access. PermissionError is caught as OSError by existing best-effort cleanup; original exception drives retries or reraising. Successful controls assert original exception class and/or exact get_file call count.',
    'original_six_events': 'Original generic log has no test/operation metadata. This new run reproduces the six-event cleanup pattern; it does not retroactively identify individual original events.',
    'no_full_suite_PASS_claim': True,
    'guarded_unit_network_attempts': 0,
    'guarded_unit_LSMS_data_root_parquets': 0,
    'intentional_synthetic_pytest_temp_parquets': len(list((P / 'unit/pytest-temp').rglob('*.parquet'))),
    'diagnostic_recording_errors': 0,
    'test_and_product_blobs_sha256': {
        str(q.relative_to(R)): hashlib.sha256(q.read_bytes()).hexdigest()
        for q in (R / 'tests/test_s3_fetch_retry.py', R / 'lsms_library/local_tools.py', R / 'tests/test_panel_ids_persistence.py')
    },
}
(P / 'attribution-proof.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))

"""Root acceptance of Uganda1205 evidence; opaque hashes and metadata only."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import re
import stat
import subprocess

C = Path('/local/job40255479/control/isa-backlog-20261021')
P = C / 'review-Uganda-1205'
Q = C / 'review-Uganda-1205-terminal-supplement'
A = C / 'uganda1205-diagnosis'


def load(path):
    return json.loads(Path(path).read_text())


def sha(path, kind='sha256'):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, kind).hexdigest()


def receipt(path):
    path = Path(path)
    s = path.lstat()
    link = os.readlink(path) if path.is_symlink() else None
    digest = hashlib.sha256(link.encode()).hexdigest() if link is not None else sha(path)
    after = path.lstat()
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mode', 'st_mtime_ns', 'st_nlink')
    assert all(getattr(s, k) == getattr(after, k) for k in fields)
    return dict(sha256=digest, bytes=s.st_size, mode=stat.S_IMODE(s.st_mode),
                mtime_ns=s.st_mtime_ns, nlink=s.st_nlink, device=s.st_dev,
                inode=s.st_ino, symlink=link is not None, link=link)


def directory(path):
    s = path.lstat()
    return dict(kind='directory', bytes=s.st_size, mode=stat.S_IMODE(s.st_mode),
                mtime_ns=s.st_mtime_ns, nlink=s.st_nlink, device=s.st_dev, inode=s.st_ino)


def authenticate(root, ready_hash, manifest_hash, allowlist_hash, count):
    assert sha(root / 'READY.json') == ready_hash
    assert sha(root / 'manifest.json') == manifest_hash
    assert sha(root / 'EXPORT-ALLOWLIST.json') == allowlist_hash
    ready = load(root / 'READY.json')
    m = load(root / 'manifest.json')
    assert len(m['files']) == ready['files'] == count
    actual = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() or p.is_symlink()}
    assert actual - set(m['omitted_self_files']) == set(m['files'])
    for name, expected in m['files'].items():
        assert receipt(root / name) == expected, ('FROZEN_RECEIPT', root.name, name)
    if 'directories' in m:
        dirs = {str(p.relative_to(root)): directory(p) for p in root.rglob('*') if p.is_dir() and not p.is_symlink()}
        assert dirs == m['directories'] and len(dirs) == ready['directories'] == 180
        expected_root = m['root_directory']
    else:
        assert not any(p.is_dir() for p in root.iterdir())
        expected_root = m['root']
    assert {k: directory(root)[k] for k in expected_root} == expected_root
    assert receipt(root / 'manifest.json') == ready['manifest']
    assert receipt(root / 'EXPORT-ALLOWLIST.json') == ready['allowlist']
    return ready, m


ready, manifest = authenticate(P, '50f65d91593d06a46f3dea9030fbe5676d7a9cd9915a3aab28670a5273ad0a42',
    '8196ff96ede2a923517b49815863118cc31a2796fd0ee4f94d17f16847526e46',
    '698fd2fae0436ee33dc7425fadd27713e440fd1f4b94f3d4ccc9b4f5c52341f4', 451)
supplement, sm = authenticate(Q, '253a347de0110e9bd204a2d34667f88a5f93184388d8d72e9668da5b605a0731',
    '6c06021a18360550fe54af0a0e79cd949141ad104c6cc8493376b5fc990cee0d',
    '5ea4c00044d208d69e84a775778525f680a3f27168471caa3080afaa010520f3', 23)
assert ready['candidate'] == '884775a461d769fd4bb5181005ca3238513b5b01'
assert ready['actual_parent'] == '491daf5654b9ee45332bec97e1296ae821ff46d5'
for n, row in ready['safe_receipts'].items():
    assert receipt(P / n) == row
for n, row in supplement['main_pins'].items():
    assert receipt(P / n) == row
assert sha(P / 'postexit-safe.json') == '684658f6bcd530942313903d6ad516d8337c732cf7302559ce2143a4afeb5f70'
assert sha(Q / 'postexit-safe.json') == 'f020bbefe8378b384d6f9c6788f2d7d832022efb802954b1648c6a96ea01605c'
assert sha(P / 'tool-terminal-records-private.json') == '526ded223fdf31a3f813a2143f0557c41439f3d73c62f0b3aa82d1a4e5b5778f'
post = load(P / 'postexit-safe.json')
assert post['status'] == 'FULL_SEPARATE_POSTEXIT_PASS' and post['files_rehashed'] == 451
assert post['all_files_full_SHA_and_physical_metadata_exact'] and not post['zero_consumers']['live_consumers']
assert receipt(P / 'READY.json') == post['READY']
tail = Q / 'terminal-receipts-private.json'
assert sha(tail) == '00c0870e842be1aa769bf1c2ddf3cf07d68d6a0d5bbdda9e98ac1ffd752bbba5'
term = load(tail)
for n, row in term['main'].items():
    assert receipt(P / n) == row
for n, row in term['receipts'].items():
    assert receipt(Q / n) == row
original = load(Q / 'original-tool-literals.json')
for key, session, chunk in [('sealer', 2617, '810b2e'), ('postexit', 1492, '12d2c6')]:
    row = original[key]
    begin = json.loads(row['start_return_utf8'])
    finish = json.loads(row['completion_return_utf8'])
    assert begin['session_id'] == session and finish['exit_code'] == 0 and finish['chunk_id'] == chunk
    assert json.loads(row['completion_input_utf8'])['session_id'] == session
    stream = 'seal' if key == 'sealer' else 'postexit'
    assert finish['output'] == ''
    command = json.loads(row['start_input_utf8'])['cmd']
    assert stream + '-stdout-private.log' in command
    output = json.loads((P / (stream + '-stdout-private.log')).read_text())
    assert output['READY_sha256'] == sha(P / 'READY.json')
assert load(Q / 'postexit-safe.json')['files_rehashed'] == 23
precision = load(Q / 'corrected-terminal-metadata-private.json')
assert len(precision['precision_differences']) == 7
assert all(r['path'].endswith('.mtime_ns') for r in precision['precision_differences'])

phase0 = load(P / 'phase0-source-private.json')
env = dict(os.environ, GIT_OPTIONAL_LOCKS='0')
tracked = 0
for role, expected in phase0['sources'].items():
    root = Path(expected['root'])
    def git(*args):
        return subprocess.check_output(['git', '--no-optional-locks', *args], cwd=root, env=env)
    assert git('rev-parse', 'HEAD').decode().strip() == expected['head']
    assert git('status', '--porcelain').decode() == expected['status'] == ''
    assert set(git('ls-files', '-z').decode().rstrip('\0').split('\0')) == set(expected['tracked'])
    for n, row in expected['tracked'].items():
        if row == {'missing': True}:
            assert not (root / n).exists() and not (root / n).is_symlink()
        else:
            assert receipt(root / n) == row, ('SOURCE_RECEIPT', role, n)
    tracked += len(expected['tracked'])
assert tracked == 29826
gate = load(P / 'author-gate-private.json')
assert len(gate['content_and_full_physical_receipts']) == 302
for n, row in gate['content_and_full_physical_receipts'].items():
    assert receipt(A / n) == row
staged = load(P / 'staged-providers-private.json')
assert len(staged['providers']) == 53 and len(staged['copies']) == 106
inodes = set()
for row in staged['providers']:
    expected = row['source_before']
    assert receipt(row['source']) == {k: v for k, v in expected.items() if k != 'md5'}
    assert sha(row['source'], 'md5') == expected['md5'] == row['declared_md5']
    if row['declared_size'] is not None:
        assert Path(row['source']).stat().st_size == row['declared_size']
for row in staged['copies']:
    expected = row['receipt']
    assert receipt(row['copy']) == {k: v for k, v in expected.items() if k != 'md5'}
    assert sha(row['copy'], 'md5') == expected['md5']
    assert expected['nlink'] == 1 and not expected['symlink']
    inodes.add((expected['device'], expected['inode']))
assert len(inodes) == 106

base = load(P / 'base-fingerprints-private.json')
candidate = load(P / 'candidate-fingerprints-private.json')
for key, count in [('country', 549), ('framework', 39), ('declared_wave', 1362)]:
    assert len(base[key]) == count and candidate[key] == base[key]
assert candidate['declaration'] == base['declaration']
assert candidate['ordered_public_names'] == base['ordered_public_names']
for label, passed, failed in [('candidate-tests', 65, 0), ('base-new33-tests', 27, 6)]:
    terminal = load(P / (label + '-terminal-private.json'))
    assert terminal['exit_code'] == bool(failed)
    log = (P / (label + '-stdout-private.log')).read_text()
    summary = log.strip().splitlines()[-1]
    assert re.search(r'\b' + str(passed) + r' passed\b', summary)
    assert (not failed or re.search(r'\b6 failed\b', summary))
    assert not re.search(r'\b\d+ (?:errors?|skipped|xfailed|xpassed)\b', summary)
    root = phase0['sources']['candidate' if label == 'candidate-tests' else 'base']['root']
    ids = [load(q) for q in (P / 'identities').glob(label + '-*-precollection.json')]
    assert len(ids) == 3 and {r['worker'] for r in ids} == {None, 'gw0', 'gw1'}
    assert all(r['root'] == root and r['package'] == root + '/lsms_library/__init__.py' and r['precollection'] for r in ids)
protection = load(P / 'protection-v2-private.json')
assert len(protection['actor_ledger']) == 21 and protection['seeded_denials'] == 12 and protection['ordinary_denials'] == 0
assert not protection['quiescence']['live_consumers']
for row in protection['actor_ledger']:
    terminal = load(P / (row['label'] + '-terminal-private.json'))
    assert terminal['actor_pid'] == row['actor_pid'] and terminal['exit_code'] == row['exit_code']
    for stream in ['stdout', 'stderr']:
        expected = terminal[stream]
        actual = receipt(P / (row['label'] + '-' + stream + '-private.log'))
        assert actual == dict(expected, symlink=expected.get('symlink', False), link=expected.get('link'))
oracle = load(P / 'raw-column-oracle/result-safe.json')
assert oracle['complete_oracle_equal'] and oracle['complete_old_key_projection_equal']
assert (oracle['old_rows'], oracle['candidate_rows'], oracle['added_keys'], oracle['lost_keys']) == (14520, 27755, 13235, 0)
assert oracle['native_HH_collisions'] == oracle['crop_area_native_sets_differ'] == 0
denominator = load(P / 'denominator-safe.json')
assert denominator['converted_source_parcel_keys'] == 31959 and denominator['unserved_converted_source_parcel_keys'] == 4204
assert denominator['area_roster_matching_converted_keys'] == 31949 and denominator['eligible_served_yield_keys'] == 27755
assert {tuple(x['raw_area_reasons']): x['converted_keys'] for x in denominator['ineligible_area_reason_signatures']} == {('both_missing',): 3918, ('nonpositive_gps',): 276}
scans = [(P / s / 'scan-results-private.jsonl').read_bytes() for s in ['base', 'candidate']]
assert scans[0] == scans[1] and len(scans[0].splitlines()) == 48
result = load(P / 'result-v2-safe.json')
assert result['normal_data']['full_strict_cold_warm_pairs'] == 12
assert result['normal_data']['full_cache_bytes_equal_pairs'] == result['normal_data']['full_footer_metadata_equal_pairs'] == 26
assert result['comparator']['inherited_negative_controls'] == 39 and result['comparator']['object_type_negative_controls'] == 30
assert result['comparator']['matching_Decimal_positive_controls_unsupported'] == 6
out = dict(status='ROOT_ACCEPTED_BOUNDED_UGANDA1205_REPAIR', at_UTC=datetime.datetime.now(datetime.timezone.utc).isoformat(),
           source_head=ready['candidate'], actual_parent=ready['actual_parent'],
           root_initial_redirected_stdout_assertion_stop_sha256=sha(C / '1205-independent-root-v1-stop.json'),
           main_READY_sha256=sha(P / 'READY.json'), main_manifest_sha256=sha(P / 'manifest.json'),
           supplement_READY_sha256=sha(Q / 'READY.json'), supplement_manifest_sha256=sha(Q / 'manifest.json'),
           full_frozen_files_checked=474, tracked_source_entries_checked=29826,
           frozen_author_entries_checked=302, genuine_providers_MD5_SHA_physical_checked=53,
           independent_copies_MD5_SHA_physical_checked=106, candidate_tests_passed=65,
           actual_parent_new_controls_failed=6, actual_parent_new_controls_passed=27,
           yields=27755, old_yields=14520, added=13235, lost=0, converted_parcels=31959,
           unserved_reasons=dict(both_area_reports_missing=3918, nonpositive_chosen_GPS=276, parcel_absent=10),
           fingerprint_changes=0, ordered_scanner_warnings_unchanged=48,
           auxiliary_precision_qualification='Seven mtime fields: three frozen metadata receipts have original exact integer authority; four stream mtimes use explicitly new current baselines. Original inaccurate excluded record retained.',
           original_seal_postexit_identity_gaps='Original tool returns and direct self PID/source/time preserved; no separate external Popen seal record or earlier postexit boot is invented.',
           Decimal_positive_domain_and_all_original_stops_retained=True,
           root_no_payload_decode_API_test_raw_build_or_historical_controller_replay=True,
           full_corpus_or_science_certification=False, human_merge_required=True,
           root_checker_sha256=sha(Path(__file__)), signed='Sue')
dest = C / '1205-INDEPENDENT-ROOT-ACCEPTANCE.json'
with dest.open('x') as stream:
    json.dump(out, stream, indent=2)
    stream.write('\n')
dest.chmod(0o600)
print(json.dumps(out), flush=True)

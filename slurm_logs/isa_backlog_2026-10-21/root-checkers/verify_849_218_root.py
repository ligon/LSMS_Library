"""Read-only root acceptance of frozen independent country reviews."""
from pathlib import Path
import ast
import collections
import copy
import datetime
import hashlib
import json
import os
import stat
import subprocess
import xml.etree.ElementTree as ET

CONTROL = Path('/local/job40255479/control/isa-backlog-20261021')
WORK = Path('/local/job40255479/worktrees')
OUT = CONTROL / 'root-acceptance-849-218'
OUT.mkdir(mode=0o700, exist_ok=False)


def digest(path, kind='sha256'):
    h = hashlib.new(kind)
    if path.is_symlink():
        h.update(os.readlink(path).encode())
    else:
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                h.update(block)
    return h.hexdigest()


def load(path):
    return json.loads(path.read_text())


def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args])


def verify_receipt(path, expected):
    s = path.lstat()
    assert digest(path) == expected['sha256'], ('sha', str(path))
    size = expected.get('bytes', expected.get('size'))
    if size is not None:
        assert s.st_size == size, ('size', str(path))
    for key, actual in [('mode', stat.S_IMODE(s.st_mode)), ('mtime_ns', s.st_mtime_ns),
                        ('inode', s.st_ino), ('device', s.st_dev), ('nlink', s.st_nlink)]:
        if key in expected:
            assert actual == expected[key], (key, str(path))
    if 'type' in expected:
        assert expected['type'] == ('link' if path.is_symlink() else 'file') or (
            path.is_symlink() and expected['type'] == 'symlink')
    if 'symlink' in expected:
        assert path.is_symlink() == expected['symlink']


def verify_manifest(root, filename, pin, count):
    manifest = root / filename
    assert digest(manifest) == pin
    data = load(manifest)
    rows = data if isinstance(data, dict) else {r['relative_path']: r for r in data}
    assert len(rows) == len(data) == count
    for rel, receipt in rows.items():
        assert not Path(rel).is_absolute() and '..' not in Path(rel).parts
        verify_receipt(root / rel, receipt)
    return rows


def verify_source(root, snapshot, pin):
    assert git(root, 'rev-parse', 'HEAD').decode().strip() == snapshot['head'] == pin
    assert git(root, 'status', '--porcelain') == b''
    index = {}
    for row in git(root, 'ls-files', '--stage', '-z').split(b'\0'):
        if row:
            meta, name = row.split(b'\t', 1)
            mode, blob, stage = meta.decode().split()
            assert stage == '0'
            index[os.fsdecode(name)] = (mode, blob)
    assert index.keys() == snapshot['files'].keys()
    for rel, expected in snapshot['files'].items():
        path = root / rel
        verify_receipt(path, expected)
        raw = os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
        assert hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() == index[rel][1]
        if 'git_mode' in expected:
            assert index[rel] == (expected['git_mode'], expected['blob'])
        if 'link' in expected:
            assert expected['link'] == (os.readlink(path) if path.is_symlink() else None)
    assert not list((root / 'lsms_library/countries').rglob('*.parquet'))
    return len(index)


def junit(path, expected):
    counts = collections.Counter()
    nodes = []
    for case in ET.parse(path).iter('testcase'):
        status = ('fail' if case.find('failure') is not None else
                  'error' if case.find('error') is not None else
                  'skip' if case.find('skipped') is not None else 'pass')
        counts[status] += 1
        nodes.append((case.get('classname'), case.get('name')))
    assert dict(counts) == expected, ('JUnit', path.name, dict(counts))
    return counts, collections.Counter(nodes)


def sources_and_copies(root):
    prep = load(root / 'source-preparation-private.json')
    origins = copies = 0
    for row in prep['sources'] + prep['comparators']:
        source = Path(row['source'])
        verify_receipt(source, row['receipt'])
        assert not source.is_symlink() and source.stat().st_nlink == 1
        md5 = row.get('md5', row.get('declared_md5'))
        if md5 is not None:
            assert digest(source, 'md5') == md5
        origins += 1
        endpoints = {(source.stat().st_dev, source.stat().st_ino)}
        for item in row['copies']:
            path = Path(item['path'])
            verify_receipt(path, item['receipt'])
            assert not path.is_symlink() and path.stat().st_nlink == 1
            endpoint = (path.stat().st_dev, path.stat().st_ino)
            assert endpoint not in endpoints
            endpoints.add(endpoint)
            copies += 1
    return {'origins': origins, 'copies': copies, 'all_receipts_MD5s_and_distinct_inodes_exact': True}


def live_consumers(roots):
    ancestors = set()
    pid = os.getpid()
    while pid and pid not in ancestors:
        ancestors.add(pid)
        try:
            pid = int(Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[1])
        except (OSError, ValueError, IndexError):
            break
    found = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit() or int(proc.name) in ancestors:
            continue
        try:
            cmd = (proc / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
            cwd = os.path.realpath(proc / 'cwd')
            env = dict(x.split(b'=', 1) for x in (proc / 'environ').read_bytes().split(b'\0') if b'=' in x)
            if any(str(r) in cmd or cwd == str(r) or cwd.startswith(str(r) + os.sep) or
                   str(r).encode() in [env.get(b'REVIEW_ROOT'), env.get(b'TASK_PRIVATE')]
                   for r in roots):
                found.append(int(proc.name))
        except (OSError, ValueError):
            pass
    assert not found, ('live consumers', found)
    return 0


results = {}
for country, number, folder, ready_name, ready_pin, mf, mf_pin, count, candidate, parent in [
    ('Uganda', 849, 'review-Uganda-849', 'FINAL-READY.json',
     '4505b86a7b9d5082bd6c7b8845f2ee9ee56a73f9735abe8bf6f833b1a25565cb',
     'private-final-manifest.json', 'dfef539aa8ffca3ff81acd1ffaba3da390b5f1c5903de47ca3410beb2997778f',
     2125, '491daf5654b9ee45332bec97e1296ae821ff46d5', '0854303bc5f60daa698b2927fc3b193822a4c090'),
    ('Malawi', 218, 'review-Malawi-218', 'READY.json',
     '36439ba6049b54df8f0a59a6a3260efca630019d6ff103a6a8db5c54f756c2b7',
     'final-private-manifest.json', '94d5e89a4fe8f3706cda0181222a2c235e5eb137184015ef2f318254437a4a83',
     1458, 'd29a56783d08a8a647e2622150a4ee5fbdd7246a', 'f8d236694c26fe9e709c5548f02c70f74ea72c33'),
]:
    p = CONTROL / folder
    assert digest(p / ready_name) == ready_pin
    ready = load(p / ready_name)
    assert ready['candidate'] == candidate and ready['parent'] == parent
    rows = verify_manifest(p, mf, mf_pin, count)
    excluded = ({mf, ready_name, 'SEAL-TERMINAL.json', 'seal-stdout-private.log',
                 'seal-stderr-private.log', 'readback-stdout-private.log', 'readback-stderr-private.log'}
                if country == 'Uganda' else {mf, ready_name, 'seal-review-proof.json'})
    actual = {str(q.relative_to(p)) for q in p.rglob('*') if q.is_file() or q.is_symlink()}
    assert actual - rows.keys() == excluded, ('frozen pathset', folder)
    assert set(q.name for q in (p / 'safe-exports').iterdir()) == set(ready['safe_exports'])
    for rel, expected in ready['safe_exports'].items():
        verify_receipt(p / 'safe-exports' / rel, expected)
    if country == 'Uganda':
        checkers = ready['checker_sources']
        for rel, expected in checkers.items():
            verify_receipt(p / rel, expected)
    else:
        assert digest(p / 'final-checker-pins.json') == ready['checker_pins_sha256']
        checkers = load(p / 'final-checker-pins.json')
        for rel, expected in checkers.items():
            assert digest(p / rel) == expected
    source_count = 0
    for side, pin in [('base', parent), ('candidate', candidate)]:
        root = WORK / ('isa-' + folder + ('-base' if side == 'base' else ''))
        source_count += verify_source(root, load(p / (side + '-initial-tracked-private.json')), pin)
    physical = sources_and_copies(p)
    for row in load(p / 'root-metadata-preparation.json')['rows']:
        assert Path(row['ignored_link']).is_symlink()
        assert Path(row['ignored_link']).resolve() == Path(row['exact_private_target'])
    if country == 'Uganda':
        new_tests, nodes = junit(p / 'candidate/review-focused-actual.xml', {'pass': 416, 'skip': 10})
        parent_tests, _ = junit(p / 'base/parent-negative.xml', {'fail': 20, 'pass': 4})
        _, author_nodes = junit(CONTROL / 'Uganda/849-fix/candidate/review-focused-actual.xml',
                                {'pass': 416, 'skip': 10})
        assert nodes == author_nodes
        a = load(p / 'base/prebuild-hashes-v2-private.json')
        b = load(p / 'candidate/prebuild-hashes-v2-private.json')
        scope = {}
        for k, total, changed in [('country_hashes', 549, 28), ('framework_fingerprints', 39, 0),
                                  ('wave_hashes', 1362, 171), ('api_wave_hashes', 2139, 224)]:
            assert len(a[k]) == len(b[k]) == total and a[k].keys() == b[k].keys()
            changes = [z for z in a[k] if a[k][z] != b[k][z]]
            assert len(changes) == changed and all(z.startswith('Uganda/') for z in changes)
            scope[k] = {'total': total, 'changed': changed}
        r, base = WORK / 'isa-review-Uganda-849', WORK / 'isa-review-Uganda-849-base'
        rel = 'lsms_library/countries/Uganda/_/uganda.py'
        x, y = ast.parse((base / rel).read_text()), ast.parse((r / rel).read_text())
        select = lambda tree: next(n for n in tree.body if isinstance(n, ast.Assign) and
            any(isinstance(z, ast.Name) and z.id == 'CROP_COLMAPS' for z in n.targets))
        ax, ay = select(x), select(y)
        mx, my = ast.literal_eval(ax.value), ast.literal_eval(ay.value)
        bindings = [(w, s, 1, 'a5' + s.lower() + 'q6d')
                    for w in ('2010-11', '2011-12', '2013-14', '2015-16') for s in ('A', 'B')]
        bindings += [('2019-20', s, n, 's5' + s.lower() + 'q06d_' + str(n))
                     for s in ('A', 'B') for n in (1, 2)]
        for w, s, n, field in bindings:
            assert 'kg_factor' not in mx[w][s]['conditions'][n - 1]
            assert my[w][s]['conditions'][n - 1].pop('kg_factor') == field
        assert mx == my
        ay.value = copy.deepcopy(ax.value)
        assert ast.dump(x, include_attributes=False) == ast.dump(y, include_attributes=False)
        rel = 'lsms_library/countries/Uganda/2019-20/_/crop_production.py'
        x, y = ast.parse((base / rel).read_text()), ast.parse((r / rel).read_text())
        assert ast.dump(ast.Module(body=x.body[1:], type_ignores=[]), include_attributes=False) == ast.dump(
            ast.Module(body=y.body[1:], type_ignores=[]), include_attributes=False)
        strict = load(p / 'own-artifact-proof-v4-private.json')
        assert strict['strict_count'] == 99 and all(strict['comparator_negative_controls'].values())
        assert len(strict['support_existing_exception']) == 4
        sweep = load(p / 'strict-artifact-sweep-proof-v3-private.json')
        assert sweep['comparisons'] == 246 and sweep['tiny_float_rejected'] and sweep['no_source_or_API_calls']
        assert load(p / 'feature-comparison-strict-v2-private.json')['strict_checks'] == 30
        warnings = load(p / 'warning-proof-v2-private.json')
        assert warnings['pairs'] == 60
        assert all(x['literal_class_full_message_multisets_exact'] for x in warnings['public_pairs'])
        assert len(warnings['oracle']) == 7 and all(x['literal_equal'] for x in warnings['oracle'])
        assert len(warnings['derived']) == 3 and all(x['literal_equal'] for x in warnings['derived'])
        scans = [(p / side / 'scanner/findings-private.json').read_bytes() for side in ('base', 'candidate')]
        scan_rows = [json.loads(x) for x in scans]
        assert scan_rows[0] == scan_rows[1] and len(scan_rows[0]) == 41
        assert all(x['status'] == 'warn' for x in scan_rows[0])
        assert git(r, 'rev-parse', 'HEAD:tests') == git(r, 'rev-parse', '66bfa1eb16442ceb6d455dfaa5726f181d713abd:tests')
        extra = {'strong_checks': 375, 'overlapping_group_counts': [99, 246, 30],
                 'existing_string_NA_exceptions': 4, 'factor_bindings': 12,
                 'source_records': 133761, 'harvest_kg_increase': 12931281.585085638,
                 'warning_literal_pairs': [60, 7, 3], 'scanner_records': 41,
                 'scanner_statuses': {'warn': 41}, 'scanner_bytes_exact': scans[0] == scans[1]}
    else:
        new_tests, _ = junit(p / 'candidate/focused-junit.xml', {'pass': 51})
        parent_tests, _ = junit(p / 'base/parent-junit.xml', {'fail': 1, 'pass': 1})
        a, b = [load(p / side / 'prebuild-hashes-private.json') for side in ('base', 'candidate')]
        assert len(a['country_hashes']) == len(b['country_hashes']) == 549
        assert a['country_hashes'].keys() == b['country_hashes'].keys()
        changes = [k for k in a['country_hashes'] if a['country_hashes'][k] != b['country_hashes'][k]]
        assert len(changes) == 26 and all(k.startswith('Malawi/') for k in changes)
        assert a['framework_hashes'] == b['framework_hashes'] and len(a['framework_hashes']) == 39
        assert a['wave_hashes'] == b['wave_hashes'] and len(a['wave_hashes']) == 97
        assert a['public_names'] == b['public_names'] and a['Malawi'] == b['Malawi']
        assert sorted((r['country'], r['table'], r['entry']) for r in a['raw_derived']) == [
            ('Malawi', 'food_expenditures', None), ('Malawi', 'food_prices', None)]
        assert b['raw_derived'] == []
        r, base = WORK / 'isa-review-Malawi-218', WORK / 'isa-review-Malawi-218-base'
        rel = 'lsms_library/countries/Malawi/_/data_scheme.yml'
        tail = b'  food_expenditures:\n  food_prices:\n'
        assert (base / rel).read_bytes().endswith(tail)
        assert (r / rel).read_bytes() == (base / rel).read_bytes()[:-len(tail)]
        assert git(r, 'rev-parse', 'HEAD:tests') == git(r, 'rev-parse', '299fb8675e785fd392b1259ab1f4fb535eb69c34:tests')
        strict = load(p / 'strict-reassessment-private.json')
        assert strict['all_exact'] and strict['strict_frame_checks'] == len(strict['comparisons']) == 209
        assert strict['unique_ordered_operand_pairs'] == len({
            (v['a'], v['b']) for v in strict['comparisons']}) == 177
        assert all(v['strict_exact'] for v in strict['comparisons'])
        assert not strict['API_source_and_pytest_replay']
        assert load(p / 'strict-final-negative-controls.json')['all_new_and_repeated_synthetic_controls'] == 15
        history = load(p / 'strict-history-private.json')
        assert history['all_classifications_confirmed'] and history['diagnostic_equal'] == 14
        assert history['diagnostic_wrong_grain_expected_rejections'] == 2
        assert history['intentional_CLI_expected_rejections'] == 6
        scans = [(p / side / 'scanner/findings-private.jsonl').read_bytes() for side in ('base', 'candidate')]
        scan_rows = [[json.loads(v) for v in x.splitlines()] for x in scans]
        assert scan_rows[0] == scan_rows[1] and len(scan_rows[0]) == 205
        statuses = collections.Counter(v['status'] for v in scan_rows[0])
        assert statuses == {'warn': 200, 'pass': 3, 'fail': 2}
        warning = load(p / 'warnings-v2-private.json')
        assert warning['endpoint_pairs'] == 67 and warning['warning_occurrences_per_head'] == 353
        scope = {'API_total': 549, 'Malawi_API_changed': 26, 'other_API_exact': 523,
                 'framework_exact': 39, 'Malawi_wave_exact': 97}
        extra = {'strong_checks': 209, 'unique_ordered_pairs': 177,
                 'negative_calibrations': 15, 'scanner_records': 205,
                 'scanner_statuses': dict(statuses), 'scanner_bytes_exact': scans[0] == scans[1],
                 'warning_qualified_pairs': 67, 'warning_occurrences_per_head': 353,
                 'historical_classifications': {'exact': 14, 'expected_rejections': 8}}
    live_consumers([p, r, base])
    result = {'status': 'ROOT_ACCEPT_BOUNDED_INDEPENDENT_REVIEW', 'country': country, 'issue': number,
              'recorded_at_UTC': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'candidate': candidate, 'actual_parent': parent, 'READY_sha256': ready_pin,
              'manifest_sha256': mf_pin, 'all_manifest_payloads_verified': count,
              'complete_frozen_pathset_verified': True, 'safe_exports_verified': len(ready['safe_exports']),
              'actual_checker_pins_verified': len(checkers), 'tracked_source_endpoints_verified': source_count,
              'source_snapshot_scope': 'All Git blobs/modes and own frozen OS receipts; no unrecorded source mtime claim',
              'physical_copies': physical, 'candidate_JUnit': dict(new_tests),
              'actual_parent_JUnit': dict(parent_tests), 'actual_prebuild_hash_scope': scope,
              'evidence': extra, 'root_read_actual_runtime_test_comparator_pair_selection_and_seal_sources': True,
              'root_verification_only_hashes_reads_JSON_XML_and_static_AST': True,
              'no_API_pickle_source_decode_or_test_replay': True, 'public_PR_or_merge_or_issue_closure': False,
              'preserved_limits': ready.get('limits', ready.get('qualifications', [])), 'signed': 'Sue'}
    results[str(number)] = result

# Both author roots remain at their separate frozen receipt conventions.
verify_manifest(CONTROL / 'Uganda/849-fix', 'final-private-manifest.json',
                'eb0e0d07305d820b05f6d34a4ef313f4ef5c18130150a95d8d66bd54655beb84', 1858)
verify_manifest(CONTROL / 'Malawi/218', 'final-private-manifest.json',
                'b60023a579e8850a1ba2d1d2a002ae2871d01be872c25f8708f8dc6198cb234f', 815)
for number, result in results.items():
    with (OUT / (number + '-ROOT-ACCEPTANCE.json')).open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    (OUT / (number + '-ROOT-ACCEPTANCE.json')).chmod(0o600)
print(json.dumps({'status': 'ROOT_ACCEPT_BOTH_BOUNDED', 'accepted': [
    {'issue': int(k), 'payloads': v['all_manifest_payloads_verified'],
     'tests': v['candidate_JUnit'], 'safe_exports': v['safe_exports_verified']}
    for k, v in results.items()]}))

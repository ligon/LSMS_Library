"""Read-only immutable Git-tree/ref proof; never imports repository code."""
import collections
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

REPO = '/local/job40255479/worktrees/framework-contracts'
ORIGIN = '/global/home/users/ligon/mirrors/LSMS_Library'
OUT = Path('/local/job40255479/control/framework-campaign/landing-independent')
CANDIDATE = '532d58700fa46e52a0b390198604eca1dc4de00e'
PUBLICATION = '23c1e2d52801bf206a15ec5900bca5d62e7f850f'
DEVELOPMENT = 'f50611161e9e58bf0676de5d771d4d6f07d0d8a3'
DOC_PIN = '78e25dff55323a0bb3993d2a289b8c577ceb58d2'
HARNESS_HEAD = 'd84061818b0b430e8b00fa26b5b6d19e4694eb96'
PREFIX = 'slurm_logs/framework_campaign_2026-10-20/1124-1125/'
EXPECTED_ADDED = {PREFIX + name for name in [
    '1124-1125-review-r2.json', '1124-1125-review-r2.org',
    '1124-1125-review.json', '1124-1125-review.org',
]}
PINS = {
    'fix/1093-declared-unit-factors': 'eaf7b9913b37e04e924bd05a17e691e0263ecb4e',
    'fix/1099-empty-subset': 'f9871610f02bca8d1f82a352c4925353439cb6bc',
    'fix/1124-1125-feature-audit-40255479': HARNESS_HEAD,
    'fix/1143-decimal-comma-40255479': '1070a4d03e764ec8e3b0e61d4f825bc793eeeae7',
    'fix/1115-1128-provenance-40255479': '44f6cd2e7d3665622d2119fe4725e5a7604a56fa',
    'fix/778-cache-race-40255479': 'f7eb194d7a54176ad44dfdeb6529e6b4b1715aeb',
    'fix/uganda-nutrition-cpu-hash-40255479': '98822ed1ab3a2106b3be4c3d90aa086076e32ef0',
    'review/1159-pooled-reducers-40255479': '45f80dcb50da189b7864034b1d92863dca68de09',
    'fix/1160-pooled-rate-40255479': CANDIDATE,
    'integration/framework-v016-40255479': '852ac2bfcd5f798533f56e7efa1f77db80c0138f',
    'integration/framework-followup-40255479': '4cab5f0db7d375ad543e652f61f43bd28fa94ecd',
    'integration/framework-rate-40255479': CANDIDATE,
    'fix/windows-shocks-digest-20261020': '1d3e8dfdf264c40cc4ce4eb9a1f77b9d71634ad6',
}
ORIGIN_ONLY_WORKERS = {
    'fix/1115-1128-provenance-40255479', 'fix/778-cache-race-40255479',
    'fix/uganda-nutrition-cpu-hash-40255479',
    'review/1159-pooled-reducers-40255479', 'fix/1160-pooled-rate-40255479',
}
PUBLISHED_REVIEW_PINS = {
    'review/1115-1128-provenance-20261020': '124c2436d7e1a279c72f4d8f650d250eab6c7d67',
    'review/778-cache-race-20261020': 'dffb0760406c74cea677f360f76be52cbed27975',
    'review/nutrition-cpu-hash-20261020': '6553e01d6cd9013a2103124dcf7a2c945025a25c',
    'review/1159-pooled-reducers-20261020': 'd124c0df70db2e04dceb4a8c19eb0d40df14dbee',
    'review/1160-pooled-rate-20261020': PUBLICATION,
}


def git(*args, cwd=REPO):
    return subprocess.check_output(['git', *args], cwd=cwd)


def tree(sha):
    result = {}
    for entry in git('ls-tree', '-r', '-z', sha).split(b'\0'):
        if entry:
            metadata, path = entry.split(b'\t', 1)
            mode, kind, oid = metadata.decode().split()
            result[path.decode()] = {'mode': mode, 'type': kind, 'oid': oid}
    return result


def ancestor(first, second):
    return subprocess.run(['git', 'merge-base', '--is-ancestor', first, second],
                          cwd=REPO, capture_output=True).returncode == 0


left, right, dev, harness = map(tree, [CANDIDATE, PUBLICATION, DEVELOPMENT, HARNESS_HEAD])
differences = []
for path in sorted(set(left) | set(right)):
    if left.get(path) == right.get(path):
        continue
    entry = {'path': path, 'candidate': left.get(path), 'publication': right.get(path)}
    entry['classification'] = 'historical independent review evidence only'
    entry['status'] = 'added' if path not in left else 'removed' if path not in right else 'changed'
    if path in right:
        blob = git('show', f'{PUBLICATION}:{path}')
        entry['bytes'] = len(blob)
        entry['lines'] = len(blob.splitlines())
        entry['sha256'] = hashlib.sha256(blob).hexdigest()
        entry['same_blob_mode_as_1157_head'] = right[path] == harness.get(path)
        entry['already_present_in_merged_development_f506'] = right[path] == dev.get(path)
    differences.append(entry)
assert {item['path'] for item in differences} == EXPECTED_ADDED
assert all(item['status'] == 'added' and item['publication']['mode'] == '100644'
           and item['same_blob_mode_as_1157_head']
           and item['already_present_in_merged_development_f506'] for item in differences)

def group(path):
    if path.startswith('lsms_library/'):
        return 'package_runtime_and_country_configuration'
    if path.startswith('tests/'):
        return 'tests_and_fixtures'
    if path.startswith(('scripts/', 'bin/', 'tools/')):
        return 'scripts_bin_tools'
    if path.startswith('bench/'):
        return 'bench_and_audit'
    if path.startswith('.github/'):
        return 'github_workflows_and_configuration'
    if Path(path).name in {'pyproject.toml', 'poetry.lock', 'Makefile', 'setup.py',
                           'setup.cfg', 'tox.ini', 'pytest.ini', '.python-version',
                           'MANIFEST.in', 'requirements.txt', 'environment.yml'}:
        return 'other_build_dependency_and_test_configuration'
    return 'other_tracked_documents_and_files'

groups = {}
for path in sorted(set(left) | set(right)):
    record = groups.setdefault(group(path), collections.Counter())
    record['candidate_paths'] += path in left
    record['publication_paths'] += path in right
    record['identical_paths'] += left.get(path) == right.get(path)
    record['different_paths'] += left.get(path) != right.get(path)

local = dict(line.split() for line in git('for-each-ref', '--format=%(refname) %(objectname)',
                                          'refs/heads', 'refs/remotes/origin').decode().splitlines())
origin = dict(line.split() for line in git('for-each-ref', '--format=%(refname) %(objectname)',
                                           'refs/heads', cwd=ORIGIN).decode().splitlines())
remote_response = subprocess.check_output([
    'gh', 'api', 'repos/ligon/LSMS_Library/git/matching-refs/heads/'
])
github = {item['ref']: item['object']['sha'] for item in json.loads(remote_response)}
pins = []
for name, expected in PINS.items():
    entry = {'branch': name, 'expected': expected,
             'local': local.get('refs/heads/' + name),
             'local_origin_tracking': local.get('refs/remotes/origin/' + name),
             'filesystem_origin': origin.get('refs/heads/' + name),
             'github': github.get('refs/heads/' + name)}
    entry['all_three_local_origin_locations_match_expected'] = all(entry[key] == expected for key in [
        'local', 'local_origin_tracking', 'filesystem_origin'])
    entry['github_publication_scope'] = ('origin-only original worker; separate review branch published'
                                         if name in ORIGIN_ONLY_WORKERS else 'same named GitHub branch')
    entry['github_matches_expected_scope'] = (entry['github'] is None if name in ORIGIN_ONLY_WORKERS
                                               else entry['github'] == expected)
    if name in ORIGIN_ONLY_WORKERS:
        direct = subprocess.run(['gh', 'api', 'repos/ligon/LSMS_Library/git/ref/heads/' + name],
                                capture_output=True)
        entry['direct_github_GET_returns404'] = direct.returncode != 0 and b'404' in direct.stderr
        assert entry['direct_github_GET_returns404'], name
    pins.append(entry)
assert all(entry['all_three_local_origin_locations_match_expected']
           and entry['github_matches_expected_scope'] for entry in pins), pins
published_pins = []
for name, expected in PUBLISHED_REVIEW_PINS.items():
    entry = {'branch': name, 'expected': expected,
             'local': local.get('refs/heads/' + name),
             'local_origin_tracking': local.get('refs/remotes/origin/' + name),
             'filesystem_origin': origin.get('refs/heads/' + name),
             'github': github.get('refs/heads/' + name)}
    entry['all_four_match_expected'] = all(entry[key] == expected for key in [
        'local', 'local_origin_tracking', 'filesystem_origin', 'github'])
    published_pins.append(entry)
assert all(entry['all_four_match_expected'] for entry in published_pins), published_pins

parents = git('show', '-s', '--format=%P', PUBLICATION).decode().strip().split()
assert parents == [CANDIDATE, DEVELOPMENT]
ancestry = {
    'publication_parents': parents,
    'candidate_is_ancestor_of_publication': ancestor(CANDIDATE, PUBLICATION),
    'merged_development_is_ancestor_of_publication': ancestor(DEVELOPMENT, PUBLICATION),
    'all_nine_original_task_tips_are_publication_ancestors': all(
        ancestor(PINS[name], PUBLICATION) for name in list(PINS)[:9]),
}
assert all(value for key, value in ancestry.items() if key != 'publication_parents')
report = json.loads(git('show', f'{PUBLICATION}:{PREFIX}1124-1125-review-r2.json'))
source_checks = []
for path, expected in report['source_hashes'].items():
    actual = hashlib.sha256(git('show', f'{CANDIDATE}:{path}')).hexdigest()
    source_checks.append({'path': path, 'recorded_r2_sha256': expected,
                          'candidate_sha256': actual, 'matches': actual == expected})
assert all(item['matches'] for item in source_checks)
result = {
    'status': 'PASS', 'scope': 'Static immutable-object identity and ancestry review only; no repository code, tests, builds, data, cache, or network catalogs executed',
    'reviewer': 'Sue / GPT-6', 'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'candidate': CANDIDATE, 'publication': PUBLICATION,
    'candidate_tree': git('rev-parse', CANDIDATE + '^{tree}').decode().strip(),
    'publication_tree': git('rev-parse', PUBLICATION + '^{tree}').decode().strip(),
    'candidate_path_count': len(left), 'publication_path_count': len(right),
    'identical_common_paths': sum(left.get(path) == right.get(path) for path in set(left) | set(right)),
    'full_tree_differences': differences,
    'namespace_counts': groups,
    'original_ref_pins': pins,
    'separate_published_review_ref_pins': published_pins,
    'github_original_worker_absence_note': 'Five original worker names return404 on direct GET; they remain pinned on filesystem origin and use separate published review/* branches. No assertion that every original worker was GitHub-published.',
    'expected_pin_provenance': 'Immutable STATUS.org at ' + DOC_PIN + ' plus prior reviewed task/integration freezes; full object IDs checked in four locations',
    'ancestry': ancestry,
    'added_review_r2_source_hash_checks': source_checks,
    'report_history_interpretation': 'Round1 FAIL is explicitly historical; round2 PASS resolves coarse EthiopiaRHS boundary and retains earlier history. Added blobs are unchanged existing PR1157 evidence already present on merged development.',
    'limitations': ['No executable validation rerun', 'A Git-tree match does not reproduce runtime environment or tagged dynamic version', 'Final merged development identity remains pending a parent-supplied immutable SHA', 'No claim of a clean semantic audit or PR-event data gate'],
    'mutations': 'Private report files only; no Git/GitHub/source/cache changes',
}
OUT.mkdir(parents=True, exist_ok=True)
(OUT / 'publication-verification.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
print(json.dumps({'status': result['status'], 'candidate_paths': len(left),
                  'publication_paths': len(right), 'identical_common_paths': result['identical_common_paths'],
                  'report_only_added_paths': len(differences), 'all_13_original_ref_pins_match_three_local_origin_locations': True,
                  'five_separate_review_publication_pins_match_all_four_locations': True,
                  'all_nine_original_task_tips_are_ancestors': True}, sort_keys=True))

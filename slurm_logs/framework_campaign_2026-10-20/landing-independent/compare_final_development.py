"""Read-only final merged-development identity/ancestry supplement."""
import concurrent.futures
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
DEVELOPMENT = '1933434b84de82ac72b3921a7fd8fe5c00154754'
PRE_MERGE = 'f50611161e9e58bf0676de5d771d4d6f07d0d8a3'
EXPECTED_PR_HEADS = {
    1154: 'eaf7b9913b37e04e924bd05a17e691e0263ecb4e',
    1155: 'f9871610f02bca8d1f82a352c4925353439cb6bc',
    1156: '1070a4d03e764ec8e3b0e61d4f825bc793eeeae7',
    1157: 'd84061818b0b430e8b00fa26b5b6d19e4694eb96',
    1161: '6553e01d6cd9013a2103124dcf7a2c945025a25c',
    1162: 'dffb0760406c74cea677f360f76be52cbed27975',
    1163: '124c2436d7e1a279c72f4d8f650d250eab6c7d67',
    1164: 'd124c0df70db2e04dceb4a8c19eb0d40df14dbee',
    1165: PUBLICATION,
}
PRIOR = ['publication-review.org', 'publication-verification.json', 'compare_publication.py']


def git(*args, cwd=REPO):
    return subprocess.check_output(['git', *args], cwd=cwd)


def ancestor(first, second):
    return subprocess.run(['git', 'merge-base', '--is-ancestor', first, second],
                          cwd=REPO, capture_output=True).returncode == 0


def hashes():
    return {name: hashlib.sha256((OUT / name).read_bytes()).hexdigest() for name in PRIOR}


def tree(sha):
    result = {}
    for entry in git('ls-tree', '-r', '-z', sha).split(b'\0'):
        if entry:
            metadata, path = entry.split(b'\t', 1)
            mode, kind, oid = metadata.decode().split()
            result[path.decode()] = {'mode': mode, 'type': kind, 'oid': oid}
    return result


def get_pr(number):
    record = json.loads(subprocess.check_output([
        'gh', 'api', f'repos/ligon/LSMS_Library/pulls/{number}'
    ]))
    expected = EXPECTED_PR_HEADS[number]
    result = {'number': number, 'merged': record['merged'], 'merged_at': record['merged_at'],
              'base_ref': record['base']['ref'], 'head_sha': record['head']['sha'],
              'expected_head_sha': expected, 'merge_commit_sha': record['merge_commit_sha']}
    result['head_matches_reviewed_publication'] = result['head_sha'] == expected
    result['head_is_ancestor_of_final_development'] = ancestor(result['head_sha'], DEVELOPMENT)
    result['merge_commit_is_ancestor_of_final_development'] = ancestor(result['merge_commit_sha'], DEVELOPMENT)
    assert result['merged'] and result['merged_at'] and result['base_ref'] == 'development', result
    assert result['head_matches_reviewed_publication'], result
    assert result['head_is_ancestor_of_final_development'], result
    assert result['merge_commit_is_ancestor_of_final_development'], result
    return result


prior_hashes = hashes()
prior = json.loads((OUT / 'publication-verification.json').read_text())
left, publication, final = map(tree, [CANDIDATE, PUBLICATION, DEVELOPMENT])
assert publication == final
diffs = []
for path in sorted(set(left) | set(final)):
    if left.get(path) != final.get(path):
        diffs.append({'path': path, 'candidate': left.get(path), 'final_development': final.get(path),
                      'classification': 'same previously reviewed PR1157 historical report-only addition'})
assert {entry['path'] for entry in diffs} == {entry['path'] for entry in prior['full_tree_differences']}
assert all(left.get(entry['path']) is None for entry in diffs)

parents = git('show', '-s', '--format=%P', DEVELOPMENT).decode().strip().split()
assert parents == [PRE_MERGE, PUBLICATION], parents
assert ancestor(CANDIDATE, DEVELOPMENT) and ancestor(PUBLICATION, DEVELOPMENT)
local_pin = git('rev-parse', 'refs/heads/development').decode().strip()
tracking_pin = git('rev-parse', 'refs/remotes/origin/development').decode().strip()
origin_pin = git('rev-parse', 'refs/heads/development', cwd=ORIGIN).decode().strip()
github_pin = json.loads(subprocess.check_output([
    'gh', 'api', 'repos/ligon/LSMS_Library/git/ref/heads/development'
]))['object']['sha']
pins = {'local': local_pin, 'local_origin_tracking': tracking_pin,
        'filesystem_origin': origin_pin, 'github': github_pin}
assert all(value == DEVELOPMENT for value in pins.values()), pins
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    prs = list(pool.map(get_pr, EXPECTED_PR_HEADS))
assert next(item for item in prs if item['number'] == 1165)['merge_commit_sha'] == DEVELOPMENT
assert hashes() == prior_hashes

result = {
    'status': 'PASS', 'reviewer': 'Sue / GPT-6',
    'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'scope': 'Immutable static tree, reference, GitHub GET, and ancestry supplement only',
    'reviewed_candidate': CANDIDATE, 'reviewed_publication': PUBLICATION,
    'final_development': DEVELOPMENT, 'development_pins': pins,
    'candidate_tree': git('rev-parse', CANDIDATE + '^{tree}').decode().strip(),
    'publication_tree': git('rev-parse', PUBLICATION + '^{tree}').decode().strip(),
    'final_development_tree': git('rev-parse', DEVELOPMENT + '^{tree}').decode().strip(),
    'publication_to_final_development_full_tree_identical': True,
    'candidate_paths': len(left), 'final_development_paths': len(final),
    'candidate_to_final_development_full_tree_differences': diffs,
    'runtime_tests_build_config_audit_and_all_other_common_paths_identical_to_candidate': True,
    'final_development_parents': parents,
    'candidate_and_publication_are_final_development_ancestors': True,
    'all_nine_prs_merged_to_development_with_reviewed_heads_and_ancestor_merge_commits': True,
    'merged_prs': prs,
    'prior_frozen_artifact_sha256_unchanged': prior_hashes,
    'limitations': ['No source modules or tests executed; no builds, survey data, cache access or catalog requests',
                    'No runtime environment or dynamic commit-derived version equivalence claim',
                    'This pin precedes the later evidence-only PR 1158 merge; that later SHA is not yet covered',
                    'No claim of a clean semantic audit or PR-event data gate'],
    'mutations': 'Only these new private aggregate report/reproduction files',
}
(OUT / 'final-development-verification.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
lines = [
    '#+TITLE: Final merged development static identity supplement', '#+AUTHOR: Sue', '',
    '* Verdict', 'PASS at immutable development =' + DEVELOPMENT + '=.',
    'All nine implementation PRs are merged to development with the exact reviewed publication heads.',
    'Every PR head and merge commit is an ancestor of this final development SHA.',
    'Local, origin-tracking, filesystem-origin and GitHub development refs all name this exact SHA.', '',
    '* Full-tree identity',
    'Development and reviewed publication 23c1 have exactly the same Git tree: =' + result['final_development_tree'] + '=.',
    'All 14,441 paths have identical blob IDs, types and modes.',
    'Against validated candidate 532, all 14,437 candidate paths are unchanged; the only additions remain the same four historical PR1157 review reports.',
    'There is no runtime/package/configuration, test/fixture, script/bin/tool, bench/audit, workflow, build/dependency, or other common-path difference.',
    'The final merge parents are exactly pre-merge development f506 and reviewed publication 23c1.', '',
    '* Merged implementation PRs',
    '| PR | Reviewed head | Merge commit |', '|----+---------------+--------------|',
]
for item in prs:
    lines.append(f"| {item['number']} | {item['head_sha']} | {item['merge_commit_sha']} |")
lines += [
    '', '* Preservation and limits',
    'SHA256 values prove the three prior frozen publication-review artifacts are unchanged.',
    'This supplement uses immutable Git objects and GitHub GET requests only; no repository code, tests, builds, data, caches, or catalogs are executed.',
    'The main dirty handoff/worktree is not an immutable evidence source.',
    'The existing exact 532 executable validation remains applicable to identical runtime/test/build/config/audit blobs; this supplement does not claim a new test run or identical commit-derived package version.',
    'Later evidence PR 1158 is outside this final-development pin and must preserve executable content when it lands.',
    'The 19 retained source/wiring gaps, five tenure findings and PR-event gating limitation remain separate readiness limits.',
    '', '* Reproduction',
    'Run standalone =compare_final_development.py= with the mounted image Python.',
    'Its only writes are the separately named final-development JSON and Org reports in this private directory.',
    'The prior publication report/controller/JSON remain frozen.', '', '-- Sue',
]
(OUT / 'final-development-review.org').write_text('\n'.join(lines) + '\n')
print(json.dumps({'status': 'PASS', 'final_development': DEVELOPMENT,
                  'same_tree_as_publication': True, 'all_nine_prs_verified_merged': True,
                  'candidate_common_paths_unchanged': len(left), 'report_only_additions': len(diffs),
                  'prior_frozen_artifacts_unchanged': True}, sort_keys=True))

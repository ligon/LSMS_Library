#!/bin/bash
# merge_on_green.sh <reviewed-approvals.json>
# Coordinator only. See ../savio-sweep.md for schema and the thread-proof limit.
# No polling into a different approval. Exit 3 means coordinator action required.
set -euo pipefail
[ "$#" = 1 ] || { echo 'usage: merge_on_green.sh <reviewed-approvals.json>' >&2; exit 2; }
exec "${PROJECT_PYTHON:-python3}" - "$1" <<'PY'
import json
import re
import subprocess
import sys

# Ledger 1423 §§3–4: approval is a reviewed full head/base pair, not a fresh poll.
def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def names(value):
    require(isinstance(value, list), 'check names must be explicit lists')
    require(all(isinstance(n, str) and n.strip() == n and n and '*' not in n
                for n in value), 'invalid check name')
    require(len(set(value)) == len(value), 'duplicate approved check name')
    return set(value)


def validate_approval(a):
    require(isinstance(a, dict), 'approval must be an object')
    require(set(a) == {'pr', 'head_sha', 'base_branch', 'base_sha',
                       'required_success_checks', 'permitted_skips'}, 'unsupported approval fields')
    require(type(a['pr']) is int and a['pr'] > 0, 'invalid PR number')
    for field in ('head_sha', 'base_sha'):
        require(isinstance(a[field], str) and re.fullmatch('[0-9a-f]{40}', a[field]),
                f'{field} must be a full lowercase SHA')
    require(a['base_branch'] == 'development', 'base must be development')
    required = names(a['required_success_checks'])
    skips = names(a['permitted_skips'])
    require(required, 'at least one required successful check is mandatory')
    require(not required & skips, 'required successful checks cannot be skipped')


def validate_observation(a, p):
    require(isinstance(p, dict), 'unsupported PR representation')
    for field, expected in (('number', a['pr']), ('state', 'OPEN'), ('isDraft', False),
                            ('headRefOid', a['head_sha']), ('baseRefName', 'development'),
                            ('baseRefOid', a['base_sha']), ('mergeable', 'MERGEABLE'),
                            ('mergeStateStatus', 'CLEAN'), ('reviewDecision', 'APPROVED')):
        require(field in p and type(p[field]) is type(expected) and p[field] == expected,
                f'{field} missing, unverified or drifted')
    checks = p.get('statusCheckRollup')
    require(isinstance(checks, list) and checks, 'missing check rollup')
    seen = set()
    for c in checks:
        require(isinstance(c, dict) and c.get('__typename') == 'CheckRun',
                'unsupported check representation (including legacy StatusContext)')
        n = c.get('name')
        require(isinstance(n, str) and n.strip() == n and n, 'missing check name')
        require(n not in seen, f'duplicate check: {n}')
        seen.add(n)
        require(c.get('status') == 'COMPLETED', f'check not completed: {n}')
        conclusion = c.get('conclusion')
        require(conclusion == 'SUCCESS' or
                (conclusion == 'SKIPPED' and n in a['permitted_skips']),
                f'check requires coordinator: {n} ({conclusion})')
    require(set(a['required_success_checks']) <= seen, 'missing required successful check')


try:
    with open(sys.argv[1], encoding='utf-8') as f:
        manifest = json.load(f, object_pairs_hook=unique_object)
    require(isinstance(manifest, dict) and
            set(manifest) == {'schema_version', 'repository', 'approvals'}, 'unsupported manifest')
    require(type(manifest['schema_version']) is int and manifest['schema_version'] == 1,
            'unsupported schema version')
    repo = manifest['repository']
    require(isinstance(repo, str) and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo),
            'explicit owner/repository required')
    approvals = manifest['approvals']
    require(isinstance(approvals, list) and approvals, 'no approvals')
    for a in approvals:
        validate_approval(a)
    require(len({a['pr'] for a in approvals}) == len(approvals), 'duplicate PR approval')
except (OSError, ValueError, TypeError, KeyError) as exc:
    print(f'needs coordinator: invalid approval manifest: {exc}', file=sys.stderr)
    sys.exit(3)

for a in approvals:
    n = a['pr']
    try:
        result = subprocess.run([
            'gh', 'pr', 'view', '-R', repo, str(n), '--json',
            'number,state,isDraft,headRefOid,baseRefName,baseRefOid,mergeable,'
            'mergeStateStatus,reviewDecision,statusCheckRollup',
        ], capture_output=True, text=True, check=True)
        p = json.loads(result.stdout, object_pairs_hook=unique_object)
        validate_observation(a, p)
    except (OSError, ValueError, TypeError, KeyError, subprocess.CalledProcessError) as exc:
        print(f'PR {n} needs coordinator: observation rejected: {exc}', file=sys.stderr)
        continue
    # gh pr view's reviewDecision is NOT a complete review/thread query. No
    # trustworthy pinned paginated thread query has been admitted for this helper.
    # Fail closed even for a fully green observation. Never execute the proposal.
    print(f'PR {n} needs coordinator: complete current review/thread proof unavailable; '
          'reconcile unresolved reviews/threads and revalidate head AND base immediately before merge')
    print('UNEXECUTED proposed argv (approved head only): ' + json.dumps([
        'gh', 'pr', 'merge', '-R', repo, str(n), '--merge',
        '--match-head-commit', a['head_sha'],
    ]))
sys.exit(3)
PY

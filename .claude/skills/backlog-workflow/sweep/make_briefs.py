#!/usr/bin/env python3
"""Build briefs from full current issue bodies and all paginated comments.

Coordinator only: this calls gh. Every snapshot records endpoints, page sizes,
counts and retrieval times, and still requires a full freshness/ruling read
before action; GitHub REST reads are not a transactional snapshot.
Ledger 1423 §§3–4: complete history is retained; task grants determine access.
"""
import argparse
import datetime
import hashlib
import json
import pathlib
import re
import subprocess


def gh_json(argv):
    return json.loads(subprocess.check_output(['gh', *argv], text=True))


def collect_issue(repo, number):
    endpoint = f'repos/{repo}/issues/{number}'
    issue_argv = ['api', '--method', 'GET', endpoint]
    comment_argv = ['api', '--method', 'GET', endpoint + '/comments?per_page=100',
                    '--paginate', '--slurp']
    before = gh_json(issue_argv)
    pages = gh_json(comment_argv)
    after = gh_json(issue_argv)
    if not isinstance(before, dict) or not isinstance(after, dict) or before != after:
        raise ValueError(f'issue {number} changed during retrieval; reconcile and regenerate')
    if (type(after.get('number')) is not int or after['number'] != number or
            not isinstance(after.get('body'), (str, type(None))) or
            not isinstance(after.get('title'), str) or
            not isinstance(after.get('updated_at'), str) or
            type(after.get('comments')) is not int or after['comments'] < 0 or
            'pull_request' in after):
        raise ValueError(f'unsupported issue representation: {number}')
    if (not isinstance(pages, list) or not pages or
            any(not isinstance(p, list) or len(p) > 100 for p in pages)):
        raise ValueError(f'unsupported paginated comment response: {number}')
    comments = [c for page in pages for c in page]
    if any(not isinstance(c, dict) or type(c.get('id')) is not int or
           not isinstance(c.get('body'), str) or
           not isinstance(c.get('updated_at'), str) for c in comments):
        raise ValueError(f'unsupported comment representation: {number}')
    if len(comments) != after['comments'] or len({c['id'] for c in comments}) != len(comments):
        raise ValueError(f'incomplete or duplicate comments for issue {number}')
    return {'issue': after, 'comments': comments, 'pagination': {
        'issue_argv': ['gh', *issue_argv], 'comments_argv': ['gh', *comment_argv],
        'page_sizes': [len(p) for p in pages], 'total_comments': len(comments),
        'issue_before_equals_after': True,
        'limit': 'REST reads are not atomic; refresh complete history and linked rulings before action',
    }}


def issue_text(record):
    i = record['issue']
    lines = [f"### #{i['number']} — {i['title']}",
             f"Updated: {i['updated_at']}; comments retained: {len(record['comments'])}",
             'BODY (complete):\n' + (i['body'] if i['body'] is not None else '')]
    for c in record['comments']:
        author = (c.get('user') or {}).get('login', '(deleted)')
        lines.append(f"COMMENT {c['id']} by {author}; updated {c['updated_at']}:\n" + c['body'])
    return '\n\n'.join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for key in ('queue', 'out', 'base-sha', 'candidate-sha', 'sweep-dir', 'python',
                'coord-clone', 'worktree-root', 'task-plan'):
        ap.add_argument('--' + key, required=True)
    ap.add_argument('--round', default='1')
    ap.add_argument('--repo', default='ligon/LSMS_Library')
    a = ap.parse_args()
    for sha in (a.base_sha, a.candidate_sha):
        if not re.fullmatch('[0-9a-f]{40}', sha):
            ap.error('base and candidate must be full lowercase SHAs')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', a.repo):
        ap.error('explicit owner/repository required')
    if not re.fullmatch('[1-9][0-9]*', a.round):
        ap.error('round must be a positive integer')
    queue = json.loads(pathlib.Path(a.queue).read_text(encoding='utf-8'))
    if not isinstance(queue, dict) or not queue:
        ap.error('queue must be a nonempty country-to-issue-list object')
    # Validate the whole input before any provider request or output write.
    for country, numbers in queue.items():
        if (not re.fullmatch('[A-Za-z][A-Za-z0-9_ -]*', country) or
                not isinstance(numbers, list) or not numbers or
                any(type(n) is not int or n <= 0 for n in numbers) or
                len(set(numbers)) != len(numbers)):
            ap.error('invalid country or issue list')
    task_plan = pathlib.Path(a.task_plan).read_text(encoding='utf-8')
    if not task_plan.strip():
        ap.error('explicit task plan cannot be empty')
    task_hash = hashlib.sha256(task_plan.encode()).hexdigest()
    tmpl = (pathlib.Path(__file__).parent / 'country_brief.md').read_text(encoding='utf-8')
    tmpl = tmpl.split('-->', 1)[1].lstrip('\n')
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    today = datetime.date.today().strftime('%Y%m%d')
    for country, numbers in queue.items():
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        records = [collect_issue(a.repo, n) for n in numbers]
        finished = datetime.datetime.now(datetime.timezone.utc).isoformat()
        snapshot = {'repository': a.repo, 'base_sha': a.base_sha, 'candidate_sha': a.candidate_sha,
                    'started_at': started, 'finished_at': finished,
                    'task_plan_sha256': task_hash, 'records': records}
        artifact = out / f'{country}-r{a.round}.issues.json'
        artifact.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        branch = f"isa/{country.lower().replace('_', '-').replace(' ', '-')}-{today}"
        text = tmpl.format_map({
            'WORKTREE': str(pathlib.Path(a.worktree_root) / country), 'PYTHON': a.python,
            'SWEEP_DIR': a.sweep_dir, 'COORD_CLONE': a.coord_clone,
            'BASE_SHA': a.base_sha, 'CANDIDATE_SHA': a.candidate_sha, 'ROUND': a.round,
            'BRANCH': branch, 'COUNTRY': country, 'REPO': a.repo,
            'TASK_PLAN': task_plan, 'TASK_PLAN_SHA256': task_hash,
            'HISTORY_EVIDENCE': str(artifact), 'RETRIEVED_AT': finished,
            'QUEUE_LIST': '\n'.join(f"- #{r['issue']['number']} {r['issue']['title']}" for r in records),
            'ISSUES': '\n\n---\n\n'.join(issue_text(r) for r in records),
        })
        (out / f'{country}-r{a.round}.md').write_text(text, encoding='utf-8')
        print(country, branch, len(text))


if __name__ == '__main__':
    main()

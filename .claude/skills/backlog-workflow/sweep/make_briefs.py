#!/usr/bin/env python3
"""make_briefs.py -- fill country_brief.md once per country from a queue file.

    make_briefs.py --queue queue.json --out DIR --base SHA [--round N] \
        --sweep-dir D --private-data P --python PY --coord-clone C --worktree-root W \
        --shared-l1 L1

queue.json: {"Malawi": [1075, 1174, ...], ...} (order = pre-sort priority).  Issue bodies and the
last two comments are fetched with `gh` and embedded, so each brief is self-contained.
Branch names are isa/<country-lowercase>-<YYYYMMDD of today>.
"""
import argparse, datetime, json, pathlib, subprocess

ap = argparse.ArgumentParser()
for k in ('queue', 'out', 'base', 'sweep-dir', 'private-data', 'python', 'coord-clone',
          'worktree-root', 'shared-l1'):
    ap.add_argument('--' + k, required=True)
ap.add_argument('--round', default='1')
ap.add_argument('--repo', default='ligon/LSMS_Library')
a = ap.parse_args()
tmpl = (pathlib.Path(__file__).parent / 'country_brief.md').read_text()
tmpl = tmpl.split('-->', 1)[1].lstrip('\n')          # drop the header comment
today = datetime.date.today().strftime('%Y%m%d')

def clip(s, n):
    s = (s or '').strip()
    return s if len(s) <= n else s[:n] + f'\n[... truncated; {len(s) - n} more chars -- read the issue on GitHub]'

out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)
for country, queue in json.load(open(a.queue)).items():
    issues = [json.loads(subprocess.check_output(
        ['gh', 'issue', 'view', '-R', a.repo, str(n), '--json', 'number,title,labels,updatedAt,body,comments']))
        for n in queue]
    blocks = []
    for i in issues:
        b = [f"### #{i['number']} -- {i['title']}",
             f"labels: {','.join(l['name'] for l in i['labels']) or '-'}; updated {i['updatedAt'][:10]}",
             'BODY:\n' + clip(i['body'], 5000)]
        b += [f"COMMENT ({c.get('createdAt', '')[:10]}):\n" + clip(c['body'], 2500) for c in i['comments'][-2:]]
        blocks.append('\n\n'.join(b))
    branch = f"isa/{country.lower().replace('_', '-')}-{today}"
    text = tmpl
    for k, v in {'{WORKTREE}': f"{a.worktree_root}/{country}", '{PYTHON}': a.python,
                 '{SWEEP_DIR}': a.sweep_dir, '{SHARED_L1}': a.shared_l1,
                 '{PRIVATE_DATA}': a.private_data, '{COORD_CLONE}': a.coord_clone,
                 '{BASE}': a.base, '{ROUND}': a.round, '{BRANCH}': branch, '{COUNTRY}': country,
                 '{QUEUE_LIST}': '\n'.join(f"- #{i['number']} {i['title']}" for i in issues),
                 '{ISSUES}': '\n\n---\n\n'.join(blocks)}.items():
        text = text.replace(k, v)
    (out / f'{country}-r{a.round}.md').write_text(text)
    print(country, branch, len(text))

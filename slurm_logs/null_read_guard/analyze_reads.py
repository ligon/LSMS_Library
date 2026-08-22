"""Summarise the read sweep: how often would a naive 'column is 100% null after
read' test fire, and on what?"""
import json, glob, sys
from collections import Counter, defaultdict

recs = []
for p in sorted(glob.glob('/tmp/claude-43292/-global-scratch-fsa-fc-jevons-ligon-mirrors-LSMS-Library/25ce0c52-31ea-4572-beb6-4e970ec341f6/scratchpad/sweep_?.jsonl')):
    for line in open(p):
        recs.append(json.loads(line))

print(f'total file-read records: {len(recs)}')
print('status:', Counter(r['status'] for r in recs))
ok = [r for r in recs if r['status'] == 'ok']
print(f'\nmeasured reads: {len(ok)}')
print(f'  zero-row frames: {sum(1 for r in ok if r["nrow"]==0)}')

# ---- Tier D: every all-null column at the raw-read level (the firehose) ----
cells = 0
files_with = 0
for r in ok:
    if r['n_allnull']:
        files_with += 1
        cells += r['n_allnull']
tot_cols = sum(r['ncol'] for r in ok)
print(f'\n[Site R, per-column] files with >=1 all-null column: {files_with}/{len(ok)}')
print(f'[Site R, per-column] all-null (file,column) cells: {cells} of {tot_cols} columns read'
      f' ({100*cells/max(tot_cols,1):.1f}%)')

# fraction histogram
buckets = Counter()
for r in ok:
    f = r['frac_allnull'] or 0
    if f == 0: b = '0'
    elif f < .25: b = '(0,25%)'
    elif f < .5: b = '[25,50%)'
    elif f < .75: b = '[50,75%)'
    elif f < 1: b = '[75,100%)'
    else: b = '100%'
    buckets[b] += 1
print('\n[Site R] fraction-of-columns-all-null histogram (files):')
for k in ['0', '(0,25%)', '[25,50%)', '[50,75%)', '[75,100%)', '100%']:
    print(f'   {k:>10}: {buckets[k]}')

whole = [r for r in ok if r['ncol'] and r['n_allnull'] == r['ncol'] and r['nrow'] > 0]
print(f'\n[Site R, whole-frame] frames 100% null (nrow>0): {len(whole)}')
for r in whole[:25]:
    print(f'    {r["country"]}/{r["wave"]}/{r["file"]}  {r["nrow"]}x{r["ncol"]}')

# ---- Tier A/B/C: all-null columns that a table actually ASKED for ----
req_allnull = []
req_absent = []
for r in ok:
    for rc, info in (r.get('requested') or {}).items():
        row = {'country': r['country'], 'wave': r['wave'], 'file': r['file'],
               'raw': rc, 'uses': info['uses'], 'nrow': r['nrow']}
        if info['state'] == 'all-null':
            req_allnull.append(row)
        elif info['state'] == 'absent':
            req_absent.append(row)
print(f'\n[Requested columns] all-null: {len(req_allnull)}   absent-from-file: {len(req_absent)}')
print('  all-null requested cells (country/wave/file :: raw -> out@table):')
for row in req_allnull:
    outs = ','.join(f"{u['out']}@{u['table']}" for u in row['uses'])
    print(f"    {row['country']}/{row['wave']}/{row['file']} :: {row['raw']} -> {outs} (nrow={row['nrow']})")

print('\n  absent requested cells (first 40):')
for row in req_absent[:40]:
    outs = ','.join(f"{u['out']}@{u['table']}" for u in row['uses'])
    print(f"    {row['country']}/{row['wave']}/{row['file']} :: {row['raw']} -> {outs}")

# unmeasured / not-held
print('\nnot-held / unmeasured by route:',
      Counter((r['status'], r['route']) for r in recs if r['status'] != 'ok'))

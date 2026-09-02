"""Site-B firing rate: required declared columns that are 100% null."""
import json, glob
from collections import Counter, defaultdict

SP = '/tmp/claude-43292/-global-scratch-fsa-fc-jevons-ligon-mirrors-LSMS-Library/25ce0c52-31ea-4572-beb6-4e970ec341f6/scratchpad/'
recs = [json.loads(l) for p in sorted(glob.glob(SP + 'build_?.jsonl')) for l in open(p)]
print('build records:', len(recs), Counter(r.get('status') for r in recs))
ok = [r for r in recs if r.get('status') == 'ok']
print('countries built:', len(set(r['country'] for r in ok)))
print('total cold build seconds (sum over shards):',
      round(sum(r.get('secs', 0) for r in recs)))

whole_hits = []      # (country, table, column)
wave_hits = []       # (country, table, column, [waves])
all_col_whole = []   # any column (incl. undeclared) whole-null -- for contrast
for r in ok:
    req = set(r['required'])
    declared = set(r['declared'])
    for c in r['whole_allnull']:
        all_col_whole.append((r['country'], r['table'], c))
        if c in req:
            whole_hits.append((r['country'], r['table'], c))
    bycol = defaultdict(list)
    for t, info in (r['perwave'] or {}).items():
        for c in info['allnull']:
            if c in r['whole_allnull']:
                continue
            bycol[c].append(t)
    for c, ws in bycol.items():
        if c in req:
            wave_hits.append((r['country'], r['table'], c, sorted(ws)))

print()
print(f'[Site B] REQUIRED declared column, 100% null in EVERY wave: {len(whole_hits)}')
for h in whole_hits:
    print('   ', '/'.join(h))
print()
print(f'[Site B] REQUIRED declared column, 100% null in SOME wave(s): {len(wave_hits)}')
for c, t, col, ws in sorted(wave_hits):
    print(f'    {c}/{t}/{col}  waves={ws}')
print()
print(f'[contrast] ANY column (declared or not) 100% null across the table: '
      f'{len(all_col_whole)}')
print()
tables_firing = {(c, t) for c, t, *_ in whole_hits} | {(c, t) for c, t, *_ in wave_hits}
print(f'tables that would emit >=1 Site-B warning: {len(tables_firing)} of {len(ok)}')
print('by country:', Counter(c for c, _ in tables_firing).most_common())

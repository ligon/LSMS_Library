import json, re, sys
from collections import defaultdict
res = json.load(open(sys.argv[1]))
def norm(s):
    s = re.sub(r'^\s*\d+\s*[\.\)]?\s*', '', s.upper())
    s = re.sub(r'[^A-Z0-9/ ]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()
by = defaultdict(lambda: defaultdict(set))
nfiles = defaultdict(lambda: defaultdict(set))
for w, files in res.items():
    for f, vars_ in files.items():
        if '_error' in vars_: continue
        for v, d in vars_.items():
            for k, lab in d['labels'].items():
                by[k][w].add(norm(lab)); nfiles[k][(w, norm(lab))].add(f)
codes = sorted(by, key=lambda x: float(x))
print('codes seen:', len(codes))
for k in codes:
    alllabs = set().union(*by[k].values())
    if len(alllabs) > 1:
        parts = []
        for w in sorted(by[k]):
            parts.append(w[:4] + ':' + '|'.join(f'{l}' + ('' if len(by[k][w]) == 1 else f'[{len(nfiles[k][(w,l)])}f]') for l in sorted(by[k][w])))
        print(f'{k:>4}  ' + '  ;  '.join(parts))

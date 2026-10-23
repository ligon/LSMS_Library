"""Multiset diff of two bench/feature_audit/scan.py result files, with each
run's checkout/data-root paths normalised.  Usage: python diff_scans.py base.jsonl cand.jsonl"""
import json, sys, re, collections
def norm(s):
    s = re.sub(r'/local/job\d+/data-(base|cand)', '<DATA>', str(s))
    return re.sub(r'/local/job\d+/(base|cand)', '<ROOT>', s)
def key(x):
    return (x.get('phase'), x.get('feature'), x.get('check'), x.get('country'),
            json.dumps(x.get('kwargs'), sort_keys=True), x.get('status'),
            norm(x.get('detail', x.get('reason', ''))))
def load(p):
    return collections.Counter(key(json.loads(l)) for l in open(p) if l.strip())
b, c = load(sys.argv[1]), load(sys.argv[2])
for name, d in (('only in base', b - c), ('only in cand', c - b)):
    print(f'== {name}: {sum(d.values())}')
    for k, n in sorted(d.items(), key=lambda kv: str(kv[0])):
        print(n, '|', *k[:6], '|', k[6][:200])

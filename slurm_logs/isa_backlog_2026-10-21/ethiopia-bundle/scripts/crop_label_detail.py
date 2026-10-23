import json, re, sys
res = json.load(open(sys.argv[1]))
codes = sys.argv[2].split(',')
for w, files in sorted(res.items()):
    for f, vars_ in sorted(files.items()):
        for v, d in sorted(vars_.items()):
            labs = {k: d['labels'].get(k) for k in codes}
            print(f'{w} {f:22} {v:12} {d["labelset"]:14} ' + ' | '.join(f'{k}={labs[k]!r}' for k in codes))

"""Compact the full value-label sweep into the evidence file for GH #1252.
usage: crop_label_sweep_summary.py <crop_labels.json> <out.json>"""
import json, re, sys
from collections import defaultdict
res = json.load(open(sys.argv[1]))
def norm(s):
    s = re.sub(r'^(\s*\d+\s*\.\s*)+', '', s.upper())
    s = re.sub(r'^\d+\.', '', s)
    s = re.sub(r'[^A-Z0-9/ ]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()
files = {w: {f: sorted(v) for f, v in fs.items()} for w, fs in res.items()}
by = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
for w, fs in res.items():
    for f, vs in fs.items():
        for v, d in vs.items():
            for k, lab in d['labels'].items():
                by[k][w][norm(lab)].add(f'{f}:{v}')
var = {}
for k in sorted(by, key=float):
    labs = set(l for w in by[k] for l in by[k][w])
    # spacing-insensitive comparison so 'HARICOTBEANS' == 'HARICOT BEANS'
    if len({l.replace(' ', '') for l in labs}) > 1:
        var[k] = {w: {l: sorted(fv) for l, fv in by[k][w].items()} for w in sorted(by[k])}
CLASS = {'9': 'MEANING: ROMAN (W1, W2; W3 stale sect7_pp_w3 set) -> MUNG BEAN/MASHO (W3 on)',
         '19': 'MEANING: CACTUS (W1, W2; W3 stale sect3_pp_w3 / sect7_pp_w3 sets) -> RED KIDENY BEANS (W3 on)',
         '86': 'MEANING: FALLOW LAND (W1 post-harvest files) / TEMPORARY GR (W1 post-planting files, W2 on)',
         '89': 'same meaning, refined: FOREST LAND (W1) -> FOREST LAND/ EUCALYPTUS TREE (W2)'}
out = {'files_and_variables_swept': files, 'codes_seen': len(by),
       'codes_whose_label_text_varies': var,
       'classification': {k: CLASS.get(k, 'spelling / prefix / truncation only (same meaning)') for k in var}}
json.dump(out, open(sys.argv[2], 'w'), indent=1)
print(len(by), {k: out['classification'][k][:40] for k in var})

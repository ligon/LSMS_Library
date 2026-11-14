"""Crude BID text search without a PDF library: inflate every FlateDecode stream and
decode text-showing operators.  usage: rt_bid.py <outdir>"""
import re, sys, zlib, json
from pathlib import Path
from lsms_library.data_access import get_data_file
out = Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
pdf = get_data_file('Benin/2021-22/Documentation/ben_ehcvm2_information_de_base_eng.pdf')
raw = Path(pdf).read_bytes()
texts = []
for m in re.finditer(rb'stream\r?\n(.*?)\r?\nendstream', raw, re.S):
    try:
        s = zlib.decompress(m.group(1))
    except Exception:
        continue
    # text in (...) Tj / [...] TJ
    parts = []
    for tj in re.finditer(rb'\[(.*?)\]\s*TJ|\((.*?)\)\s*Tj', s, re.S):
        chunk = tj.group(1) or tj.group(2)
        strs = re.findall(rb'\(((?:\\.|[^\\)])*)\)', chunk) if tj.group(1) else [chunk]
        parts.append(b''.join(strs))
    if parts:
        texts.append(b'\n'.join(parts).decode('latin-1', 'replace'))
full = '\n=====\n'.join(texts)
(out / 'bid_crude.txt').write_text(full)
hits = [m.start() for m in re.finditer(r'PanelHH|merging|panel survey', full)]
res = {'n_streams_with_text': len(texts), 'hits': [full[max(0, h - 700): h + 400] for h in hits][:8],
       'raw_bytes_PanelHH': raw.count(b'PanelHH')}
json.dump(res, open(out / 'bid.json', 'w'), indent=1)
print(json.dumps(res, indent=1))

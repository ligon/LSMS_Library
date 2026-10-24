"""Search mutant pytest outputs for Mali household ids, inside each FAILURE
block and, separately, in the rest of the file (warnings summary etc.).
Adapted from the red-team review (idleak.py / leak_by_test.py).
usage: idleak.py <crop_production.parquet (reset_index)> <dir with *.pytest.txt>"""
import sys, re, glob, os
import pandas as pd
ids = {x for x in pd.read_parquet(sys.argv[1], columns=['i'])['i'].astype(str) if len(x) >= 5}
print('Mali crop_production ids (>= 5 chars):', len(ids))
TOK = re.compile(r'(?<![\w.])(\d{5,7})(?![\w.])')
for f in sorted(glob.glob(f'{sys.argv[2]}/*.pytest.txt')):
    txt = open(f, encoding='utf-8', errors='replace').read()
    last = txt.strip().splitlines()[-1] if txt.strip() else ''
    m = re.search(r'\n=+ FAILURES =+\n(.*?)(\n=+ (warnings summary|short test summary info) =+\n|\Z)', txt, re.S)
    fail = m.group(1) if m else ''
    rest = txt.replace(fail, '') if fail else txt
    blocks = re.split(r'\n_{3,} (\S+) _{3,}\n', '\n' + fail)
    per = {name: len(set(TOK.findall(body)) & ids) for name, body in zip(blocks[1::2], blocks[2::2])}
    hit_blocks = {k: v for k, v in per.items() if v}
    print(f"{os.path.basename(f)}: failure blocks {len(per)}, with an id {len(hit_blocks)} {hit_blocks}; "
          f"ids elsewhere in the output {len(set(TOK.findall(rest)) & ids)} :: {last}")

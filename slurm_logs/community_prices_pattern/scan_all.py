"""Corpus-wide sweep: every .dta/.sav DVC source whose blob is ALREADY in L1,
flagged for price-like variable labels.  No S3 pulls (warm=False)."""
import sys, os, re, yaml, json, time
sys.path.insert(0, os.environ['WT'])
import lsms_library
assert 'wt-gsps-inputs' in lsms_library.__file__, lsms_library.__file__
from pathlib import Path
from lsms_library.paths import countries_root, data_root
assert str(countries_root()).startswith(os.environ['WT'])
import pyreadstat
SCRATCH=os.environ['SCRATCH']; ROOT=countries_root()
PRICE_LAB = re.compile(r'(\bprice\b|\bprices\b|\bprix\b|precio|preco|unit value|per kg|market price|prevailing|selling price|cours du)', re.I)
QTY_LAB   = re.compile(r'(quantit|\bunit\b|\bunite\b|unité|\bkg\b|weight|mesure)', re.I)

cands=[]
for line in open(os.path.join(SCRATCH,'all_dvc.txt')):
    p=Path(line.strip())
    if '/Documentation/' in str(p): continue
    base=p.name[:-4]
    if Path(base).suffix.lower() not in ('.dta','.sav'): continue
    cands.append(p)
print("candidates", len(cands), flush=True)
out=[]; t0=time.time(); nomiss=0
for i,sc in enumerate(cands):
    src=sc.parent/sc.name[:-4]
    try: md5=yaml.safe_load(sc.read_text())['outs'][0]['md5']
    except Exception: continue
    q=Path(data_root())/'dvc-cache'/md5[:2]/md5[2:]
    if not q.exists(): nomiss+=1; continue
    m=None
    for enc in (None,'latin1'):
        try:
            kw={'metadataonly':True}
            if enc: kw['encoding']=enc
            m = pyreadstat.read_dta(str(q),**kw)[1] if src.suffix.lower()=='.dta' else pyreadstat.read_sav(str(q),**kw)[1]
            break
        except Exception: pass
    if m is None: continue
    labs=m.column_names_to_labels or {}
    hits={k:str(v) for k,v in labs.items() if v and PRICE_LAB.search(str(v))}
    if not hits: continue
    out.append(dict(rel=str(src.relative_to(ROOT)), rows=m.number_rows, cols=m.number_columns,
                    file_label=str(m.file_label), n_price=len(hits),
                    price_vars=dict(list(hits.items())[:14]),
                    colnames=list(labs)[:60]))
    if i%400==0: print(f"{i}/{len(cands)} hits={len(out)} miss={nomiss} {time.time()-t0:.0f}s", flush=True)
json.dump(dict(scanned=len(cands), not_in_L1=nomiss, hits=out),
          open(os.path.join(SCRATCH,'scan_all.json'),'w'), indent=1)
print("DONE hits", len(out), "not_in_L1", nomiss, f"{time.time()-t0:.0f}s")

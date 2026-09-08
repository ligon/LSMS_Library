import sys, os, yaml
sys.path.insert(0, os.environ['WT'])
import lsms_library
from pathlib import Path
from lsms_library.paths import countries_root, data_root
from lsms_library.local_tools import _ensure_dvc_pulled
import pyreadstat
ROOT=countries_root()
for rel in sys.argv[1:]:
    src=ROOT/rel; sc=src.parent/(src.name+'.dvc')
    md5=yaml.safe_load(sc.read_text())['outs'][0]['md5']
    q=Path(data_root())/'dvc-cache'/md5[:2]/md5[2:]
    if not q.exists(): _ensure_dvc_pulled(str(src))
    m=None
    for enc in (None,'latin1'):
        try:
            kw={'metadataonly':True}
            if enc: kw['encoding']=enc
            m=pyreadstat.read_dta(str(q),**kw)[1] if src.suffix.lower()=='.dta' else pyreadstat.read_sav(str(q),**kw)[1]
            break
        except Exception: pass
    print(f"### {rel} rows={m.number_rows} cols={m.number_columns}")
    for k,v in (m.column_names_to_labels or {}).items(): print(f"   {k}: {v}")

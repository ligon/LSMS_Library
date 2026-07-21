import glob, yaml, os, re
import pandas as pd
from lsms_library.local_tools import get_dataframe

countries = ['Benin','Burkina_Faso','CotedIvoire','Guinea-Bissau','Mali','Niger','Senegal','Togo']
root = 'lsms_library/countries'

def get_block(yml_path):
    with open(yml_path) as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict):
        return None
    blk = doc.get('interview_date')
    return blk if isinstance(blk, dict) else None

for c in countries:
    waves = sorted(glob.glob(f'{root}/{c}/*/_/data_info.yml'))
    for yml in waves:
        wave = yml.split('/')[-3]
        blk = get_block(yml)
        if blk is None:
            continue
        fn = blk.get('file')
        myv = blk.get('myvars', {})
        src = myv.get('Int_t') or myv.get('int_t')
        datadir = os.path.dirname(os.path.dirname(yml)) + '/Data'
        path = f'{datadir}/{fn}' if fn else None
        tag = f'{c}/{wave}'
        try:
            df = get_dataframe(path)
            cols = list(df.columns)
            # date-ish: the configured source + any s00q23* + anything with 'date' in name
            base = re.sub(r'[a-z]$','', str(src)) if src else ''
            sibs = sorted([x for x in cols if base and x.lower().startswith(base.lower())])
            dateish = sorted(set(sibs) | {x for x in cols if 'date' in x.lower()})
            info = []
            for col in dateish:
                nn = df[col].dropna()
                sample = str(nn.iloc[0]) if len(nn) else 'EMPTY'
                info.append(f'{col}({df[col].dtype},nn={len(nn)},e.g.={sample[:19]})')
            print(f'{tag:28s} file={fn} src={src}')
            print(f'    date-ish cols: {", ".join(info) if info else "NONE FOUND"}')
        except Exception as e:
            print(f'{tag:28s} file={fn} src={src}  -- LOAD FAILED: {type(e).__name__}: {str(e)[:80]}')

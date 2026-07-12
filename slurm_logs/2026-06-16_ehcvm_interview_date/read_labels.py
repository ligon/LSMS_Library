import glob, yaml, os, re
import pyreadstat
from lsms_library.local_tools import data_root

CACHE = os.path.join(str(data_root()), 'dvc-cache')

def blob_for(md5):
    for cand in (os.path.join(CACHE, 'files', 'md5', md5[:2], md5[2:]),
                 os.path.join(CACHE, md5[:2], md5[2:])):
        if os.path.exists(cand):
            return cand
    return None

def md5_of(dvc_path):
    with open(dvc_path) as f:
        d = yaml.safe_load(f)
    return d['outs'][0]['md5']

countries = ['Benin','Burkina_Faso','CotedIvoire','Guinea-Bissau','Mali','Niger','Senegal','Togo']
root = 'lsms_library/countries'

for c in countries:
    for yml in sorted(glob.glob(f'{root}/{c}/*/_/data_info.yml')):
        wave = yml.split('/')[-3]
        with open(yml) as f:
            doc = yaml.safe_load(f)
        blk = doc.get('interview_date') if isinstance(doc, dict) else None
        if not isinstance(blk, dict):
            continue
        fn = blk.get('file')
        if not isinstance(fn, str):
            continue  # skip multi-file EACI (out of scope)
        datadir = os.path.dirname(os.path.dirname(yml)) + '/Data'
        # handle ../DataN prefixes
        dvc = os.path.normpath(os.path.join(datadir, fn)) + '.dvc'
        if not os.path.exists(dvc):
            # try literal join (Togo ../Data1)
            dvc = os.path.normpath(os.path.join(os.path.dirname(os.path.dirname(yml)), fn)) + '.dvc'
        try:
            blob = blob_for(md5_of(dvc))
            _, meta = pyreadstat.read_dta(blob, metadataonly=True)
            l = meta.column_names_to_labels
            print(f'{c}/{wave}:')
            for col in ('s00q23a','s00q23b','s00q23','s00q22a','s00q22b','s00q24a','s00q24b'):
                if col in l:
                    print(f'    {col}: {l[col]!r}')
        except Exception as e:
            print(f'{c}/{wave}: LABEL READ FAILED ({type(e).__name__}: {str(e)[:70]})  dvc={dvc}')

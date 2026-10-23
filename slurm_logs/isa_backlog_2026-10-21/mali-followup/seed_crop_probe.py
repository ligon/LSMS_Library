"""Mali plot_inputs: why do some SEED rows carry no crop?

For each EAC-I seed roster (2014-15 EACIS1E_p2 s1eq03b; 2017-18
eaci17_s11cp1 s11cq03), counts rows whose raw crop is blank versus recorded
but unmapped by mali._crop_labels (harmonize_food), with the unmapped raw
labels.  Aggregate counts only.

Usage (PYTHONPATH=<checkout>, private LSMS_DATA_DIR):
    python seed_crop_probe.py <checkout> <out.json>
"""
import json
import os
import sys
import warnings
from pathlib import Path

checkout, out_path = Path(sys.argv[1]).resolve(), sys.argv[2]
import pandas as pd  # noqa: E402
import lsms_library  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(str(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
mali_dir = checkout / 'lsms_library/countries/Mali'
sys.path.insert(0, str(mali_dir / '_'))

SPEC = {'2014-15': ('../Data/EACIS1E_p2.dta', 's1eq03b', 's1eq05a'),
        '2017-18': ('../Data/eaci17_s11cp1.dta', 's11cq03', 's11cq11a')}
res = {}
for wave, (fn, ccol, qcol) in SPEC.items():
    cwd = os.getcwd()
    os.chdir(mali_dir / wave / '_')
    try:
        import mali  # noqa: E402  (resolves categorical tables from this cwd)
        df = get_dataframe(fn)
        mapped = mali._crop_labels(df[ccol])
    finally:
        os.chdir(cwd)
    raw = df[ccol].astype('string').str.strip()
    blank = raw.isna() | (raw == '')
    unmapped = ~blank & mapped.isna()
    q = pd.to_numeric(df[qcol], errors='coerce')
    res[wave] = {'rows': int(len(df)), 'raw_crop_blank': int(blank.sum()),
                 'raw_crop_blank_with_quantity': int((blank & q.notna()).sum()),
                 'raw_crop_recorded_unmapped': int(unmapped.sum()),
                 'unmapped_labels': {str(k): int(v) for k, v in raw[unmapped].value_counts().items()}}

with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, sort_keys=True, ensure_ascii=False)
print(json.dumps(res, indent=1, sort_keys=True, ensure_ascii=False))

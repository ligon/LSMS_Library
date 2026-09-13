"""Collect GLSS5/6/7 Section 9A item code -> label, from each wave's own source."""
import warnings, os, re; warnings.filterwarnings('ignore')
import pyreadstat, pandas as pd
from lsms_library.data_access import get_data_file

def _norm(s):
    s = str(s).strip().lower()
    s = re.sub(r'\s+', ' ', s)
    s = s.replace('’', "'")
    return s.strip(' .,:;-')

def value_labels(wave, fn, var):
    p = get_data_file(f'countries/GhanaLSS/{wave}/Data/{fn}')
    _, m = pyreadstat.read_dta(p, metadataonly=True)
    return {int(k): _norm(v) for k, v in (m.variable_value_labels.get(var) or {}).items()}

def glss7_names(repo):
    from lsms_library.local_tools import get_dataframe
    cwd = os.getcwd(); os.chdir(f'{repo}/lsms_library/countries/GhanaLSS/2016-17/_')
    try:
        a = get_dataframe('../Data/g7sec9a.dta', convert_categoricals=False)
    finally:
        os.chdir(cwd)
    nm = a[['lfreqcd', 's9aname']].copy()
    nm['n'] = nm.s9aname.astype(str).map(_norm)
    nm = nm[nm.n.ne('') & nm.n.ne('nan')]
    # modal name per code (1 of 508 codes is ambiguous)
    mode = nm.groupby('lfreqcd').n.agg(lambda s: s.value_counts().idxmax())
    return {int(k): v for k, v in mode.items()}

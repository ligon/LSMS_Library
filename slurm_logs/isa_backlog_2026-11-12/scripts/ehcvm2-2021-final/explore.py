"""Read the covers' region / milieu labels and Benin 2021-22's `strat` (light reads).

usage: python explore.py OUT_JSON
Reads through lsms_library.local_tools.get_dataframe only (private L1).
"""
import json, os, sys, warnings
warnings.filterwarnings('ignore')
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep)
from lsms_library import paths
from lsms_library.local_tools import get_dataframe
import pandas as pd

R = paths.countries_root()
COVERS = {
    ('Togo', '2018'): 'Togo/2018/Data1/s00_me_tgo2018.dta',
    ('Togo', '2021-22'): 'Togo/2021-22/Data/s00_me_tgo2021.dta',
    ('Benin', '2018-19'): 'Benin/2018-19/Data/s00_me_ben2018.dta',
    ('Benin', '2021-22'): 'Benin/2021-22/Data/s00_me_ben2021.dta',
    ('CotedIvoire', '2018-19'): 'CotedIvoire/2018-19/Data/Menage/s00_me_CIV2018.dta',
    ('CotedIvoire', '2021-22'): 'CotedIvoire/2021-22/Data/s00_me_civ2021.dta',
}
out = {}
for (c, w), rel in COVERS.items():
    lab = get_dataframe(R / rel, convert_categoricals=True)
    raw = get_dataframe(R / rel, convert_categoricals=False)
    rec = {'rows': len(lab)}
    for v in ('s00q01', 's00q04'):
        rec[f'{v}_dtype'] = str(lab[v].dtype)
        rec[f'{v}_labels'] = {repr(k): int(n) for k, n in lab[v].astype('object').value_counts(dropna=False).items()}
        pairs = pd.DataFrame({'code': raw[v], 'label': lab[v].astype('object')}).drop_duplicates()
        rec[f'{v}_code_label'] = sorted([[None if pd.isna(a) else float(a), None if pd.isna(b) else str(b)]
                                         for a, b in pairs.itertuples(index=False)], key=lambda x: (x[0] is None, x[0] or 0))
    cell = lab[['s00q01', 's00q04']].astype('object').astype(str)
    rec['cells_households'] = {f'{a} | {b}': int(n) for (a, b), n in cell.value_counts().items()}
    g = pd.DataFrame({'grappe': raw['grappe'], 'cell': cell['s00q01'] + ' | ' + cell['s00q04']})
    per = g.groupby('grappe')['cell'].nunique()
    rec['grappes'] = int(per.size)
    rec['grappes_two_cells'] = {str(k): int(v) for k, v in per[per > 1].items()}
    if 's00q08' in lab:
        rec['s00q08'] = {repr(k): int(n) for k, n in lab['s00q08'].astype('object').value_counts(dropna=False).items()}
    out[f'{c}|{w}'] = rec
# Benin 2021-22 weights file: every column name, and strat
wl = get_dataframe(R / 'Benin/2021-22/Data/ehcvm_ponderations_ben2021.dta', convert_categoricals=True)
wr = get_dataframe(R / 'Benin/2021-22/Data/ehcvm_ponderations_ben2021.dta', convert_categoricals=False)
out['benin_weights_columns'] = list(wr.columns)
if 'strat' in wl:
    out['benin_strat_labels'] = {repr(k): int(n) for k, n in wl['strat'].astype('object').value_counts(dropna=False).items()}
    pairs = pd.DataFrame({'code': wr['strat'], 'label': wl['strat'].astype('object')}).drop_duplicates()
    out['benin_strat_code_label'] = sorted([[None if pd.isna(a) else float(a), str(b)] for a, b in pairs.itertuples(index=False)],
                                           key=lambda x: (x[0] is None, x[0] or 0))
json.dump(out, open(sys.argv[1], 'w'), indent=1, ensure_ascii=False, default=str)
print('ok')

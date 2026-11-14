"""Benin 2021-22: the weights file's own `strat` against the constructed strata.

usage: python benin_strat.py CAND_SAMPLE_PARQUET OUT_JSON   (reads via get_dataframe)
"""
import json, os, sys, warnings
warnings.filterwarnings('ignore')
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep)
from lsms_library import paths
from lsms_library.local_tools import get_dataframe
import pandas as pd

R = paths.countries_root() / 'Benin' / '2021-22' / 'Data'
w = get_dataframe(R / 'ehcvm_ponderations_ben2021.dta', convert_categoricals=False)
wl = get_dataframe(R / 'ehcvm_ponderations_ben2021.dta', convert_categoricals=True)
cov = get_dataframe(R / 's00_me_ben2021.dta', convert_categoricals=False)
covl = get_dataframe(R / 's00_me_ben2021.dta', convert_categoricals=True)
out = {'weights_columns': list(w.columns), 'cover_panel_columns': [c for c in cov.columns if 'anel' in c]}
out['strat_dtype'] = str(wl['strat'].dtype)
out['vague_labels'] = {str(k): str(v) for k, v in dict(zip(w['vague'], wl['vague'].astype('object'))).items()}
w['i'] = ['E2_' + str(int(g)) + f'{int(m):03d}' for g, m in zip(w['grappe'], w['menage'])]
cand = pd.read_parquet(sys.argv[1])
cand = cand[cand['t'].astype(str) == '2021-22'].set_index('i')
w = w.set_index('i')
w['strata'] = cand['strata'].reindex(w.index)
out['joined'] = int(w['strata'].notna().sum())
out['rows'] = len(w)
w['strat'] = w['strat'].astype('Int64')
x = pd.crosstab(w['strata'], w['strat'])
out['crosstab_strata_by_strat'] = {r: {str(k): int(v) for k, v in x.loc[r].items() if v} for r in x.index}
out['n_strat'] = int(w['strat'].nunique())
out['n_strata'] = int(w['strata'].nunique())
# Hypotheses for the second digit
dept = w['s00q01'].astype('Int64') if 's00q01' in w else None
if dept is not None:
    out['strat_eq_10dept_plus_vague'] = int((w['strat'] == dept * 10 + w['vague'].astype('Int64')).sum())
    out['strat_eq_10dept_plus_milieu'] = int((w['strat'] == dept * 10 + w['s00q04'].astype('Int64')).sum())
    out['strat_first_digits_eq_dept'] = int(((w['strat'] // 10) == dept).sum())
xv = pd.crosstab(w['strat'] % 10, w['vague'])
out['crosstab_strat_lastdigit_by_vague'] = {str(r): {str(k): int(v) for k, v in xv.loc[r].items()} for r in xv.index}
xm = pd.crosstab(w['strat'] % 10, w['s00q04'])
out['crosstab_strat_lastdigit_by_milieu_code'] = {str(r): {str(k): int(v) for k, v in xm.loc[r].items()} for r in xm.index}
per = w.groupby('grappe')['strat'].nunique()
out['grappes'] = int(per.size)
out['grappes_with_two_strat'] = int((per > 1).sum())
# PanelHH, if the cover carries it
for col in out['cover_panel_columns']:
    cv = cov[['grappe', 'menage', col]].copy()
    cv['i'] = ['E2_' + str(int(g)) + f'{int(m):03d}' for g, m in zip(cv['grappe'], cv['menage'])]
    cv = cv.set_index('i')[col].reindex(w.index)
    xp = pd.crosstab(w['strat'] % 10, cv)
    out[f'crosstab_strat_lastdigit_by_{col}'] = {str(r): {str(k): int(v) for k, v in xp.loc[r].items()} for r in xp.index}
    labels = dict(zip(cov[col], covl[col].astype('object')))
    out[f'{col}_labels'] = {str(k): str(v) for k, v in labels.items()}
json.dump(out, open(sys.argv[2], 'w'), indent=1, ensure_ascii=False, default=str)
print('ok', out.get('n_strat'), out.get('n_strata'), out.get('strat_eq_10dept_plus_vague'), out.get('strat_eq_10dept_plus_milieu'))

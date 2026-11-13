"""Raw oracle for GH #1252: for every §9 row with crop code 9 / 19 / 86, the label
its OWN file's Stata value label gives -> the served crop_production j on that
(plot_id) must be that label.  usage: oracle_1252.py <checkout> <api.parquet> <out.json>"""
import os, sys, json, re, warnings
checkout, api, out = sys.argv[1:4]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
warnings.simplefilter('ignore')
import pandas as pd, numpy as np, pyreadstat
from lsms_library.data_access import get_data_file
from lsms_library.local_tools import format_id
VOCAB = {'CACTUS': 'Cactus', 'ROMAN': 'Roman', 'RED KIDENY BEANS': 'Red Kidney Beans',
         'MUNG BEAN/ MASHO': 'Mung bean', 'MUNG BEAN OR MASHO': 'Mung bean',
         'FALLOW LAND': 'Fallow Land', 'TEMPORARY GR': 'Temporary Grass', 'REDKIDENY BEANS': 'Red Kidney Beans'}
def norm(s):
    s = re.sub(r'^(\s*\d+\s*\.\s*)+', '', str(s)).strip().upper()
    return re.sub(r'\s+', ' ', s)
F = {'2011-12': ('sect9_ph_w1.dta', 'crop_code', 'ph_s9q12_a', None), '2013-14': ('sect9_ph_w2.dta', 'crop_code', 'ph_s9q04_a', 'ph_s9q04_b'),
     '2015-16': ('sect9_ph_w3.dta', 'crop_code', 'ph_s9q04_a', 'ph_s9q04_b'), '2018-19': ('sect9_ph_w4.dta', 's9q00b', 's9q05a', 's9q05b'),
     '2021-22': ('sect9_ph_w5.dta', 's9q00b', 's9q05a', 's9q05b')}
cp = pd.read_parquet(api).reset_index()
res = {}
for w, (f, cc, qv, uv) in F.items():
    p = get_data_file(f'Ethiopia/{w}/Data/{f}')
    df, meta = pyreadstat.read_dta(str(p), apply_value_formats=False)
    labels = meta.value_labels[meta.variable_to_label[cc]]
    code = pd.to_numeric(df[cc], errors='coerce')
    sel = code.isin([9, 19, 86])
    d = df[sel]
    plot = (d['holder_id'].apply(format_id).astype(str) + '_' + d['parcel_id'].apply(format_id).astype(str) + '_' + d['field_id'].apply(format_id).astype(str))
    exp = code[sel].map(lambda c: VOCAB.get(norm(labels.get(c, labels.get(float(c))))))
    kept = (pd.to_numeric(d[qv], errors='coerce').notna() | (pd.to_numeric(d[uv], errors='coerce').notna() if uv else True))
    E = pd.DataFrame({'plot_id': plot.values, 'code': code[sel].astype(int).values, 'expected': exp.values, 'kept': np.asarray(kept, bool)})
    nk = int((~E.kept).sum()); E = E[E.kept]
    s = cp[cp['t'] == w][['plot_id', 'j']]
    m = E.merge(s, on='plot_id', how='left')
    # a plot carries several crops: the expected label must be among the plot's served j
    got = m.groupby(['plot_id', 'code', 'expected'])['j'].apply(lambda x: set(x.dropna()))
    ok = [e in js for (pl, c, e), js in got.items()]
    served = [len(js) > 0 for js in got.values]
    r = {'raw_s9_rows_without_unit_or_quantity_excluded': nk, 'raw_s9_rows_by_code_and_file_label': {f'{c} {e}': int(n) for (c, e), n in E.value_counts(['code', 'expected']).items()},
         'plots_with_any_served_row': int(sum(served)),
         'plots_whose_served_j_includes_the_file_label': int(sum(o for o, sv in zip(ok, served) if sv)),
         'plots_served_but_label_missing': int(sum((not o) and sv for o, sv in zip(ok, served)))}
    lab_served = cp[cp['t'] == w]['j'].value_counts()
    r['served_rows_by_label'] = {k: int(lab_served.get(k, 0)) for k in ['Cactus', 'Roman', 'Red Kidney Beans', 'Mung bean', 'Fallow Land', 'Temporary Grass']}
    res[w] = r
json.dump(res, open(out, 'w'), indent=1); print(json.dumps(res, indent=1))

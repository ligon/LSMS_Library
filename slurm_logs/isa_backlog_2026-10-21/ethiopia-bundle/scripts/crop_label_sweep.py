"""Sweep every crop-code value label in every Ethiopia crop module, per wave.
usage: crop_label_sweep.py <checkout> <out.json>"""
import os, sys, json, re, warnings
checkout, out = sys.argv[1], sys.argv[2]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
import pyreadstat
from lsms_library.data_access import get_data_file
root = f'{checkout}/lsms_library/countries/Ethiopia'
WAVES = ['2011-12', '2013-14', '2015-16', '2018-19', '2021-22']
CROPWORDS = {'MAIZE', 'TEFF', 'SORGHUM', 'BARLEY', 'WHEAT'}
res = {}
for w in WAVES:
    res[w] = {}
    files = sorted(f[:-4] for f in os.listdir(f'{root}/{w}/Data') if f.endswith('.dta.dvc'))
    files = [f for f in files if re.search(r'(_pp_|_ph_|crop_cf|Crop_CF|sect9a)', f, re.I)]
    for f in files:
        try:
            p = get_data_file(f'Ethiopia/{w}/Data/{f}')
            _, meta = pyreadstat.read_dta(str(p), metadataonly=True)
        except Exception as e:
            res[w][f] = {'_error': repr(e)[:200]}
            continue
        for var, lblset in meta.variable_to_label.items():
            labels = meta.value_labels.get(lblset, {})
            up = {str(v).upper() for v in labels.values()}
            if any(any(cw in u for cw in CROPWORDS) for u in up) and len(labels) > 20:
                res[w].setdefault(f, {})[var] = {
                    'labelset': lblset,
                    'varlabel': meta.column_names_to_labels.get(var),
                    'labels': {str(int(k)) if float(k).is_integer() else str(k): str(v) for k, v in labels.items()}}
json.dump(res, open(out, 'w'), indent=1, sort_keys=True)
for w in res:
    print(w, {f: list(v) for f, v in res[w].items()})

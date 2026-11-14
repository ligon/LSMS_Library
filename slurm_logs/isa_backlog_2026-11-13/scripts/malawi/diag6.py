"""Sixth read-only diagnostic: hh_x02 in all four later halves (relabelled lines from
the inputs frame), and the plot-level join of every keyed row to its inputs.

usage: python diag6.py <clone> <verify-dir> <outdir>
"""
import json
import re
import shutil
import sys
import tempfile
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyreadstat

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, vdir, outdir = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
outdir.mkdir(parents=True, exist_ok=True)
head = assert_env(clone)
warnings.simplefilter('ignore')
from lsms_library.local_tools import get_dataframe, format_id, _dvc_cache_path  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

ROOT = countries_root() / 'Malawi'
fr = vdir / 'frames'
RES = {'head': head}


def dump():
    (outdir / 'diag6.json').write_text(json.dumps(RES, indent=1, default=str))


def value_labels(rel, var):
    get_dataframe(str(ROOT / rel), convert_categoricals=False)   # pulls the blob
    blob = _dvc_cache_path(str(ROOT / rel))
    tmp = Path(tempfile.mkdtemp()) / Path(rel).name
    shutil.copyfile(blob, tmp)
    try:
        _, m = pyreadstat.read_dta(str(tmp), metadataonly=True)
    finally:
        shutil.rmtree(tmp.parent, ignore_errors=True)
    ls = m.variable_to_label.get(var)
    return dict(zip(m.column_names, m.column_labels)).get(var), (m.value_labels.get(ls, {}) if ls else {})


HALVES = [('2016-17', 'Data/Cross_Sectional', 'hh_mod_x.dta', 'ag_mod_g.dta', 'case_id', 'cs-17-'),
          ('2016-17', 'Data/Panel', 'hh_mod_x_16.dta', 'ag_mod_g_16.dta', 'y3_hhid', ''),
          ('2019-20', 'Data/Cross_Sectional', 'HH_MOD_X.dta', 'ag_mod_g.dta', 'case_id', 'cs-19-'),
          ('2019-20', 'Data/Panel', 'hh_mod_x_19.dta', 'ag_mod_g_19.dta', 'y4_hhid', '')]


def season(g):
    mo = pd.to_numeric(g['ag_g05a'], errors='coerce')
    yr = pd.to_numeric(g['ag_g05b'], errors='coerce')
    ok = mo.between(1, 12) & yr.between(2000, 2030)
    return pd.Series(np.where(ok, np.where(mo >= 7, yr, yr - 1), np.nan), index=g.index)


RES['hh_x02'] = {}
for w, d, xf, gf, hh, prefix in HALVES:
    rec = {}
    try:
        label, vl = value_labels(f'{w}/{d}/{xf}', 'hh_x02')
        start = {}
        for k, v in vl.items():
            mm = re.search(r'(20\d\d)\s*[/-]', str(v))
            if mm:
                start[float(k)] = int(mm.group(1))
        rec['label'], rec['value_labels'] = label, {str(k): v for k, v in vl.items()}
        x = get_dataframe(str(ROOT / w / d / xf), convert_categoricals=False)
        x['_h'] = x[hh].apply(format_id).astype(str)
        x['_S_x'] = pd.to_numeric(x['hh_x02'], errors='coerce').map(start)
        rec['households'] = int(x['_h'].nunique())
        rec['hh_x02_counts'] = {str(k): int(v) for k, v in pd.to_numeric(x['hh_x02'], errors='coerce')
                                .value_counts(dropna=False).items()}
        sx = x.drop_duplicates('_h').set_index('_h')['_S_x']
        g = get_dataframe(str(ROOT / w / d / gf), convert_categoricals=False)
        g['_h'] = g[hh].apply(format_id).astype(str)
        g['_S_g'] = season(g)
        g['_S_x'] = g['_h'].map(sx)
        dated = g[g['_S_g'].notna() & g['_S_x'].notna()]
        rec['g_lines'] = int(len(g))
        rec['dated_lines'] = int(len(dated))
        rec['equal'] = int((dated['_S_g'] == dated['_S_x']).sum())
        # the relabelled lines, from the candidate's inputs frame for the key
        inp = pd.read_parquet(fr / 'cand.inputs' / f'crop_production__cowpea-from-crop-text__{w}.parquet')
        inp = inp[inp['source'].astype(str).eq(f'{d}/{gf}')].copy()
        inp['_h'] = inp[hh].apply(format_id).astype(str)
        inp['_S_g'] = season(inp)
        inp['_S_x'] = inp['_h'].map(sx)
        di = inp[inp['_S_g'].notna() & inp['_S_x'].notna()]
        rec['relabelled_lines'] = int(len(inp))
        rec['relabelled_dated'] = int(len(di))
        rec['relabelled_equal'] = int((di['_S_g'] == di['_S_x']).sum())
        rec['relabelled_hh_x02_missing'] = int(inp['_S_x'].isna().sum())
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
    RES['hh_x02'][f'{w}/{d}'] = rec
    print(w, d, rec, flush=True)
    dump()

# plot-level join of keyed rows to the cowpea key's inputs
RES['plot_join'] = {}
for table in ('crop_production', 'plot_inputs'):
    served = pd.read_parquet(fr / f'cand.{table}.parquet').reset_index()
    served = served[served['Derivation'].astype('string').str.contains('cowpea-from-crop-text', regex=False)
                    .fillna(False)]
    for w in ('2010-11', '2016-17', '2019-20'):
        inp = pd.read_parquet(fr / 'cand.inputs' / f'{table}__cowpea-from-crop-text__{w}.parquet')
        s = served[served['t'].astype(str).eq(w)]
        plot = s[s['plot_id'].astype(str).ne('Farm-level')]
        farm = s[s['plot_id'].astype(str).eq('Farm-level')]
        k4 = set(zip(inp['t'].astype(str), inp['i'].astype(str), inp['crop'].astype(str), inp['plot_id'].astype(str)))
        k3 = set(zip(inp['t'].astype(str), inp['i'].astype(str), inp['crop'].astype(str)))
        hit4 = [k in k4 for k in zip(plot['t'].astype(str), plot['i'].astype(str), plot['crop'].astype(str),
                                     plot['plot_id'].astype(str))]
        hit3 = [k in k3 for k in zip(farm['t'].astype(str), farm['i'].astype(str), farm['crop'].astype(str))]
        RES['plot_join'][f'{table}|{w}'] = {
            'keyed_plot_rows': int(len(plot)), 'joined_on_plot_id': int(sum(hit4)),
            'keyed_farm_level_rows': int(len(farm)), 'farm_level_joined_on_household': int(sum(hit3)),
            'inputs_rows': int(len(inp)), 'inputs_unique_on_t_i_crop_plot': bool(not inp.duplicated(
                ['t', 'i', 'crop', 'plot_id']).any())}
        print(table, w, RES['plot_join'][f'{table}|{w}'], flush=True)
dump()
print('DIAG6 DONE', flush=True)

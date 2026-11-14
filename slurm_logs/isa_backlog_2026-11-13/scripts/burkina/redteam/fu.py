"""Delta follow-up.  build: fu.py build LABEL FR OUT.json ; cmp: fu.py cmp BASE_FR CAND_FR OUT.json"""
import json
import sys
import traceback
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd


def _sk(o):
    if isinstance(o, dict):
        return {str(k): _sk(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_sk(v) for v in o]
    return o


mode = sys.argv[1]
res = {}
if mode == 'build':
    import lsms_library as ll
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root
    label, FR, OJ = sys.argv[2:5]
    for name, cs in (('bf', ['Burkina_Faso']), ('bf_mali', ['Burkina_Faso', 'Mali']),
                     ('bf_mali_niger', ['Burkina_Faso', 'Mali', 'Niger']), ('mali_niger', ['Mali', 'Niger'])):
        try:
            f = ll.Feature('crop_production')(cs).reset_index()
            res[name] = {'rows_by_country': f.groupby('country').size().astype(int).to_dict(),
                         'dtypes': {c: str(f[c].dtype) for c in ('production_obs', 'u', 'condition', 'j', 'plot_id')}}
            for col in f.columns:
                if f[col].dtype == object or str(f[col].dtype) == 'category':
                    f[col] = f[col].astype('string')
            f.to_parquet(f'{FR}/feat_{name}.parquet')
        except Exception:  # noqa: BLE001
            res[name] = 'ERR ' + traceback.format_exc()[-1500:]
    if label == 'cand':
        try:
            R = countries_root()
            c = get_dataframe(str(R / 'Burkina_Faso/2018-19/Data/s16c_me_bfa2018.dta'), convert_categoricals=False)
            fin = pd.to_numeric(c['s16cq10'], errors='coerce').eq(1).to_numpy()
            h = pd.to_numeric(c['s16cq12a'], errors='coerce').to_numpy()
            hu = pd.to_numeric(c['s16cq12b'], errors='coerce').to_numpy()
            parts = [('s16cq13a', 's16cq13b'), ('s16cq14a', 's16cq14b'), ('s16cq16a', 's16cq16b'), ('s16cq22a', 's16cq22b')]
            q = np.column_stack([pd.to_numeric(c[a], errors='coerce').to_numpy() for a, _ in parts])
            u = np.column_stack([pd.to_numeric(c[b], errors='coerce').to_numpy() for _, b in parts])
            pos = q > 0
            same = (~pos) | (u == hu[:, None])
            all_same = same.all(axis=1)
            tot = np.where(pos, q, 0).sum(axis=1)
            anypos = pos.any(axis=1)
            out = {'rows': len(c), 'finished': int(fin.sum()), 'finished_all_pos_in_harvest_unit': int((fin & all_same).sum()),
                   'finished_all_pos_in_harvest_unit_and_some_disp': int((fin & all_same & anypos).sum())}
            for tag, m in (('A_fin_allsame', fin & all_same), ('B_fin_allsame_anypos', fin & all_same & anypos),
                           ('C_fin_allsame_anypos_hpos', fin & all_same & anypos & (h > 0))):
                with np.errstate(divide='ignore', invalid='ignore'):
                    r = tot[m] / h[m]
                out[tag] = {'n': int(m.sum()), 'exact': round(float(np.mean(tot[m] == h[m])), 4),
                            'within10': round(float(np.mean(np.abs(r - 1) <= 0.1)), 4),
                            'median_ratio': float(np.nanmedian(r)) if m.any() else None,
                            'over_1_1': round(float(np.mean(r > 1.1)), 4), 'under_0_9': round(float(np.mean(r < 0.9)), 4),
                            'harvest_na': int(np.isnan(h[m]).sum()), 'harvest_unit_na': int(np.isnan(hu[m]).sum())}
            res['inrow_2018'] = out
        except Exception:  # noqa: BLE001
            res['inrow_2018'] = 'ERR ' + traceback.format_exc()[-1500:]
    json.dump(_sk(res), open(OJ, 'w'), indent=1, default=str)
    print(json.dumps(_sk(res), default=str)[:3000])
else:
    BF, CF, OJ = sys.argv[2:5]
    for name in ('bf', 'bf_mali', 'bf_mali_niger', 'mali_niger'):
        try:
            b, c = pd.read_parquet(f'{BF}/feat_{name}.parquet'), pd.read_parquet(f'{CF}/feat_{name}.parquet')
            rec = {}
            for country in sorted(set(b['country']) | set(c['country'])):
                for tag, sel in (('all', None), ('2018-19', '2018-19')):
                    x = b[b['country'] == country]
                    y = c[c['country'] == country]
                    if sel and country == 'Burkina_Faso':
                        x, y = x[x['t'].astype(str) == sel], y[y['t'].astype(str) == sel]
                    elif sel:
                        continue
                    if country == 'Burkina_Faso' and not sel:
                        continue
                    cols = [k for k in x.columns if k in y.columns]
                    xs = x[cols].astype('string').fillna('<NA>').sort_values(cols).reset_index(drop=True)
                    ys = y[cols].astype('string').fillna('<NA>').sort_values(cols).reset_index(drop=True)
                    rec[f'{country}:{tag}'] = {'rows': [len(x), len(y)], 'values_equal_as_strings': bool(xs.equals(ys)),
                                               'cand_extra_cols': sorted(set(y.columns) - set(x.columns))}
            res[name] = rec
        except Exception:  # noqa: BLE001
            res[name] = 'ERR ' + traceback.format_exc()[-1500:]
    json.dump(_sk(res), open(OJ, 'w'), indent=1, default=str)
    print(json.dumps(_sk(res), default=str))

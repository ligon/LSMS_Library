"""Follow-up checks.  Usage: extra.py LABEL FRAMES_DIR OUT.json  (cold private root)
crop consumers (gross_crop_revenue, crop_diversity, median_price_valuation) saved for a cross-arm diff;
candidate-only raw probes are run when LABEL == cand."""
import json
import sys
import traceback
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

from lsms_library import transformations as T
from lsms_library.country import Country
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

label, FR, OJ = sys.argv[1:4]
res = {'label': label}


def _sk(o):
    if isinstance(o, dict):
        return {str(k): _sk(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_sk(v) for v in o]
    return o


def save():
    json.dump(_sk(res), open(OJ, 'w'), indent=1, default=str)


def sect(name, fn):
    try:
        res[name] = fn()
    except Exception:  # noqa: BLE001
        res[name] = 'ERR ' + traceback.format_exc()[-2000:]
    save()
    print('done', name, flush=True)


cp = Country('Burkina_Faso').crop_production()


def store(name, df):
    f = (df.to_frame() if isinstance(df, pd.Series) else df).reset_index()
    for col in f.columns:
        if f[col].dtype == object or str(f[col].dtype) == 'category':
            f[col] = f[col].astype('string')
    f.to_parquet(f'{FR}/{name}.parquet')
    return f


def consumers():
    out = {}
    g = store('gcr', T.gross_crop_revenue(cp))
    val = [c for c in g.columns if c not in ('t', 'i', 'v', 'country')]
    out['gcr_cols'] = list(g.columns)
    out['gcr_sum_by_t'] = g.groupby(g['t'].astype(str))[val[-1]].sum().to_dict()
    d = store('cdiv', T.crop_diversity(cp, weight='count'))
    out['cdiv_rows_by_t'] = d.groupby(d['t'].astype(str)).size().to_dict()
    out['cdiv_attrs'] = {k: str(v)[:400] for k, v in T.crop_diversity(cp, weight='count').attrs.items()}
    m = T.median_price_valuation(cp, ['v'], item_keys=('crop',))
    mf = store('mpv', m)
    out['mpv_cols'] = list(m.columns)
    out['mpv_attrs'] = {k: str(v)[:600] for k, v in m.attrs.items()}
    out['mpv_value_by_t'] = mf.groupby(mf['t'].astype(str))['Value'].sum().to_dict() if 'Value' in mf else None
    # Does the price pool read the Farm-level sale rows?  Re-run without them.
    flat = cp.reset_index()
    farm = flat['plot_id'].astype(str).eq('Farm-level')
    out['farm_rows'] = int(farm.sum())
    if farm.any():
        cp2 = flat[~farm].set_index(cp.index.names)
        m2 = T.median_price_valuation(cp2, ['v'], item_keys=('crop',))
        a = m.reset_index()
        a = a[a['plot_id'].astype(str) != 'Farm-level']
        b = m2.reset_index()
        keys = [k for k in cp.index.names]
        ab = a.merge(b, on=keys, suffixes=('_with', '_without'))
        col = 'Value'
        x, y = ab[col + '_with'].astype(float), ab[col + '_without'].astype(float)
        out['mpv_rows_changed_by_dropping_farm_level'] = int((~np.isclose(x, y, equal_nan=True)).sum())
        out['mpv_rows_compared'] = len(ab)
        out['mpv_2021_value_with_without'] = [float(x[ab['t'].astype(str) == '2021-22'].sum()),
                                              float(y[ab['t'].astype(str) == '2021-22'].sum())]
    return out


sect('consumers', consumers)

if label == 'cand':
    R = countries_root()

    def dropped_rows():
        c = get_dataframe(str(R / 'Burkina_Faso/2021-22/Data/s16c_me_bfa2021.dta'), convert_categoricals=False)
        drop = c[c['s16cq04'].isna()]
        cols = [k for k in drop.columns if k not in ('grappe', 'menage', 'hhid', 'vague')]
        filled = {k: int((drop[k].notna() & drop[k].astype('string').str.strip().ne('').fillna(False)).sum()) for k in cols}
        kept = c[c['s16cq04'].notna()]
        return {'dropped': len(drop), 'filled_in_dropped': {k: v for k, v in filled.items() if v},
                's16cq16c_notna_kept': int(pd.to_numeric(kept['s16cq16c'], errors='coerce').notna().sum()),
                's16cq16c_positive_kept': int((pd.to_numeric(kept['s16cq16c'], errors='coerce') > 0).sum()),
                'dropped_households_with_kept_lines': int(len(set(zip(drop['grappe'], drop['menage']))
                                                          & set(zip(kept['grappe'], kept['menage']))))}

    sect('s16c_dropped_rows', dropped_rows)

    def zd_median_control():
        """autocons vs qautocons x zd median purchase uv, against the same test on achat with a
        leave-one-out zd median (the row's own price removed)."""
        num = get_dataframe(str(R / 'Burkina_Faso/2014/Data/emc2014_p1_conso7jours_16032015.dta'),
                            convert_categoricals=False)
        a, qa = pd.to_numeric(num['achat'], errors='coerce'), pd.to_numeric(num['qachat'], errors='coerce')
        ac, qp = pd.to_numeric(num['autocons'], errors='coerce'), pd.to_numeric(num['qautocons'], errors='coerce')
        fr = pd.DataFrame({'zd': num['zd'], 'prod': num['product'], 'u': num['uachat'], 'uv': a / qa, 'a': a, 'qa': qa})
        fr = fr[(a > 0) & (qa > 0) & num['uachat'].notna()].copy()
        # leave-one-out median: median of the cell's other rows
        out = {}
        grp = fr.groupby(['zd', 'prod', 'u'])['uv']
        loo = []
        for _, s in grp:
            v = s.to_numpy()
            if len(v) < 2:
                loo.extend([np.nan] * len(v))
                continue
            for k in range(len(v)):
                loo.append(np.median(np.delete(v, k)))
        idx = [i for _, s in grp for i in s.index]
        fr.loc[idx, 'loo'] = loo
        ok = fr['loo'].notna()
        out['achat_eq_qachat_x_loo_zd_median'] = [int(((fr['qa'] * fr['loo'] - fr['a']).abs() <= 1)[ok].sum()), int(ok.sum())]
        # autocons against the zd median of purchases (not its own row, which is not a purchase)
        med = grp.median()
        key = pd.MultiIndex.from_arrays([num['zd'], num['product'], num['uautocons']])
        zmed = pd.Series(med.reindex(key).to_numpy(), index=num.index)
        n = grp.size().reindex(key).to_numpy()
        ok2 = (qp > 0) & (ac > 0) & zmed.notna() & pd.Series(n >= 1, index=num.index)
        out['autocons_eq_qautocons_x_zd_median'] = [int(((qp * zmed - ac).abs() <= 1)[ok2].sum()), int(ok2.sum())]
        # And excluding rows that repeat the C04 answer (achat == autocons)
        nodup = ok2 & ~(a == ac)
        out['autocons_eq_..._excl_achat_eq_autocons'] = [int(((qp * zmed - ac).abs() <= 1)[nodup].sum()), int(nodup.sum())]
        return out

    sect('zd_median_control', zd_median_control)
save()
print('extra complete', flush=True)

"""Compare red-team base and candidate frames.  Usage: compare.py BASE_DIR CAND_DIR BASE.json CAND.json OUT.json"""
import json
import sys
import traceback
import warnings

warnings.simplefilter('ignore')
import numpy as np
import pandas as pd

BD, CD, BJ, CJ, OJ = sys.argv[1:6]
bj, cj = json.load(open(BJ)), json.load(open(CJ))
res = {'base': bj['head'], 'cand': cj['head']}


def _sk(o):
    if isinstance(o, dict):
        return {str(k): _sk(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_sk(v) for v in o]
    return o


def save():
    with open(OJ, 'w') as fh:
        json.dump(_sk(res), fh, indent=1, default=str, sort_keys=True)


def load(d, name):
    try:
        return pd.read_parquet(f'{d}/{name}.parquet')
    except Exception:  # noqa: BLE001
        return None


def norm(df):
    d = df.copy()
    for col in d.columns:
        if str(d[col].dtype) in ('category',):
            d[col] = d[col].astype('string')
    return d


def rowkeys(df, cols):
    return pd.MultiIndex.from_frame(df[cols].astype('string').fillna('<NA>'))


def frame_diff(name, keycols=None):
    b, c = load(BD, name), load(CD, name)
    rec = {}
    if b is None or c is None:
        return {'missing': [b is None, c is None]}
    b, c = norm(b), norm(c)
    rec['rows'] = [len(b), len(c)]
    rec['cols_only_base'] = sorted(set(b.columns) - set(c.columns))
    rec['cols_only_cand'] = sorted(set(c.columns) - set(b.columns))
    rec['dtype_diff'] = {k: [str(b[k].dtype), str(c[k].dtype)] for k in set(b.columns) & set(c.columns)
                         if str(b[k].dtype) != str(c[k].dtype)}
    if keycols is None:
        keycols = [k for k in (bj['calls'].get(name, {}).get('index') or []) if k in b.columns and k in c.columns]
    rec['keys'] = keycols
    common_cols = [x for x in b.columns if x in c.columns]
    try:
        bs = b[common_cols].sort_values(common_cols, na_position='last').reset_index(drop=True)
        cs = c[common_cols].sort_values(common_cols, na_position='last').reset_index(drop=True)
        pd.testing.assert_frame_equal(bs, cs, check_dtype=True)
        rec['equal_on_common_columns_with_dtype'] = True
    except Exception as e:  # noqa: BLE001
        rec['equal_on_common_columns_with_dtype'] = f'{type(e).__name__}: {str(e)[:400]}'
    if keycols:
        bk, ck = rowkeys(b, keycols), rowkeys(c, keycols)
        rec['dup_keys'] = [int(bk.duplicated().sum()), int(ck.duplicated().sum())]
        sb, sc = set(bk), set(ck)
        rec['keys_only_base'] = len(sb - sc)
        rec['keys_only_cand'] = len(sc - sb)
        if not bk.duplicated().any() and not ck.duplicated().any():
            bi = b.set_index(bk)
            ci = c.set_index(ck)
            common = bi.index.intersection(ci.index)
            moved = {}
            for col in common_cols:
                if col in keycols:
                    continue
                x, y = bi.loc[common, col], ci.loc[common, col]
                try:
                    xv = pd.to_numeric(x, errors='raise').astype('float64')
                    yv = pd.to_numeric(y, errors='raise').astype('float64')
                    diff = ~(np.isclose(xv, yv, rtol=1e-12, atol=0, equal_nan=True))
                except Exception:  # noqa: BLE001
                    xs, ys = x.astype('string').fillna('<NA>'), y.astype('string').fillna('<NA>')
                    diff = (xs != ys).to_numpy()
                moved[col] = int(diff.sum())
            rec['common_rows'] = len(common)
            rec['common_rows_moved_by_col'] = moved
    return rec


def safe(name, fn):
    try:
        res[name] = fn()
    except Exception:  # noqa: BLE001
        res[name] = 'ERR ' + traceback.format_exc()[-2000:]
    save()
    print('done', name, flush=True)


for name in ('crop_production', 'crop_production_2018', 'harvest_kg', 'yield_kg_parcel', 'yield_kg_plot',
             'plot_features', 'food_acquired', 'food_expenditures', 'food_expenditures_total',
             'food_expenditures_total_valued', 'food_prices', 'food_prices_unitvalue', 'food_quantities',
             'food_quantities_units', 'plot_inputs', 'nitrogen_kg', 'seed_kg', 'interview_date',
             'feature_plot_inputs'):
    safe('diff_' + name, lambda name=name: frame_diff(name))


def cp_2018_vs_base():
    b, c = load(BD, 'crop_production'), load(CD, 'crop_production')
    c18 = c[c['t'].astype(str) == '2018-19'].copy()
    out = {'cand_cols_extra': sorted(set(c18.columns) - set(b.columns))}
    if 'Derivation' in c18:
        out['cand_2018_Derivation_nonnull'] = int(c18['Derivation'].notna().sum())
        out['cand_Derivation_dtype'] = str(c18['Derivation'].dtype)
    cols = [x for x in b.columns]
    bs = norm(b[cols]).sort_values(cols, na_position='last').reset_index(drop=True)
    cs = norm(c18[cols]).sort_values(cols, na_position='last').reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(bs, cs, check_dtype=True)
        out['equal_2018_rows_on_base_columns'] = True
    except Exception as e:  # noqa: BLE001
        out['equal_2018_rows_on_base_columns'] = f'{type(e).__name__}: {str(e)[:600]}'
    c21 = c[c['t'].astype(str) == '2021-22']
    out['2021_rows'] = len(c21)
    out['2021_farm_level'] = int((c21['plot_id'] == 'Farm-level').sum())
    out['2021_quantity_notna'] = int(c21['Quantity'].notna().sum())
    out['2021_value_sold_sum'] = float(c21['Value_sold'].sum())
    out['2021_value_sold_rows'] = int(c21['Value_sold'].notna().sum())
    out['2021_qsold_or_vsold_rows'] = int((c21['Quantity_sold'].notna() | c21['Value_sold'].notna()).sum())
    out['2021_u_counts'] = c21['u'].astype(str).value_counts().head(25).to_dict()
    out['2021_farm_u'] = c21.loc[c21['plot_id'] == 'Farm-level', 'u'].astype(str).value_counts().to_dict()
    out['2021_condition'] = c21['condition'].astype(str).value_counts().to_dict()
    out['2021_crop_top'] = c21['crop'].astype(str).value_counts().head(60).to_dict()
    out['2018_crop_top'] = c[c['t'].astype(str) == '2018-19']['crop'].astype(str).value_counts().head(60).to_dict()
    out['2021_intercropped'] = c21['intercropped'].astype(str).value_counts(dropna=False).to_dict()
    out['2021_unit_sold'] = c21['Unit_sold'].astype(str).value_counts(dropna=False).head(30).to_dict()
    d = c21['Derivation'].astype('string')
    out['2021_derivation'] = d.value_counts(dropna=False).to_dict()
    out['2018_u_na_or_sentinel'] = c[c['t'].astype(str) == '2018-19']['u'].astype(str).value_counts().tail(10).to_dict()
    out['cols_2021_all_na'] = [x for x in c21.columns if c21[x].isna().all()]
    # Farm-level vs single line by sale; same household-crop with both a keyed line and a farm row?
    fl = c21[c21['plot_id'] == 'Farm-level']
    lines = c21[c21['plot_id'] != 'Farm-level']
    fk = set(zip(fl['i'], fl['crop']))
    lk = lines.groupby(['i', 'crop']).size()
    out['farm_level_pairs_line_count'] = pd.Series([int(lk.get(p, 0)) for p in fk]).value_counts().to_dict()
    return out


safe('crop_production_detail', cp_2018_vs_base)


def food_acquired_detail():
    b, c = load(BD, 'food_acquired'), load(CD, 'food_acquired')
    keys = [k for k in bj['calls']['food_acquired']['index'] if k in b.columns]
    out = {'keys': keys}
    for tag, f in (('base', b), ('cand', c)):
        g = f.groupby([f['t'].astype(str), f['s'].astype(str)])
        out[f'{tag}_rows_by_t_s'] = {f'{k[0]}|{k[1]}': int(v) for k, v in g.size().items()}
        out[f'{tag}_exp_by_t_s'] = {f'{k[0]}|{k[1]}': float(v) for k, v in g['Expenditure'].sum().items()}
        out[f'{tag}_qty_by_t_s'] = {f'{k[0]}|{k[1]}': float(v) for k, v in g['Quantity'].sum().items()}
    bk, ck = rowkeys(b, keys), rowkeys(c, keys)
    bi, ci = b.set_index(bk), c.set_index(ck)
    common = bi.index.intersection(ci.index)
    newk = ci.index.difference(bi.index)
    out['common'] = len(common)
    out['only_base'] = len(bi.index.difference(ci.index))
    out['only_cand'] = len(newk)
    x, y = bi.loc[common], ci.loc[common]
    out['common_quantity_moved'] = int((~np.isclose(x['Quantity'].astype(float), y['Quantity'].astype(float), equal_nan=True)).sum())
    ex, ey = x['Expenditure'].astype(float), y['Expenditure'].astype(float)
    moved = ~np.isclose(ex, ey, equal_nan=True)
    out['common_expenditure_moved'] = int(moved.sum())
    out['common_expenditure_moved_by_s'] = y.loc[moved, 's'].astype(str).value_counts().to_dict()
    out['common_expenditure_moved_from_na'] = int((moved & ex.isna().to_numpy()).sum())
    out['common_purchased_moved'] = int((moved & (y['s'].astype(str) == 'purchased').to_numpy()).sum())
    n = ci.loc[newk]
    out['new_by_t_s_visit'] = n.groupby([n['t'].astype(str), n['s'].astype(str), n['visit'].astype(str)]).size().astype(int).to_dict() if 'visit' in n else None
    out['new_quantity_notna'] = int(n['Quantity'].notna().sum())
    out['new_u'] = n['u'].astype(str).value_counts().head(10).to_dict()
    out['new_expenditure_sum'] = float(n['Expenditure'].sum())
    out['new_exp_na'] = int(n['Expenditure'].isna().sum())
    out['new_rows_2018_2021'] = int(n['t'].astype(str).isin(['2018-19', '2021-22']).sum())
    return {k: (v if not isinstance(v, dict) else {str(a): b_ for a, b_ in v.items()}) for k, v in out.items()}


safe('food_acquired_detail', food_acquired_detail)


def kg_detail():
    b, c = load(BD, 'kg_factors'), load(CD, 'kg_factors')
    idx = [x for x in b.columns if x not in ('kg_per_unit', 'KgFactorSource', 'Quantity', 'Expenditure')]
    bk, ck = rowkeys(b, idx), rowkeys(c, idx)
    out = {'idx': idx, 'dups': [int(bk.duplicated().sum()), int(ck.duplicated().sum())]}
    bi, ci = b.set_index(bk), c.set_index(ck)
    bi = bi[~bi.index.duplicated()]
    ci = ci[~ci.index.duplicated()]
    common = bi.index.intersection(ci.index)
    x, y = bi.loc[common], ci.loc[common]
    kx, ky = x['kg_per_unit'].astype(float), y['kg_per_unit'].astype(float)
    mv = ~np.isclose(kx, ky, rtol=1e-12, equal_nan=True)
    sx, sy = x['KgFactorSource'].astype(str), y['KgFactorSource'].astype(str)
    out['common'] = len(common)
    out['factor_moved'] = int(mv.sum())
    out['source_moved'] = int((sx != sy).sum())
    trans = pd.DataFrame({'from': sx[mv | (sx != sy).to_numpy()], 'to': sy[mv | (sx != sy).to_numpy()]})
    out['transitions'] = trans.value_counts().astype(int).reset_index().astype(str).values.tolist()
    mvf = y[mv]
    out['moved_t_s'] = mvf.reset_index(drop=True).groupby([mvf['t'].astype(str).to_numpy(), mvf['s'].astype(str).to_numpy()]).size().astype(int).reset_index().astype(str).values.tolist() if 't' in mvf else None
    out['moved_ratio'] = (ky[mv] / kx[mv]).describe().to_dict()
    out['moved_rows'] = y.loc[mv, [k for k in ('t', 'j', 'u', 's', 'visit') if k in y.columns]].astype(str).values.tolist()[:40]
    out['cand_only_sources'] = ci.loc[ci.index.difference(bi.index), 'KgFactorSource'].astype(str).value_counts().to_dict()
    # Total kg on common purchased rows by t
    for tag, f in (('base', x), ('cand', y)):
        q = pd.to_numeric(f['Quantity'], errors='coerce') * pd.to_numeric(f['kg_per_unit'], errors='coerce')
        out[f'{tag}_kg_common_by_t'] = q.groupby(f['t'].astype(str).to_numpy()).sum().to_dict()
    return out


safe('kg_detail', kg_detail)


def prices_detail():
    out = {}
    for name in ('food_prices', 'food_prices_unitvalue'):
        b, c = load(BD, name), load(CD, name)
        keys = [k for k in bj['calls'][name]['index'] if k in b.columns]
        bi, ci = b.set_index(rowkeys(b, keys)), c.set_index(rowkeys(c, keys))
        common = bi.index.intersection(ci.index)
        col = [x for x in b.columns if x not in keys][0]
        x, y = bi.loc[common, col].astype(float), ci.loc[common, col].astype(float)
        newk = ci.index.difference(bi.index)
        nn = ci.loc[newk]
        out[name] = {'col': col, 'rows': [len(b), len(c)], 'common': len(common),
                     'moved': int((~np.isclose(x, y, rtol=1e-12, equal_nan=True)).sum()),
                     'only_base': len(bi.index.difference(ci.index)), 'only_cand': len(newk),
                     'new_by_t_s': nn.groupby([nn['t'].astype(str), nn['s'].astype(str)]).size().astype(int).reset_index().astype(str).values.tolist() if 's' in nn else None,
                     'attrs_base': bj['calls'][name].get('attrs', {}).get('price_rows_dropped'),
                     'attrs_cand': cj['calls'][name].get('attrs', {}).get('price_rows_dropped')}
    for name in ('food_quantities', 'food_quantities_units', 'food_expenditures', 'food_expenditures_total'):
        b, c = load(BD, name), load(CD, name)
        col = [x for x in b.columns if x not in bj['calls'][name]['index']][0]
        out[name] = {'col': col, 'by_t_base': b.groupby(b['t'].astype(str))[col].sum().to_dict(),
                     'by_t_cand': c.groupby(c['t'].astype(str))[col].sum().to_dict(),
                     'rows_by_t_base': b.groupby(b['t'].astype(str)).size().astype(int).to_dict(),
                     'rows_by_t_cand': c.groupby(c['t'].astype(str)).size().astype(int).to_dict()}
    return out


safe('prices_detail', prices_detail)


def inputs_join():
    c = load(CD, 'crop_production')
    c21 = c[c['t'].astype(str) == '2021-22']
    out = {}
    for key, nm in (('Burkina_Faso::crop_production::hh-crop-sale-single-row', 'hh-crop-sale-single-row'),
                    ('Burkina_Faso::crop_production::harvest-to-date', 'harvest-to-date')):
        inp = load(CD, 'inputs_' + nm)
        keyed = c21[c21['Derivation'].astype('string').str.contains(key, regex=False).fillna(False)]
        if inp is None:
            out[nm] = 'no inputs frame'
            continue
        lv = ['t', 'i', 'plot_id', 'crop']
        ik = set(zip(*(inp[k].astype(str) for k in lv)))
        kk = list(zip(*(keyed[k].astype(str) for k in lv)))
        out[nm] = {'inputs_rows': len(inp), 'keyed_rows': len(keyed), 'keyed_unique_at_4': len(set(kk)) == len(kk),
                   'inputs_unique_at_4': len(ik) == len(inp), 'keyed_in_inputs': int(sum(p in ik for p in kk)),
                   'inputs_in_keyed': int(len(ik & set(kk)))}
        if nm == 'hh-crop-sale-single-row':
            m = keyed.merge(inp, on=lv, how='inner')
            out[nm]['value_equal'] = int(np.isclose(m['Value_sold'].astype(float), pd.to_numeric(m['s16dq06'], errors='coerce').astype(float), equal_nan=True).sum())
            out[nm]['qty_equal'] = int(np.isclose(m['Quantity_sold'].astype(float), pd.to_numeric(m['s16dq05a'], errors='coerce').astype(float), equal_nan=True).sum())
            out[nm]['merged'] = len(m)
        else:
            m = keyed.merge(inp, on=lv, how='inner')
            out[nm]['qty_equal'] = int(np.isclose(m['Quantity'].astype(float), pd.to_numeric(m['s16cq16a'], errors='coerce').astype(float), equal_nan=True).sum())
            out[nm]['merged'] = len(m)
            out[nm]['s16cq11'] = m['s16cq11'].astype(str).value_counts().to_dict()
    return out


safe('inputs_join', inputs_join)


def harvest_detail():
    out = {}
    for name in ('harvest_kg', 'yield_kg_parcel', 'yield_kg_plot'):
        c = load(CD, name)
        b = load(BD, name)
        val = [x for x in c.columns if x not in cj['calls'][name]['index']]
        out[name] = {'rows_by_t_cand': c.groupby(c['t'].astype(str)).size().astype(int).to_dict(),
                     'rows_by_t_base': b.groupby(b['t'].astype(str)).size().astype(int).to_dict(),
                     'value_cols': val,
                     'nonnull_by_t_cand': {k: int(v) for k, v in c.groupby(c['t'].astype(str))[val[0]].apply(lambda s: s.notna().sum()).items()},
                     'median_by_t_cand': c.groupby(c['t'].astype(str))[val[0]].median().to_dict()}
    return out


safe('harvest_detail', harvest_detail)
save()
print('compare complete', flush=True)

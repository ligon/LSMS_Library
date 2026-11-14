"""Cold Nigeria build for one arm (GH #587 final): save every compared table.

argv: clone frames_dir results_dir arm [tables...]
Frames (cluster-level detail) go to the PRIVATE frames_dir; aggregates only to
results_dir/<arm>.build.json.  For the candidate arm (arm == 'cand') it also
runs labels='Aggregate', the Feature assembly and the registry checks.
"""
import json
import os
import sys
import time
import traceback
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import assert_env, Recorder  # noqa: E402

clone, frames, res, arm = sys.argv[1:5]
TABLES = sys.argv[5:]
head = assert_env(clone)
OUT = Path(frames) / arm
OUT.mkdir(parents=True, exist_ok=True)
rec = Recorder(os.path.join(res, f'{arm}.build.json'))
rec.data['head'] = head

import lsms_library as ll  # noqa: E402
from lsms_library.null_read_audit import null_read_reports  # noqa: E402
from lsms_library.null_index_audit import null_index_reports  # noqa: E402
from lsms_library.quantity_audit import quantity_reports  # noqa: E402

c = ll.Country('Nigeria')


def jsonable(x):
    return json.loads(json.dumps(x, default=str))


def save(name, df):
    out = df.copy()
    out.attrs = {}
    out.to_parquet(OUT / f'{name}.parquet')
    (OUT / f'{name}.attrs.json').write_text(json.dumps(jsonable(dict(df.attrs)), indent=1))


def build(table):
    def run():
        t0 = time.time()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            df = getattr(c, table)()
        save(table, df)
        return {'seconds': round(time.time() - t0, 1), 'shape': list(df.shape),
                'index_names': list(df.index.names), 'index_unique': bool(df.index.is_unique),
                'warnings': sorted({f'{type(x.message).__name__}: {str(x.message)[:300]}'
                                    for x in caught}),
                'null_read': len(null_read_reports(country='Nigeria', table=table)),
                'null_index': len(null_index_reports(country='Nigeria', table=table)),
                'quantity': len(quantity_reports(country='Nigeria', table=table))}
    return run


for table in TABLES:
    rec.step(table, build(table))

PP_T = ['2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3']


def cand_checks():
    from lsms_library.countries.Nigeria._ import pp_community_prices as pp
    import importlib
    D = importlib.import_module('lsms_library.derivations')   # ll.derivations is a function
    out = {}
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = c.community_prices()
        recs = c.derivations()
    r = cp.reset_index()
    pp_rows = r[r['t'].isin(PP_T)]
    out['pp_served_by_t_obs'] = {f'{t}|{o}': int(v) for (t, o), v in pp_rows.groupby(['t', 'obs']).size().items()}
    out['pp_served_by_t_key'] = {f'{t}|{k}': int(v) for (t, k), v in
                                 pp_rows.assign(k=pp_rows['Derivation'].fillna('none'))
                                 .groupby(['t', 'k']).size().items()}
    out['total_rows'] = int(len(cp))
    out['pp_rows'] = int(len(pp_rows))
    out['ph_rows'] = int(len(r) - len(pp_rows))
    out['index_unique'] = bool(cp.index.is_unique)
    out['null_keys'] = int(r[['t', 'v', 'j', 'u', 'variety', 'obs']].isna().sum().sum())
    out['nonpositive_or_na_price'] = int((~(cp['Price'] > 0)).sum())
    # labels outside the shared tables
    from lsms_library.local_tools import df_from_orgfile
    org = str(c.file_path / '_' / 'categorical_mapping.org')
    hf = df_from_orgfile(org, name='harmonize_food')
    ut = df_from_orgfile(org, name='u')
    jp = set(hf['Preferred Label'].dropna().astype(str).str.strip())
    up = set(ut['Preferred Label'].dropna().astype(str).str.strip())
    out['pp_j_not_preferred'] = {k: int(v) for k, v in
                                 pp_rows.loc[~pp_rows['j'].isin(jp), 'j'].value_counts().items()}
    out['pp_u_not_preferred'] = {k: int(v) for k, v in
                                 pp_rows.loc[~pp_rows['u'].isin(up), 'u'].value_counts().items()}
    out['pp_u_tokens'] = {u: {t: int(v) for t, v in pp_rows.loc[pp_rows['u'].eq(u), 't'].value_counts().items()}
                          for u in ('Congo/Mudu', 'ml', 'Tomatoes cup', 'Unknown')}
    out['pepper_rows_by_t'] = {k: int(v) for k, v in pp_rows.loc[pp_rows['j'].eq('Pepper'), 't'].value_counts().items()}
    sem = pp_rows[pp_rows['j'].eq('Semovita')]
    out['semovita'] = {'rows': int(len(sem)), 'by_t_u': {f'{t}|{u}': int(v) for (t, u), v in
                                                         sem.groupby(['t', 'u']).size().items()},
                       'price_median': float(sem['Price'].median()) if len(sem) else None}
    # Semovita vs the same round's W2 PP prices of Wheat flour / Other grains, Kg only (medians)
    w2 = pp_rows[pp_rows['t'].eq('2012Q3') & pp_rows['u'].eq('Kg')]
    out['w2_kg_medians'] = {j: float(w2.loc[w2['j'].eq(j), 'Price'].median())
                            for j in ('Semovita', 'Wheat flour', 'Other grains and flour', 'Maize flour',
                                      'Rice--imported', 'Bread') if w2['j'].eq(j).any()}
    # W3 161
    out['w3_palm'] = {f'{j}': int(v) for j, v in r.loc[r['t'].eq('2015Q3') & r['j'].isin(['Palm wine', 'Palm oil']),
                                                     'j'].value_counts().items()}
    # registry: constructions join and reproduce; omissions disjoint
    keys = sorted(k for k in recs if '::community_prices::pp-' in k)
    reg = {}
    for key in keys:
        e = {}
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                inp = c.derivation_inputs(key)
            e['inputs'] = int(len(inp))
            e['inputs_unique'] = bool(inp.index.is_unique)
            served = cp[cp['Derivation'].eq(key)]
            e['stamped'] = int(len(served))
            if recs[key].rows == 0:
                e['kind'] = 'omission'
                e['disjoint_from_served'] = not bool(set(inp.index) & set(cp.index))
                e['attrs_rows'] = cp.attrs.get('derivations', {}).get('Nigeria', {}).get(key, {}).get('rows')
                if 'Disposition' in inp:
                    e['dispositions'] = {k: int(v) for k, v in inp['Disposition'].value_counts().items()}
                if len(inp):
                    lv = inp.index.to_frame(index=False)
                    e['by_t_u'] = {f'{t}|{u}': int(v) for (t, u), v in lv.groupby(['t', 'u']).size().items()}
                    if 'sc2q3b' in inp:
                        n = pd.to_numeric(inp['sc2q3b'], errors='coerce')
                        e['N_min_max'] = [float(n.min()), float(n.max())]
            else:
                e['kind'] = 'construction'
                e['join_equal'] = set(inp.index) == set(served.index)
                fn = D.resolve_callable(recs[key].function)
                if 'number-of-units' in key:
                    again = fn(inp['sc2q3a'], inp['sc2q3b'])
                    n = pd.to_numeric(inp['sc2q3b'], errors='coerce')
                    lv = inp.index.to_frame(index=False)
                    cls = lv['u'].where(lv['u'].isin(['Kg', 'g', 'l', 'ml', 'Unknown']), 'non-metric')
                    band = pd.cut(n.to_numpy(), [1, 5, 50, np.inf], labels=['2-5', '6-50', '>50']).astype(str)
                    e['by_class_band'] = {f'{a}|{b}': int(v) for (a, b), v in
                                          pd.DataFrame({'c': cls.to_numpy(), 'b': band}).groupby(['c', 'b']).size().items() if v}
                elif key.endswith('metric-unit-quantity'):
                    again = fn(inp['c2q3'], inp['c2q2a'])
                else:
                    labels = {}
                    for source in ('W4', 'W5'):
                        labels.update(pp._card(pp.pp_source(source), 'size'))
                    qty = inp['c2q2c'].map(labels).map(pp._ng._c8_size_metric_grams)
                    again = fn(inp['c2q3'], qty)
                    lv = inp.index.to_frame(index=False)
                    e['by_t_u'] = {f'{t}|{u}': int(v) for (t, u), v in lv.groupby(['t', 'u']).size().items()}
                    if 'c2q2a' in inp:
                        q = pd.to_numeric(inp['c2q2a'], errors='coerce')
                        e['w5_c2q2a_not_1'] = int((q.notna() & q.ne(1)).sum())
                got = pd.Series(np.asarray(again, dtype='float64'), index=inp.index).sort_index()
                want = served['Price'].astype('float64')
                want = want.reindex(got.index) if e['join_equal'] else want
                e['price_reproduced'] = bool(np.allclose(got.to_numpy(), want.to_numpy(), rtol=1e-12, equal_nan=False)) \
                    if e['join_equal'] else False
        except Exception as exc:  # noqa: BLE001
            e['error'] = f'{type(exc).__name__}: {exc}'[:500]
            e['traceback'] = traceback.format_exc()[-2000:]
        reg[key] = e
    out['registry'] = reg
    # the 63 W5 Bottle/Can rows: divided price vs same-item W5 pp-c2-metric-size medians
    tlev = cp.index.get_level_values('t')
    w5 = cp[(tlev == '2023Q3') & cp['Derivation'].eq(pp.PP_METRIC_SIZE_KEY).to_numpy()]
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            inp = c.derivation_inputs(pp.PP_METRIC_SIZE_KEY, wave='2023-24')
        q = pd.to_numeric(inp['c2q2a'], errors='coerce')
        new = set(inp.index[(q.notna() & q.ne(1)).to_numpy()])
        nf = w5.assign(new=[ix in new for ix in w5.index]).reset_index()
        ph = r[r['t'].eq('2024Q1') & r['Derivation'].eq('Nigeria::community_prices::c8-metric-size')]

        def summ(ratio):
            ratio = ratio.dropna()
            return {'matched': int(len(ratio)),
                    'median': float(ratio.median()) if len(ratio) else None,
                    'p10': float(ratio.quantile(.1)) if len(ratio) else None,
                    'p90': float(ratio.quantile(.9)) if len(ratio) else None,
                    'within2x': float(((ratio > .5) & (ratio < 2)).mean()) if len(ratio) else None}
        ref = nf[~nf['new']].groupby(['j', 'u'])['Price'].median().rename('ref')
        m = nf[nf['new']].join(ref, on=['j', 'u'])
        phref = ph.groupby(['j', 'u'])['Price'].median().rename('ref')
        m2 = nf[nf['new']].join(phref, on=['j', 'u'])
        out['w5_bottle_can_63'] = {'rows': int(nf['new'].sum()),
                                   'size_labels': {k: int(v) for k, v in nf.loc[nf['new'], 'Size'].value_counts().items()},
                                   'by_u': {k: int(v) for k, v in nf.loc[nf['new'], 'u'].value_counts().items()},
                                   'vs_same_round_pp_c2_metric_size_item_unit_median': summ(m['Price'] / m['ref']),
                                   'vs_ph_2024Q1_c8_metric_size_item_unit_median': summ(m2['Price'] / m2['ref'])}
    except Exception as exc:  # noqa: BLE001
        out['w5_bottle_can_63'] = {'error': f'{type(exc).__name__}: {exc}'[:500],
                                   'traceback': traceback.format_exc()[-1500:]}
    return out


def aggregate():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        agg = c.community_prices(labels='Aggregate')
    save('community_prices_Aggregate', agg)
    lp = [x for x in caught if type(x.message).__name__ == 'LabelPassThroughWarning']
    r = agg.reset_index()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        pref = c.community_prices().reset_index()
    w2p, w2a = pref[pref['t'].eq('2012Q3')], r[r['t'].eq('2012Q3')]
    return {'shape': list(agg.shape), 'dup_rows': int(agg.index.duplicated(keep=False).sum()),
            'labels_unmatched': jsonable(agg.attrs.get('labels_unmatched')),
            'pass_through_msgs': [str(x.message)[:500] for x in lp],
            'semovita_in_aggregate_j': int(r['j'].eq('Semovita').sum()),
            'w2_other_grains_preferred': int(w2p['j'].eq('Other grains and flour').sum()),
            'w2_semovita_preferred': int(w2p['j'].eq('Semovita').sum()),
            'w2_other_grains_aggregate': int(w2a['j'].eq('Other grains and flour').sum())}


def feature():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        f = ll.Feature('community_prices')(['Nigeria', 'Tanzania', 'Uganda'])
    save('Feature_community_prices', f)
    return {'shape': list(f.shape), 'unique': bool(f.index.is_unique),
            'countries': {k: int(v) for k, v in f.index.get_level_values('country').value_counts().items()},
            'warnings': sorted({f'{type(x.message).__name__}: {str(x.message)[:300]}' for x in caught})[:30]}


if arm == 'cand':
    rec.step('cand_checks', cand_checks)
    rec.step('aggregate', aggregate)
    rec.step('feature', feature)
print('BUILD DONE', arm, flush=True)

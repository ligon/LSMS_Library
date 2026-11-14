"""Compare base / prev / cand frames (aggregates only).

usage: python compare.py <framesdir> <resultsdir>
"""
import json
import sys
import traceback
from pathlib import Path

import pandas as pd

frames, results = Path(sys.argv[1]), Path(sys.argv[2])
PP_Q = ['2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3']
out = {}


def load(side, name):
    p = frames / f'{side}.{name}.parquet'
    return pd.read_parquet(p) if p.exists() else None


def guard(name, fn):
    try:
        out[name] = fn()
    except Exception as e:  # noqa: BLE001
        out[name] = {'error': f'{type(e).__name__}: {e}'[:1500], 'tb': traceback.format_exc()[-3000:]}
    (results / 'compare.json').write_text(json.dumps(out, indent=1, default=str))


def frame_equal(a, b):
    try:
        pd.testing.assert_frame_equal(a.sort_index(), b.sort_index())
        return {'equal': True, 'rows': int(len(a))}
    except AssertionError as e:
        rec = {'equal': False, 'msg': str(e)[:800], 'rows_a': int(len(a)), 'rows_b': int(len(b))}
        try:
            ia, ib = set(a.index), set(b.index)
            rec['a_only'] = len(ia - ib)
            rec['b_only'] = len(ib - ia)
            if 'j' in a.index.names:
                ja = a.index.get_level_values('j').value_counts()
                jb = b.index.get_level_values('j').value_counts()
                d = (jb.reindex(ja.index.union(jb.index), fill_value=0)
                     - ja.reindex(ja.index.union(jb.index), fill_value=0))
                rec['j_count_delta'] = {str(k): int(v) for k, v in d[d != 0].items()}
                if 't' in a.index.names:
                    ta = a.reset_index().groupby(['t', 'j']).size()
                    tb = b.reset_index().groupby(['t', 'j']).size()
                    u = ta.index.union(tb.index)
                    dd = tb.reindex(u, fill_value=0) - ta.reindex(u, fill_value=0)
                    rec['t_j_count_delta'] = {f'{t}|{j}': int(v) for (t, j), v in dd[dd != 0].items()}
        except Exception as e2:  # noqa: BLE001
            rec['diag_error'] = str(e2)[:300]
        return rec


def eq_tables():
    res = {}
    for name in ('sample', 'household_roster', 'food_acquired', 'crop_production', 'plot_features',
                 'plot_inputs', 'food_acquired_aggregate', 'food_acquired_2012Q3',
                 'food_expenditures_aggregate', 'food_quantities'):
        r = {}
        for other in ('base', 'prev'):
            a, b = load(other, name), load('cand', name)
            if a is None or b is None:
                r[f'cand_vs_{other}'] = {'missing': [s for s, x in ((other, a), ('cand', b)) if x is None]}
                continue
            r[f'cand_vs_{other}'] = frame_equal(a, b)
        res[name] = r
    return res


guard('tables', eq_tables)


def keyed(a, b, la='a', lb='b'):
    ra, rb = a.reset_index(), b.reset_index()
    idx = list(a.index.names)
    m = ra.merge(rb, on=idx, how='outer', indicator=True, suffixes=(f'_{la}', f'_{lb}'))
    return m, idx


def cp_ph():
    res = {}
    c = load('cand', 'community_prices')
    for other in ('prev', 'base'):
        a = load(other, 'community_prices')
        pa = a[~a.index.get_level_values('t').isin(PP_Q)]
        pc = c[~c.index.get_level_values('t').isin(PP_Q)]
        cols = [x for x in pa.columns if x in pc.columns]
        rec = {'rows_other': int(len(pa)), 'rows_cand': int(len(pc)),
               'cand_extra_cols': [x for x in pc.columns if x not in pa.columns]}
        rec['frame_equal_common_cols'] = frame_equal(pa[cols], pc[cols])
        if 'Size' in pc:
            rec['cand_size_nonnull_on_ph'] = int(pc['Size'].notna().sum())
        m, idx = keyed(pa[cols], pc[cols], 'o', 'c')
        rec['keyed'] = {str(k): int(v) for k, v in m['_merge'].value_counts().items()}
        lo, ro = m[m['_merge'].eq('left_only')], m[m['_merge'].eq('right_only')]
        rec['other_only_by_t_u'] = {f'{t}|{u}': int(n) for (t, u), n in lo.groupby(['t', 'u']).size().items()}
        rec['cand_only_by_t_u'] = {f'{t}|{u}': int(n) for (t, u), n in ro.groupby(['t', 'u']).size().items()}
        both = m[m['_merge'].eq('both')]
        rec['common_value_diffs'] = {
            col: int((~((both[f'{col}_o'] == both[f'{col}_c'])
                        | (both[f'{col}_o'].isna() & both[f'{col}_c'].isna()))).sum())
            for col in cols}
        res[f'cand_vs_{other}'] = rec
    return res


guard('community_prices_ph', cp_ph)


def cp_pp():
    a, c = load('prev', 'community_prices'), load('cand', 'community_prices')
    pa = a[a.index.get_level_values('t').isin(PP_Q)]
    pc = c[c.index.get_level_values('t').isin(PP_Q)]
    rec = {'rows_prev_by_t': {str(k): int(v) for k, v in
                              pa.index.get_level_values('t').value_counts().sort_index().items()},
           'rows_cand_by_t': {str(k): int(v) for k, v in
                              pc.index.get_level_values('t').value_counts().sort_index().items()},
           'rows_prev': int(len(pa)), 'rows_cand': int(len(pc)),
           'cand_unique': bool(pc.index.is_unique)}
    m, idx = keyed(pa, pc, 'p', 'c')
    rec['keyed'] = {str(k): int(v) for k, v in m['_merge'].value_counts().items()}
    lo, ro = m[m['_merge'].eq('left_only')], m[m['_merge'].eq('right_only')]
    both = m[m['_merge'].eq('both')]
    rec['left_by_t_u_derivation'] = {f'{t}|{u}|{d}': int(n) for (t, u, d), n in
                                     lo.assign(d=lo['Derivation_p'].fillna('none'))
                                     .groupby(['t', 'u', 'd']).size().items()}
    rec['left_by_t'] = {str(k): int(v) for k, v in lo['t'].value_counts().items()}
    rec['arrived_by_t_u_j_derivation'] = {
        f'{t}|{u}|{j}|{d}': int(n) for (t, u, j, d), n in
        ro.assign(d=ro['Derivation_c'].fillna('none')).groupby(['t', 'u', 'j', 'd']).size().items()
        if n}
    rec['arrived_by_t'] = {str(k): int(v) for k, v in ro['t'].value_counts().items()}
    pe = ~((both['Price_p'] == both['Price_c']) | (both['Price_p'].isna() & both['Price_c'].isna()))
    rec['common_price_diffs'] = int(pe.sum())
    se = ~((both['Size_p'] == both['Size_c']) | (both['Size_p'].isna() & both['Size_c'].isna()))
    rec['common_size_diffs'] = int(se.sum())
    tr = both.assign(dp=both['Derivation_p'].fillna('none'), dc=both['Derivation_c'].fillna('none'))
    tr = tr[tr['dp'] != tr['dc']]
    rec['derivation_transitions'] = {f'{a}->{b}': int(n) for (a, b), n in
                                     tr.groupby(['dp', 'dc']).size().items()}
    rec['derivation_transitions_by_u'] = {f'{a}->{b}|{u}': int(n) for (a, b, u), n in
                                          tr.groupby(['dp', 'dc', 'u']).size().items()}
    rec['cand_derivation_counts'] = {str(k): int(v) for k, v in
                                     pc['Derivation'].fillna('none').value_counts().items()}
    rec['cand_derivation_by_t'] = {f'{t}|{d}': int(n) for (t, d), n in
                                   pc.reset_index().assign(d=lambda x: x['Derivation'].fillna('none'))
                                   .groupby(['t', 'd']).size().items()}
    # labels on served PP rows
    r = pc.reset_index()
    rec['u_counts_named'] = {f'{t}|{u}': int(n) for (t, u), n in r.groupby(['t', 'u']).size().items()
                             if u in ('Congo/Mudu', 'ml', 'Tomatoes cup', 'Unknown')}
    rec['j_named'] = {f'{t}|{j}': int(n) for (t, j), n in r.groupby(['t', 'j']).size().items()
                      if j in ('Semovita', 'Pepper', 'Bread', 'Palm wine')}
    rec['null_index'] = {lvl: int(r[lvl].isna().sum()) for lvl in pc.index.names}
    return rec


guard('community_prices_pp', cp_pp)


def cp_agg():
    res = {}
    c = load('cand', 'community_prices_aggregate')
    for other in ('prev', 'base'):
        a = load(other, 'community_prices_aggregate')
        pa = a[~a.index.get_level_values('t').isin(PP_Q)]
        pc = c[~c.index.get_level_values('t').isin(PP_Q)]
        cols = [x for x in pa.columns if x in pc.columns]
        res[f'ph_cand_vs_{other}'] = frame_equal(pa[cols], pc[cols])
    r = c.reset_index()
    pp = r[r['t'].isin(PP_Q)]
    res['pp_rows'] = int(len(pp))
    res['pp_dup_rows'] = int(c[c.index.get_level_values('t').isin(PP_Q)].index.duplicated(keep=False).sum())
    res['pp_j_named'] = {f'{t}|{j}': int(n) for (t, j), n in pp.groupby(['t', 'j']).size().items()
                         if j in ('Semovita', 'Pepper', 'Other grains and flour', 'Bread')}
    return res


guard('community_prices_aggregate', cp_agg)


def cp_2012q3():
    res = {}
    a, c = load('base', 'community_prices_2012Q3'), load('cand', 'community_prices_2012Q3')
    if a is None or c is None:
        return {'missing': True}
    for side, f in (('base', a), ('cand', c)):
        r = f.reset_index()
        res[f'{side}_j_named'] = {f'{t}|{j}': int(n) for (t, j), n in r.groupby(['t', 'j']).size().items()
                                  if j in ('Semovita', 'Bread')}
    return res


guard('community_prices_2012Q3', cp_2012q3)


def warnings_and_attrs():
    res = {}
    js = {}
    for side in ('base', 'prev', 'cand'):
        p = results / f'{side}.build.json'
        if p.exists():
            js[side] = json.loads(p.read_text())
    for step in js.get('cand', {}):
        if not isinstance(js['cand'][step], dict) or 'warnings' not in js['cand'][step]:
            continue
        res[step] = {s: {'warnings': js[s].get(step, {}).get('warnings'),
                         'error': js[s].get(step, {}).get('error')} for s in js}
        for s in js:
            rr = js[s].get(step, {}).get('result')
            if isinstance(rr, dict):
                res[step][s]['rows'] = rr.get('rows')
                if 'attrs_labels_unmatched' in rr:
                    res[step][s]['labels_unmatched'] = rr['attrs_labels_unmatched']
        # identical warning message sets?
        msgs = {s: js[s].get(step, {}).get('warning_msgs') for s in js}
        res[step]['warning_msgs_equal_base_cand'] = msgs.get('base') == msgs.get('cand')
        res[step]['warning_msgs_equal_prev_cand'] = msgs.get('prev') == msgs.get('cand')
        if msgs.get('base') != msgs.get('cand'):
            sb, sc = set(msgs.get('base') or []), set(msgs.get('cand') or [])
            res[step]['msgs_cand_only'] = sorted(sc - sb)[:12]
            res[step]['msgs_base_only'] = sorted(sb - sc)[:12]
    for s in js:
        rr = (js[s].get('community_prices') or {}).get('result') or {}
        der = rr.get('attrs_derivations') or {}
        res.setdefault('cp_attrs_derivations_rows', {})[s] = {
            k: (v.get('rows') if isinstance(v, dict) else v)
            for k, v in (der.get('Nigeria') or der).items() if 'pp-' in k or 'c8-' in k}
        res.setdefault('audit_reports', {})[s] = (js[s].get('audit_reports') or {}).get('result')
    return res


guard('warnings_attrs', warnings_and_attrs)
print(json.dumps(out, indent=1, default=str)[:30000])

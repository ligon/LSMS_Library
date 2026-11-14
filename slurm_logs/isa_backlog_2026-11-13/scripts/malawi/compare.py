"""Compare base vs candidate frames written by build.py.

usage: python compare.py <framesdir> <base-arm> <cand-arm> <out.json>

For each frame: rows by t; frame equality; and, aligning on the index with the
crop axis removed (crop is the one level the #1228 change may move), the count
of rows whose crop moved (from -> to, by t) and the count of cells that moved
in every other column, by t.  Plus Derivation / sale / ladder-replay diffs and
warning diffs from the two build.json logs.
"""
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

fr, A, B, outp = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
res = {'base': A, 'cand': B, 'frames': {}}
la = json.loads((fr / f'{A}.build.json').read_text())
lb = json.loads((fr / f'{B}.build.json').read_text())
res['heads'] = [la['head'], lb['head']]
CROP_LEVELS = ('crop', 'j')


def same(x, y):
    """NA-aware cell equality on two aligned Series."""
    xn, yn = x.isna().to_numpy(), y.isna().to_numpy()
    try:
        xv = pd.to_numeric(x, errors='raise').astype('float64').to_numpy()
        yv = pd.to_numeric(y, errors='raise').astype('float64').to_numpy()
        # blank strings convert to NaN: equal when both sides are NaN after conversion
        eq = (xv == yv) | (np.isnan(xv) & np.isnan(yv))
    except (ValueError, TypeError):
        eq = (x.astype('string').fillna('<NA>').to_numpy() == y.astype('string').fillna('<NA>').to_numpy())
    return (xn & yn) | (~xn & ~yn & eq)


for name in sorted(set(la['tables']) | set(lb['tables'])):
    pa, pb = fr / f'{A}.{name}.parquet', fr / f'{B}.{name}.parquet'
    rec = {'rows': [la['tables'].get(name, {}).get('rows'), lb['tables'].get(name, {}).get('rows')],
           'rows_by_t': [la['tables'].get(name, {}).get('rows_by_t'), lb['tables'].get(name, {}).get('rows_by_t')],
           'errors': [la['tables'].get(name, {}).get('error'), lb['tables'].get(name, {}).get('error')]}
    wa = la['tables'].get(name, {}).get('warnings', {})
    wb = lb['tables'].get(name, {}).get('warnings', {})
    rec['warnings'] = [wa, wb]
    ma = la['tables'].get(name, {}).get('warning_messages', [])
    mb = lb['tables'].get(name, {}).get('warning_messages', [])
    rec['warning_messages_only_base'] = sorted(set(ma) - set(mb))[:30]
    rec['warning_messages_only_cand'] = sorted(set(mb) - set(ma))[:30]
    if not (pa.exists() and pb.exists()):
        res['frames'][name] = rec
        continue
    a, b = pd.read_parquet(pa), pd.read_parquet(pb)
    try:
        rec['frame_equal'] = bool(a.sort_index().equals(b.sort_index()))
    except Exception as e:  # noqa: BLE001
        rec['frame_equal'] = f'ERR {e}'
    rec['all_null_columns'] = [sorted(c for c in a.columns if a[c].isna().all()),
                               sorted(c for c in b.columns if b[c].isna().all())]

    def _blank_all(df):
        out = []
        for col in df.columns:
            v = df[col]
            if (v.isna() | v.astype('string').str.strip().eq('').fillna(False)).all():
                out.append(col)
        return sorted(out)
    # Blank-aware: a column holding only '' strings is empty too, but isna() misses it.
    rec['all_blank_columns'] = [_blank_all(a), _blank_all(b)]
    rec['columns_only_base'] = sorted(set(a.columns) - set(b.columns))
    rec['columns_only_cand'] = sorted(set(b.columns) - set(a.columns))
    names = list(a.index.names)
    crop = [n for n in names if n in CROP_LEVELS]
    key = [n for n in names if n not in CROP_LEVELS]
    ra, rb = a.reset_index(), b.reset_index()
    for r in (ra, rb):
        for k in key:
            r[k] = r[k].astype('string')
    ua = not ra.duplicated(key).any()
    ub = not rb.duplicated(key).any()
    rec['align_key'] = key
    rec['align_unique'] = [bool(ua), bool(ub)]
    if ua and ub and key:
        m = ra.merge(rb, on=key, how='outer', suffixes=('_a', '_b'), indicator=True)
        rec['only_base'] = int((m['_merge'] == 'left_only').sum())
        rec['only_cand'] = int((m['_merge'] == 'right_only').sum())
        both = m[m['_merge'] == 'both']
        tcol = 't' if 't' in key else None
        moved = {}
        for c in crop:
            ch = ~same(both[f'{c}_a'], both[f'{c}_b'])
            sub = both[ch]
            moved[c] = {'rows': int(ch.sum()),
                        'by_t_from_to': {f'{t}|{x}->{y}': int(n) for (t, x, y), n in Counter(zip(
                            sub[tcol] if tcol else ['-'] * len(sub), sub[f'{c}_a'].astype(str),
                            sub[f'{c}_b'].astype(str))).items()}}
        rec['crop_moved'] = moved
        cells = {}
        for c in sorted((set(a.columns) & set(b.columns))):
            ch = ~same(both[f'{c}_a'] if f'{c}_a' in both else both[c], both[f'{c}_b'] if f'{c}_b' in both else both[c])
            if ch.any():
                sub = both[ch]
                by_t = Counter(sub[tcol]) if tcol else {'-': int(ch.sum())}
                cells[c] = {'cells': int(ch.sum()), 'by_t': {str(k): int(v) for k, v in by_t.items()}}
                if c == 'Derivation':
                    cells[c]['from_to'] = {f'{x}->{y}': int(n) for (x, y), n in Counter(zip(
                        sub['Derivation_a'].astype(str), sub['Derivation_b'].astype(str))).items()}
        rec['cells_moved_excluding_crop'] = cells
    # totals of numeric columns by t, for frames whose grain is not alignable
    num = [c for c in a.columns if c in b.columns and pd.api.types.is_numeric_dtype(a[c])
           and not pd.api.types.is_bool_dtype(a[c])]
    if 't' in names:
        tot = {}
        for c in num[:20]:
            sa = pd.to_numeric(a[c], errors='coerce').groupby(level='t').sum()
            sb = pd.to_numeric(b[c], errors='coerce').groupby(level='t').sum()
            d = {str(t): [float(sa.get(t, np.nan)), float(sb.get(t, np.nan))] for t in sorted(set(sa.index) | set(sb.index))}
            if any(not np.isclose(x, y, equal_nan=True) for x, y in d.values()):
                tot[c] = d
        rec['numeric_totals_that_moved_by_t'] = tot
    if crop and 't' in names:
        c = crop[0]
        for arm, df in (('base', a), ('cand', b)):
            lv = df.index.get_level_values(c).astype(str)
            tt = df.index.get_level_values('t').astype(str)
            rec[f'{arm}_other_cowpea_by_t'] = {f'{t}|{x}': int(n) for (t, x), n in Counter(zip(tt, lv)).items()
                                              if x in ('Other', 'Cowpea (Khobwe)')}
    if name == 'crop_production':
        for arm, df in (('base', a), ('cand', b)):
            r = df.reset_index()
            fl = r['plot_id'].astype(str).eq('Farm-level')
            rec[f'{arm}_derivation_by_t'] = {f'{t}|{d}': int(n) for (t, d), n in Counter(zip(
                r['t'].astype(str), r['Derivation'].astype(str))).items()}
            rec[f'{arm}_sale_by_t'] = {str(t): {'Quantity_sold': float(pd.to_numeric(g['Quantity_sold'], errors='coerce').sum()),
                                                 'Value_sold': float(pd.to_numeric(g['Value_sold'], errors='coerce').sum()),
                                                 'value_rows': int(pd.to_numeric(g['Value_sold'], errors='coerce').gt(0).sum()),
                                                 'farm_level_rows': int(fl[g.index].sum()),
                                                 'cowpea_value_rows': int((pd.to_numeric(g['Value_sold'], errors='coerce').gt(0)
                                                                           & g['crop'].astype(str).eq('Cowpea (Khobwe)')).sum()),
                                                 'cowpea_value_sum': float(pd.to_numeric(g.loc[g['crop'].astype(str).eq('Cowpea (Khobwe)'), 'Value_sold'], errors='coerce').sum()),
                                                 'cowpea_farm_level_rows': int((fl[g.index] & g['crop'].astype(str).eq('Cowpea (Khobwe)')).sum())}
                                     for t, g in r.groupby(r['t'].astype(str))}
    res['frames'][name] = rec

# ladder replay attrs
res['replay'] = {}
for w in sorted(set(la.get('replay', {})) | set(lb.get('replay', {}))):
    xa, xb = la['replay'].get(w, {}), lb['replay'].get(w, {})
    diff = {k: [xa.get(k), xb.get(k)] for k in sorted(set(xa) | set(xb))
            if k != 'seconds' and json.dumps(xa.get(k), sort_keys=True) != json.dumps(xb.get(k), sort_keys=True)}
    res['replay'][w] = diff
res['derivation_join'] = {'base': la.get('derivation_join'), 'cand': lb.get('derivation_join')}

# derivation_inputs frames, key x wave, base against candidate
res['inputs_frames'] = {}
ia, ib = fr / f'{A}.inputs', fr / f'{B}.inputs'
names = sorted({p.name for p in ia.glob('*.parquet')} | {p.name for p in ib.glob('*.parquet')})
for n in names:
    pa_, pb_ = ia / n, ib / n
    if not (pa_.exists() and pb_.exists()):
        res['inputs_frames'][n] = 'only ' + ('base' if pa_.exists() else 'cand')
        continue
    x, y = pd.read_parquet(pa_), pd.read_parquet(pb_)
    rec = {'rows': [len(x), len(y)], 'cols_only_base': sorted(set(x.columns) - set(y.columns)),
           'cols_only_cand': sorted(set(y.columns) - set(x.columns))}
    rec['equal'] = bool(x.shape == y.shape and list(x.columns) == list(y.columns)
                        and x.reset_index(drop=True).equals(y.reset_index(drop=True)))
    if not rec['equal'] and len(x) == len(y):
        common = [col for col in x.columns if col in y.columns]
        rec['cells_differ'] = {col: int((~same(x[col].reset_index(drop=True), y[col].reset_index(drop=True))).sum())
                               for col in common}
        rec['cells_differ'] = {k: v for k, v in rec['cells_differ'].items() if v}
    res['inputs_frames'][n] = rec
res['inputs_frames_summary'] = {
    'compared': sum(1 for v in res['inputs_frames'].values() if isinstance(v, dict)),
    'equal': sum(1 for v in res['inputs_frames'].values() if isinstance(v, dict) and v['equal']),
    'only': {k: v for k, v in res['inputs_frames'].items() if not isinstance(v, dict)}}

# Site Q offenders by identity
res['site_q'] = {}
for table in ('crop_production', 'plot_inputs'):
    qa = (la.get('site_q') or {}).get(table, {})
    qb = (lb.get('site_q') or {}).get(table, {})
    sa = {json.dumps(r, sort_keys=True, default=str) for r in qa.get('rows', [])}
    sb = {json.dumps(r, sort_keys=True, default=str) for r in qb.get('rows', [])}
    res['site_q'][table] = {'offenders': [qa.get('offenders'), qb.get('offenders')],
                            'by_wave': [qa.get('by_wave'), qb.get('by_wave')],
                            'only_base': sorted(sa - sb)[:10], 'only_cand': sorted(sb - sa)[:10]}
Path(outp).write_text(json.dumps(res, indent=1, default=str))
for n, r in res['frames'].items():
    print(n, 'rows', r['rows'], 'equal', r.get('frame_equal'), 'only', r.get('only_base'), r.get('only_cand'),
          'crop_moved', {k: v['rows'] for k, v in r.get('crop_moved', {}).items()},
          'cells', {k: v['cells'] for k, v in r.get('cells_moved_excluding_crop', {}).items()})
print('inputs frames', res['inputs_frames_summary'])
print('site_q', {t: (v['offenders'], v['by_wave']) for t, v in res['site_q'].items()})

"""Red team (malawi): base vs candidate, cell by cell.  usage: python compare.py <framesdir> <out.json>

For each saved frame: rows by t, column sets, a key without the crop level (uniqueness
checked on both arms), only-in-one-arm rows, per-column cell differences by t (NA == NA),
crop from->to by t.  For transforms keyed on crop (j), additionally compares after
mapping the candidate's later-wave cowpea back to Other and summing.  Warnings, replay
attrs, raw-line labels and derivation inputs (exact key match on the '+' split) too.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

fr, outp = Path(sys.argv[1]), Path(sys.argv[2])
COW, OTH = 'Cowpea (Khobwe)', 'Other'
LATER = {'2016-17', '2019-20'}


def vc(df):
    s = df.value_counts()
    return {('|'.join(map(str, k)) if isinstance(k, tuple) else str(k)): int(v) for k, v in s.items()}
res = {}
logs = {a: json.loads((fr / f'{a}.log.json').read_text()) for a in ('base', 'cand')}


def eqcells(a, b):
    an, bn = a.isna(), b.isna()
    try:
        same = (a.astype(object).values == b.astype(object).values)
    except Exception:  # noqa: BLE001
        same = (a.astype(str).values == b.astype(str).values)
    return (an.values & bn.values) | (~an.values & ~bn.values & same)


for name in ('crop_production', 'plot_inputs', 'harvest_kg', 'harvest_kg_shipped', 'crop_diversity',
             'gross_crop_revenue', 'seed_kg', 'nitrogen_kg'):
    rec = {}
    try:
        B = pd.read_parquet(fr / f'base.{name}.parquet')
        C = pd.read_parquet(fr / f'cand.{name}.parquet')
        rec['rows'] = [len(B), len(C)]
        rec['index'] = [list(B.index.names), list(C.index.names)]
        rec['cols_only_base'] = sorted(set(B.columns) - set(C.columns))
        rec['cols_only_cand'] = sorted(set(C.columns) - set(B.columns))
        b, c = B.reset_index(), C.reset_index()
        if 't' in b.columns:
            rec['rows_by_t'] = [b['t'].astype(str).value_counts().sort_index().to_dict(),
                                c['t'].astype(str).value_counts().sort_index().to_dict()]
        crop = [x for x in ('crop', 'j') if x in b.columns]
        key = [x for x in B.index.names if x not in crop]
        rec['key'] = key
        rec['key_unique'] = [bool(not b.duplicated(key).any()), bool(not c.duplicated(key).any())]
        if all(rec['key_unique']) and key:
            kb = b[key].astype(str).agg('|'.join, axis=1)
            kc = c[key].astype(str).agg('|'.join, axis=1)
            b.index, c.index = kb.values, kc.values
            rec['only_base'] = int((~b.index.isin(c.index)).sum())
            rec['only_cand'] = int((~c.index.isin(b.index)).sum())
            common = b.index.intersection(c.index)
            bb, cc = b.loc[common], c.loc[common]
            tt = bb['t'].astype(str) if 't' in bb.columns else pd.Series('all', index=bb.index)
            moved = {}
            for col in [x for x in b.columns if x in c.columns and x not in key]:
                same = eqcells(bb[col], cc[col])
                if (~same).any():
                    moved[col] = tt[~same].value_counts().to_dict()
            rec['cells_moved'] = moved
            for cr in crop:
                diff = ~eqcells(bb[cr], cc[cr])
                rec[f'{cr}_from_to'] = (pd.DataFrame({'t': tt[diff], 'f': bb.loc[diff, cr].astype(str),
                                                      'to': cc.loc[diff, cr].astype(str)})
                                        .pipe(vc))
                if 'production_obs' in bb.columns:
                    rec[f'{cr}_moved_obs'] = cc.loc[diff, 'production_obs'].astype(str).str.split('/').str[0].value_counts().to_dict()
                if 'input' in bb.columns:
                    rec[f'{cr}_moved_input_plot'] = (cc.loc[diff, ['input', 'plot_id']].astype(str)
                                                     .assign(plot_id=lambda x: np.where(x['plot_id'] == 'Farm-level', 'Farm-level', 'plot'))
                                                     .pipe(vc))
        elif crop:
            # map later-wave cowpea back to Other and compare grouped sums
            cr = crop[0]
            c2 = c.copy()
            later = c2['t'].astype(str).isin(LATER)
            c2.loc[later & c2[cr].astype(str).eq(COW), cr] = OTH
            full = list(B.index.names)
            num = [x for x in b.columns if x in c2.columns and x not in full and pd.api.types.is_numeric_dtype(b[x])]
            gb = b.groupby(full, dropna=False)[num].sum(min_count=1)
            gc = c2.groupby(full, dropna=False)[num].sum(min_count=1)
            rec['mapped_rows'] = [len(gb), len(gc)]
            j = gb.join(gc, lsuffix='_b', rsuffix='_c', how='outer')
            rec['mapped_only'] = int(j[[f'{n}_b' for n in num]].isna().all(axis=1).sum() +
                                     j[[f'{n}_c' for n in num]].isna().all(axis=1).sum())
            rec['mapped_max_abs_diff'] = {n: float((j[f'{n}_b'] - j[f'{n}_c']).abs().max()) for n in num}
            rec['mapped_cells_diff'] = {n: int((~eqcells(j[f'{n}_b'].round(9), j[f'{n}_c'].round(9))).sum()) for n in num}
        if crop and 't' in b.columns:
            cr = crop[0]
            for arm, f in (('base', b), ('cand', c)):
                rec[f'{arm}_cowpea_other_by_t'] = (f[f[cr].astype(str).isin([COW, OTH])][['t', cr]].astype(str)
                                                   .pipe(vc))
            num = [x for x in b.columns if x not in B.index.names and pd.api.types.is_numeric_dtype(b[x])]
            rec['wave_totals_moved'] = {n: {t: [float(b.loc[b['t'].astype(str) == t, n].sum()),
                                               float(c.loc[c['t'].astype(str) == t, n].sum())]
                                           for t in sorted(b['t'].astype(str).unique())
                                           if not np.isclose(b.loc[b['t'].astype(str) == t, n].sum(),
                                                             c.loc[c['t'].astype(str) == t, n].sum(), rtol=1e-12, atol=1e-9)}
                                        for n in num}
            rec['wave_totals_moved'] = {k: v for k, v in rec['wave_totals_moved'].items() if v}
    except Exception as e:  # noqa: BLE001
        import traceback
        rec['error'] = f'{type(e).__name__}: {e}'
        rec['tb'] = traceback.format_exc()[-2000:]
    wb = logs['base']['tables'].get(name, {}).get('warnings', [])
    wc = logs['cand']['tables'].get(name, {}).get('warnings', [])
    rec['warnings_n'] = [len(wb), len(wc)]
    rec['warnings_only_base'] = sorted(set(wb) - set(wc))[:20]
    rec['warnings_only_cand'] = sorted(set(wc) - set(wb))[:20]
    res[name] = rec
    print(name, json.dumps({k: v for k, v in rec.items() if k not in ('tb',)}, default=str)[:2500], flush=True)

# replay attrs
rep = {}
for w in ('2010-11', '2013-14', '2016-17', '2019-20'):
    ab = logs['base']['replay'].get(w, {}).get('attrs', {})
    ac = logs['cand']['replay'].get(w, {}).get('attrs', {})
    rep[w] = {'keys_only_cand': sorted(set(ac) - set(ab)), 'keys_only_base': sorted(set(ab) - set(ac)),
              'attrs_differ': sorted(k for k in set(ab) & set(ac) if ab[k] != ac[k]),
              'crop_text_left_other': ac.get('crop_text_left_other'),
              'crop_text_recovered': [ab.get('crop_text_recovered'), ac.get('crop_text_recovered')],
              'raw_crop_counts': [logs['base']['replay'][w].get('raw_crop_counts'), logs['cand']['replay'][w].get('raw_crop_counts')]}
    try:
        rb = pd.read_parquet(fr / f'base.raw_{w}.parquet')
        rc = pd.read_parquet(fr / f'cand.raw_{w}.parquet')
        cols = [x for x in rb.columns if x != 'crop']
        same_shape = rb.shape == rc.shape
        rep[w]['raw_same_except_crop'] = bool(same_shape and rb[cols].astype(str).equals(rc[cols].astype(str)))
        if same_shape:
            d = rb['crop'].astype(str) != rc['crop'].astype(str)
            rep[w]['raw_crop_from_to'] = (pd.DataFrame({'f': rb.loc[d, 'crop'].astype(str), 't': rc.loc[d, 'crop'].astype(str)})
                                          .pipe(vc))
            if 'Value_sold' in rc.columns:
                rep[w]['raw_moved_positive_value'] = int(pd.to_numeric(rc.loc[d, 'Value_sold'], errors='coerce').gt(0).sum())
    except Exception as e:  # noqa: BLE001
        rep[w]['raw_error'] = f'{type(e).__name__}: {e}'
res['replay'] = rep
print('replay', json.dumps(rep, default=str)[:3000], flush=True)

# derivation inputs: exact key match, and base vs cand inputs
dj = {}
served = {t: {a: pd.read_parquet(fr / f'{a}.{t}.parquet').reset_index() for a in ('base', 'cand')}
          for t in ('crop_production', 'plot_inputs')}
for path in sorted(fr.glob('cand.dinp__*.parquet')):
    tag = path.name[len('cand.dinp__'):-len('.parquet')]
    table, short, w = tag.split('__')
    key = f'Malawi::{table}::{short}'
    rec = {}
    try:
        ic = pd.read_parquet(path)
        pb = fr / f'base.dinp__{tag}.parquet'
        ib = pd.read_parquet(pb) if pb.exists() else None
        rec['rows'] = [None if ib is None else len(ib), len(ic)]
        if ib is not None and 'crop' in ic.columns and ib.shape == ic.shape:
            cols = [x for x in ib.columns if x != 'crop']
            rec['same_except_crop'] = bool(ib[cols].astype(str).equals(ic[cols].astype(str)))
            d = ib['crop'].astype(str) != ic['crop'].astype(str)
            rec['crop_from_to'] = (pd.DataFrame({'f': ib.loc[d, 'crop'].astype(str), 't': ic.loc[d, 'crop'].astype(str)})
                                   .pipe(vc))
        elif ib is not None:
            rec['equal'] = bool(ib.astype(str).equals(ic.astype(str)))
        for arm, inp in (('base', ib), ('cand', ic)):
            if inp is None:
                continue
            s = served[table][arm]
            s = s[s['t'].astype(str) == w]
            has = s['Derivation'].astype('string').fillna('').str.split('+').map(lambda ks: key in ks) if 'Derivation' in s else None
            if has is None:
                continue
            sub = s[has.to_numpy(dtype=bool)]
            # the inputs callables' own index: (t,i,crop,production_obs) | (t,i,crop,u) | (t,i,input_obs)
            if 'input_obs' in inp.columns:
                names = ['t', 'i', 'input_obs']
            elif 'production_obs' in inp.columns:
                names = ['t', 'i', 'crop', 'production_obs']
            else:
                names = ['t', 'i', 'crop', 'u']
            names = [n for n in names if n in sub.columns and n in inp.columns]
            kin = set(map(tuple, inp[names].astype(str).itertuples(index=False)))
            hit = np.array([tuple(x) in kin for x in sub[names].astype(str).itertuples(index=False)], dtype=bool)
            rec[f'{arm}_served_keyed'] = int(len(sub))
            rec[f'{arm}_joined'] = int(hit.sum())
            rec[f'{arm}_join_on'] = names
            if 'crop' in sub.columns:
                cw = sub['crop'].astype(str).eq(COW).to_numpy()
                rec[f'{arm}_keyed_cowpea'] = int(cw.sum())
                rec[f'{arm}_keyed_cowpea_joined'] = int((hit & cw).sum())
    except Exception as e:  # noqa: BLE001
        rec['error'] = f'{type(e).__name__}: {e}'
    dj[f'{key}|{w}'] = rec
res['derivation_inputs'] = dj
print('dinp', json.dumps(dj, default=str)[:6000], flush=True)
outp.write_text(json.dumps(res, indent=1, default=str))
print('COMPARE DONE', flush=True)

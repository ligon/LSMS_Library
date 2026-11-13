import json, pandas as pd, numpy as np
d = pd.read_parquet('/local/job40390255/data-private/637-ng/scratch/cp_dev.parquet')
b = pd.read_parquet('/local/job40390255/data-private/637-ng/scratch/cp_branch.parquet')
res = {'dev_index': list(d.index.names), 'branch_index': list(b.index.names),
       'dev_cols': {c: str(t) for c, t in d.dtypes.items()},
       'branch_cols': {c: str(t) for c, t in b.dtypes.items()},
       'branch_obs_dtype': str(b.index.get_level_values('obs').dtype),
       'branch_unique': bool(b.index.is_unique)}
per = {}
for t in sorted(set(d.index.get_level_values('t'))):
    dt = d.xs(t, level='t', drop_level=False); bt = b.xs(t, level='t', drop_level=False)
    p = {'dev_rows': len(dt), 'branch_rows': len(bt),
         'branch_by_obs': {int(k): int(v) for k, v in bt.groupby(level='obs').size().items()}}
    if set(bt.index.get_level_values('obs')) == {1}:
        try:
            pd.testing.assert_frame_equal(dt, bt.droplevel('obs'), check_exact=True)
            p['frame_equal_after_dropping_obs'] = True
        except AssertionError as e:
            p['frame_equal_after_dropping_obs'] = str(e)[:300]
    else:
        w = bt['Price'].unstack('obs')
        der = bt['Derivation'].unstack('obs') if 'Derivation' in bt else None
        dd = dt.join(w, how='left')
        at1 = dd[1].notna(); at2only = dd[1].isna() & dd[2].notna()
        p['dev_rows_with_obs1'] = int(at1.sum())
        p['dev_rows_obs2_only'] = int(at2only.sum())
        p['dev_rows_unmatched'] = int((~at1 & ~at2only).sum())
        p['dev_price_eq_branch_obs1'] = int((dd.loc[at1, 'Price'] == dd.loc[at1, 1]).sum())
        p['dev_price_eq_branch_obs2_only'] = int((dd.loc[at2only, 'Price'] == dd.loc[at2only, 2]).sum())
        if der is not None:
            dd2 = dt[['Derivation']].join(der, how='left')
            ok1 = (dd2.loc[at1, 'Derivation'].fillna('') == dd2.loc[at1, 1].fillna('')).sum()
            ok2 = (dd2.loc[at2only, 'Derivation'].fillna('') == dd2.loc[at2only, 2].fillna('')).sum()
            p['derivation_equal'] = int(ok1 + ok2)
        both = w.dropna()
        p['cells_both_obs'] = len(both)
        p['cells_both_obs_price_differs'] = int((both[1] != both[2]).sum())
        p['cells_both_obs_price_equal'] = int((both[1] == both[2]).sum())
        r = np.exp(np.abs(np.log(both[1] / both[2])))
        r = r[both[1] != both[2]]
        p['differing_ratio_median'] = round(float(r.median()), 3); p['differing_ratio_max'] = round(float(r.max()), 2)
        p['rows_added'] = len(bt) - len(dt)
        p['derived_rows_branch'] = int(bt['Derivation'].notna().sum()) if 'Derivation' in bt else 0
        p['derived_rows_dev'] = int(dt['Derivation'].notna().sum()) if 'Derivation' in dt else 0
    per[t] = p
res['per_round'] = per
print(json.dumps(res, indent=1, default=str))

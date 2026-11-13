import json, pandas as pd, numpy as np
B = '/local/job40390255/data-private/820/out-base'; F = '/local/job40390255/data-private/820/out-f7f5'; N = '/local/job40390255/data-private/820/out-branch'
b = pd.read_parquet(f'{B}/crop_production_api.parquet').sort_index()
f = pd.read_parquet(f'{F}/crop_production_api.parquet').sort_index()
n = pd.read_parquet(f'{N}/crop_production_api.parquet').sort_index()
res = {}
assert f.index.equals(n.index) and b.index.equals(n.index)
pd.testing.assert_frame_equal(f, n.drop(columns=['Derivation']))
res['branch_equals_f7f567c82_except_Derivation'] = True
key = 'Ethiopia::crop_production::sect12-sale-period'
d = n['Derivation']
stamped = (d == key).fillna(False).to_numpy(dtype=bool)
filled = ((b.Value_sold.isna() & n.Value_sold.notna()) | (b.Quantity_sold.isna() & n.Quantity_sold.notna())).to_numpy(dtype=bool)
res['stamped_rows'] = int(stamped.sum()); res['filled_rows_vs_development'] = int(filled.sum())
res['stamped_equals_filled'] = bool((stamped == filled).all())
res['other_keys'] = sorted(set(d.dropna()) - {key})
t = n.index.get_level_values('t')
res['per_wave'] = {w: {'stamped': int((stamped & (t == w)).sum()),
                       'stamped_positive_value': int((stamped & (t == w) & (n.Value_sold > 0).fillna(False).to_numpy()).sum()),
                       'positive_value_rows_dev': int(((b.Value_sold > 0).fillna(False) & (t == w)).sum()),
                       'positive_value_rows_branch': int(((n.Value_sold > 0).fillna(False) & (t == w)).sum())}
                   for w in sorted(set(t))}
res['attrs_summary'] = None
# sold-exceeds-harvest symptom: rows with Quantity>0 and Quantity_sold>0 (same row => same u), by source
both = (n.Quantity > 0).fillna(False) & (n.Quantity_sold > 0).fillna(False)
exc = (n.Quantity_sold > n.Quantity).fillna(False)
sym = {}
for w in ['2011-12', '2013-14', '2015-16']:
    m = both.to_numpy() & (t == w)
    s12 = m & stamped; s11 = m & ~stamped
    sym[w] = {'sect12_rows': int(s12.sum()), 'sect12_sold_gt_harvest': round(float(exc.to_numpy()[s12].mean()), 4),
              'sect11_rows': int(s11.sum()), 'sect11_sold_gt_harvest': round(float(exc.to_numpy()[s11].mean()), 4)}
res['quantity_sold_exceeds_harvest'] = sym
print(json.dumps(res, indent=1))
json.dump(res, open('/local/job40390255/worktrees/820/slurm_logs/isa_backlog_2026-10-21/820-ethiopia-sect12/round2_derivation_stamp.json', 'w'), indent=1)

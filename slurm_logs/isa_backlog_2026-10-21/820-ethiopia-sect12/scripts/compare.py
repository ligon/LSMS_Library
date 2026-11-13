import json, sys, os
import pandas as pd, numpy as np
B = '/local/job40390255/data-private/820/out-base'; N = '/local/job40390255/data-private/820/out-branch'
EV = '/local/job40390255/worktrees/820/slurm_logs/isa_backlog_2026-10-21/820-ethiopia-sect12'
b = pd.read_parquet(f'{B}/crop_production_api.parquet'); n = pd.read_parquet(f'{N}/crop_production_api.parquet')
b = b.sort_index(); n = n.sort_index()
res = {}
res['rows'] = {'base': len(b), 'branch': len(n)}
assert b.index.equals(n.index), 'index differs'
res['index_identical'] = True
other = [c for c in b.columns if c not in ('Quantity_sold', 'Value_sold')]
pd.testing.assert_frame_equal(b[other], n[other])
res['non_sale_columns_identical'] = other
for col in ['Value_sold', 'Quantity_sold']:
    was = b[col].notna()
    pd.testing.assert_series_equal(b.loc[was, col], n.loc[was, col])
res['sale_cells_nonnull_on_development_identical'] = True
t = n.index.get_level_values('t')
per = {}
for w in sorted(set(t)):
    m = (t == w)
    bb, nn = b[m], n[m]
    per[w] = {
        'rows': int(m.sum()),
        'Value_sold_nonnull': [int(bb.Value_sold.notna().sum()), int(nn.Value_sold.notna().sum())],
        'Value_sold_positive': [int((bb.Value_sold > 0).sum()), int((nn.Value_sold > 0).sum())],
        'Quantity_sold_nonnull': [int(bb.Quantity_sold.notna().sum()), int(nn.Quantity_sold.notna().sum())],
        'Quantity_sold_positive': [int((bb.Quantity_sold > 0).sum()), int((nn.Quantity_sold > 0).sum())],
        'Value_sold_birr_total': [round(float(bb.Value_sold.sum()), 2), round(float(nn.Value_sold.sum()), 2)],
        'rows_changed': int(((bb.Value_sold.isna() & nn.Value_sold.notna()) | (bb.Quantity_sold.isna() & nn.Quantity_sold.notna())).sum()),
    }
res['per_wave'] = per
# by crop: positive Value_sold counts, rows that changed only
chg = (b.Value_sold.isna() & n.Value_sold.notna())
j = n.index.get_level_values('j')
bycrop = (pd.DataFrame({'t': t, 'j': j, 'pos_before': (b.Value_sold > 0).values, 'pos_after': (n.Value_sold > 0).values})
          .groupby(['t', 'j'])[['pos_before', 'pos_after']].sum())
bycrop = bycrop[bycrop.pos_before != bycrop.pos_after]
bycrop['added'] = bycrop.pos_after - bycrop.pos_before
bycrop = bycrop.sort_values(['t', 'added'], ascending=[True, False])
bycrop.reset_index().to_csv(f'{EV}/positive_value_sold_by_wave_crop.csv', index=False)
top = bycrop.groupby(level='t').head(12)
print(top.to_string())
res['crops_gaining_positive_sales'] = {w: int((bycrop.loc[w].added > 0).sum()) for w in bycrop.index.get_level_values('t').unique()}
# perennial share
pr = n.perennial.astype('boolean').fillna(False).values
res['added_positive_rows_by_perennial_flag'] = {
    'perennial': int(((n.Value_sold > 0).fillna(False).to_numpy() & ~(b.Value_sold > 0).fillna(False).to_numpy() & pr).sum()),
    'not_perennial': int(((n.Value_sold > 0).fillna(False).to_numpy() & ~(b.Value_sold > 0).fillna(False).to_numpy() & ~pr).sum())}
# warnings
def wsum(d):
    # Category counts plus the message's leading 'Country/table wave:' tag only --
    # the bodies of some warnings name households, which are not committed.
    import re
    w = json.load(open(f'{d}/warnings.json'))
    out = {}
    for x in w:
        tag = re.split(r':', x['message'], maxsplit=1)[0][:80]
        out.setdefault(x['category'], []).append(tag)
    return out
res['warnings_base'] = wsum(B); res['warnings_branch'] = wsum(N)
for nm in ['grain_reports', 'null_read_reports', 'null_index_reports', 'quantity_reports']:
    gb = json.load(open(f'{B}/{nm}.json')); gn = json.load(open(f'{N}/{nm}.json'))
    strip = lambda L: [{k: v for k, v in r.items() if k in ('wave', 'rows', 'dropped', 'destroyed', 'conflicting_groups', 'nan_key_rows', 'n_flagged', 'column', 'levels', 'n_rows', 'flagged')} for r in L]
    res[nm] = {'base': strip(gb), 'branch': strip(gn), 'identical': gb == gn}
json.dump(res, open(f'{EV}/api_before_after.json', 'w'), indent=1, default=str)
print(json.dumps({k: v for k, v in res.items() if k not in ('warnings_base', 'warnings_branch')}, indent=1, default=str)[:5000])

"""GH #1559 red-team F1: Mali 2017-18's unwired s7fq13d ("Equivalent en Kg"), joined to
the SERVED rows, as a kg-per-unit factor and as an answer key for the price rung.

Usage: analysis3.py FRAMES_DIR OUT_JSON, in the candidate clone.  Aggregates only.
Keys are rebuilt exactly as Mali/2017-18/_/crop_production.py builds them: i =
eaci17_i([grappe, exploitation]) (2017-18 has an empty updated_ids map, so no walk),
plot_id = "{s7fq01}_{s7fq02}", crop / u through the harmonize_food / u tables.
"""
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from lsms_library import paths
from lsms_library import local_tools as tools
from lsms_library import quantity_audit as Q
from lsms_library.transformations import harvest_kg_factors

frames, out_json = Path(sys.argv[1]), Path(sys.argv[2])
warnings.simplefilter('ignore')
root = Path(paths.countries_root()) / 'Mali'
sys.path.insert(0, str(root / '_'))
import mali  # noqa: E402


def f64(s):
    return pd.to_numeric(s, errors='coerce').astype('float64')


def agree(r):
    r = pd.Series(r, dtype='float64').replace([np.inf, -np.inf], np.nan).dropna()
    r = r[r > 0]
    if r.empty:
        return {'n': 0}
    lr = np.abs(np.log(r))
    return {'n': int(r.size), 'median': round(float(r.median()), 4),
            'p10': round(float(r.quantile(.1)), 4), 'p90': round(float(r.quantile(.9)), 4),
            'within_2x': round(float((lr <= np.log(2)).mean()), 4)}


W = '2017-18'
df = pd.read_pickle(frames / 'Mali_crop_production.pkl')
s7f = tools.get_dataframe(str(root / W / 'Data' / 'eaci17_s7fp2.dta'))
dirs = [f'{root / W / "_"}/', f'{root / "_"}/']
crop_map = tools.get_categorical_mapping(tablename='harmonize_food', idxvars='Code', dirs=dirs,
                                         **{'Preferred Label': 'Preferred Label'})
unit_map = tools.get_categorical_mapping(tablename='u', idxvars='Code', dirs=dirs,
                                         **{'Preferred Label': 'Preferred Label'})
raw = pd.DataFrame({
    'i': s7f.apply(lambda r: mali.eaci17_i(pd.Series([r['grappe'], r['exploitation']])), axis=1),
    'plot_id': (s7f['s7fq01'].astype('Int64').astype('string') + '_'
                + s7f['s7fq02'].astype('Int64').astype('string')),
    'crop': s7f['s7fq03'].astype('string').str.strip().map(crop_map).astype('string'),
    'u_raw': s7f['s7fq13c'].astype('string').str.strip().map(unit_map).astype('string'),
    'q_raw': f64(s7f['s7fq13a']),
    'kgf': f64(s7f['s7fq13d']),
}).dropna(subset=['i', 'plot_id', 'crop'])
dups = int(raw.duplicated(['i', 'plot_id', 'crop']).sum())
raw = raw.drop_duplicates(['i', 'plot_id', 'crop'], keep='first')

flat = df.reset_index()
flat['i'] = flat['i'].astype(str)
cp = flat[(flat['t'] == W) & (flat['plot_id'].astype(str) != 'Farm-level')].copy()
hk = harvest_kg_factors(df).reset_index(drop=True)
cp['src'] = hk.loc[cp.index, 'KgFactorSource'].to_numpy()
cp['kpu_now'] = hk.loc[cp.index, 'kg_per_unit'].to_numpy()
m = cp.merge(raw, on=['i', 'plot_id', 'crop'], how='left', indicator=True)
assert len(m) == len(cp)
qty = f64(m['Quantity'])
joined = (m['_merge'] == 'both').to_numpy()
pos = (qty > 0).to_numpy()
kgf_ok = joined & (m['kgf'] > 0).to_numpy()
R = {'raw_rows': int(len(s7f)), 'raw_duplicate_keys': dups,
     'served_seasonal_rows': int(len(cp)), 'joined': int(joined.sum()),
     'positive_quantity': int(pos.sum()), 'positive_quantity_joined': int((pos & joined).sum()),
     'positive_quantity_with_kgf': int((pos & kgf_ok).sum()),
     'quantity_equal_raw': int((joined & pos & np.isclose(qty, m['q_raw'].to_numpy())).sum()),
     'unit_equal_raw': int((joined & (m['u'].astype(str) == m['u_raw'].astype(str)).to_numpy()).sum()),
     'kgf_is_1_on_kg_rows': agree(m.loc[kgf_ok & m['u'].astype(str).str.lower().isin(['kg', 'kilogramme']).to_numpy(), 'kgf']),
     'source_of_positive_rows': {str(k): int(v) for k, v in m.loc[pos, 'src'].value_counts().items()}}

# What wiring it as the REPORTED KgFactor would serve: the ladder itself, with its
# reported-factor screen (KG_FACTOR_MAX, kg == 1), on the whole Mali frame.
sim = df.copy()
flat_all = sim.reset_index()
key = flat_all['t'].astype(str) + '|' + flat_all['i'].astype(str) + '|' + flat_all['plot_id'].astype(str) + '|' + flat_all['crop'].astype(str)
rk = W + '|' + raw['i'].astype(str) + '|' + raw['plot_id'].astype(str) + '|' + raw['crop'].astype(str)
kmap = pd.Series(raw['kgf'].to_numpy(), index=rk.to_numpy())
sim['KgFactor'] = key.map(kmap).to_numpy()
hk2 = harvest_kg_factors(sim).reset_index(drop=True)
q_all = f64(flat_all['Quantity'])
wave = (flat_all['t'] == W).to_numpy()
kg_now = (q_all * hk['kg_per_unit']).where(hk['KgFactorSource'] != 'survey_kg')
kg_wired = (q_all * hk2['kg_per_unit']).where(hk2['KgFactorSource'] != 'survey_kg')
R['if_wired_as_KgFactor'] = {
    'kg_served_now': round(float(np.nansum(kg_now[wave])), 1),
    'kg_if_wired': round(float(np.nansum(kg_wired[wave])), 1),
    'sources_now': {str(k): int(v) for k, v in hk.loc[wave, 'KgFactorSource'].value_counts().items()},
    'sources_if_wired': {str(k): int(v) for k, v in hk2.loc[wave, 'KgFactorSource'].value_counts().items()},
    'reported_rejected_by_screen': int(hk2.loc[wave, 'kg_reported_rejected'].sum()),
    'other_waves_unchanged': bool(np.allclose(np.nan_to_num(kg_now[~wave].to_numpy()),
                                              np.nan_to_num(kg_wired[~wave].to_numpy()))),
}
hh = flat_all.loc[wave].assign(kg_now=kg_now[wave].to_numpy(), kg_wired=kg_wired[wave].to_numpy())
g = hh.groupby(['i', 'crop'])[['kg_now', 'kg_wired']].sum(min_count=1)
R['if_wired_as_KgFactor']['household_crops'] = int(len(g))
R['if_wired_as_KgFactor']['share_household_crops_with_kg_now'] = round(float((g['kg_now'] > 0).mean()), 4)
R['if_wired_as_KgFactor']['share_household_crops_with_kg_if_wired'] = round(float((g['kg_wired'] > 0).mean()), 4)
R['if_wired_as_KgFactor']['median_kg_per_household_crop_if_wired'] = {
    c: round(float(x['kg_wired'].median()), 1) for c, x in g.reset_index().groupby('crop')
    if c in ('Mil', 'Maïs', 'Arachide', 'Sorgho', 'Riz', 'Coton')}

# The price rung's masses against s7fq13d, row by row, on the rows the rung would serve
qp = f64(flat_all['Quantity'])
fac = Q.survey_kg_price_factors(flat_all.assign(Quantity_kg=np.where(qp > 0, qp, np.nan))).reset_index(drop=True)
served = ((hk['KgFactorSource'] == 'none') & (qp > 0) & fac['kg_survey'].notna()
          & fac['kg_price_implied'].notna()).to_numpy() & wave
comp = pd.DataFrame({'crop': flat_all['crop'], 'u': flat_all['u'], 'implied': fac['kg_price_implied'],
                     'kgf': sim['KgFactor'].to_numpy()})[served & (sim['KgFactor'].to_numpy() > 0)]
R['rung_vs_s7fq13d'] = {
    'rung_rows': int(served.sum()), 'rung_rows_with_s7fq13d': int(len(comp)),
    'all': agree(comp['implied'] / comp['kgf']),
    'by_crop_unit': {f'{c} / {u}': {'rows': int(len(x)), 'rung': round(float(x['implied'].median()), 2),
                                    's7fq13d': round(float(x['kgf'].median()), 2),
                                    'rung_over_s7fq13d': agree(x['implied'] / x['kgf']).get('median')}
                     for (c, u), x in comp.groupby(['crop', 'u']) if len(x) >= 30}}
out_json.write_text(json.dumps({'Mali': R}, indent=1, default=str, ensure_ascii=False))
print(json.dumps(R, default=str)[:4000])

"""Downstream totals on a WARM build: harvest_kg (+shipped), gross_crop_revenue, crop_diversity(value), median_price_valuation(v)."""
import os, sys, json, warnings
checkout, out = sys.argv[1], sys.argv[2]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')
import pandas as pd, numpy as np
from lsms_library import Country
from lsms_library import transformations as T
sys.path.insert(0, f'{checkout}/lsms_library/countries/Ethiopia/_')
import ethiopia as E
cp = Country('Ethiopia').crop_production()
res = {}
def by_t(s):
    return {k: round(float(v), 2) for k, v in s.groupby(level='t').sum().items()}
hk = T.harvest_kg(cp)
col = 'Harvest_kg' if 'Harvest_kg' in hk.columns else hk.columns[0]
res['harvest_kg'] = by_t(hk[col])
hk2 = T.harvest_kg(cp, shipped_factors=E.crop_conversion_factors())
res['harvest_kg_shipped'] = by_t(hk2[col])
g = T.gross_crop_revenue(cp)
res['gross_crop_revenue'] = by_t(g['Gross_crop_revenue'])
res['gross_crop_revenue_households'] = {k: int(v) for k, v in g['Gross_crop_revenue'].groupby(level='t').count().items()}
try:
    d = T.crop_diversity(cp, weight='value')
    c0 = d.columns[0]
    res['crop_diversity_value_nonnull'] = {k: int(v) for k, v in d[c0].groupby(level='t').count().items()}
    res['crop_diversity_value_mean'] = {k: round(float(v), 4) for k, v in d[c0].groupby(level='t').mean().items()}
except Exception as e:
    res['crop_diversity_value'] = repr(e)[:300]
try:
    cpk = cp.copy()
    fac = T.harvest_kg_factors(cp, shipped_factors=E.crop_conversion_factors())
    assert fac.index.equals(cp.index)
    cpk['Harvest_kg'] = (cp['Quantity'].astype(float) * fac['kg_per_unit'].astype(float)).values
    res['harvest_kg_rows_served'] = int(cpk['Harvest_kg'].notna().sum())
    # Within-wave ladder (item_keys carries t so no price crosses a wave);
    # values each row's kg harvest (shipped factors) at the EA->national median kg price.
    mp = T.median_price_valuation(cpk, geo_levels=['v'], item_keys=('t', 'j'), quantity_col='Harvest_kg')
    oc = 'Value' if 'Value' in mp.columns else mp.columns[-1]
    res['median_price_valuation_v_total'] = by_t(mp[oc])
    res['median_price_valuation_v_nonnull'] = {k: int(v) for k, v in mp[oc].groupby(level='t').count().items()}
except Exception as e:
    res['median_price_valuation'] = repr(e)[:300]
json.dump(res, open(out, 'w'), indent=1)
print(json.dumps(res, indent=1))

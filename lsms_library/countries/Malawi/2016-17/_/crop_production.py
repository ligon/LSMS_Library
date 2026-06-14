"""Build crop_production for Malawi IHS4 2016-17 (GAP 1).

IHS4 ships a Cross_Sectional half (cs-17-prefixed case_id) and a Panel
half (bare y3_hhid), concatenated into the single 2016-17 wave -- exactly
like plot_features.  Plot key is gardenid_plotid in both halves.

Module naming in IHS4: the seasonal modules (G, I) carry a harmonized
`crop_code` (codes 1-48); the perennial modules (P, Q) carry their own
crop code -- P uses ag_p0c, Q uses crop_code -- in the perennial
namespace (offset +1000 in harmonize_crop).  Sale (I, Q) is at the
household-crop grain (no plot id); attached to single-plot (i, crop)
rows only.  See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import (_harvest_block, _sale_block, assemble_crop_production)


WAVE = '2016-17'


def _plotkey(df):
    return (df['gardenid'].apply(format_id) + '_'
            + df['plotid'].apply(format_id))


def _half(g, p, i_mod, q, hh_of):
    """Build harvest + sale pieces for one XS/Panel half.

    ``hh_of`` maps each raw module DataFrame's hhid column to the
    canonical wave id string (cs-17 prefix for XS, bare y3_hhid for
    Panel)."""
    for df in (g, p, i_mod, q):
        df['hhid'] = hh_of(df)
    g['plotkey'] = _plotkey(g)
    p['plotkey'] = _plotkey(p)
    # Perennial crop code lives in ag_p0c (Cross_Sectional) or crop_code
    # (Panel) -- both in the 1-18 perennial namespace.
    p_crop = 'ag_p0c' if 'ag_p0c' in p.columns else 'crop_code'
    harvest = [
        _harvest_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code',
                       qty='ag_g13a', unit='ag_g13b', condition='ag_g13c',
                       plant_m='ag_g05a', plant_y='ag_g05b', harv_m='ag_g12b',
                       intercrop='ag_g01', perennial=False, t=WAVE),
        _harvest_block(p, hhid='hhid', plotkey='plotkey', cropcode=p_crop,
                       qty='ag_p09a', unit='ag_p09b', plant_y='ag_p04',
                       harv_m='ag_p06c', perennial=True, t=WAVE),
    ]
    sale = [
        _sale_block(i_mod, hhid='hhid', cropcode='crop_code', sold_flag='ag_i01',
                    qty_sold='ag_i02a', value_sold='ag_i03', perennial=False),
        _sale_block(q, hhid='hhid', cropcode='crop_code', sold_flag='ag_q01',
                    qty_sold='ag_q02a', value_sold='ag_q03', perennial=True),
    ]
    return harvest, sale


harvest_all, sale_all = [], []

# --- Cross-sectional half (cs-17 prefix) ---
g_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_g.dta', convert_categoricals=False)
p_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_p.dta', convert_categoricals=False)
i_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_i.dta', convert_categoricals=False)
q_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_q.dta', convert_categoricals=False)
h, s = _half(g_xs, p_xs, i_xs, q_xs,
             lambda df: 'cs-17-' + df['case_id'].apply(format_id))
harvest_all += h
sale_all += s

# --- Panel half (bare y3_hhid) ---
g_pn = get_dataframe('../Data/Panel/ag_mod_g_16.dta', convert_categoricals=False)
p_pn = get_dataframe('../Data/Panel/ag_mod_p_16.dta', convert_categoricals=False)
i_pn = get_dataframe('../Data/Panel/ag_mod_i_16.dta', convert_categoricals=False)
q_pn = get_dataframe('../Data/Panel/ag_mod_q_16.dta', convert_categoricals=False)
h, s = _half(g_pn, p_pn, i_pn, q_pn,
             lambda df: df['y3_hhid'].apply(format_id))
harvest_all += h
sale_all += s

df = assemble_crop_production(WAVE, harvest_all, sale_all)

assert df.index.is_unique, f"Non-unique (t,i,plot,crop) in crop_production {WAVE}"
assert len(df) > 0, f"crop_production {WAVE} produced no rows"

to_parquet(df, 'crop_production.parquet')

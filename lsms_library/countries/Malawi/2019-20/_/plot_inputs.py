"""Build plot_inputs for Malawi IHS5 2019-20 (GAP 2).

IHS5 ships a Cross_Sectional half (bare case_id) and a Panel half
(y4_hhid), concatenated into the single 2019-20 wave -- exactly like
plot_features / crop_production (sample().i for 2019-20 is the bare
cross-sectional case_id, NOT cs-prefixed).  Plot key is gardenid_plotid
in both halves.

Module D layout is the IHS4/IHS5 generation: slot-1 fertilizer type
ag_d39a / applied-qty ag_d39d / unit ag_d39e; slot-2 type ag_d39g /
applied-qty ag_d39j (no distinct slot-2 applied-unit -> u NaN slot 2).
Module G: harmonized crop_code, seed ag_g04a/ag_g04b, improved flag
ag_g0f (2 = improved).  Module H purchased seed naming differs by half:
Cross_Sectional uses ag_h07*/ag_h09 + ag_h38*/ag_h40; Panel uses the
harmonized ag_h16*/ag_h19 + ag_h26*/ag_h29.

Neither IHS5 ag_mod_d ships with the latin-1 bad byte that the IHS4
cross-section had, so a plain get_dataframe read suffices.
See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import (_fertilizer_block, _organic_block, _pesticide_block,
                    _seed_block, _seed_purchase_block, assemble_plot_inputs)


WAVE = '2019-20'


def _plotkey(df):
    return (df['gardenid'].apply(format_id) + '_'
            + df['plotid'].apply(format_id))


def _half(d, g, h, hh_of, seed_qty, seed_unit, seed_value):
    for df in (d, g, h):
        df['hhid'] = hh_of(df)
    d['plotkey'] = _plotkey(d)
    g['plotkey'] = _plotkey(g)
    pieces = [
        _fertilizer_block(d, hhid='hhid', plotkey='plotkey',
                          type1='ag_d39a', qty1='ag_d39d', unit1='ag_d39e',
                          type2='ag_d39g', qty2='ag_d39j', unit2=None,
                          t=WAVE),
        _organic_block(d, hhid='hhid', plotkey='plotkey', flag='ag_d36',
                       t=WAVE),
        # IHS5 pesticide layout shifted vs IHS3/IHS4: ag_d41d is now a
        # Yes/No "applied a second agrochemical?" gate, so slot 2 is
        # (ag_d41e type, ag_d41f qty, ag_d41g unit).
        _pesticide_block(d, hhid='hhid', plotkey='plotkey', gate='ag_d40',
                         slots=[('ag_d41a', 'ag_d41b', 'ag_d41c'),
                                ('ag_d41e', 'ag_d41f', 'ag_d41g')], t=WAVE),
        _seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code',
                    qty='ag_g04a', unit='ag_g04b', improved='ag_g0f', t=WAVE),
    ]
    seed_purchase = [
        _seed_purchase_block(h, hhid='hhid', cropcode='crop_code',
                             qty_cols=seed_qty, unit_cols=seed_unit,
                             value_cols=seed_value),
    ]
    return pieces, seed_purchase


all_pieces, all_seed_purchase = [], []

# --- Cross-sectional half (bare case_id) ---
d_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_d.dta', convert_categoricals=False)
g_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_g.dta', convert_categoricals=False)
h_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_h.dta', convert_categoricals=False)
p, s = _half(d_xs, g_xs, h_xs, lambda df: df['case_id'].apply(format_id),
             seed_qty=['ag_h07a', 'ag_h38a'],
             seed_unit=['ag_h07b', 'ag_h38b'],
             seed_value=['ag_h09', 'ag_h40'])
all_pieces += p
all_seed_purchase += s

# --- Panel half (y4_hhid) ---
d_pn = get_dataframe('../Data/Panel/ag_mod_d_19.dta', convert_categoricals=False)
g_pn = get_dataframe('../Data/Panel/ag_mod_g_19.dta', convert_categoricals=False)
h_pn = get_dataframe('../Data/Panel/ag_mod_h_19.dta', convert_categoricals=False)
p, s = _half(d_pn, g_pn, h_pn, lambda df: df['y4_hhid'].apply(format_id),
             seed_qty=['ag_h16a', 'ag_h26a'],
             seed_unit=['ag_h16b', 'ag_h26b'],
             seed_value=['ag_h19', 'ag_h29'])
all_pieces += p
all_seed_purchase += s

df = assemble_plot_inputs(WAVE, all_pieces, all_seed_purchase)

assert df.index.is_unique, f"Non-unique (t,i,plot,input,crop,u) in plot_inputs {WAVE}"
assert len(df) > 0, f"plot_inputs {WAVE} produced no rows"

to_parquet(df, 'plot_inputs.parquet')

"""Build plot_inputs for Malawi IHS5 2019-20 (GAP 2).

IHS5 ships a Cross_Sectional half (bare case_id) and a Panel half
(y4_hhid), concatenated into the single 2019-20 wave -- exactly like
plot_features / crop_production (sample().i for 2019-20 is the bare
cross-sectional case_id, NOT cs-prefixed).  Plot key is gardenid_plotid
in both halves.

Module D layout is the IHS4/IHS5 generation: slot-1 fertilizer type
ag_d39a / enumerator-total kg ag_d39d; slot-2 type ag_d39g /
enumerator-total kg ag_d39j. Native quantity/unit pairs b/c and h/i
are separate from these selected totals; ag_d39e records timing.
Module G: harmonized crop_code, seed ag_g04a/ag_g04b, improved flag
ag_g0f (2 = improved).  Module H naming differs by half (GH #1190):
Cross_Sectional purchase is ag_h07a/b with value ag_h10 (ag_h09 is the
TRANSPORT payment); Panel uses the harmonized ag_h16*/ag_h19 +
ag_h26*/ag_h29.  Seed received for free is ag_h38a/b in both halves
(ag_h40 is its transport payment) and is served in its own columns,
never as a purchase.

Neither IHS5 ag_mod_d ships with the latin-1 bad byte that the IHS4
cross-section had, so a plain get_dataframe read suffices.
See lsms_library/countries/Malawi/_/malawi.py.
"""
import sys

sys.path.append('../../_/')
import pandas as pd
import numpy as np

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from malawi import (_fertilizer_block, _organic_block, _pesticide_block,
                    _seed_block, _seed_purchase_block, _seed_gift_block,
                    assemble_plot_inputs, _seed_source_reference)


WAVE = '2019-20'
G_XS_SOURCE = '../Data/Cross_Sectional/ag_mod_g.dta'
G_PANEL_SOURCE = '../Data/Panel/ag_mod_g_19.dta'


def _plotkey(df):
    return (df['gardenid'].apply(format_id) + '_'
            + df['plotid'].apply(format_id))


def _half(d, g, h, hh_of, seed_qty, seed_unit, seed_value, source_ref, source_half):
    g_rows = np.arange(len(g))
    for df in (d, g, h):
        df['hhid'] = hh_of(df)
    d['plotkey'] = _plotkey(d)
    g['plotkey'] = _plotkey(g)
    # Q39 (IHS5 agriculture questionnaire p.10) labels d/j TOTAL KGs.
    # Bind those unchanged amounts before the additive full-key grouping
    # (ledger #1111 sections 3-4), without interpreting timing as a unit.
    fertilizer = _fertilizer_block(
        d, hhid='hhid', plotkey='plotkey',
        type1='ag_d39a', qty1='ag_d39d', unit1=None,
        type2='ag_d39g', qty2='ag_d39j', unit2=None, t=WAVE)
    fertilizer['u'] = pd.Series('Kilogramme', index=fertilizer.index,
                                dtype='string')
    pieces = [
        fertilizer,
        _organic_block(d, hhid='hhid', plotkey='plotkey', flag='ag_d36',
                       t=WAVE),
        # IHS5 pesticide layout shifted vs IHS3/IHS4: ag_d41d is now a
        # Yes/No "applied a second agrochemical?" gate, so slot 2 is
        # (ag_d41e type, ag_d41f qty, ag_d41g unit).
        _pesticide_block(d, hhid='hhid', plotkey='plotkey', gate='ag_d40',
                         slots=[('ag_d41a', 'ag_d41b', 'ag_d41c'),
                                ('ag_d41e', 'ag_d41f', 'ag_d41g')], t=WAVE),
        _seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code',
                    qty='ag_g04a', unit='ag_g04b', improved='ag_g0f', t=WAVE,
                    source_ref=source_ref, source_half=source_half,
                    row_positions=g_rows),
    ]
    seed_purchase = [
        _seed_purchase_block(h, hhid='hhid', cropcode='crop_code',
                             qty_cols=seed_qty, unit_cols=seed_unit,
                             value_cols=seed_value, source_half=source_half),
    ]
    seed_gift = [_seed_gift_block(h, hhid='hhid', cropcode='crop_code',
                                  source_half=source_half)]
    return pieces, seed_purchase, seed_gift


all_pieces, all_seed_purchase, all_seed_gift = [], [], []

# --- Cross-sectional half (bare case_id) ---
d_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_d.dta', convert_categoricals=False)
g_xs = get_dataframe(G_XS_SOURCE, convert_categoricals=False)
h_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_h.dta', convert_categoricals=False)
p, s, gft = _half(d_xs, g_xs, h_xs, lambda df: df['case_id'].apply(format_id),
             seed_qty=['ag_h07a'],
             seed_unit=['ag_h07b'],
             seed_value=['ag_h10'],
             source_ref=_seed_source_reference(G_XS_SOURCE, script_path=__file__),
             source_half='Cross_Sectional')
all_pieces += p
all_seed_purchase += s
all_seed_gift += gft

# --- Panel half (y4_hhid) ---
d_pn = get_dataframe('../Data/Panel/ag_mod_d_19.dta', convert_categoricals=False)
g_pn = get_dataframe(G_PANEL_SOURCE, convert_categoricals=False)
h_pn = get_dataframe('../Data/Panel/ag_mod_h_19.dta', convert_categoricals=False)
p, s, gft = _half(d_pn, g_pn, h_pn, lambda df: df['y4_hhid'].apply(format_id),
             seed_qty=['ag_h16a', 'ag_h26a'],
             seed_unit=['ag_h16b', 'ag_h26b'],
             seed_value=['ag_h19', 'ag_h29'],
             source_ref=_seed_source_reference(G_PANEL_SOURCE, script_path=__file__),
             source_half='Panel')
all_pieces += p
all_seed_purchase += s
all_seed_gift += gft

df = assemble_plot_inputs(WAVE, all_pieces, all_seed_purchase, all_seed_gift)

assert df.index.is_unique, f"Non-unique source-observation grain in plot_inputs {WAVE}"
assert len(df) > 0, f"plot_inputs {WAVE} produced no rows"

to_parquet(df, 'plot_inputs.parquet')

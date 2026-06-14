"""Build plot_inputs for Malawi IHS4 2016-17 (GAP 2).

IHS4 ships a Cross_Sectional half (cs-17-prefixed case_id) and a Panel
half (bare y3_hhid), concatenated into the single 2016-17 wave -- exactly
like plot_features / crop_production.  Plot key is gardenid_plotid in both
halves (Module D and Module G agree).

Module D layout is the IHS4/IHS5 generation: slot-1 fertilizer type
ag_d39a / applied-qty ag_d39d / unit ag_d39e; slot-2 type ag_d39g /
applied-qty ag_d39j (no distinct slot-2 applied-unit column -> u NaN for
slot 2).  Module G carries a harmonized `crop_code`, the seed columns
ag_g04a/ag_g04b, and the improved-seed flag ag_g0f (2 = improved).
Module H purchased seed: ag_h16a/b + ag_h19, ag_h26a/b + ag_h29;
crop_code.

The Cross_Sectional ag_mod_d.dta has a latin-1 bad byte in an unused
free-text column that breaks a full read; we read only the columns we
need via the _read_usecols helper (same recipe as plot_features 2016-17).
See lsms_library/countries/Malawi/_/malawi.py.
"""
import os
import sys
import tempfile

sys.path.append('../../_/')
import pandas as pd
import pyreadstat

from lsms_library.local_tools import (get_dataframe, to_parquet, format_id,
                                       DVCFS, _ensure_dvc_pulled,
                                       _dvc_working_directory, _COUNTRIES_DIR)
from malawi import (_fertilizer_block, _organic_block, _pesticide_block,
                    _seed_block, _seed_purchase_block, assemble_plot_inputs)


WAVE = '2016-17'

# Module D columns needed for the input blocks (avoid the latin-1 free-text
# column by restricting the read).
_D_USECOLS = ['case_id', 'y3_hhid', 'gardenid', 'plotid',
              'ag_d36', 'ag_d38', 'ag_d39a', 'ag_d39d', 'ag_d39e',
              'ag_d39g', 'ag_d39j', 'ag_d40',
              'ag_d41a', 'ag_d41b', 'ag_d41c',
              'ag_d41d', 'ag_d41e', 'ag_d41f']


def _read_usecols(countries_rel, usecols):
    """Read only ``usecols`` from a DVC-tracked .dta that a full read
    cannot decode (latin-1 bad byte in an unused column)."""
    _ensure_dvc_pulled(countries_rel)
    with _dvc_working_directory(_COUNTRIES_DIR):
        with DVCFS.open(countries_rel) as f:
            data = f.read()
    with tempfile.NamedTemporaryFile(suffix='.dta', delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    try:
        df, _ = pyreadstat.read_dta(tmp_path, usecols=usecols,
                                    apply_value_formats=False)
        return df
    finally:
        os.unlink(tmp_path)


def _plotkey(df):
    return (df['gardenid'].apply(format_id) + '_'
            + df['plotid'].apply(format_id))


def _fert_args(d):
    return dict(type1='ag_d39a', qty1='ag_d39d', unit1='ag_d39e',
                type2='ag_d39g', qty2='ag_d39j', unit2=None)


def _half(d, g, h, hh_of, seed_qty, seed_unit, seed_value):
    for df in (d, g, h):
        df['hhid'] = hh_of(df)
    d['plotkey'] = _plotkey(d)
    g['plotkey'] = _plotkey(g)
    pieces = [
        _fertilizer_block(d, hhid='hhid', plotkey='plotkey', t=WAVE,
                          **_fert_args(d)),
        _organic_block(d, hhid='hhid', plotkey='plotkey', flag='ag_d36',
                       t=WAVE),
        _pesticide_block(d, hhid='hhid', plotkey='plotkey', gate='ag_d40',
                         slots=[('ag_d41a', 'ag_d41b', 'ag_d41c'),
                                ('ag_d41d', 'ag_d41e', 'ag_d41f')], t=WAVE),
        _seed_block(g, hhid='hhid', plotkey='plotkey', cropcode='crop_code',
                    qty='ag_g04a', unit='ag_g04b', improved='ag_g0f', t=WAVE),
    ]
    # Seed purchase column naming differs by half: the Cross_Sectional file
    # keeps the original IHS4 names (ag_h07*/ag_h09 source 1, ag_h38*/ag_h40
    # source 2); the Panel file uses the harmonized ag_h16*/ag_h19,
    # ag_h26*/ag_h29 that the WB code reads.
    seed_purchase = [
        _seed_purchase_block(h, hhid='hhid', cropcode='crop_code',
                             qty_cols=seed_qty, unit_cols=seed_unit,
                             value_cols=seed_value),
    ]
    return pieces, seed_purchase


all_pieces, all_seed_purchase = [], []

# --- Cross-sectional half (cs-17 prefix) ---
d_xs = _read_usecols('Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta',
                     _D_USECOLS)
g_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_g.dta',
                     convert_categoricals=False)
h_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_h.dta',
                     convert_categoricals=False)
p, s = _half(d_xs, g_xs, h_xs,
             lambda df: 'cs-17-' + df['case_id'].apply(format_id),
             seed_qty=['ag_h07a', 'ag_h38a'],
             seed_unit=['ag_h07b', 'ag_h38b'],
             seed_value=['ag_h09', 'ag_h40'])
all_pieces += p
all_seed_purchase += s

# --- Panel half (bare y3_hhid) ---
d_pn = get_dataframe('../Data/Panel/ag_mod_d_16.dta', convert_categoricals=False)
g_pn = get_dataframe('../Data/Panel/ag_mod_g_16.dta', convert_categoricals=False)
h_pn = get_dataframe('../Data/Panel/ag_mod_h_16.dta', convert_categoricals=False)
p, s = _half(d_pn, g_pn, h_pn, lambda df: df['y3_hhid'].apply(format_id),
             seed_qty=['ag_h16a', 'ag_h26a'],
             seed_unit=['ag_h16b', 'ag_h26b'],
             seed_value=['ag_h19', 'ag_h29'])
all_pieces += p
all_seed_purchase += s

df = assemble_plot_inputs(WAVE, all_pieces, all_seed_purchase)

assert df.index.is_unique, f"Non-unique (t,i,plot,input,crop,u) in plot_inputs {WAVE}"
assert len(df) > 0, f"plot_inputs {WAVE} produced no rows"

to_parquet(df, 'plot_inputs.parquet')

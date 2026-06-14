"""Build plot_labor for Malawi IHS4 / IHPS 2016-17 (GAP 3, plot grain).

Item-level (t, i, plot, source) plot-labor feature.  IHS4 ships a
Cross_Sectional half (cs-17-prefixed case_id) and a Panel half (bare
y3_hhid), concatenated into the single 2016-17 wave -- exactly like
plot_inputs / crop_production.  Plot key is gardenid_plotid in both halves.

Module D plot-labor layout is the IHS4/IHS5 generation:
  * family labor = ag_d4{2,3,4}b{n} * ag_d4{2,3,4}c{n} day*#people products
    across occurrence suffixes n;
  * hired labor  = ag_d47a1/a2/a3 + ag_d48a1/a2/a3 days, wages ag_d47b1/b2/b3
    + ag_d48b1/b2/b3 (the '1/2/3' hired suffix);
  * other labor  = ag_d52a/b/c + ag_d54a/b/c -- PRESENT in the Panel half,
    but the Cross_Sectional Module D drops those columns, so the CS half
    emits only family + hired rows (reported, not faked).

The Cross_Sectional ag_mod_d.dta has a latin-1 bad byte in an unused
free-text column; we read only the labor columns via _read_usecols (same
recipe as plot_inputs 2016-17).  This is the SAME Module D block the WB
code (MWI_IHPS3.do:737-) reads then collapses to per-plot totals.  See
lsms_library/countries/Malawi/_/malawi.py:_plot_labor_block.
"""
import os
import sys
import tempfile

sys.path.append('../../_/')
import pyreadstat

from lsms_library.local_tools import (get_dataframe, to_parquet, format_id,
                                       DVCFS, _ensure_dvc_pulled,
                                       _dvc_working_directory, _COUNTRIES_DIR)
from malawi import _plot_labor_block, assemble_plot_labor


WAVE = '2016-17'

# Module D labor columns (avoid the latin-1 free-text column by restricting
# the read on the Cross_Sectional half).
_D_LABOR_COLS = ['case_id', 'y3_hhid', 'gardenid', 'plotid']
for _blk in ('ag_d42', 'ag_d43', 'ag_d44'):
    for _n in range(1, 14):
        _D_LABOR_COLS += [f'{_blk}b{_n}', f'{_blk}c{_n}']
for _n in (1, 2, 3):
    _D_LABOR_COLS += [f'ag_d47a{_n}', f'ag_d47b{_n}',
                      f'ag_d48a{_n}', f'ag_d48b{_n}']
# "other" labor columns (present only in the Panel half).
_D_LABOR_COLS += ['ag_d52a', 'ag_d52b', 'ag_d52c',
                  'ag_d54a', 'ag_d54b', 'ag_d54c']


def _read_usecols(countries_rel, usecols):
    """Read only ``usecols`` from a DVC-tracked .dta that a full read cannot
    decode (latin-1 bad byte in an unused column).  Absent columns are
    silently skipped by pyreadstat."""
    _ensure_dvc_pulled(countries_rel)
    with _dvc_working_directory(_COUNTRIES_DIR):
        with DVCFS.open(countries_rel) as f:
            data = f.read()
    with tempfile.NamedTemporaryFile(suffix='.dta', delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    try:
        # Restrict to columns actually present (usecols errors on absent).
        df0, meta = pyreadstat.read_dta(tmp_path, metadataonly=True)
        present = [c for c in usecols if c in meta.column_names]
        df, _ = pyreadstat.read_dta(tmp_path, usecols=present,
                                    apply_value_formats=False)
        return df
    finally:
        os.unlink(tmp_path)


def _plotkey(df):
    return (df['gardenid'].apply(format_id) + '_'
            + df['plotid'].apply(format_id))


pieces = []

# --- Cross_Sectional half (cs-17 prefix; no "other" labor columns) ---
d_xs = _read_usecols('Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta',
                     _D_LABOR_COLS)
d_xs['hhid'] = 'cs-17-' + d_xs['case_id'].apply(format_id)
d_xs['plotkey'] = _plotkey(d_xs)
pieces.append(_plot_labor_block(d_xs, hhid='hhid', plotkey='plotkey', t=WAVE,
                                hired_suffix='1/2/3', include_other=False))

# --- Panel half (bare y3_hhid; has "other" labor) ---
d_pn = get_dataframe('../Data/Panel/ag_mod_d_16.dta', convert_categoricals=False)
d_pn['hhid'] = d_pn['y3_hhid'].apply(format_id)
d_pn['plotkey'] = _plotkey(d_pn)
pieces.append(_plot_labor_block(d_pn, hhid='hhid', plotkey='plotkey', t=WAVE,
                                hired_suffix='1/2/3', include_other=True))

df = assemble_plot_labor(WAVE, pieces)

assert df.index.is_unique, f"Non-unique (t,i,plot,source) in plot_labor {WAVE}"
assert len(df) > 0, f"plot_labor {WAVE} produced no rows"

to_parquet(df, 'plot_labor.parquet')

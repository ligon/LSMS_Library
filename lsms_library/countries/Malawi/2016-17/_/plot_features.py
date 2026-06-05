"""Build plot_features for Malawi IHS4 2016-17 (GH #167).

IHS4 ships a Cross_Sectional half (12,447 HH, keyed on bare case_id) and
a Panel half (2,508 HH, keyed on dashed y3_hhid).  We build both and
concat into the single 2016-17 wave t, exactly like the roster.

Two wave-specific wrinkles (validated in the recon refuter pass,
slurm_logs/2026-06-03_session/RECON_Malawi.md):

1. ID: sample().i is 'cs-17-'-prefixed for the cross-sectional half (the
   cs_i mapping in mapping.py / data_info.yml).  We apply the same prefix
   here so XS plots are not orphaned ~100% against sample().i.  The Panel
   half uses bare y3_hhid which id_walk chains to the canonical id.

2. Tenure: ag_d03 (acquire) is ABSENT from IHS4 ag_mod_d; ag_d02 there is
   "ID of Respondent", NOT tenure.  So Tenure / TenureSystem are NaN for
   this wave (no acquire column passed to the helper).

3. Latin-1: Cross_Sectional/ag_mod_d.dta has a bad byte in an unused
   free-text column that makes a full read raise ReadstatError.  A
   column-restricted read (usecols=...) succeeds (15,724 rows), so we
   read only the columns we need for that one file.

Plot key in IHS4 is (gardenid, plotid) -> 'gardenid_plotid'.
"""
import os
import sys
import tempfile

sys.path.append('../../_/')
import pyreadstat

from lsms_library.local_tools import (get_dataframe, to_parquet, format_id,
                                       DVCFS, _ensure_dvc_pulled,
                                       _dvc_working_directory, _COUNTRIES_DIR)
from malawi import plot_features_for_wave


WAVE = '2016-17'


def _read_usecols(countries_rel, usecols):
    """Read only ``usecols`` from a DVC-tracked .dta that a full read
    cannot decode (latin-1 bad byte in an unused column).  Goes through
    the DVC cache like get_dataframe, but restricts the column set so
    pyreadstat never touches the offending free-text column."""
    _ensure_dvc_pulled(countries_rel)
    # DVCFS.open resolves ``countries_rel`` against os.getcwd(); under
    # ``make`` the cwd is the wave ``_/`` dir, which would double the
    # path (Malawi/2016-17/_/Malawi/2016-17/Data/...).  Pin the cwd to
    # the countries dir so the relative path resolves regardless of cwd.
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


colmap = dict(
    area_est     = 'ag_c04a',
    area_unit    = 'ag_c04b',
    area_gps     = 'ag_c04c',
    soil_type    = 'ag_d21',
    water_source = 'ag_d28a',
    # acquire omitted: ag_d03 absent in IHS4 -> Tenure NaN.
)

pieces = []

# --- Cross-sectional half (cs-17 prefix) ---
c_xs = get_dataframe('../Data/Cross_Sectional/ag_mod_c.dta',
                     convert_categoricals=False)
d_xs = _read_usecols(
    'Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta',
    usecols=['case_id', 'hhid', 'gardenid', 'plotid',
             'ag_d02', 'ag_d21', 'ag_d28a'])

c_xs['hhid'] = 'cs-17-' + c_xs['case_id'].apply(format_id)
c_xs['plotkey'] = _plotkey(c_xs)
d_xs['hhid'] = 'cs-17-' + d_xs['case_id'].apply(format_id)
d_xs['plotkey'] = _plotkey(d_xs)
pieces.append(plot_features_for_wave(WAVE, c_xs, d_xs, colmap))

# --- Panel half (bare y3_hhid) ---
c_pn = get_dataframe('../Data/Panel/ag_mod_c_16.dta',
                     convert_categoricals=False)
d_pn = get_dataframe('../Data/Panel/ag_mod_d_16.dta',
                     convert_categoricals=False)

c_pn['hhid'] = c_pn['y3_hhid'].apply(format_id)
c_pn['plotkey'] = _plotkey(c_pn)
d_pn['hhid'] = d_pn['y3_hhid'].apply(format_id)
d_pn['plotkey'] = _plotkey(d_pn)
pieces.append(plot_features_for_wave(WAVE, c_pn, d_pn, colmap))

import pandas as pd
df = pd.concat(pieces)

assert df.index.is_unique, f"Non-unique (t,i,plot_id) in plot_features {WAVE}"
assert len(df) > 0, f"plot_features {WAVE} produced no rows"

to_parquet(df, 'plot_features.parquet')

#!/usr/bin/env python
"""Build plot_labor for Ethiopia ESS 2018-19 (Wave 4; GAP 3.2).

Plot labor person-DAYS at (t, i, plot_id, source).  W4 RENUMBERED the §3-PP
labor blocks vs W1-W3 (and dropped the hh_/pp_ prefix): hired s3q30a..i,
family s3q29* (4 member slots), other s3q31a..f.  §10-PH labor: hired
s10q01a..i, family s10q02* (4 member slots), other s10q03a..f.  The W4 §10
file has NO crop_code, but our grain is (plot, source) so the helper sums
over crops to the plot regardless.
i = household_id (W4 is an entirely new sample; matches sample().i).
See ../../2011-12/_/plot_labor.py for the column-block layout.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_labor_for_wave, labor_family_triples


pp = get_dataframe('../Data/sect3_pp_w4.dta',  convert_categoricals=False)
ph = get_dataframe('../Data/sect10_ph_w4.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',
    pp_filter=('s3q03', (1, 2)),
    pp_h_n_m='s3q30a', pp_h_d_m='s3q30b', pp_h_w_m='s3q30c',
    pp_h_n_w='s3q30d', pp_h_d_w='s3q30e', pp_h_w_w='s3q30f',
    pp_h_n_c='s3q30g', pp_h_d_c='s3q30h', pp_h_w_c='s3q30i',
    pp_f_members=labor_family_triples('s3q29', 4, sep=''),
    pp_o_n_m='s3q31a', pp_o_d_m='s3q31b',
    pp_o_n_w='s3q31c', pp_o_d_w='s3q31d',
    pp_o_n_c='s3q31e', pp_o_d_c='s3q31f',
    ph_h_n_m='s10q01a', ph_h_d_m='s10q01b', ph_h_w_m='s10q01c',
    ph_h_n_w='s10q01d', ph_h_d_w='s10q01e', ph_h_w_w='s10q01f',
    ph_h_n_c='s10q01g', ph_h_d_c='s10q01h', ph_h_w_c='s10q01i',
    ph_f_members=labor_family_triples('s10q02', 4, sep=''),
    ph_o_n_m='s10q03a', ph_o_d_m='s10q03b',
    ph_o_n_w='s10q03c', ph_o_d_w='s10q03d',
    ph_o_n_c='s10q03e', ph_o_d_c='s10q03f',
)

df = plot_labor_for_wave('2018-19', pp, ph, colmap)

assert len(df) > 0, "plot_labor 2018-19 produced no rows"
to_parquet(df, 'plot_labor.parquet')

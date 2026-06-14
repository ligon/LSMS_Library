#!/usr/bin/env python
"""Build plot_labor for Ethiopia ESS 2013-14 (Wave 2; GAP 3.2).

Plot labor person-DAYS at (t, i, plot_id, source).  Same §3-PP / §10-PH
variable naming as W1 (pp_s3q27/28/29, ph_s10q01/02/03); the PP/PH family
blocks carry 7 / 8 member slots this wave (28 / 32 sub-columns).
i = household_id2 (matches sample().i / plot_features for W2).
See ../../2011-12/_/plot_labor.py for the column-block layout.
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_labor_for_wave, labor_family_triples


pp = get_dataframe('../Data/sect3_pp_w2.dta',  convert_categoricals=False)
ph = get_dataframe('../Data/sect10_ph_w2.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id2', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',
    pp_filter=('pp_s3q03', (1, 2)),
    pp_h_n_m='pp_s3q28_a', pp_h_d_m='pp_s3q28_b', pp_h_w_m='pp_s3q28_c',
    pp_h_n_w='pp_s3q28_d', pp_h_d_w='pp_s3q28_e', pp_h_w_w='pp_s3q28_f',
    pp_h_n_c='pp_s3q28_g', pp_h_d_c='pp_s3q28_h', pp_h_w_c='pp_s3q28_i',
    pp_f_members=labor_family_triples('pp_s3q27', 7),
    pp_o_n_m='pp_s3q29_a', pp_o_d_m='pp_s3q29_b',
    pp_o_n_w='pp_s3q29_c', pp_o_d_w='pp_s3q29_d',
    pp_o_n_c='pp_s3q29_e', pp_o_d_c='pp_s3q29_f',
    ph_h_n_m='ph_s10q01_a', ph_h_d_m='ph_s10q01_b', ph_h_w_m='ph_s10q01_c',
    ph_h_n_w='ph_s10q01_d', ph_h_d_w='ph_s10q01_e', ph_h_w_w='ph_s10q01_f',
    ph_h_n_c='ph_s10q01_g', ph_h_d_c='ph_s10q01_h', ph_h_w_c='ph_s10q01_i',
    ph_f_members=labor_family_triples('ph_s10q02', 8),
    ph_o_n_m='ph_s10q03_a', ph_o_d_m='ph_s10q03_b',
    ph_o_n_w='ph_s10q03_c', ph_o_d_w='ph_s10q03_d',
    ph_o_n_c='ph_s10q03_e', ph_o_d_c='ph_s10q03_f',
)

df = plot_labor_for_wave('2013-14', pp, ph, colmap)

assert len(df) > 0, "plot_labor 2013-14 produced no rows"
to_parquet(df, 'plot_labor.parquet')

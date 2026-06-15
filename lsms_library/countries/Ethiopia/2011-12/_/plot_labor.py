#!/usr/bin/env python
"""Build plot_labor for Ethiopia ESS 2011-12 (Wave 1; GAP 3.2).

Plot labor person-DAYS at (t, i, plot_id, source) with source in
{family, hired, other}, plus the reported cash wage paid to hired labor.
Reproduces the REPORTED item-level data the WB code reads
(ETH_ESS1.do:554-799) -- NOT the WB total_labor_days / total_*_labor_days
sums or the valuation_median_wages imputed wage.

W1 sources / vars:
  post-PLANTING §3 plot roster (sect3_pp_w1): hired pp_s3q28_a..i
    (a=#men b=days/man c=man-wage d=#women e=days/woman f=woman-wage
    g=#child h=days/child i=child-wage); family pp_s3q27_* (6 member slots,
    did/weeks/days at letter-stride 4); other pp_s3q29_a..f (a=#men
    b=days/man c=#women d=days/woman e=#child f=days/child).
    Restricted to cultivated plots (pp_s3q03 in {1,2}), as the WB does.
  post-HARVEST §10 labor (sect10_ph_w1): hired ph_s10q01_a..i; family
    ph_s10q02_* (8 member slots); other ph_s10q03_a..f.  This file is
    plot x crop; the helper sums its days over crops to the plot.
i = household_id (matches sample().i for W1).
"""
import sys

sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from ethiopia import plot_labor_for_wave, labor_family_triples


pp = get_dataframe('../Data/sect3_pp_w1.dta',  convert_categoricals=False)
ph = get_dataframe('../Data/sect10_ph_w1.dta', convert_categoricals=False)

colmap = dict(
    hhid='household_id', holder_id='holder_id',
    parcel_id='parcel_id', field_id='field_id',
    pp_filter=('pp_s3q03', (1, 2)),
    # PP hired
    pp_h_n_m='pp_s3q28_a', pp_h_d_m='pp_s3q28_b', pp_h_w_m='pp_s3q28_c',
    pp_h_n_w='pp_s3q28_d', pp_h_d_w='pp_s3q28_e', pp_h_w_w='pp_s3q28_f',
    pp_h_n_c='pp_s3q28_g', pp_h_d_c='pp_s3q28_h', pp_h_w_c='pp_s3q28_i',
    # PP family (6 member slots)
    pp_f_members=labor_family_triples('pp_s3q27', 6),
    # PP other
    pp_o_n_m='pp_s3q29_a', pp_o_d_m='pp_s3q29_b',
    pp_o_n_w='pp_s3q29_c', pp_o_d_w='pp_s3q29_d',
    pp_o_n_c='pp_s3q29_e', pp_o_d_c='pp_s3q29_f',
    # PH hired
    ph_h_n_m='ph_s10q01_a', ph_h_d_m='ph_s10q01_b', ph_h_w_m='ph_s10q01_c',
    ph_h_n_w='ph_s10q01_d', ph_h_d_w='ph_s10q01_e', ph_h_w_w='ph_s10q01_f',
    ph_h_n_c='ph_s10q01_g', ph_h_d_c='ph_s10q01_h', ph_h_w_c='ph_s10q01_i',
    # PH family (8 member slots)
    ph_f_members=labor_family_triples('ph_s10q02', 8),
    # PH other
    ph_o_n_m='ph_s10q03_a', ph_o_d_m='ph_s10q03_b',
    ph_o_n_w='ph_s10q03_c', ph_o_d_w='ph_s10q03_d',
    ph_o_n_c='ph_s10q03_e', ph_o_d_c='ph_s10q03_f',
)

df = plot_labor_for_wave('2011-12', pp, ph, colmap)

assert len(df) > 0, "plot_labor 2011-12 produced no rows"
to_parquet(df, 'plot_labor.parquet')

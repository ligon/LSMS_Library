#!/usr/bin/env python
"""Build item-level plot_labor for Nigeria GHS-Panel (GAP 3, grain 2).

Natural grain (t, i, plot, source): one row per labor SOURCE used on a
plot, source in {family, hired, other}.  Source: the post-harvest plot
labor roster secta2_harvest{wN} (W1-W3 a single wide file; W4/W5 split it
into secta2a_harvest{wN} family + secta2b_harvest{wN} hired/other).  This
is the construct NGA_GHS{1..5}.do reads then collapses to the household
totals total_labor_days / total_family_labor_days / total_hired_labor_days
/ hired_labor_value.  We keep the PRE-collapse per-(plot, source) rows.

Stores REPORTED item-level fields ONLY:
  PersonDays  reported person-days of that source on the plot
              (family = Sigma slots #workers*days; hired/other = Sigma
              man/woman/child #*days; mirrors the WB per-group products
              BEFORE the rowtotal/total collapse).
  Wage        cash paid to hired labor = Sigma man/woman/child of
              (reported daily wage * hired days).  NaN for family/other.
NO total_labor_days / total_family_labor_days / total_hired_labor_days /
hired_labor_value -- those are SUM / median-wage transformations over
these rows, NEVER stored here.

`source` (index) is a harmonize_labor_source Preferred Label; `v`
auto-joins from sample() at API time.  plot (= plotid, format_id) aligns
with crop_production / plot_inputs on (t, i, plot); t = PH_QUARTER[wave]
(post-harvest round, matching crop_production).

Per-wave source structure:

  W1 2010-11  secta2_harvestw1   wide sa2q* (family slots a-d, hired
              man/woman/child, other sa2q12a/b/c).
  W2 2012-13  secta2_harvestw2   wide sa2q* (same scheme as W1).
  W3 2015-16  secta2_harvestw3   wide sa2q* (same scheme as W1).
  W4 2018-19  secta2a_harvestw4  long family roster (sa2aq1b days);
              secta2b_harvestw4  plot-level hired/other (sa2bq* scheme).
  W5 2023-24  secta2a_harvestw5  long family roster (sa2aq* differs --
              sa2aq1/2/3, NOT sa2aq1b); secta2b_harvestw5 (sa2bq*_N
              indexed).  W5's secta2 variable scheme diverges sharply from
              W4 and from the W5 .do (which still references W4 names), so
              W5 plot_labor is left to a follow-up -- documented partial.

PP-round plot labor (sect11c1_planting{wN}, W2-W5) is NOT included: it is
a second round of the same source on the same plot; folding it onto the
same (plot, source) row would need a PP+PH SUM (the forbidden total).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import (PH_QUARTER, plot_labor_wide, plot_labor_split)

pieces = []

# ----------------------------- W1 2010-11 -----------------------------
t = PH_QUARTER['2010-11']
f = '../2010-11/Data/Post Harvest Wave 1/Agriculture/secta2_harvestw1.dta'
raw = get_dataframe(f, convert_categoricals=False)
pieces.append(plot_labor_wide(t, raw))

# ----------------------------- W2 2012-13 -----------------------------
t = PH_QUARTER['2012-13']
f = '../2012-13/Data/Post Harvest Wave 2/Agriculture/secta2_harvestw2.dta'
raw = get_dataframe(f, convert_categoricals=False)
pieces.append(plot_labor_wide(t, raw))

# ----------------------------- W3 2015-16 -----------------------------
t = PH_QUARTER['2015-16']
f = '../2015-16/Data/secta2_harvestw3.dta'
raw = get_dataframe(f, convert_categoricals=False)
pieces.append(plot_labor_wide(t, raw))

# ----------------------------- W4 2018-19 -----------------------------
t = PH_QUARTER['2018-19']
fam = get_dataframe('../2018-19/Data/secta2a_harvestw4.dta',
                    convert_categoricals=False)
hire = get_dataframe('../2018-19/Data/secta2b_harvestw4.dta',
                     convert_categoricals=False)
pieces.append(plot_labor_split(t, fam, hire))

# ----------------------------- W5 2023-24 -----------------------------
# secta2a_harvestw5 / secta2b_harvestw5 use a divergent variable scheme
# (sa2aq1/2/3, sa2bq*_N) that neither the W4 helper nor the W5 .do (which
# references W4-style names) can read faithfully.  Left as a documented
# partial; the four earlier waves are wired.

# ----------------------------- combine -------------------------------
df = pd.concat(pieces, axis=0)
df = df.sort_index()

to_parquet(df, '../var/plot_labor.parquet')

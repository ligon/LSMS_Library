#!/usr/bin/env python
import pandas as pd
import numpy as np
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             '..', '..', '_'))
# GH #1260: the IHPS panel-coordinate rule lives ONCE, in Malawi/_/malawi.py;
# `cluster_features` below is this wave's thin caller.  `ihps_baseline_status`
# (the `mapping:` this wave's data_info.yml names) is imported so the wave
# module says where it comes from; it would resolve through malawi.py anyway.
from malawi import ihps_baseline_status  # noqa: E402,F401
from malawi import ihps_panel_cluster_features as _ihps_panel_cluster_features  # noqa: E402

# --- food_security (8-item adult food-insecurity scale) -----------------
# HH_MOD_T_13 (IHPS 2013) carries an 8-item adult food-insecurity battery
# in hh_t13..hh_t20.  NOTE: this is NOT the standard FAO FIES wording/order
# used in IHS4/IHS5 (2016-17, 2019-20); the items differ in order and the
# "healthy diet" concept is captured by a lack-of-money item.  The mapping
# to the canonical FAO-FIES columns (best-fit by concept) is:
#   hh_t13 worried (would run out)        -> Worried
#   hh_t15 lacked money/resources for food-> HealthyDiet
#   hh_t16 diet based on only a few kinds -> FewFoods
#   hh_t17 did not eat breakfast/lunch/...-> SkippedMeal
#   hh_t18 ate less than you should       -> AteLess
#   hh_t14 ran out of food               -> RanOut
#   hh_t19 hungry but didn't eat          -> Hungry
#   hh_t20 ate only one meal / went a day -> WholeDay
# Value labels: {1: YES, 2: NO}.  Binarize yes->True, no->False; missing->NA.
# Recall period: last 12 months.

def _fies_yesno(x):
    """Map a single item (string label) to a nullable boolean."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().lower()
    if s in ('yes', '1', 'true', 'oui'):
        return True
    if s in ('no', '2', 'false', 'non'):
        return False
    return pd.NA

def fies_yesno(row):
    """Row-wise wrapper: data_info passes a single-column row (Series)."""
    return _fies_yesno(row.iloc[0])

def fies_score(row):
    """Count of True across the 8 items in a row; NA only if all 8 missing."""
    vals = [_fies_yesno(v) for v in row]
    if all(pd.isna(v) for v in vals):
        return pd.NA
    return int(sum(1 for v in vals if v is True))


# --- cluster_features: IHPS panel coordinates (GH #1260, 2013-14) ----------
# The rule (one implementation: malawi.ihps_panel_cluster_features, shared
# with 2016-17 and 2019-20), applied to IHPS 2013, the short-term panel
# (catalog 2248): 204 IHS3 EAs, 4,000 households, every one of them a panel
# household (y2_hhid, NNNN-NNN).  102 of the 204 EAs continue into the
# long-term panel (2016-17, 2019-20); the other 102 are short-term only.
#
# The households still at their baseline location (HH_MOD_A_FILT_13.dta
# `dist_to_IHS3location` <= 1 km: 2,660 households, at least 4 in each of the
# 204 EAs) share EXACTLY ONE coordinate per EA: the EA's displaced fix.  It
# equals the EA's 2010-11 coordinate in all 204 EAs, and the 2016-17 and
# 2019-20 stayers' fix in all 102 continuing EAs (within 2e-6 degrees).  It is
# NOT the coordinate an independent cross-section serves for the same EA: one
# of the 102 short-term EAs is drawn again by IHS4 in 2016-17 (cross-sectional
# households only), and IHS4's own displaced fix there is 17.5 km away.
#
# The hook keeps every coordinate that IS that fix (3,276 households: the
# 2,660 stayers and 616 movers who sit on it) and blanks the rest (724, the
# households whose geovariables carry `distY1Y2` -- the only ones the geo file
# relocated).  Before it, the cluster row served whichever household the grain
# collapse selected -- a mover's location in 24 of the 204 EAs (11 of the
# continuing 102), up to 608 km from the EA's fix.  Blanking is judged by
# coordinate, not by the distance alone: 70 households more than 20 km from
# baseline still carry their EA's fix.  1 km is the threshold
# tests/test_gh323_malawi_cluster_key.py uses on the same variable; every
# household here has a distance, so its `fillna(0)` changes nothing.
#
# Fails closed (see malawi.py).  2013-14 reads ONE cover file, so the status is
# a plain df_main myvar, and a misspelled distance variable raises KeyError at
# extraction, before the hook (no missing_ok); the hook's null-status guard
# covers the same mistake should the cover ever be read with missing_ok.

_STATUS_SOURCE = ("df_main's `_ihps_baseline_status` myvar "
                  "(HH_MOD_A_FILT_13.dta, the wave's one cover file)")


def cluster_features(df):
    """GH #1260 hook for 2013-14: keep each EA's stayers' fix, blank the rest.

    A thin caller of ``malawi.ihps_panel_cluster_features``.  The status
    column comes from df_main's ``_ihps_baseline_status`` myvar on the
    wave's single cover file (every 2013-14 household is a panel household).
    """
    return _ihps_panel_cluster_features(df, status_source=_STATUS_SOURCE)

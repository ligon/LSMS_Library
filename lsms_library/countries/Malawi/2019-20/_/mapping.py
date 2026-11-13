#!/usr/bin/env python
import pandas as pd
import numpy as np
from lsms_library.local_tools import format_id
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

def cs_i(value):
    return 'cs-19-'+format_id(value.iloc[0])

# --- FAO FIES (food_security) -------------------------------------------
# HH_MOD_T carries the FAO Food Insecurity Experience Scale 8-item battery
# in vars hh_t13..hh_t20 (FAO order).  Value labels: {NO, YES, DON'T KNOW,
# REFUSED}.  Binarize yes->True, no->False; DON'T KNOW / refused / NaN -> NA.
# Recall period: last 12 months.

def _fies_yesno(x):
    """Map a single FIES item (string label) to a nullable boolean."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().lower()
    if s in ('yes', '1', 'true', 'oui'):
        return True
    if s in ('no', '2', 'false', 'non'):
        return False
    # "DON'T KNOW", "refused", and anything unexpected -> missing
    return pd.NA

def fies_yesno(row):
    """Row-wise wrapper: data_info passes a single-column row (Series)."""
    return _fies_yesno(row.iloc[0])

def fies_score(row):
    """Count of True across the 8 FIES items in a row.

    NA only when every one of the 8 items is missing; otherwise NA items
    are treated as not-affirmed (count of True), matching the brief's
    'count of True across the 8 items' definition.
    """
    vals = [_fies_yesno(v) for v in row]
    if all(pd.isna(v) for v in vals):
        return pd.NA
    return int(sum(1 for v in vals if v is True))


# --- cluster_features: IHPS panel coordinates (GH #1260) -------------------
# The rule (one implementation: malawi.ihps_panel_cluster_features, shared
# with 2013-14 and the other long-term panel wave).  The IHPS household
# geovariables give each panel household its CURRENT, displaced location, and
# IHPS tracks movers -- so a panel EA's households can sit hundreds of km
# apart.  The households still at their baseline location
# (Panel/hh_mod_a_filt_19.dta `dist_to_IHS3location` <= 1 km: 2,136 households,
# at least one in each of the 102 panel EAs) share EXACTLY ONE coordinate per
# EA: the EA's displaced fix.  It is the same coordinate in every IHPS round
# (2013-14, 2016-17, 2019-20: within 1 m for all 102 EAs) and equals the EA's
# 2010-11 coordinate.  It is the IHPS fix only: an independent cross-section's
# households in the same EA carry that survey's own displaced fix (IHS4 / IHS5
# displace separately; CONTENTS.org, GH #1260).
#
# The hook keeps every panel coordinate that IS that fix (2,165 households,
# movers who sit on it included) and blanks the rest.  Before it, the cluster
# row served whichever panel household the grain collapse selected -- a
# mover's location in 16 EAs, up to 608 km from the EA's fix.  1 km is the
# threshold the 2013-14 measurement uses on the same variable
# (tests/test_gh323_malawi_cluster_key.py, Malawi/_/CONTENTS.org "the GPS is
# the proof"); any threshold that picks only households on the fix gives the
# same result.  Coordinates are blanked, never rows dropped: Region / District
# / Rural of every household still reach the cluster.
#
# Fails closed (see malawi.py).  Here the status is a file override on the
# panel cover only, and two cover files make get_data pass missing_ok, so a
# misspelled distance variable arrives as an all-NaN status and the hook
# raises on it.

_STATUS_SOURCE = ("df_main's Panel/hh_mod_a_filt_19.dta file override "
                  "(panel rows only; cross-sectional rows carry NaN)")


def cluster_features(df):
    """GH #1260 hook for 2019-20: keep each panel EA's stayers' fix, blank the rest.

    A thin caller of ``malawi.ihps_panel_cluster_features``.  The status
    column comes from the panel cover's file override in df_main
    (``Panel/hh_mod_a_filt_19.dta``); cross-sectional rows carry NaN.
    """
    return _ihps_panel_cluster_features(df, status_source=_STATUS_SOURCE)

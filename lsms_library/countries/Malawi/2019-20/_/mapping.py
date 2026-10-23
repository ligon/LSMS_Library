#!/usr/bin/env python
import pandas as pd
import numpy as np
from lsms_library.local_tools import format_id

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
# The IHPS household geovariables give each panel household its CURRENT,
# displaced location, and IHPS tracks movers -- so a panel EA's households can
# sit hundreds of km apart.  The households still at their baseline location
# (Panel/hh_mod_a_filt_19.dta `dist_to_IHS3location` <= 1 km: 2,136 households, at
# least one in each of the 102 panel EAs) share EXACTLY ONE coordinate per EA:
# the EA's displaced fix.  That is the only panel coordinate that locates the
# EA, so it is the only one this table keeps.  Before this hook the cluster row
# served whichever panel household the grain collapse selected, and in 16 EAs
# that was a mover's location (up to 608 km from the EA's fix).
#
# 1 km is the threshold the 2013-14 measurement uses on the same variable
# (tests/test_gh323_malawi_cluster_key.py, Malawi/_/CONTENTS.org "the GPS is
# the proof").  Coordinates are blanked, never rows dropped: the household's
# Region / District / Rural still reach the cluster, and a stayer's row --
# now the most complete -- is the one the collapse selects.

IHPS_STAYER_KM = 1.0
_STATUS = '_ihps_baseline_status'


def ihps_baseline_status(row):
    """'stayer' / 'moved' / 'unrecorded' from `dist_to_IHS3location` (km).

    A STRING on purpose: a numeric sentinel for "unrecorded" (+inf) is
    rewritten to NaN by grab_data's >1e99 missing-value rule, which made an
    unrecorded panel household look cross-sectional.  Cross-sectional rows
    never reach this (the column is a panel-file override) and carry NaN.
    """
    x = row.iloc[0]
    if pd.isna(x):
        return 'unrecorded'
    return 'stayer' if float(x) <= IHPS_STAYER_KM else 'moved'


def cluster_features(df):
    """Keep only panel coordinates that locate the EA itself (GH #1260).

    1. A panel household that is not a 'stayer' (more than
       ``IHPS_STAYER_KM`` from its baseline EA, or no recorded distance) has
       its Latitude/Longitude blanked.
    2. In an EA the cross-section also covers, the panel coordinates are
       blanked too, so the cluster keeps the cross-section's single EA fix.
       IHS4/IHS5 and IHPS displace independently, so the two fixes of one EA
       differ (2016-17 EA 30305580: 4.1 km); keeping the cross-section's is
       what this table served before the panel file was held.
    """
    if _STATUS not in df.columns:
        raise RuntimeError(
            f"Malawi cluster_features: the `{_STATUS}` column the panel cover "
            f"override supplies is missing, so movers cannot be told from "
            f"stayers (GH #1260).  Check df_main's Panel file override in "
            f"data_info.yml.")
    status = df.pop(_STATUS)
    coords = [c for c in ('Latitude', 'Longitude') if c in df.columns]
    if not coords:
        return df
    panel = status.notna().to_numpy()
    stayer = status.eq('stayer').fillna(False).to_numpy(dtype=bool)
    moved = panel & ~stayer
    df.loc[moved, coords] = np.nan
    v = df.index.get_level_values('v')
    cs_fixed = v[~panel & df['Latitude'].notna().to_numpy()].unique()
    df.loc[panel & v.isin(cs_fixed), coords] = np.nan
    return df

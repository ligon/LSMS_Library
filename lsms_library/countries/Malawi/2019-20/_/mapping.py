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
# the EA's displaced fix.  It is the same coordinate in every IHPS round
# (2013-14, 2016-17, 2019-20: within 1 m for all 102 EAs) and equals the EA's
# 2010-11 coordinate, so it is THE coordinate of the EA across rounds.
#
# The hook keeps every panel coordinate that IS that fix (2,165 households,
# movers who sit on it included) and blanks the rest.  Before it, the cluster
# row served whichever panel household the grain collapse selected -- a
# mover's location in 16 EAs, up to 608 km from the EA's fix.  Blanking is
# judged by coordinate, not by the distance alone: the distance only finds the
# fix.  1 km is the threshold the 2013-14 measurement uses on the same variable
# (tests/test_gh323_malawi_cluster_key.py, Malawi/_/CONTENTS.org "the GPS is
# the proof"); any threshold that picks only households on the fix gives the
# same result.  Coordinates are blanked, never rows dropped: Region / District
# / Rural of every household still reach the cluster.
#
# FAILS CLOSED.  A panel household (id NNNN-NNN, the y{3,4}_hhid shape) whose
# status is null means the distance variable did not arrive -- e.g. renamed
# upstream, which missing_ok turns into an all-NaN column -- and serving every
# panel coordinate, movers included, would be the silent result.  So it
# raises, as does a status on a non-panel id and stayers that disagree.

IHPS_STAYER_KM = 1.0
_STATUS = '_ihps_baseline_status'
_PANEL_ID = r'\d{4}-\d{3}'


def ihps_baseline_status(row):
    """'stayer' / 'moved' / 'unrecorded' from `dist_to_IHS3location` (km).

    A STRING on purpose: a numeric sentinel for "unrecorded" (+inf) is
    rewritten to NaN by grab_data's >1e99 missing-value rule.  Only panel
    rows carry a status (the column is a panel-file override); a null status
    is therefore never "unrecorded" -- it is a missing column, and the hook
    refuses it.
    """
    x = row.iloc[0]
    if pd.isna(x):
        return 'unrecorded'
    return 'stayer' if float(x) <= IHPS_STAYER_KM else 'moved'


def cluster_features(df):
    """Keep only panel coordinates that are their EA's fix (GH #1260).

    Per panel EA, the stayers' single coordinate is the EA fix; a panel
    household's coordinate is kept iff it equals that fix.  Stayers' rows are
    moved first, so the collapse takes a stayer's Region / District / Rural.
    Cross-sectional rows keep their values.  Raises ``RuntimeError`` if a panel household has no
    status, a non-panel household has one, or an EA's stayers disagree.
    """
    if _STATUS not in df.columns:
        raise RuntimeError(
            f"Malawi cluster_features: the `{_STATUS}` column the panel cover "
            f"override supplies is missing, so movers cannot be told from "
            f"stayers (GH #1260).  Check df_main's Panel file override in "
            f"data_info.yml.")
    status = df.pop(_STATUS)
    ids = pd.Series(df.index.get_level_values('i'), dtype='string')
    panel = ids.str.fullmatch(_PANEL_ID).fillna(False).to_numpy(dtype=bool)
    known = status.notna().to_numpy()
    if (panel & ~known).any():
        raise RuntimeError(
            f"Malawi cluster_features: {int((panel & ~known).sum())} of "
            f"{int(panel.sum())} panel households have NO baseline status -- "
            f"the panel cover's dist_to_IHS3location did not arrive (renamed or "
            f"misspelled?), and without it movers' locations would be served "
            f"as their EA's (GH #1260).")
    if (~panel & known).any():
        raise RuntimeError(
            f"Malawi cluster_features: {int((~panel & known).sum())} "
            f"households carry a baseline status but not a panel id "
            f"(NNNN-NNN) -- the panel id shape changed (GH #1260).")
    if not {'Latitude', 'Longitude'} <= set(df.columns):
        return df
    coords = ['Latitude', 'Longitude']
    xy = pd.DataFrame({
        'v': df.index.get_level_values('v').astype(str),
        'lat': df['Latitude'].to_numpy(dtype=float),
        'lon': df['Longitude'].to_numpy(dtype=float)})
    stayer = (panel & status.eq('stayer').fillna(False).to_numpy(dtype=bool)
              & xy['lat'].notna().to_numpy() & xy['lon'].notna().to_numpy())
    fix = xy[stayer].drop_duplicates()
    if fix['v'].duplicated().any():
        raise RuntimeError(
            f"Malawi cluster_features: the stayers of "
            f"{int(fix['v'].duplicated().sum())} panel EA(s) disagree on the "
            f"EA coordinate, so there is no single EA fix to keep (GH #1260).")
    fix = fix.set_index('v')
    at_fix = ((xy['lat'].to_numpy() == fix['lat'].reindex(xy['v']).to_numpy())
              & (xy['lon'].to_numpy() == fix['lon'].reindex(xy['v']).to_numpy()))
    df.loc[panel & ~at_fix, coords] = np.nan
    # A mover sitting on the fix keeps its coordinate, so its row is as
    # complete as a stayer's -- but its Region / District / Rural describe
    # where it lives now.  The grain collapse keeps the most complete row and
    # breaks ties by ORDER (country.most_complete_row), so put each panel
    # EA's stayers first: the cluster then takes a stayer's attributes.
    is_stayer = panel & status.eq('stayer').fillna(False).to_numpy(dtype=bool)
    return df.iloc[np.argsort(~is_stayer, kind='stable')]

#!/usr/bin/env python
import pandas as pd
import numpy as np

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
# The same rule as 2016-17 and 2019-20 (see those waves' mapping.py), extended
# to IHPS 2013, the short-term panel (catalog 2248): 204 IHS3 EAs, 4,000
# households, every one of them a panel household (y2_hhid, NNNN-NNN).  102 of
# the 204 EAs continue into the long-term panel (2016-17, 2019-20); the other
# 102 are short-term only.
#
# IHPS gives each household its CURRENT, displaced location and tracks movers,
# so an EA's households can sit hundreds of km apart.  The households still at
# their baseline location (HH_MOD_A_FILT_13.dta `dist_to_IHS3location` <= 1 km:
# 2,660 households, at least 4 in each of the 204 EAs) share EXACTLY ONE
# coordinate per EA: the EA's displaced fix.  It equals the EA's 2010-11
# coordinate in all 204 EAs, and the 2016-17 and 2019-20 stayers' fix in all
# 102 continuing EAs (within 2e-6 degrees), so it is THE coordinate of the EA
# across rounds.
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
# FAILS CLOSED, as in the later waves: a null status, a status off the panel id
# shape, or stayers that disagree raise.  2013-14 reads one cover file, so a
# misspelled distance variable raises KeyError at extraction instead (no
# missing_ok); the null-status guard covers the same mistake should the cover
# ever be read with missing_ok.

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

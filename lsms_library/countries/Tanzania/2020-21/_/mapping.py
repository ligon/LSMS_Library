"""Formatting functions for Tanzania 2020-21."""
import sys
from pathlib import Path
import pandas as pd
from lsms_library.local_tools import format_id

# `mapping.py` is loaded by importlib from an absolute path, so a RELATIVE
# sys.path entry does not find the country module.  Resolve it from __file__,
# the same way Uganda/Ethiopia reach their `_age_helpers`.
_COUNTRY = str(Path(__file__).resolve().parent.parent.parent / '_')
if _COUNTRY not in sys.path:
    sys.path.insert(0, _COUNTRY)
import tanzania

_RESIDENT_STATUS = 'HOUSEHOLD FOUND - ORIGINAL LOCATION'
_BOOSTER = 'BOOSTER SAMPLE (NEW HOUSEHOLD)'


def v(x):
    """Ensure cluster ID is a clean string (strip .0 from float)."""
    return format_id(x)


def cluster_features(df):
    """Restrict the cluster grain to households resident in the cluster (GH #323).

    NPS Y5 states residency outright.  `hh_tracking_status` distinguishes a
    household FOUND AT ITS ORIGINAL LOCATION from one found at a NEW one, and
    `tracking_class` marks the BOOSTER households, which were drawn fresh in
    2020-21 and are therefore in their cluster by construction.  Everything else
    -- households tracked to a new location, and the 1,269 split-off rows -- is
    carrying its parent's `y5_cluster` while living somewhere else.

    Contested cluster-attribute cells: 557 of 1,254 before, 51 of 1,455 after,
    and the cluster count RISES 418 -> 485 because the booster clusters, whose
    `clusterid` was blank, stop being deleted on a NaN key.  The 51 that remain
    are within-region: 7 clusters disagree on Region, 31 on District and 13 on
    Rural, which is what a 2012-2020 district split and an EA straddling a town
    boundary look like.  Reported, not resolved.
    """
    status = df['_status'].astype(str).str.upper().str.strip()
    cls = df['_class'].astype(str).str.upper().str.strip()
    resident = (status == _RESIDENT_STATUS) | (cls == _BOOSTER)
    out = df.drop(columns=['_status', '_class'])
    # This wave alone writes its place names in mixed case ('arusha', 'meru',
    # 'ARUSHA URBAN' all occur); 2008-15 and 2019-20 are uppercase.  Normalise
    # so a cluster's Region is the same string across waves and two spellings of
    # one district stop counting as a disagreement.
    for col in ('Region', 'District'):
        out[col] = out[col].map(
            lambda x: pd.NA if pd.isna(x) else str(x).strip().upper())
    if not bool(resident.any()):           # pragma: no cover - defensive
        return out
    return tanzania.keep_cluster_residents(out, region='Region', scheme='y5',
                                           extra_mask=resident.to_numpy(),
                                           label='2020-21')

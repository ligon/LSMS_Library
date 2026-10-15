"""Formatting functions for Tanzania 2019-20."""
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


def v(x):
    """Ensure cluster ID is a clean string (strip .0 from float)."""
    return format_id(x)


def cluster_features(df):
    """Restrict the cluster grain to households resident in the cluster (GH #323).

    Two exclusions, in this order:

    1. SPLIT-OFF households -- `sdd_cluster` blank.  The SDD file stamps them
       with a `clusterid` they were never sampled in (see the YAML comment);
       nothing about them is evidence about that cluster.  326 of 1,184 rows.
    2. Households whose `t0_region` is not the region carried by their own
       cluster's geocode -- they were tracked out of the cluster between t0 and
       the SDD round.

    Contested cluster-attribute cells: 257 of 741 before, 126 of 441 after
    (247 -> 147 clusters; the 100 clusters that disappear are exactly the ones
    that existed only because split-off rows were pooled under a stray code).
    The residual is genuine: 14 SDD clusters hold frame households reporting
    different t0 regions, and 54 different t0 districts.  That is a property of
    the shipped file, not something this hook can resolve -- it is left to the
    framework's grain audit to report rather than papered over.
    """
    # Stata writes an unpopulated STRING variable as '', not as missing, so
    # `.notna()` alone keeps every split-off row.  (This exact trap cost a
    # measurement round: 63 rows dropped instead of 389.)
    frame = df['_frame'].map(
        lambda x: not (pd.isna(x) or str(x).strip() in ('', 'nan', '<NA>')))
    out = df[frame.to_numpy()].drop(columns='_frame')
    if len(out) == 0:                      # pragma: no cover - defensive
        return df.drop(columns=['_frame', '_status'])
    # Of the frame households, only those found AT THEIR ORIGINAL LOCATION are
    # still in the cluster; `HOUSEHOLD FOUND - NEW LOCATION` means the survey
    # followed them somewhere else.  This is the wave's own statement of
    # residency, so it takes precedence over the geocode test -- which then
    # still runs, and catches the frame households whose recorded t0 region is
    # not the region in their own cluster's code.
    resident = (out['_status'].astype(str).str.upper().str.strip()
                == _RESIDENT_STATUS)
    out = out.drop(columns='_status')
    return tanzania.keep_cluster_residents(out, region='Region', scheme='sdd',
                                           extra_mask=resident.to_numpy(),
                                           label='2019-20')

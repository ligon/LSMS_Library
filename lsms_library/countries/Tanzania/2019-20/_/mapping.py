"""Formatting functions for Tanzania 2019-20."""
import sys
from pathlib import Path
import pandas as pd
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

# `mapping.py` is loaded by importlib from an absolute path, so a RELATIVE
# sys.path entry does not find the country module.  Resolve it from __file__,
# the same way Uganda/Ethiopia reach their `_age_helpers`.
_COUNTRY = str(Path(__file__).resolve().parent.parent.parent / '_')
if _COUNTRY not in sys.path:
    sys.path.insert(0, _COUNTRY)
import tanzania

_RESIDENT_STATUS = 'HOUSEHOLD FOUND - ORIGINAL LOCATION'
SPLIT_OFF_PARENT_DERIVATION = 'Tanzania::sample::split-off-parent-cluster'


def v(x):
    """Ensure cluster ID is a clean string (strip .0 from float)."""
    return format_id(x)


def _has_sdd_cluster(series):
    """True for a FRAME household.  Stata writes an unpopulated STRING variable
    as '', not as missing, so `.notna()` alone would call every split-off frame."""
    return series.map(lambda x: not (pd.isna(x) or str(x).strip() in ('', 'nan', '<NA>')))


def derive_split_off_parent_cluster(df):
    """Give a split-off with a stray `clusterid` its parent's cluster (GH #1441).

    A split-off (`sdd_cluster` blank) carries a `clusterid` that is not a sampling
    cluster.  Where that id is in no frame cluster and a frame household has the
    same `y4_hhid`, `v` becomes that household's `clusterid`, which is what a
    tracked household carries, and the row gets the registry key.  Everything else
    keeps its `v`: a frame household, a split-off whose id is a frame cluster, and
    a split-off with no frame household on its `y4_hhid`.  `strata`, `Rural` and the
    weights stay this household's own.

    Args:
        df: the `sample` frame, with `v`, `_frame` (`sdd_cluster`) and `_y4_hhid`.

    Returns:
        A copy without the two helper columns, with `v` rekeyed and `Derivation`
        added.

    Raises:
        ValueError: if the frame households on a split-off's `y4_hhid` span more
            than one cluster, so that there is no one parent cluster to take.
    """
    in_frame = _has_sdd_cluster(df['_frame']).to_numpy(dtype=bool)
    frame = df[in_frame]
    stray = ~in_frame & ~df['v'].isin(set(frame['v'])).to_numpy()
    clusters = {y4: set(g) for y4, g in frame.groupby('_y4_hhid')['v']}
    parents = df['_y4_hhid'].map(clusters)
    rekey = stray & parents.notna().to_numpy()
    if (parents[rekey].map(len) > 1).any():
        raise ValueError('sample 2019-20: a split-off\'s y4_hhid has frame households '
                         'in more than one cluster (GH #1441)')
    out = df.drop(columns=['_frame', '_y4_hhid'])
    out.loc[rekey, 'v'] = parents[rekey].map(lambda s: next(iter(s)))
    out['Derivation'] = pd.Series(pd.NA, index=out.index, dtype='string')
    out.loc[rekey, 'Derivation'] = SPLIT_OFF_PARENT_DERIVATION
    return out


def inputs_split_off_parent_cluster():
    """The raw cover answers at `(t, i)`, every household: a parent's cluster is
    on a frame household's row, not on the split-off's."""
    raw = get_dataframe(countries_root() / 'Tanzania' / '2019-20' / 'Data' / 'HH_SEC_A.dta')
    out = raw[['sdd_hhid', 'y4_hhid', 'clusterid', 'sdd_cluster']].assign(
        t='2019-20', i=raw['sdd_hhid'])
    return out.set_index(['t', 'i'])


def sample(df):
    """`df_edit` hook for `sample`."""
    return derive_split_off_parent_cluster(df)


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
    # This exact trap cost a measurement round: 63 rows dropped instead of 389.
    frame = _has_sdd_cluster(df['_frame'])
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

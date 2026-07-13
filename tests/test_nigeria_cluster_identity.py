"""GH #323 -- Nigeria: the cluster key, the cartesian geo merge, and the
per-unit asset roster.

Every test here FAILS on `development`:

  * cluster_features collapsed a non-unique (t, v) index with groupby().first()
    because `v` was the bare `ea` serial, which is unique only WITHIN an LGA.
    W1's 500-EA design came back as 411 clusters, and 890/5000 households were
    handed another EA's District.
  * the same `dfs:` block merged two household-level frames ON `v`, a cartesian
    product within each EA (W2: 4,859 households -> 62,538 rows).
  * Latitude/Longitude were silently absent from 3 of the 4 waves (wrong column
    casing / no `ea` column in W4's geovariables -> GH #515 drops the sub-df).
  * assets kept item_seq unit #1 and discarded the rest: N147,297,485 of
    reported household asset value (25.6%) in each of W2's two quarters.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.country import Country

pytestmark = pytest.mark.slow

# The GHS-Panel W1 design: 500 EAs x 10 households.  Independently confirmed by
# the household geovariables, which carry exactly 500 distinct coordinates.
W1_CLUSTERS = 500

# Sum of the reported resale value (s5q4) over EVERY row of sect5b_plantingw2 --
# i.e. over every unit of every asset, not just unit #1.
W2_ASSET_VALUE = 576_299_043


@pytest.fixture(scope='module')
def nigeria():
    return Country('Nigeria')


@pytest.fixture(scope='module')
def cf(nigeria):
    return nigeria.cluster_features()


def test_w1_recovers_the_500_ea_design(cf):
    """`ea` alone conflates 500 real EAs into 411 codes."""
    n = cf.loc['2010Q3'].index.get_level_values('v').nunique()
    assert n == W1_CLUSTERS, (
        f'2010Q3 has {n} clusters, expected {W1_CLUSTERS}.  A count near 411 '
        f'means `v` is the bare `ea` serial again (GH #323).'
    )


def test_cluster_features_index_is_unique(cf):
    """The canonical (t, v) grain must not need a duplicate-collapse at all."""
    dups = int(cf.index.duplicated().sum())
    assert dups == 0, f'{dups} duplicate (t, v) tuples in cluster_features'


def test_no_silent_collapse_warning(nigeria):
    """Rebuilding must not trip the GH #323 groupby().first() warning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        nigeria.cluster_features()
    offenders = [
        str(w.message) for w in caught
        if 'GH #323' in str(w.message) and 'groupby().first()' in str(w.message)
    ]
    assert not offenders, f'silent-collapse warning still fires: {offenders[:2]}'


def test_geo_merge_is_not_a_cartesian_product(cf):
    """Merging two household-level frames on `v` exploded W2 to 62,538 rows."""
    per_wave = cf.groupby('t').size()
    assert per_wave.max() < 5_000, (
        f'cluster_features has {per_wave.max()} rows in one quarter; a count in '
        f'the tens of thousands is the cartesian geo merge (merge_on: [v]).'
    )


@pytest.mark.parametrize('t', ['2010Q3', '2012Q3', '2015Q3', '2018Q3'])
def test_every_wave_has_coordinates(cf, t):
    """Lat/Lon were silently missing from 3 of the 4 waves."""
    wave = cf.loc[t]
    assert wave['Latitude'].notna().any(), f'{t}: no Latitude at all'
    assert wave['Longitude'].notna().any(), f'{t}: no Longitude at all'


def test_w1_coordinates_are_not_lost_to_the_unique_reducer(cf):
    """W1's coordinates are genuinely invariant within cluster -> keep all 500."""
    w1 = cf.loc['2010Q3']
    assert int(w1['Latitude'].notna().sum()) == W1_CLUSTERS


def test_moved_households_are_singletons_never_a_shared_cluster(nigeria):
    """`ea == 0` is the 'Moved' sentinel, not a cluster.

    Pooling it welds unrelated households into fake clusters like
    (Lagos, ETI-OSA, 'Moved').  Each moved household must get its own id -- and
    must NOT get a NaN `v`, which the downstream groupby() would silently drop.
    """
    s = nigeria.sample()
    assert s['v'].isna().sum() == 0, 'a NaN v is silently dropped downstream'

    moved = s[s['v'].astype(str).str.startswith('moved-')]
    assert len(moved) > 0, 'expected the Moved sentinel in the panel waves'
    # one household per singleton, per wave
    per = moved.reset_index().groupby(['t', 'v']).size()
    assert int(per.max()) == 1, 'a "moved" id is shared by >1 household'


def test_sample_and_cluster_features_share_one_keyspace(nigeria, cf):
    """sample.v is stamped onto every household table; it must resolve."""
    s = nigeria.sample().reset_index()
    s = s[~s['t'].isin(['2023Q3', '2024Q1'])]  # W5 has no cluster_features
    known = set(map(tuple, cf.reset_index()[['t', 'v']].drop_duplicates().values))
    got = set(map(tuple, s[['t', 'v']].dropna().drop_duplicates().values))
    orphans = got - known
    assert not orphans, f'{len(orphans)} sample clusters absent from cluster_features'


def test_assets_index_unique_and_value_not_discarded(nigeria):
    """The item_seq per-unit roster must be summed, not thrown away."""
    a = nigeria.assets()
    assert int(a.index.duplicated().sum()) == 0

    total = a.reset_index().query("t == '2012Q3'")['Value'].sum()
    assert total == pytest.approx(W2_ASSET_VALUE, rel=1e-6), (
        f'2012Q3 asset value is {total:,.0f}; the full per-unit total is '
        f'{W2_ASSET_VALUE:,.0f}.  ~429,001,558 means groupby().first() kept '
        f'only item_seq #1 (GH #323).'
    )


def test_assets_quantity_is_not_multiplied_by_the_unit_count(nigeria):
    """Quantity comes from the sect5a grid; summing it over item_seq would
    multiply it by the number of units."""
    a = nigeria.assets().reset_index()
    w2 = a[a['t'] == '2012Q3']
    # s5q1 is a count of units owned; a blanket sum reducer would inflate it
    # far past any plausible household holding.
    assert w2['Quantity'].max() <= 500, (
        'Quantity looks multiplied by the item_seq unit count -- the reducer '
        'must be `first` for Quantity, not `sum`.'
    )


# --------------------------------------------------------------------------
# the aggregation policy itself: it must be ENFORCED, not merely tolerated
# --------------------------------------------------------------------------

def test_aggregation_policy_is_parsed():
    from lsms_library.country import _aggregation_policy
    entry = {'index': '(t, v)', 'aggregation': {'Latitude': 'unique',
                                                'Value': 'sum'}}
    assert _aggregation_policy(entry) == {'Latitude': 'unique', 'Value': 'sum'}


def test_unique_reducer_keeps_constants_and_refuses_to_guess():
    """Constant within group -> keep it.  Disagreement -> <NA>, never a pick."""
    from lsms_library.country import _apply_aggregation_policy
    idx = pd.MultiIndex.from_tuples(
        [('t', 'A'), ('t', 'A'), ('t', 'B'), ('t', 'B')], names=['t', 'v'])
    df = pd.DataFrame({'Latitude': [1.0, 1.0, 5.0, 9.0]}, index=idx)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = _apply_aggregation_policy(
            df, {'Latitude': 'unique'}, ['t', 'v'], 'cluster_features', 2)

    assert out.loc[('t', 'A'), 'Latitude'] == 1.0     # agreed -> kept
    assert pd.isna(out.loc[('t', 'B'), 'Latitude'])   # disagreed -> NA, not 5.0
    assert any('NOT constant' in str(w.message) for w in caught), \
        'a disagreement must be reported loudly'


def test_sum_reducer_keeps_an_all_nan_group_missing():
    """`sum` must not turn "never reported" into a reported 0."""
    from lsms_library.country import _apply_aggregation_policy
    idx = pd.MultiIndex.from_tuples(
        [('t', 'A'), ('t', 'A'), ('t', 'B'), ('t', 'B')], names=['t', 'i'])
    df = pd.DataFrame({'Value': [10.0, 5.0, np.nan, np.nan]}, index=idx)

    out = _apply_aggregation_policy(
        df, {'Value': 'sum'}, ['t', 'i'], 'assets', 2)

    assert out.loc[('t', 'A'), 'Value'] == 15.0
    assert pd.isna(out.loc[('t', 'B'), 'Value']), 'all-NaN group became 0.0'


def test_uncovered_column_is_reported_not_silently_first():
    """A column with no declared reducer is exactly the GH #323 silent discard."""
    from lsms_library.country import _apply_aggregation_policy
    idx = pd.MultiIndex.from_tuples([('t', 'A'), ('t', 'A')], names=['t', 'i'])
    df = pd.DataFrame({'Value': [1.0, 2.0], 'Undeclared': ['x', 'y']}, index=idx)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        _apply_aggregation_policy(df, {'Value': 'sum'}, ['t', 'i'], 'assets', 1)

    assert any('does not cover column' in str(w.message) for w in caught)

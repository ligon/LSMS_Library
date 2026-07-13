"""GH #323 -- Nigeria: cluster identity `v`, and its behaviour ACROSS ROUNDS.

Nigeria is a post-planting / post-harvest country: every wave FOLDER carries two
quarters (2010-11 -> 2010Q3 planting + 2011Q1 harvest), and each quarter is a
separate ``Wave``.  So every claim below has to hold in BOTH rounds, not just
one -- a fix that is right for the planting quarter and silently wrong for the
harvest quarter is not a fix.

What these tests pin down (measured 2026-07-13 with LSMS_NO_CACHE=1; the
"development" figures are from an actual baseline build of `development`, not
from memory):

  * `v` was the bare `ea` serial, which is unique only WITHIN an LGA.  W1's
    500-EA design came back as 411 clusters -- IN BOTH ROUNDS (2010Q3: 411,
    2011Q1: 411).  The composite cluster_id(state, lga, ea) recovers 500 in
    both.
  * Latitude/Longitude were absent from 3 of the 4 waves that declare
    cluster_features (development: lat_notna = 0 / 409 / 0 / 0 for
    2010Q3 / 2012Q3 / 2015Q3 / 2018Q3) -- wrong column casing, and a
    household-level geo frame merged on `v`, which GH #515 then dropped.
  * `ea == 0` is the 'Moved' sentinel, not a cluster; pooling it welds
    unrelated households into fake clusters.

Deliberately NOT tested here: the `assets` item_seq per-unit roster.  It is a
real and large defect (N147,297,485 -- 25.6% of W2's reported asset value --
destroyed by groupby().first(), in each of the wave's two quarters), but no
change to Nigeria's config can fix it; see the KNOWN OPEN DEFECT note in
Nigeria/_/data_scheme.yml for the two core edits that would.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import Country

pytestmark = pytest.mark.slow

# The GHS-Panel W1 design: 500 EAs x 10 households.  Independently confirmed by
# the household geovariables, which carry exactly 500 distinct coordinates.
W1_CLUSTERS = 500

# (post-planting quarter, post-harvest quarter) for each wave folder.
ROUNDS = [
    ('2010Q3', '2011Q1'),   # 2010-11
    ('2012Q3', '2013Q1'),   # 2012-13
    ('2015Q3', '2016Q1'),   # 2015-16
    ('2018Q3', '2019Q1'),   # 2018-19
    ('2023Q3', '2024Q1'),   # 2023-24  (no cluster_features declared)
]
# Waves that declare cluster_features (W5 does not).
CF_ROUNDS = ROUNDS[:4]
CF_QUARTERS = [q for pair in CF_ROUNDS for q in pair]

# community_prices comes from the COMMUNITY questionnaire's Section C8, which is
# administered in the post-harvest round only.  Verified structurally: across all
# five waves there are 11 `sectc8*` files and every one is a `_harvest` file --
# there is no `sectc8*_planting*` anywhere.  The planting community files that do
# carry an `item_cd` (sectc2_plantingw1, sectc2a/b_plantingw3) have no price
# column at all; C2 is an availability module.  So no planting-round price data
# is being dropped -- there is none to drop.
PH_QUARTERS = ['2011Q1', '2013Q1', '2016Q1', '2019Q1', '2024Q1']


@pytest.fixture(scope='module')
def nigeria():
    return Country('Nigeria')


@pytest.fixture(scope='module')
def sample(nigeria):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return nigeria.sample()


@pytest.fixture(scope='module')
def cf(nigeria):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return nigeria.cluster_features()


# --------------------------------------------------------------------------
# the cluster key itself
# --------------------------------------------------------------------------

@pytest.mark.parametrize('t', ['2010Q3', '2011Q1'])
def test_w1_recovers_the_500_ea_design_in_both_rounds(cf, t):
    """`ea` alone conflates 500 real EAs into 411 codes -- in BOTH rounds.

    development returns 411 for 2010Q3 *and* 2011Q1; the composite key returns
    500 for both.  Parametrised precisely so a fix that only lands on the
    planting quarter cannot pass.
    """
    n = cf.loc[t].index.get_level_values('v').nunique()
    assert n == W1_CLUSTERS, (
        f'{t} has {n} clusters, expected {W1_CLUSTERS}.  A count near 411 means '
        f'`v` is the bare `ea` serial again (GH #323).'
    )


def test_cluster_features_index_is_unique(cf):
    """The canonical (t, v) grain must not need a duplicate-collapse at all."""
    dups = int(cf.index.duplicated().sum())
    assert dups == 0, f'{dups} duplicate (t, v) tuples in cluster_features'


@pytest.mark.parametrize('t', CF_QUARTERS)
def test_every_wave_has_coordinates_in_every_round(cf, t):
    """Lat/Lon were silently missing from 3 of the 4 waves that declare
    cluster_features -- and missing in both of each wave's quarters."""
    wave = cf.loc[t]
    assert wave['Latitude'].notna().any(), f'{t}: no Latitude at all'
    assert wave['Longitude'].notna().any(), f'{t}: no Longitude at all'


@pytest.mark.parametrize('t', ['2010Q3', '2011Q1'])
def test_w1_every_cluster_has_a_coordinate(cf, t):
    """W1's coordinates are genuinely invariant within cluster -> all 500 keep
    one.  (development: 0.)"""
    assert int(cf.loc[t]['Latitude'].notna().sum()) == W1_CLUSTERS


def test_moved_households_are_singletons_never_a_shared_cluster(sample):
    """`ea == 0` is the 'Moved' sentinel, not a cluster.

    Pooling it welds unrelated households into fake clusters like
    (Lagos, ETI-OSA, 'Moved').  Each moved household must get its own id -- and
    must NOT get a NaN `v`, which the downstream groupby() would silently drop.
    """
    assert sample['v'].isna().sum() == 0, 'a NaN v is silently dropped downstream'

    moved = sample[sample['v'].astype(str).str.startswith('moved-')]
    assert len(moved) > 0, 'expected the Moved sentinel in the panel waves'
    # one household per singleton, per wave
    per = moved.reset_index().groupby(['t', 'v']).size()
    assert int(per.max()) == 1, 'a "moved" id is shared by >1 household'


def test_moved_sentinel_does_not_fire_in_the_baseline_wave(sample):
    """W1 is the frame's own baseline: no household can have moved OUT of an EA
    it was only just drawn from, and indeed `ea == 0` never occurs in W1's cover
    page.  So the sentinel must fire ZERO times in 2010Q3/2011Q1 -- it must fire
    when it should and ONLY when it should."""
    s = sample.reset_index()
    w1 = s[s.t.isin(['2010Q3', '2011Q1'])]
    n_moved = w1['v'].astype(str).str.startswith('moved-').sum()
    assert n_moved == 0, f'{n_moved} spurious moved-singletons in W1'

    # ...and it DOES fire in the later panel waves.
    later = s[s.t.isin(['2012Q3', '2015Q3', '2018Q3', '2023Q3'])]
    assert later['v'].astype(str).str.startswith('moved-').sum() > 0


# --------------------------------------------------------------------------
# ROUND SAFETY -- the pp/ph question
# --------------------------------------------------------------------------

@pytest.mark.parametrize('pp,ph', ROUNDS)
def test_v_is_round_invariant_within_a_wave(sample, pp, ph):
    """A household must get the SAME cluster in the planting and the harvest
    quarter of one wave.

    It does, by construction: both quarters of a wave read the SAME post-planting
    cover page (the wave folder has one data_info.yml and both Wave(t)s use it),
    so there is no second geo-coding to drift from.  This test is the guard that
    keeps it that way -- e.g. if someone later points the harvest quarter at
    secta_harvest*.dta, 17/41/66/64 households in W2/W3/W4/W5 would flip to a
    `moved-` singleton (they relocate BETWEEN the two visits) and `v` would stop
    being a stable within-wave cluster key.
    """
    s = sample.reset_index()
    a = s[s.t == pp][['i', 'v']].rename(columns={'v': 'v_pp'})
    b = s[s.t == ph][['i', 'v']].rename(columns={'v': 'v_ph'})
    m = a.merge(b, on='i', how='inner')
    assert len(m) > 0, f'no households shared between {pp} and {ph}'
    differs = m[m.v_pp != m.v_ph]
    assert len(differs) == 0, (
        f'{len(differs)} households change cluster between {pp} and {ph}; '
        f'`v` must be constant within a wave.  e.g.\n{differs.head().to_string()}'
    )


@pytest.mark.parametrize('pp,ph', CF_ROUNDS)
def test_cluster_features_carries_both_rounds_and_they_agree(cf, pp, ph):
    """Both quarters must be present, with the same cluster set and identical
    attributes -- neither round may overwrite or collide with the other."""
    c = cf.reset_index()
    A = c[c.t == pp].set_index('v').drop(columns='t')
    B = c[c.t == ph].set_index('v').drop(columns='t')

    assert len(A) > 0, f'{pp} missing from cluster_features'
    assert len(B) > 0, f'{ph} missing from cluster_features'
    assert set(A.index) == set(B.index), (
        f'{pp} and {ph} disagree on the cluster set '
        f'({len(A)} vs {len(B)} clusters)'
    )
    for col in ['Region', 'District', 'Rural', 'Latitude', 'Longitude']:
        x, y = A[col], B.loc[A.index, col]
        mismatched = int(((x != y) & ~(x.isna() & y.isna())).sum())
        assert mismatched == 0, (
            f'{col} differs between {pp} and {ph} for {mismatched} cluster(s)'
        )


def test_sample_is_one_row_per_household_per_round(sample):
    """sample is keyed (i, t) with t the QUARTER, so a household in a pp/ph wave
    has exactly two rows -- one per round.  The GH #323 rekey changed `v`'s
    VALUE, and must not have changed this grain (development and this branch both
    return 49,770 rows)."""
    assert sample.index.names == ['i', 't'] or set(sample.index.names) == {'i', 't'}
    assert int(sample.index.duplicated().sum()) == 0

    s = sample.reset_index()
    for pp, ph in ROUNDS:
        n_pp = s[s.t == pp]['i'].nunique()
        n_ph = s[s.t == ph]['i'].nunique()
        assert n_pp == n_ph and n_pp > 0, (
            f'{pp} has {n_pp} households but {ph} has {n_ph}; both rounds of a '
            f'wave are populated from the same cover page and must match'
        )


# --------------------------------------------------------------------------
# one keyspace: sample.v / cluster_features.v / community_prices.v
# --------------------------------------------------------------------------

def test_sample_and_cluster_features_share_one_keyspace(nigeria, cf, sample):
    """sample.v is stamped onto every household table by _join_v_from_sample;
    it must resolve against cluster_features -- in BOTH rounds."""
    s = sample.reset_index()
    s = s[s['t'].isin(CF_QUARTERS)]          # W5 declares no cluster_features
    known = set(map(tuple, cf.reset_index()[['t', 'v']].drop_duplicates().values))
    got = set(map(tuple, s[['t', 'v']].dropna().drop_duplicates().values))
    orphans = got - known
    assert not orphans, f'{len(orphans)} sample clusters absent from cluster_features'


def test_community_prices_is_post_harvest_only(nigeria):
    """Section C8 is asked in the post-harvest round only (there is no
    sectc8*_planting* file in any wave), so community_prices must land on the PH
    quarter -- and on NO planting quarter."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = nigeria.community_prices()
    ts = set(cp.reset_index()['t'].unique())
    assert ts == set(PH_QUARTERS), (
        f'community_prices t values {sorted(ts)}; expected exactly the '
        f'post-harvest quarters {PH_QUARTERS}'
    )


def test_community_prices_joins_the_household_keyspace(nigeria, sample):
    """A cluster's prices are useless if they join no households: community_prices
    must be built from the SAME composite key as sample.  (On the bare-`ea` key,
    and on any label-built key, this join silently degrades.)"""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = nigeria.community_prices()
    cpr = cp.reset_index()
    s = sample.reset_index()
    for t, sub in cpr.groupby('t'):
        vs = set(sub['v'].dropna())
        known = set(s[s.t == t]['v'].dropna())
        rate = len(vs & known) / max(len(vs), 1)
        assert rate > 0.95, (
            f'{t}: only {rate:.1%} of community_prices clusters join sample.v; '
            f'the two sides are not in the same keyspace'
        )

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
import importlib.util
import os
from functools import lru_cache
import warnings
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.country import Country


@lru_cache(maxsize=1)
def _round_oracle():
    """Load the sibling oracle without requiring tests/ on PYTHONPATH."""
    spec = importlib.util.spec_from_file_location(
        "_nigeria_round_oracle", Path(__file__).with_name("nigeria_round_oracle.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _aws_creds_available() -> bool:
    """True iff DVC could perform an S3 pull right now.

    Every test in this module BUILDS REAL NIGERIA DATA, which goes through
    DVC -> S3.  Without credentials they raise ``NoCredentialsError``
    regardless of whether the test logic is correct -- which is exactly what
    happened: this file turned the PR red with 27 errors while 649 other
    data-dependent tests skipped cleanly around it.

    Delegates to the shared resolver in ``tests/conftest.py`` (exported
    ``AWS_*`` -> the user-config ``s3_creds`` -> the legacy in-tree
    ``countries/.dvc/s3_creds``).  This used to check only the in-tree
    file, which the library stopped writing in GH #1001, so a fresh
    checkout with user-config credentials silently skipped here.  Loaded by
    path because both import spellings are broken (see
    ``tests/test_tests_package_not_shadowed.py``).
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_shared_conftest", Path(__file__).with_name("conftest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not _aws_creds_available(),
        reason="needs S3 credentials to build Nigeria; the unit-tests job is "
               "deliberately data-free (see data-tests for the credentialed run)",
    ),
    pytest.mark.requires_s3,
]

# The GHS-Panel W1 design: 500 EAs x 10 households.  Independently confirmed by
# the household geovariables, which carry exactly 500 distinct coordinates.
W1_CLUSTERS = 500

# (post-planting quarter, post-harvest quarter) for each wave folder.
ROUNDS = [
    ('2010Q3', '2011Q1'),   # 2010-11
    ('2012Q3', '2013Q1'),   # 2012-13
    ('2015Q3', '2016Q1'),   # 2015-16
    ('2018Q3', '2019Q1'),   # 2018-19
    ('2023Q3', '2024Q1'),   # 2023-24
]
# Every wave declares cluster_features (W5 since GH #841).
CF_ROUNDS = ROUNDS
CF_QUARTERS = [q for pair in CF_ROUNDS for q in pair]
# Waves with acquired and wired household geovariables. W5 is still the
# v01 extract: WB v02 supplies NGA_HouseholdGeovariables_Y5.dta, queued for
# acquisition and wiring under GH #978. Its coordinates remain missing here.
GEO_ROUNDS = ROUNDS[:4]
GEO_QUARTERS = [q for pair in GEO_ROUNDS for q in pair]
NO_GEO_QUARTERS = [q for pair in ROUNDS[4:] for q in pair]

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


@pytest.mark.parametrize('t', GEO_QUARTERS)
def test_every_wave_has_coordinates_in_every_round(cf, t):
    """Lat/Lon were silently missing from 3 of the 4 waves that declare
    cluster_features -- and missing in both of each wave's quarters."""
    wave = cf.loc[t]
    assert wave['Latitude'].notna().any(), f'{t}: no Latitude at all'
    assert wave['Longitude'].notna().any(), f'{t}: no Longitude at all'


@pytest.mark.parametrize('t', NO_GEO_QUARTERS)
def test_w5_has_geography_but_no_coordinates_in_either_round(cf, t):
    """GH #841: W5 declares cluster_features from the cover page alone.  Wave 5
    v01 lacks the geovariables file supplied in WB v02 (GH #978), so
    Latitude/Longitude remain all-NaN while Region/District/Rural are
    populated. Retire this assertion and extend GEO_QUARTERS when the v02
    file is acquired and wired as df_geo on hhid, as W4 does."""
    wave = cf.loc[t]
    assert len(wave) > 0, f'{t} missing from cluster_features'
    for col in ['Region', 'District', 'Rural']:
        assert wave[col].notna().all(), f'{t}: {col} has nulls'
    assert wave['Latitude'].isna().all(), f'{t}: unexpected Latitude -- W5 geovars acquired?'
    assert wave['Longitude'].isna().all(), f'{t}: unexpected Longitude -- W5 geovars acquired?'


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

# GH #1120 D1 (owner ruling 2026-10-23; comment6380186741 refinement):
# each round uses its OWN cover. The old PP-for-both assertions were preserved
# in the dispatch evidence (9 failures / 2 passes) before reconciliation.
@pytest.mark.parametrize('pp,ph', ROUNDS)
def test_v_matches_each_rounds_recorded_location(sample, pp, ph):
    recorded_round = _round_oracle().recorded_round
    for t in (pp, ph):
        actual = sample.xs(t, level='t')
        expected = recorded_round(t)
        assert actual.index.is_unique
        assert set(actual.index) == set(expected.index)
        assert actual['v'].to_dict() == expected['v'].to_dict()


@pytest.mark.parametrize('pp,ph', CF_ROUNDS)
def test_cluster_features_matches_each_rounds_recorded_geography(cf, pp, ph):
    recorded_geo = _round_oracle().recorded_geo
    for t in (pp, ph):
        actual = cf.xs(t, level='t')
        expected = recorded_geo(t)
        assert actual.index.is_unique and len(actual) > 0
        assert set(actual.index) == set(expected['v'])
        # A served fix is one recorded household pair, never a cross-column
        # composite or a mean. Missing W5 geo remains missing, not zero.
        def pair(values):
            return tuple(None if pd.isna(x) else float(x) for x in values)
        for v, group in expected.groupby('v'):
            observed = {pair(row) for row in
                        group[['Latitude', 'Longitude']].to_numpy()}
            assert pair(actual.loc[v, ['Latitude', 'Longitude']]) in observed


def test_sample_is_one_row_per_household_per_round(sample):
    recorded_round = _round_oracle().recorded_round
    assert_numeric_exact = _round_oracle().assert_numeric_exact
    assert set(sample.index.names) == {'i', 't'}
    assert sample.index.is_unique
    for pp, ph in ROUNDS:
        for t in (pp, ph):
            actual = sample.xs(t, level='t').sort_index()
            expected = recorded_round(t)
            assert set(actual.index) == set(expected.index) and len(actual) > 0
            expected = expected.loc[actual.index]
            for col in ('weight', 'panel_weight'):
                raw = expected[col].astype(float)
                mean = raw.groupby(pd.Series(t, index=raw.index)).transform('mean')
                assert_numeric_exact(actual[col], raw / mean)


# --------------------------------------------------------------------------
# one keyspace: sample.v / cluster_features.v / community_prices.v
# --------------------------------------------------------------------------

def test_sample_and_cluster_features_share_one_keyspace(nigeria, cf, sample):
    """sample.v is stamped onto every household table by _join_v_from_sample;
    it must resolve against cluster_features -- in BOTH rounds."""
    s = sample.reset_index()
    s = s[s['t'].isin(CF_QUARTERS)]          # every quarter, W5 included (GH #841)
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


# --------------------------------------------------------------------------
# GH #834 -- the C8 SIZE field (W3-W5): metric package sizes are divided
# through to per-unit prices; an unrecorded package size is dropped, never
# served mis-scaled under `g`/`cl`.
# --------------------------------------------------------------------------

def test_community_prices_metric_sizes_are_per_unit(nigeria):
    """W4/W5 coded metric sizes (c8q2c / c8aq2_c: '34. 250 GRAMS', '24. 75 CL')
    mean the reported price is of that PACKAGE.  The build divides it by the
    decoded quantity and re-units the row to 'g'/'cl', stamping the Derivation
    key.  Measured pre-fix: median community/household ratio 1/189 on `g` and
    1/42 on `cl`; post-fix ~1.0 where a household unit value exists."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = nigeria.community_prices()
    assert 'Derivation' in cp.columns
    key = 'Nigeria::community_prices::c8-metric-size'
    r = cp.reset_index()
    derived = r[r['Derivation'].eq(key)]
    assert len(derived) > 10000
    assert set(derived['u']) <= {'g', 'cl'}, derived['u'].unique()
    # Every derived row sits in a W4/W5 quarter (W3 contributes one row).
    assert set(derived['t']) <= {'2016Q1', '2019Q1', '2024Q1'}
    # A per-unit price in Naira per gram / centilitre is a small number;
    # the pre-fix package prices were tens-to-hundreds of Naira per "g".
    for t in ('2019Q1', '2024Q1'):
        d = derived[derived['t'].eq(t)]
        assert d['Price'].median() < 50, (t, d['Price'].median())


def test_community_prices_unrecorded_package_sizes_dropped(nigeria):
    """W3's c8q2b is blank on 13,767 of 13,768 metric-unit rows and W4's
    'Grams(g)' rows carry no c8q2c code: a price of an unrecorded package
    cannot be normalised, so those rows are dropped rather than served ~190x
    off as `g`/`cl`.  Post-fix 2016Q1 serves exactly ONE cl row (the single
    '50cl' free-text row) and no g rows; 2019Q1's `g` rows are exactly the
    coded-metric-size derivation."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = nigeria.community_prices()
    r = cp.reset_index()
    w3 = r[r['t'].eq('2016Q1')]
    assert int((w3['u'] == 'g').sum()) == 0
    assert int((w3['u'] == 'cl').sum()) == 1
    key = 'Nigeria::community_prices::c8-metric-size'
    w4 = r[r['t'].eq('2019Q1')]
    assert int((w4['u'] == 'g').sum()) > 1000
    assert w4.loc[w4['u'].isin(['g', 'cl']), 'Derivation'].eq(key).all()


def test_community_prices_g_cl_match_the_household_unit_value(nigeria):
    """The end-to-end check the fix is judged by: on 2019Q1 (the one quarter
    where the household side records cl-labelled purchases), the median
    community/household price ratio on matched (v, j, u) cells is ~1.0 on
    both metric units -- the same agreement Kg and l always showed."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = nigeria.community_prices()
        fa = nigeria.food_acquired()
    f = fa.reset_index()
    f = f[f['s'].eq('purchased')][['t', 'v', 'j', 'u', 'Quantity',
                                   'Expenditure']].dropna()
    f = f[(f['Quantity'] > 0) & (f['Expenditure'] > 0)]
    f['uv'] = f['Expenditure'] / f['Quantity']
    fmed = f.groupby(['t', 'v', 'j', 'u'])['uv'].median()
    d = cp.reset_index()[['t', 'v', 'j', 'u', 'Price']].dropna()
    d = d[d['Price'] > 0]
    cmed = d.groupby(['t', 'v', 'j', 'u'])['Price'].median()
    cells = pd.concat([cmed.rename('community'), fmed.rename('household')],
                      axis=1).dropna()
    cells = cells[cells.index.get_level_values('t') == '2019Q1']
    cells['ratio'] = cells['community'] / cells['household']
    for u, n_min in (('g', 100), ('cl', 100)):
        sub = cells[cells.index.get_level_values('u') == u]
        assert len(sub) >= n_min, (u, len(sub))
        med = sub['ratio'].median()
        assert 0.8 < med < 1.25, (u, med)

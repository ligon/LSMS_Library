"""GH #323: a non-unique DECLARED index must never be collapsed by a silent
``groupby().first()``.

Two distinct defects, both live on Senegal before this lands:

1. ``plot_inputs`` (2018-19) -- an INTENDED many-to-one label merge
   (harmonize_input maps all 11 seed types -> 'Seed'; harmonize_seed_crop maps
   the three CATCH-ALL seed labels -> 'Autre crop') makes the declared index
   non-unique for 15 groups / 30 rows.  ``first()`` DESTROYED 819.5 of the
   1,478.5 seed units in those groups (55%) and resolved a conflicting
   ``Purchased`` flag by row order.  The colliding rows are DISTINCT source
   items in the SAME unit (verified against s16b_me_sen2018.dta, which has
   ZERO duplicate records), so ``sum`` is exact -- it neither double-counts nor
   discards.  Now DECLARED via ``aggregation.on_duplicate_index``.

2. ``cluster_features`` (both waves) -- declared at (t, v) but extracted from
   the HOUSEHOLD cover file, so it emitted 7,156 rows for 598 grappes.  That
   collapse is value-LOSSLESS (0 of 598 grappes carry a conflicting attribute),
   but the table's correctness rested on the ACCIDENT that ``first()`` picked
   from identical rows.  Now a grain correction with an ENFORCED invariant.

The point of the fix is that ``aggregation:`` in ``data_scheme.yml`` was parsed
and IGNORED repo-wide -- prose, not enforcement.  ``on_duplicate_index`` is
consumed; a table that does not declare it is untouched.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.country import _normalize_dataframe_index, _reduce_duplicate_index
from lsms_library.transformations import collapse_to_cluster_grain


def _plot_inputs_like() -> pd.DataFrame:
    """The real i=593008 / i=341003 collisions, in miniature."""
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '593008', 'Seed', 'Autre crop', 'Kg'),   # 'Autres semences'
         ('2018-19', '593008', 'Seed', 'Autre crop', 'Kg'),   # 'Plants/boutures'
         ('2018-19', '341003', 'Seed', 'Autre crop', 'Kg'),   # purchased Non
         ('2018-19', '341003', 'Seed', 'Autre crop', 'Kg'),   # purchased Oui, 0.5
         ('2018-19', '100001', 'Urea', '(not crop-specific)', 'Kg')],
        names=['t', 'i', 'input', 'crop', 'u'],
    )
    return pd.DataFrame(
        {'Quantity': pd.array([52.5, 4.0, 0.5, 0.5, 3.0], dtype='Float64'),
         'Purchased': pd.array([False, False, False, True, True], dtype='boolean'),
         'Quantity_purchased': pd.array([None, None, None, 0.5, 3.0], dtype='Float64')},
        index=idx,
    )


SCHEME = {
    'index': '(t, i, input, crop, u)',
    'aggregation': {'on_duplicate_index': {
        'Quantity': 'sum', 'Purchased': 'any', 'Quantity_purchased': 'sum'}},
}


# --------------------------------------------------------------------------
# The declared reducer replaces the destructive default
# --------------------------------------------------------------------------

def test_declared_sum_recovers_what_first_would_destroy():
    df = _plot_inputs_like()
    out = _normalize_dataframe_index(df, SCHEME, None, 'plot_inputs')

    assert len(out) == 3, 'the two intended merges collapse to one row each'
    # 52.5 + 4.0 = 56.5.  groupby().first() would have kept 52.5 and DESTROYED 4.0.
    assert out.loc[('2018-19', '593008', 'Seed', 'Autre crop', 'Kg'), 'Quantity'] == 56.5
    # No mass destroyed anywhere.
    assert out['Quantity'].sum() == df['Quantity'].sum() == 60.5


def test_any_resolves_a_conflicting_flag_deterministically():
    # i=341003 reports the SAME bucket twice: Purchased=Non and Purchased=Oui(0.5).
    # first() answers by ROW ORDER; any() is well-defined.
    df = _plot_inputs_like()
    out = _normalize_dataframe_index(df, SCHEME, None, 'plot_inputs')
    row = out.loc[('2018-19', '341003', 'Seed', 'Autre crop', 'Kg')]
    assert row['Purchased'] is True or row['Purchased'] == True  # noqa: E712
    assert row['Quantity_purchased'] == 0.5
    assert row['Quantity'] == 1.0

    # ... and it does NOT depend on row order.
    shuffled = df.iloc[::-1]
    out2 = _normalize_dataframe_index(shuffled, SCHEME, None, 'plot_inputs')
    key = ('2018-19', '341003', 'Seed', 'Autre crop', 'Kg')
    assert bool(out2.loc[key, 'Purchased']) == bool(out.loc[key, 'Purchased'])


def test_sum_does_not_turn_all_na_into_zero():
    """min_count=1: 'never asked' must not become a confident 0.0."""
    df = _plot_inputs_like()
    out = _normalize_dataframe_index(df, SCHEME, None, 'plot_inputs')
    # i=593008's two rows both have Quantity_purchased NA -> must stay NA, not 0.0.
    assert pd.isna(out.loc[('2018-19', '593008', 'Seed', 'Autre crop', 'Kg'),
                           'Quantity_purchased'])


def test_any_does_not_turn_all_na_into_false():
    idx = pd.MultiIndex.from_tuples(
        [('t1', 'h1'), ('t1', 'h1')], names=['t', 'i'])
    df = pd.DataFrame({'Flag': pd.array([None, None], dtype='boolean')}, index=idx)
    out = _reduce_duplicate_index(df, ['t', 'i'], {'Flag': 'any'}, 'x')
    assert pd.isna(out.loc[('t1', 'h1'), 'Flag']), 'all-NA group must stay NA'


# --------------------------------------------------------------------------
# The declaration must be COMPLETE -- no silent fallback to first()
# --------------------------------------------------------------------------

def test_uncovered_column_raises_rather_than_silently_firsting():
    df = _plot_inputs_like()
    scheme = {'index': '(t, i, input, crop, u)',
              'aggregation': {'on_duplicate_index': {'Quantity': 'sum'}}}
    with pytest.raises(ValueError, match=r'no reducer'):
        _normalize_dataframe_index(df, scheme, None, 'plot_inputs')


def test_unknown_reducer_raises():
    df = _plot_inputs_like()
    scheme = {'index': '(t, i, input, crop, u)',
              'aggregation': {'on_duplicate_index': {
                  'Quantity': 'median_of_vibes', 'Purchased': 'any',
                  'Quantity_purchased': 'sum'}}}
    with pytest.raises(ValueError, match=r'unknown reducer'):
        _normalize_dataframe_index(df, scheme, None, 'plot_inputs')


# --------------------------------------------------------------------------
# Tables that do NOT opt in are untouched (the other 39 countries)
# --------------------------------------------------------------------------

def test_undeclared_table_keeps_legacy_warn_and_first():
    """No aggregation block -> unchanged behaviour, including the #323 warning.

    This is the regression guard for every country that has not (yet) declared
    a policy: adding the consumer must not move their numbers.
    """
    df = _plot_inputs_like()
    with pytest.warns(RuntimeWarning, match='GH #323'):
        out = _normalize_dataframe_index(
            df, {'index': '(t, i, input, crop, u)'}, None, 'plot_inputs')
    assert len(out) == 3
    assert out.loc[('2018-19', '593008', 'Seed', 'Autre crop', 'Kg'),
                   'Quantity'] == 52.5  # first(), the old destructive default


def test_level_keyed_aggregation_is_not_consumed():
    """9 countries declare `aggregation: {visit: first}` on interview_date.

    That is a downstream hint, NOT an on_duplicate_index policy; it must keep
    falling through to the legacy path rather than being read as a reducer map.
    """
    df = _plot_inputs_like()
    scheme = {'index': '(t, i, input, crop, u)', 'aggregation': {'visit': 'first'}}
    with pytest.warns(RuntimeWarning, match='GH #323'):
        out = _normalize_dataframe_index(df, scheme, None, 'plot_inputs')
    assert len(out) == 3


# --------------------------------------------------------------------------
# cluster_features: grain correction with an ENFORCED invariant
# --------------------------------------------------------------------------

def test_collapse_to_cluster_grain_dedups_identical_household_rows():
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '105'), ('2018-19', '105'), ('2018-19', '107')],
        names=['t', 'v'])
    df = pd.DataFrame({'Region': ['Dakar', 'Dakar', 'Thies'],
                       'Rural': ['Urban', 'Urban', 'Rural']}, index=idx)
    out = collapse_to_cluster_grain(df)
    assert len(out) == 2
    assert out.loc[('2018-19', '105'), 'Region'] == 'Dakar'


def test_collapse_to_cluster_grain_raises_when_an_attribute_conflicts():
    """The invariant the old first() silently ASSUMED is now enforced."""
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '105'), ('2018-19', '105')], names=['t', 'v'])
    df = pd.DataFrame({'Region': ['Dakar', 'Thies'],   # a real conflict
                       'Rural': ['Urban', 'Urban']}, index=idx)
    with pytest.raises(ValueError, match=r'more than one distinct value'):
        collapse_to_cluster_grain(df)


def test_collapse_to_cluster_grain_treats_nan_as_a_value():
    idx = pd.MultiIndex.from_tuples(
        [('2018-19', '105'), ('2018-19', '105')], names=['t', 'v'])
    df = pd.DataFrame({'Latitude': [14.7, None]}, index=idx)
    with pytest.raises(ValueError, match=r'more than one distinct value'):
        collapse_to_cluster_grain(df)


# --------------------------------------------------------------------------
# End-to-end on the real Senegal data (the cells GH #323 reported)
# --------------------------------------------------------------------------

@pytest.fixture(scope='module')
def senegal():
    from lsms_library.country import Country
    try:
        c = Country('Senegal')
        c.plot_inputs()
    except Exception as exc:  # no DVC / no .dta on disk
        pytest.skip(f'Senegal data unavailable: {exc}')
    return c


def test_senegal_plot_inputs_destroys_no_quantity(senegal):
    """Every unit of input reported in s16b_me_sen2018.dta reaches the API.

    Pre-fix the API returned 909,501.2 of the 910,320.8 units in the source --
    819.5 units of seed silently destroyed by groupby().first().
    """
    from lsms_library.local_tools import get_dataframe
    src = get_dataframe(
        'lsms_library/countries/Senegal/2018-19/Data/s16b_me_sen2018.dta')
    expected = float(src['s16bq03a'].sum())

    got = float(senegal.plot_inputs()['Quantity'].sum())
    assert got == pytest.approx(expected, rel=1e-9), (
        f'plot_inputs destroyed {expected - got:,.1f} units of input '
        f'(source {expected:,.1f} -> API {got:,.1f})')


def test_senegal_plot_inputs_merged_rows_are_summed(senegal):
    pi = senegal.plot_inputs().reset_index()
    row = pi[(pi['i'] == '593008') & (pi['input'] == 'Seed')
             & (pi['crop'] == 'Autre crop')]
    assert len(row) == 1
    # 4.0 ('Plants/boutures de tubercules') + 52.5 ('Autres semences').
    # first() kept 4.0 and threw 52.5 away.
    assert float(row['Quantity'].iloc[0]) == pytest.approx(56.5)


def test_senegal_cluster_features_is_cluster_grain(senegal):
    cf = senegal.cluster_features()
    assert cf.index.is_unique
    assert cf.groupby(level='t').size().to_dict() == {'2018-19': 598, '2021-22': 596}


def test_senegal_builds_without_a_323_collapse_warning(senegal):
    """No table may reach the API via a silent groupby().first() collapse.

    NOTE this assertion is only meaningful on a COLD build: the #323 warning
    fires when the collapse happens, and in warm operation the collapse is
    already baked into the L2 cache -- the bug hides behind the cache it
    poisoned.  The two structural tests below are the cache-proof net.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        senegal.cluster_features()
        senegal.plot_inputs()
        senegal.food_acquired()
    offenders = [str(w.message) for w in caught if 'GH #323' in str(w.message)]
    assert not offenders, f'silent collapse still happening: {offenders}'


# --------------------------------------------------------------------------
# Structural: cache-proof.  A warm (already-collapsed) cache cannot make these
# pass, because they inspect the CONFIG rather than the built data.
# --------------------------------------------------------------------------

def test_senegal_cluster_features_grain_hook_is_wired():
    """Both waves must route cluster_features through the enforcing collapse."""
    from lsms_library.country import Country
    c = Country('Senegal')
    for wave in ('2018-19', '2021-22'):
        hook = c[wave].formatting_functions.get('cluster_features')
        assert hook is collapse_to_cluster_grain, (
            f'Senegal/{wave} cluster_features is extracted at HOUSEHOLD grain; '
            f'without the hook its (t, v) correctness rests on a silent '
            f'groupby().first()')


@pytest.mark.parametrize('table,expected', [
    ('food_acquired', {'Quantity': 'sum', 'Expenditure': 'sum'}),
    ('plot_inputs', {'Quantity': 'sum', 'Purchased': 'any',
                     'Quantity_purchased': 'sum'}),
])
def test_senegal_declares_its_intended_aggregations(table, expected):
    """The intended many-to-one merges must be DECLARED, not silent."""
    from lsms_library.country import Country
    entry = Country('Senegal')._materialization_entry(table) or {}
    policy = (entry.get('aggregation') or {}).get('on_duplicate_index')
    assert policy == expected, (
        f'{table} collapses a non-unique declared index; it must declare a '
        f'reducer for every column (GH #323)')


def test_senegal_food_acquired_conserves_mass(senegal):
    """The 218 intended harmonize_food merges must SUM, not drop.

    (This already held via the hardcoded _ADDITIVE_MEASURE_COLUMNS map; the
    declaration in data_scheme.yml now makes the intent explicit and pins it.)
    """
    fa = senegal.food_acquired()
    assert fa.index.is_unique
    w = fa.xs('2021-22', level='t')
    assert len(w) == 220447
    assert float(w['Expenditure'].sum()) == pytest.approx(185_854_825.0, rel=1e-9)
    assert float(w['Quantity'].sum()) == pytest.approx(1_137_283.9, rel=1e-6)

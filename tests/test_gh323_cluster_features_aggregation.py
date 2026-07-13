"""GH #323 -- the silent ``groupby().first()`` collapse in ``cluster_features``.

``Wave.cluster_features()`` collapses a household-grain extraction (``i`` in
``idxvars``) down to the canonical ``(t, v)`` cluster index.  It did so with an
unguarded ``.first()`` whose comment *asserted* that the reduced columns are
"invariant within a cluster by construction of the LSMS-ISA sampling design" --
but nothing ever checked that assertion.  An unchecked ``.first()`` over a
household grain is indistinguishable from merging genuinely distinct entities,
and it is SILENT: it never reaches the guarded branch in
``_normalize_dataframe_index``, so the GH #323 warning never fired for it.

17 countries carry household grain in ``cluster_features``; on a cold build the
assertion is actually FALSE in ten of them.

These tests pin the enforcement:

* a DECLARED ``aggregation: {i: unique}`` RAISES when the invariant is violated
  (rather than silently keeping whichever household sorted first);
* an UNDECLARED collapse WARNS when the invariant is violated;
* Tajikistan's collapse is lossless, so declaring ``unique`` is a pure no-op.

The private helpers are imported INSIDE the unit tests, so that on a pre-fix
tree this module still collects and the behavioural tests below fail on
BEHAVIOUR (no raise / no warning) rather than on an ImportError.
"""
import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library import Country

# Tajikistan clusters per wave (the two-stage sampling take).
TAJIK_PSUS = {'1999': 125, '2003': 208, '2007': 267, '2009': 167}


def _poison_region(monkeypatch, wave_suffix):
    """Make one household inside a cluster disagree about its Region.

    dtype-safe: swaps in another value ALREADY PRESENT in the column rather than
    a sentinel string.  ``Region`` is int8 raw codes in some countries (Guyana)
    and mapped labels in others (Tajikistan); a string sentinel raises
    ``LossySetitemError`` on the former.
    """
    real = C.Wave.grab_data

    def poisoned(self, request):
        df = real(self, request)
        if request == 'cluster_features' and self.name.endswith(wave_suffix):
            df = df.copy()
            col = df['Region']
            current = col.iloc[0]
            alt = next((v for v in col.dropna().unique() if v != current), None)
            assert alt is not None, "need >=2 distinct Regions to build a conflict"
            df.iloc[0, df.columns.get_loc('Region')] = alt
        return df

    monkeypatch.setattr(C.Wave, 'grab_data', poisoned)


def _aggregation_of(country):
    entry = Country(country)._materialization_entry('cluster_features')
    return (entry.get('aggregation') or {}).get('i')


# --------------------------------------------------------------------------
# BEHAVIOUR (these fail on a pre-fix tree: no raise, no warning)
# --------------------------------------------------------------------------

def test_declared_unique_raises_when_a_cluster_straddles_two_regions(monkeypatch):
    """The point of the whole exercise: a conflict must FAIL LOUDLY, not be
    quietly resolved in favour of whichever household sorted first.

    Pre-fix this silently returned 125 rows with one Region invented."""
    assert _aggregation_of('Tajikistan') == 'unique', \
        "Tajikistan must DECLARE its household->cluster collapse"
    _poison_region(monkeypatch, '1999')
    with pytest.raises(ValueError, match='conflicting values'):
        Country('Tajikistan')['1999'].cluster_features()


def test_undeclared_country_warns_instead_of_collapsing_silently(monkeypatch):
    """The CLASS, not just the instance: a country with NO declared policy must
    still surface the conflict rather than discard rows in silence."""
    assert _aggregation_of('Guyana') is None, \
        "Guyana must have NO declared policy for this test to mean anything"
    _poison_region(monkeypatch, '1992')
    with pytest.warns(RuntimeWarning, match='GH #323'):
        Country('Guyana')['1992'].cluster_features()


def test_tajikistan_collapse_is_lossless_no_op():
    """0 of 767 cluster-waves carry a conflicting (Region, Rural), so enforcing
    ``unique`` must change nothing about the returned data."""
    for wave, n_clusters in TAJIK_PSUS.items():
        df = Country('Tajikistan')[wave].cluster_features()
        assert list(df.index.names) == ['t', 'v']
        assert len(df) == n_clusters, f"{wave}: expected {n_clusters}, got {len(df)}"


def test_tajikistan_country_level_row_count():
    df = Country('Tajikistan').cluster_features()
    assert len(df) == sum(TAJIK_PSUS.values()) == 767
    assert list(df.index.names) == ['t', 'v']


# --------------------------------------------------------------------------
# The `aggregation:` key must actually be READ.  It used to be inert: declared
# in nine countries' data_scheme.yml, but referenced in *.py only to be
# EXCLUDED from column parsing.  Nothing consumed it as a policy.
# --------------------------------------------------------------------------

def test_aggregation_key_is_read_not_inert():
    from lsms_library.country import _declared_reducer
    entry = Country('Tajikistan')._materialization_entry('cluster_features')
    assert _declared_reducer(entry, ['i']) == 'unique'


def test_reducer_ignored_for_a_level_that_survives_in_the_index():
    # Senegal-style: `visit` is IN the declared index, so it is never collapsed
    # and its reducer must not fire.  This is what keeps the nine pre-existing
    # `visit: first` declarations no-ops.
    from lsms_library.country import _declared_reducer
    entry = {'index': '(t, i, visit)', 'aggregation': {'visit': 'first'}}
    assert _declared_reducer(entry, []) is None


def test_unknown_reducer_is_rejected():
    from lsms_library.country import _declared_reducer
    with pytest.raises(ValueError, match='Unknown `aggregation:` reducer'):
        _declared_reducer({'aggregation': {'i': 'average'}}, ['i'])


# --------------------------------------------------------------------------
# The guard itself
# --------------------------------------------------------------------------

def _frame(regions):
    idx = pd.MultiIndex.from_tuples(
        [('1999', 'psu1', f'hh{n}') for n in range(len(regions))],
        names=['t', 'v', 'i'],
    )
    return pd.DataFrame(
        {'Region': regions, 'Latitude': [1.0, 2.0, 3.0][:len(regions)]}, index=idx,
    )


def test_constant_group_is_lossless_and_passes():
    from lsms_library.country import _assert_constant_within_groups, _nonconstant_groups
    df = _frame(['Sogd', 'Sogd', 'Sogd'])
    offenders, bad = _nonconstant_groups(df, ['t', 'v'], ['Region'])
    assert len(offenders) == 0 and bad == []
    _assert_constant_within_groups(df, ['t', 'v'], ['Region'], ['i'], 'test')


def test_conflicting_group_is_detected_and_raises():
    from lsms_library.country import _assert_constant_within_groups, _nonconstant_groups
    df = _frame(['Sogd', 'Khatlon', 'Sogd'])
    offenders, bad = _nonconstant_groups(df, ['t', 'v'], ['Region'])
    assert len(offenders) == 1 and bad == ['Region']
    with pytest.raises(ValueError, match='conflicting values'):
        _assert_constant_within_groups(df, ['t', 'v'], ['Region'], ['i'], 'test')


def test_gps_columns_are_exempt_from_the_constancy_check():
    # Latitude/Longitude are HH-level BY DESIGN (averaged to a cluster
    # centroid), so they must be allowed to vary.  Were GPS checked, every
    # GPS-carrying country would false-positive.
    from lsms_library.country import _nonconstant_groups
    df = _frame(['Sogd', 'Sogd', 'Sogd'])          # Latitude varies 1/2/3
    offenders, _ = _nonconstant_groups(df, ['t', 'v'], ['Region'])
    assert len(offenders) == 0, "GPS must not be in the checked column set"


# --------------------------------------------------------------------------
# The second collapse site: `_normalize_dataframe_index`.  No country routes a
# cluster_features collapse through it today (Wave.cluster_features pre-collapses),
# but the `aggregation:` policy is wired there too -- so pin it rather than ship
# an unexercised branch.
# --------------------------------------------------------------------------

def _flat_frame(regions, lat):
    idx = pd.MultiIndex.from_tuples(
        [('1999', 'psu1')] * len(regions), names=['t', 'v'],
    )
    return pd.DataFrame({'Region': regions, 'Latitude': lat}, index=idx)


def test_apply_declared_reducer_unique_collapses_when_lossless():
    from lsms_library.country import _apply_declared_reducer
    df = _flat_frame(['Sogd', 'Sogd', 'Sogd'], [1.0, 1.0, 1.0])
    out = _apply_declared_reducer(df, 'unique', ['t', 'v'], ['i'], 'cluster_features', '1999')
    assert len(out) == 1
    assert out['Region'].iloc[0] == 'Sogd'


def test_apply_declared_reducer_checks_every_column_including_gps():
    """The GENERIC reducer has NO GPS exemption, deliberately.

    ``unique`` means "collapsing this level loses nothing" -- so a varying
    Latitude IS a loss and must raise.  The mean-centroid exemption is a rule
    specific to ``Wave.cluster_features`` (where GPS is averaged on purpose),
    and it must not leak into the generic path.
    """
    from lsms_library.country import _apply_declared_reducer
    df = _flat_frame(['Sogd', 'Sogd', 'Sogd'], [1.0, 2.0, 3.0])
    with pytest.raises(ValueError, match='Latitude'):
        _apply_declared_reducer(df, 'unique', ['t', 'v'], ['i'], 'cluster_features', '1999')


def test_apply_declared_reducer_unique_raises_when_lossy():
    from lsms_library.country import _apply_declared_reducer
    df = _flat_frame(['Sogd', 'Khatlon', 'Sogd'], [1.0, 1.0, 1.0])
    with pytest.raises(ValueError, match='conflicting values'):
        _apply_declared_reducer(df, 'unique', ['t', 'v'], ['i'], 'cluster_features', '1999')


def test_apply_declared_reducer_first_is_a_declared_discard():
    """`first` is lossy BY DECLARATION: it collapses without raising."""
    from lsms_library.country import _apply_declared_reducer
    df = _flat_frame(['Sogd', 'Khatlon', 'Sogd'], [1.0, 1.0, 1.0])
    out = _apply_declared_reducer(df, 'first', ['t', 'v'], ['i'], 'cluster_features', '1999')
    assert len(out) == 1

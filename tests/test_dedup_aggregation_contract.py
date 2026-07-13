"""GH #323: a grain collapse must be DECLARED, and the declaration ENFORCED.

Several tables are built from a source whose grain is FINER than the table's
canonical index -- Cambodia's ``cluster_features`` reads the household cover
page (1512 households) and returns one row per cluster (252).  The framework
reached that shape by dropping the undeclared ``i`` level and collapsing the
resulting duplicates with ``groupby().first()``: SILENTLY, and correct only
because Region/Rural happen to be constant within a cluster.  Nothing checked
that.  Where the assumption fails (a cluster code reused across districts),
``.first()`` keeps one value at random -- silently WRONG.

The contract: a country declares the projection in ``data_scheme.yml``

    cluster_features:
      index: (t, v)
      aggregation: {i: dedup}

and the framework PROVES it at build time -- every retained column must be
invariant within the surviving index -- raising if it is not.  Loudly missing
beats quietly wrong.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.country import Country, Wave, _normalize_dataframe_index
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


def _contract_api():
    """The enforcement API (imported lazily so that pre-fix each test fails on
    its own behaviour rather than the whole module failing to collect)."""
    from lsms_library.country import _aggregation_policy, _assert_dedup_lossless
    return _aggregation_policy, _assert_dedup_lossless

# (t, i, v) household-grain cover page: 2 clusters x 2 households, the cluster
# attributes repeated on each household record -- Cambodia's shape in miniature.
IDX = pd.MultiIndex.from_tuples(
    [('2019-20', 'h1', 'c1'), ('2019-20', 'h2', 'c1'),
     ('2019-20', 'h3', 'c2'), ('2019-20', 'h4', 'c2')],
    names=['t', 'i', 'v'],
)
DEDUP_ENTRY = {'index': '(t, v)', 'aggregation': {'i': 'dedup'},
               'Region': 'str', 'Rural': 'str'}


def _invariant_frame() -> pd.DataFrame:
    return pd.DataFrame({'Region': ['North', 'North', 'South', 'South'],
                         'Rural': ['Rural', 'Rural', 'Urban', 'Urban']},
                        index=IDX)


def _conflicting_frame() -> pd.DataFrame:
    # h2 disagrees with h1 about which Region cluster c1 is in: the cluster code
    # is NOT globally unique, so this collapse would merge two real clusters.
    df = _invariant_frame()
    df.loc[('2019-20', 'h2', 'c1'), 'Region'] = 'South'
    return df


# --------------------------------------------------------------- the policy
def test_policy_parses_declared_block():
    _aggregation_policy, _ = _contract_api()
    assert _aggregation_policy(DEDUP_ENTRY) == {'i': 'dedup'}


def test_policy_absent_is_empty():
    # No declaration -> no collapse may claim to be intentional.
    _aggregation_policy, _ = _contract_api()
    assert _aggregation_policy({'index': '(t, v)'}) == {}
    assert _aggregation_policy(None) == {}
    assert _aggregation_policy({'index': '(t, v)', 'aggregation': 'dedup'}) == {}


# ------------------------------------------- declared + verified => lossless
def test_declared_dedup_collapses_without_warning():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = _normalize_dataframe_index(_invariant_frame(), DEDUP_ENTRY,
                                         None, 'cluster_features')
    assert list(out.index.names) == ['t', 'v']
    assert len(out) == 2                       # 4 household rows -> 2 clusters
    assert out.loc[('2019-20', 'c1'), 'Region'] == 'North'
    assert out.loc[('2019-20', 'c2'), 'Region'] == 'South'
    # The projection is declared AND proven lossless, so it is not a "possible
    # silent data loss" event -- it must not cry wolf.
    assert not [w for w in caught if 'GH #323' in str(w.message)]


def test_declared_dedup_tolerates_missing_values():
    # A null is not a disagreement: .first() skips nulls, so {NA, 'North'}
    # collapses to 'North' and loses nothing.
    df = _invariant_frame()
    df.loc[('2019-20', 'h1', 'c1'), 'Region'] = pd.NA
    out = _normalize_dataframe_index(df, DEDUP_ENTRY, None, 'cluster_features')
    assert out.loc[('2019-20', 'c1'), 'Region'] == 'North'


# ------------------------------------------ declared + VIOLATED => loud stop
def test_declared_dedup_raises_when_violated():
    with pytest.raises(RuntimeError) as exc:
        _normalize_dataframe_index(_conflicting_frame(), DEDUP_ENTRY,
                                   None, 'cluster_features')
    msg = str(exc.value)
    assert 'dedup' in msg and 'VIOLATED' in msg
    assert 'Region' in msg          # names the offending column
    assert 'c1' in msg              # ... and the offending group
    assert 'c2' not in msg          # ... and only the offending group


def test_assert_dedup_lossless_is_the_load_bearing_check():
    _, _assert_dedup_lossless = _contract_api()
    # Directly: the same conflict, caught on the pre-collapse frame.
    with pytest.raises(RuntimeError, match='VIOLATED'):
        _assert_dedup_lossless(_conflicting_frame().droplevel('i'), ['t', 'v'],
                               collapsing=['i'], table_name='cluster_features')
    # ... and no false positive on the invariant frame.
    _assert_dedup_lossless(_invariant_frame().droplevel('i'), ['t', 'v'],
                           collapsing=['i'], table_name='cluster_features')


# ------------------------------- undeclared collapse stays LOUD (GH #323 core)
def test_undeclared_collapse_still_warns():
    # No aggregation: block -> the historical silent-data-loss path must keep
    # warning.  (Mali's roster, Kosovo's housing, ... rely on this.)
    entry = {'index': '(t, v)', 'Region': 'str', 'Rural': 'str'}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = _normalize_dataframe_index(_conflicting_frame(), entry,
                                         None, 'cluster_features')
    assert len(out) == 2
    assert [w for w in caught if 'GH #323' in str(w.message)], \
        'an UNDECLARED collapse must never go quiet'


def test_non_dedup_reducer_is_not_enforced():
    # `first` / `sum` are documentary hints (the forward-looking collapse());
    # only `dedup` asserts losslessness.  A `first` declaration must NOT
    # silently suppress the #323 warning.
    entry = {'index': '(t, v)', 'aggregation': {'i': 'first'},
             'Region': 'str', 'Rural': 'str'}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        _normalize_dataframe_index(_conflicting_frame(), entry, None,
                                   'cluster_features')
    assert [w for w in caught if 'GH #323' in str(w.message)]


# ------------------------------------------------------------------ Cambodia
def test_cambodia_declares_the_contract():
    ds = load_yaml(countries_root() / 'Cambodia' / '_' / 'data_scheme.yml')
    entry = ds['Data Scheme']['cluster_features']
    assert entry.get('aggregation') == {'i': 'dedup'}, (
        'Cambodia projects a 1512-row household-grain cover page onto 252 '
        'clusters; that collapse must be DECLARED, not silent (GH #323)'
    )


def test_cambodia_cluster_features_is_the_declared_projection():
    df = ll.Country('Cambodia').cluster_features()
    assert list(df.index.names) == ['t', 'v']
    assert len(df) == 252
    assert df.index.is_unique


def test_cambodia_contract_is_enforced_end_to_end(monkeypatch):
    """The guard must not be vacuous: poison one household and the build stops.

    Cambodia's collapse fires in ``Wave.cluster_features`` (before
    ``_normalize_dataframe_index`` ever sees the frame), so that is where the
    contract has to bite.  Give one household of one cluster a different Region
    -- the "cluster code reused across districts" failure -- and the build must
    RAISE instead of keeping one Region at random.
    """
    real_grab = Wave.grab_data

    def poisoned(self, request, *args, **kwargs):
        out = real_grab(self, request, *args, **kwargs)
        if request == 'cluster_features' and isinstance(out, pd.DataFrame) and len(out):
            out = out.copy()
            regions = out['Region'].astype(str)
            other = next(r for r in regions.unique() if r != regions.iloc[0])
            out['Region'] = regions
            out.iloc[0, out.columns.get_loc('Region')] = other
        return out

    monkeypatch.setattr(Wave, 'grab_data', poisoned)
    monkeypatch.setenv('LSMS_NO_CACHE', '1')       # the collapse is cold-path
    with pytest.raises(RuntimeError, match='VIOLATED'):
        Country('Cambodia').cluster_features()

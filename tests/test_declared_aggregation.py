"""GH #323: a non-unique DECLARED index must never be collapsed silently.

`_normalize_dataframe_index` used to reduce duplicate index tuples with a bare
`groupby().first()`, discarding the losing rows.  Where that reduction is
genuinely INTENDED -- the source is finer-grained than the table's contract --
it must be DECLARED in the country's data_scheme.yml `aggregation:` block and
ENFORCED by the framework, not fallen into.

Before this landed, `aggregation:` was read by no code at all (Malawi's block
even said so: "nothing reads this yet"), so declaring it was pure prose.  These
tests pin that it is now load-bearing.

The motivating case is South Africa `cluster_features`: its source STRATA2.dta is
a HOUSEHOLD-level file (8809 rows) but the table is CLUSTER-level (355 rows), so
each cluster's attributes repeat once per household.  The reduction is lossless
-- but only because the data happens to be clean.  The `unique` reducer makes
that a checked invariant.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.country import _normalize_dataframe_index


def _cluster_frame(regions, rurals):
    """Household-level frame (t, v, i) for a cluster-level (t, v) table."""
    idx = pd.MultiIndex.from_tuples(
        [('1993', 'c1', 'h1'), ('1993', 'c1', 'h2'), ('1993', 'c2', 'h3')],
        names=['t', 'v', 'i'],
    )
    return pd.DataFrame({'Region': regions, 'Rural': rurals}, index=idx)


SCHEME = {'index': '(t, v)', 'Region': 'str', 'Rural': 'str',
          'aggregation': {'i': 'unique'}}


def test_declared_unique_collapses_without_warning():
    """The repetitions collapse, and NO #323 warning fires: it is declared."""
    df = _cluster_frame(['1', '1', '2'], ['Urban', 'Urban', 'Rural'])
    with warnings.catch_warnings():
        warnings.simplefilter('error', RuntimeWarning)  # any #323 warning => failure
        out = _normalize_dataframe_index(df, SCHEME, None, 'cluster_features')
    assert list(out.index.names) == ['t', 'v']
    assert out.index.is_unique
    assert len(out) == 2, 'one row per cluster'
    assert out.loc[('1993', 'c1'), 'Region'] == '1'
    assert out.loc[('1993', 'c2'), 'Rural'] == 'Rural'


def test_declared_unique_RAISES_when_a_group_varies():
    """The whole point: a reduction that would lose information fails LOUDLY.

    Here cluster c1's two households disagree on Region.  `first` would quietly
    keep '1' and bin '9'.  `unique` must refuse.
    """
    df = _cluster_frame(['1', '9', '2'], ['Urban', 'Urban', 'Rural'])
    with pytest.raises(ValueError, match=r'(?s)unique.*vary.*Region'):
        _normalize_dataframe_index(df, SCHEME, None, 'cluster_features')


def test_undeclared_collapse_still_warns():
    """Backward compat: no policy => historical first() + the #323 warning."""
    df = _cluster_frame(['1', '9', '2'], ['Urban', 'Urban', 'Rural'])
    scheme = {'index': '(t, v)', 'Region': 'str', 'Rural': 'str'}
    with pytest.warns(RuntimeWarning, match='GH #323'):
        out = _normalize_dataframe_index(df, scheme, None, 'cluster_features')
    assert len(out) == 2


def test_declared_first_collapses_without_warning():
    df = _cluster_frame(['1', '9', '2'], ['Urban', 'Urban', 'Rural'])
    scheme = dict(SCHEME, aggregation={'i': 'first'})
    with warnings.catch_warnings():
        warnings.simplefilter('error', RuntimeWarning)
        out = _normalize_dataframe_index(df, scheme, None, 'cluster_features')
    assert out.loc[('1993', 'c1'), 'Region'] == '1'


def test_policy_naming_a_DECLARED_level_stays_inert():
    """`visit: first` on a table whose index declares `visit` must not fire.

    Nine countries carry exactly such a block on interview_date.  `visit` is in
    their canonical index, so nothing is ever collapsed and the policy must be a
    no-op -- activating the mechanism must not silently change their output.
    """
    idx = pd.MultiIndex.from_tuples(
        [('2010', 'h1', 1), ('2010', 'h1', 2)], names=['t', 'i', 'visit'])
    df = pd.DataFrame({'Int_t': ['2010-01-01', '2010-06-01']}, index=idx)
    scheme = {'index': '(t, i, visit)', 'Int_t': 'datetime',
              'aggregation': {'visit': 'first'}}
    out = _normalize_dataframe_index(df, scheme, None, 'interview_date')
    assert len(out) == 2, 'visit is declared; nothing collapses'
    assert list(out.index.names) == ['t', 'i', 'visit']


def test_unknown_reducer_is_a_config_error():
    df = _cluster_frame(['1', '1', '2'], ['Urban', 'Urban', 'Rural'])
    scheme = dict(SCHEME, aggregation={'i': 'median-ish'})
    with pytest.raises(ValueError, match='unknown aggregation reducer'):
        _normalize_dataframe_index(df, scheme, None, 'cluster_features')


def test_south_africa_declares_the_reduction():
    """The config itself must carry the policy -- not just the machinery."""
    import yaml
    from lsms_library.paths import countries_root

    class _L(yaml.SafeLoader):
        pass

    _L.add_multi_constructor('', lambda l, s, n: None)
    scheme = yaml.load(
        (countries_root() / 'South Africa' / '_' / 'data_scheme.yml').read_text(),
        Loader=_L,
    )['Data Scheme']['cluster_features']
    assert scheme['index'].replace(' ', '') == '(t,v)'
    assert scheme['aggregation'] == {'i': 'unique'}, (
        'South Africa cluster_features reads a household-level source into a '
        'cluster-level table; the reduction must be declared + asserted.'
    )

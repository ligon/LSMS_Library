"""Tests for the all-NaN-``v`` repair in ``Country._finalize_result``
(GH #172).

Some countries' ``household_roster`` parquets carry ``v`` in the
YAML-declared index but with all-NaN values (Guyana, Azerbaijan,
Serbia and Montenegro).  Pre-fix, ``_join_v_from_sample``'s
short-circuit treated this as "v already populated" and downstream
``roster_to_characteristics`` silently dropped every row.

This test verifies that ``_finalize_result``:
  - Strips the all-NaN ``v`` (whether in the index or a column),
  - Then re-invokes ``_join_v_from_sample`` to re-populate ``v``
    from ``sample()``.
"""
from unittest.mock import MagicMock, PropertyMock, patch

import numpy as np
import pandas as pd
import pytest

from lsms_library.country import Country


@pytest.fixture
def country():
    """A real Country whose ``sample`` is in ``data_scheme`` so the
    framework's v-join branch fires."""
    return Country('Uganda')


def _frame_with_nan_v_in_index():
    """Roster-shaped DataFrame: index ``(t, v, i)`` with v all-NaN."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', np.nan, 'h1'),
         ('2020', np.nan, 'h2'),
         ('2020', np.nan, 'h3')],
        names=['t', 'v', 'i'],
    )
    return pd.DataFrame({'Sex': ['M', 'F', 'M'], 'Age': [30, 25, 5]}, index=idx)


def _frame_with_nan_v_in_columns():
    """Roster-shaped DataFrame: index ``(t, i)`` with ``v`` as an
    all-NaN column."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'h1'), ('2020', 'h2'), ('2020', 'h3')],
        names=['t', 'i'],
    )
    return pd.DataFrame(
        {'v': [pd.NA, pd.NA, pd.NA], 'Sex': ['M', 'F', 'M']}, index=idx,
    )


def _frame_with_populated_v():
    """Roster-shaped DataFrame: index ``(t, v, i)`` with v actually populated."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'cluster_a', 'h1'), ('2020', 'cluster_a', 'h2')],
        names=['t', 'v', 'i'],
    )
    return pd.DataFrame({'Sex': ['M', 'F']}, index=idx)


def test_all_nan_v_in_index_triggers_rejoin(country):
    """When ``v`` is in the index but all-NaN, _finalize_result strips
    it and calls _join_v_from_sample."""
    df = _frame_with_nan_v_in_index()
    sentinel_after_join = pd.DataFrame({'Sex': ['M', 'F', 'M']})  # placeholder
    with patch.object(Country, '_join_v_from_sample',
                      return_value=sentinel_after_join) as mock_join:
        country._finalize_result(df, scheme_entry=None,
                                 method_name='household_roster')
    assert mock_join.called, "_join_v_from_sample should have been called"
    # The df passed to _join_v_from_sample should NOT have v in its index
    df_passed = mock_join.call_args[0][0]
    assert 'v' not in (df_passed.index.names or []), \
        "Stripped v should not appear in index passed to _join_v_from_sample"


def test_all_nan_v_in_columns_triggers_rejoin(country):
    """When ``v`` is a column with all-NaN values, same repair applies."""
    df = _frame_with_nan_v_in_columns()
    sentinel = pd.DataFrame({'Sex': ['M', 'F', 'M']})
    with patch.object(Country, '_join_v_from_sample',
                      return_value=sentinel) as mock_join:
        country._finalize_result(df, scheme_entry=None,
                                 method_name='household_roster')
    assert mock_join.called
    df_passed = mock_join.call_args[0][0]
    assert 'v' not in df_passed.columns, "Stripped v should not be a column"


def test_populated_v_skips_rejoin(country):
    """When ``v`` is already populated, _join_v_from_sample is NOT called
    (preserves the pre-existing skip behaviour for legacy scripts)."""
    df = _frame_with_populated_v()
    with patch.object(Country, '_join_v_from_sample') as mock_join:
        country._finalize_result(df, scheme_entry=None,
                                 method_name='household_roster')
    assert not mock_join.called, \
        "_join_v_from_sample should NOT be called when v is populated"


def test_partial_nan_v_skips_rejoin(country):
    """When ``v`` is partially populated (some NaN, some not), do NOT
    strip it — only fully-empty placeholder columns trigger the repair.
    Mixed-NaN is genuine data and should pass through."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'cluster_a', 'h1'),
         ('2020', np.nan, 'h2'),
         ('2020', 'cluster_b', 'h3')],
        names=['t', 'v', 'i'],
    )
    df = pd.DataFrame({'Sex': ['M', 'F', 'M']}, index=idx)
    with patch.object(Country, '_join_v_from_sample') as mock_join:
        country._finalize_result(df, scheme_entry=None,
                                 method_name='household_roster')
    assert not mock_join.called, \
        "Partial-NaN v is genuine data; should not trigger re-join"


def test_no_v_join_methods_unaffected(country):
    """Tables in ``_no_v_join`` (sample, cluster_features, etc.) never get
    a v-join, regardless of whether v looks all-NaN."""
    df = _frame_with_nan_v_in_index()
    with patch.object(Country, '_join_v_from_sample') as mock_join:
        country._finalize_result(df, scheme_entry=None, method_name='sample')
    assert not mock_join.called, \
        "sample table should never trigger v-join"

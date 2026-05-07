"""Tests for the generalized ``harmonize_<method_name>`` auto-dispatch
hook in :meth:`Country._finalize_result` (GH #180).

Verifies:
- The original assets behaviour (GH #168) is preserved.
- A categorical_mapping table named ``harmonize_<X>`` auto-applies to
  ``Country(...).<X>()`` output when ``j`` is in the index — generalising
  beyond just ``assets``.
- Absence of a matching ``harmonize_<X>`` table is a silent no-op.
- Tables present in ``data_scheme`` whose method_name does not match
  any ``harmonize_*`` table behave identically before and after.
"""
from unittest.mock import PropertyMock, patch

import numpy as np
import pandas as pd
import pytest

from lsms_library.country import Country


@pytest.fixture
def country():
    """A minimal Country stub.  The constructor reads disk, so we use any
    real country and stub out the categorical_mapping property per-test."""
    return Country('Uganda')


def _frame_with_j(j_values, index_extra=()):
    """Build a small DataFrame with ('j', ...) MultiIndex and a single 'v' col."""
    idx_names = ['j', *index_extra]
    tuples = [(j, *([0] * len(index_extra))) for j in j_values]
    idx = pd.MultiIndex.from_tuples(tuples, names=idx_names)
    return pd.DataFrame({'v': np.arange(len(j_values), dtype='float64')}, index=idx)


def _harmonize_table(rows):
    """Build a minimal harmonize_<X>-shaped DataFrame.

    Each row is (raw_label, preferred_label).  Mimics the categorical_mapping
    output shape: ``Original Label`` + ``Preferred Label`` columns.
    """
    return pd.DataFrame(rows, columns=['Original Label', 'Preferred Label'])


def test_assets_behaviour_preserved(country):
    """GH #168 regression: harmonize_assets still auto-applies to assets."""
    df = _frame_with_j(['Bicycle (raw)', 'Mobile (raw)', 'unmapped'])
    fake_mapping = {
        'harmonize_assets': _harmonize_table([
            ('Bicycle (raw)', 'Bicycle'),
            ('Mobile (raw)', 'Mobile Phone'),
        ]),
    }
    with patch.object(type(country), 'categorical_mapping',
                      new_callable=PropertyMock, return_value=fake_mapping):
        out = country._finalize_result(df, scheme_entry={}, method_name='assets')
    j_vals = list(out.index.get_level_values('j'))
    assert 'Bicycle' in j_vals
    assert 'Mobile Phone' in j_vals
    # Unmapped values pass through unchanged
    assert 'unmapped' in j_vals


def test_generalised_to_arbitrary_method_name(country):
    """A future ``harmonize_education`` table auto-applies to
    ``Country(X).education()`` output without code change."""
    df = _frame_with_j(['Pri (raw)', 'Sec (raw)'])
    fake_mapping = {
        'harmonize_education': _harmonize_table([
            ('Pri (raw)', 'Primary'),
            ('Sec (raw)', 'Secondary'),
        ]),
    }
    with patch.object(type(country), 'categorical_mapping',
                      new_callable=PropertyMock, return_value=fake_mapping):
        out = country._finalize_result(df, scheme_entry={}, method_name='education')
    j_vals = sorted(out.index.get_level_values('j'))
    assert j_vals == ['Primary', 'Secondary']


def test_no_matching_harmonize_table_is_noop(country):
    """If no ``harmonize_<method_name>`` table exists, the hook silently
    does nothing — original j values pass through."""
    df = _frame_with_j(['Foo', 'Bar'])
    fake_mapping = {
        # harmonize_assets present but method_name is something else
        'harmonize_assets': _harmonize_table([('Foo', 'X'), ('Bar', 'Y')]),
    }
    with patch.object(type(country), 'categorical_mapping',
                      new_callable=PropertyMock, return_value=fake_mapping):
        out = country._finalize_result(df, scheme_entry={}, method_name='cluster_features')
    j_vals = sorted(out.index.get_level_values('j'))
    assert j_vals == ['Bar', 'Foo']


def test_missing_preferred_label_column_skipped(country):
    """A ``harmonize_<X>`` table without a ``Preferred Label`` column is
    skipped (e.g. malformed user table)."""
    df = _frame_with_j(['Foo', 'Bar'])
    bad = pd.DataFrame([('Foo', 'X')], columns=['Original Label', 'Other'])
    fake_mapping = {'harmonize_assets': bad}
    with patch.object(type(country), 'categorical_mapping',
                      new_callable=PropertyMock, return_value=fake_mapping):
        out = country._finalize_result(df, scheme_entry={}, method_name='assets')
    j_vals = sorted(out.index.get_level_values('j'))
    # Skipped silently; raw values pass through
    assert j_vals == ['Bar', 'Foo']


def test_no_j_in_index_skipped(country):
    """The hook only fires when ``j`` is in the MultiIndex."""
    idx = pd.MultiIndex.from_tuples([('A', 0), ('B', 1)], names=['t', 'i'])
    df = pd.DataFrame({'v': [1.0, 2.0]}, index=idx)
    fake_mapping = {'harmonize_assets': _harmonize_table([('A', 'X')])}
    with patch.object(type(country), 'categorical_mapping',
                      new_callable=PropertyMock, return_value=fake_mapping):
        out = country._finalize_result(df, scheme_entry={}, method_name='assets')
    # No 'j' level → no rename; original index unchanged
    assert list(out.index.get_level_values('t')) == ['A', 'B']

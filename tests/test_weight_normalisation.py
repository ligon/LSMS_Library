"""Unit tests for within-wave sampling-weight normalisation.

Data-free by design: these exercise the pure helper
``lsms_library.country._normalise_sample_weights`` on hand-built frames, so
they run in any checkout regardless of which microdata is available.  The
corpus-level assertions live in ``tests/test_sample.py``.

The decision under test: for each ``(country, wave)``, divide ``weight`` and
``panel_weight`` by that wave's own mean over non-null values.  No threshold,
no configuration — a column that is already mean-1 is divided by 1.0000 and is
unchanged; an expansion column becomes mean-1.
"""

import numpy as np
import pandas as pd
import pytest

from lsms_library.country import (
    _normalise_sample_weights,
    WeightNormalisationWarning,
)


def _frame(rows, columns=('weight',)):
    """rows: list of (i, t, *values)."""
    idx = pd.MultiIndex.from_tuples([(r[0], r[1]) for r in rows], names=['i', 't'])
    data = {c: [r[2 + k] for r in rows] for k, c in enumerate(columns)}
    return pd.DataFrame(data, index=idx, dtype='float64')


# ---------------------------------------------------------------------------
# The core rule
# ---------------------------------------------------------------------------

def test_expansion_weights_become_mean_one():
    df = _frame([('a', '2010', 100.0), ('b', '2010', 300.0), ('c', '2010', 200.0)])
    out = _normalise_sample_weights(df, country='Testland')
    assert out['weight'].mean() == pytest.approx(1.0, abs=1e-15)
    # relative magnitudes preserved exactly
    assert out['weight'].tolist() == pytest.approx([0.5, 1.5, 1.0])


def test_already_normalised_weights_are_unchanged():
    """The division is a no-op on a column that already has mean 1."""
    df = _frame([('a', '2010', 0.5), ('b', '2010', 1.5), ('c', '2010', 1.0)])
    before = df['weight'].copy()
    out = _normalise_sample_weights(df, country='Testland')
    pd.testing.assert_series_equal(out['weight'], before, rtol=1e-15, atol=0)


def test_each_wave_normalised_by_its_own_mean():
    """The mixed-scale case: one wave expansion, one already normalised."""
    df = _frame([('a', '1985', 0.8), ('b', '1985', 1.2),        # mean 1
                 ('c', '2018', 4000.0), ('d', '2018', 6000.0)])  # mean 5000
    out = _normalise_sample_weights(df, country='CotedIvoire-ish')
    for wave in ('1985', '2018'):
        w = out.xs(wave, level='t')['weight']
        assert w.mean() == pytest.approx(1.0, abs=1e-12), wave
    assert out['weight'].tolist() == pytest.approx([0.8, 1.2, 0.8, 1.2])


def test_idempotent():
    df = _frame([('a', '2010', 100.0), ('b', '2010', 300.0), ('c', '2011', 7.0)])
    once = _normalise_sample_weights(df.copy(), country='T')
    twice = _normalise_sample_weights(once.copy(), country='T')
    pd.testing.assert_frame_equal(once, twice, rtol=1e-15, atol=0)


def test_weighted_ratio_is_invariant():
    """The guarantee that makes this safe: shares do not move."""
    df = _frame([('a', '2010', 100.0, 1.0), ('b', '2010', 300.0, 0.0),
                 ('c', '2010', 200.0, 1.0)], columns=('weight', 'Rural'))
    w0, x = df['weight'].copy(), df['Rural']
    share0 = (w0 * x).sum() / w0.sum()
    out = _normalise_sample_weights(df, country='T')
    share1 = (out['weight'] * x).sum() / out['weight'].sum()
    assert share0 == pytest.approx(share1, abs=1e-15)


def test_panel_weight_uses_its_own_non_null_mean():
    """panel_weight is NaN for refreshment households; those rows must not
    enter the divisor, and `weight` must be normalised independently."""
    df = _frame([('a', '2010', 100.0, 50.0),
                 ('b', '2010', 300.0, 150.0),
                 ('c', '2010', 200.0, np.nan)],   # refreshment
                columns=('weight', 'panel_weight'))
    out = _normalise_sample_weights(df, country='T')
    assert out['weight'].mean() == pytest.approx(1.0)
    pw = out['panel_weight'].dropna()
    assert len(pw) == 2 and pw.mean() == pytest.approx(1.0)
    assert pd.isna(out['panel_weight'].iloc[2])


def test_single_household_wave():
    df = _frame([('a', '2010', 8658.97)])
    out = _normalise_sample_weights(df, country='T')
    assert out['weight'].tolist() == pytest.approx([1.0])


# ---------------------------------------------------------------------------
# Edge cases: no division by NaN, 0 or a negative
# ---------------------------------------------------------------------------

def test_all_null_column_is_skipped_silently():
    """An all-null weight column is normal (GhanaLSS GLSS1-4) — no divisor
    exists, and it must neither warn nor produce NaN-by-division."""
    import warnings as _w
    df = _frame([('a', '2010', np.nan), ('b', '2010', np.nan)])
    with _w.catch_warnings(record=True) as rec:
        _w.simplefilter('always')
        out = _normalise_sample_weights(df, country='T')
    assert [r for r in rec
            if issubclass(r.category, WeightNormalisationWarning)] == []
    assert out['weight'].isna().all()


def test_zero_mean_wave_is_left_alone_and_warns():
    """Never emit inf."""
    df = _frame([('a', '2010', 0.0), ('b', '2010', 0.0),
                 ('c', '2011', 500.0)])
    with pytest.warns(WeightNormalisationWarning, match='zero, negative or non-finite'):
        out = _normalise_sample_weights(df, country='T')
    w = out['weight']
    assert np.isfinite(w).all()
    assert w.xs('2010', level='t').tolist() == [0.0, 0.0]   # untouched
    assert w.xs('2011', level='t').tolist() == pytest.approx([1.0])  # still done


def test_negative_weights_warn_and_are_reported():
    df = _frame([('a', '2010', -5.0), ('b', '2010', 15.0)])
    with pytest.warns(WeightNormalisationWarning, match='1 negative value'):
        out = _normalise_sample_weights(df, country='T')
    # mean is +5 > 0, so the rescale proceeds and the sign is preserved
    assert out['weight'].tolist() == pytest.approx([-1.0, 3.0])


def test_negative_mean_wave_is_left_alone():
    df = _frame([('a', '2010', -30.0), ('b', '2010', 10.0)])
    with pytest.warns(WeightNormalisationWarning):
        out = _normalise_sample_weights(df, country='T')
    assert out['weight'].tolist() == [-30.0, 10.0]


def test_zero_weights_within_a_positive_wave_survive_as_zero():
    df = _frame([('a', '2010', 0.0), ('b', '2010', 200.0), ('c', '2010', 100.0)])
    out = _normalise_sample_weights(df, country='T')
    assert out['weight'].tolist() == pytest.approx([0.0, 2.0, 1.0])
    assert out['weight'].mean() == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Shape / dtype / attrs contracts
# ---------------------------------------------------------------------------

def test_no_weight_columns_is_a_noop():
    df = pd.DataFrame({'v': ['1', '2']},
                      index=pd.MultiIndex.from_tuples([('a', '2010'), ('b', '2010')],
                                                      names=['i', 't']))
    out = _normalise_sample_weights(df, country='T')
    assert out is df and list(out.columns) == ['v']


def test_t_as_a_column_still_groups_by_wave():
    """Falling back to a whole-frame mean when `t` is a COLUMN would pool every
    wave into one divisor -- the exact bug this function prevents."""
    df = pd.DataFrame({'t': ['2010', '2010', '2018', '2018'],
                       'weight': [100.0, 300.0, 4000.0, 6000.0]},
                      index=pd.Index(['a', 'b', 'c', 'd'], name='i'))
    out = _normalise_sample_weights(df, country='T')
    assert out['weight'].tolist() == pytest.approx([0.5, 1.5, 0.8, 1.2])


def test_no_t_level_normalises_over_the_whole_frame():
    df = pd.DataFrame({'weight': [100.0, 300.0]},
                      index=pd.Index(['a', 'b'], name='i'))
    out = _normalise_sample_weights(df, country='T')
    assert out['weight'].mean() == pytest.approx(1.0)


def test_nullable_float_dtype_is_preserved():
    df = _frame([('a', '2010', 100.0), ('b', '2010', 300.0)])
    df['weight'] = df['weight'].astype('Float64')
    out = _normalise_sample_weights(df, country='T')
    assert isinstance(out['weight'].dtype, pd.Float64Dtype)
    assert float(out['weight'].mean()) == pytest.approx(1.0)


def test_integer_weights_are_not_rounded_back_to_int():
    """An int weight column divided by its mean must stay fractional."""
    df = _frame([('a', '2010', 100.0), ('b', '2010', 300.0)])
    df['weight'] = df['weight'].astype('Int64')
    out = _normalise_sample_weights(df, country='T')
    assert out['weight'].tolist() == pytest.approx([0.5, 1.5])


def test_attrs_survive():
    """`id_converted` must not be dropped — see CLAUDE.md §Panel ID chains."""
    df = _frame([('a', '2010', 100.0)])
    df.attrs['id_converted'] = True
    df.attrs['country'] = 'T'
    out = _normalise_sample_weights(df, country='T')
    assert out.attrs.get('id_converted') is True
    assert out.attrs.get('country') == 'T'


def test_nan_wave_label_rows_are_grouped_not_dropped():
    df = _frame([('a', np.nan, 100.0), ('b', np.nan, 300.0),
                 ('c', '2010', 7.0)])
    out = _normalise_sample_weights(df, country='T')
    assert out['weight'].tolist() == pytest.approx([0.5, 1.5, 1.0])


def test_row_count_and_index_unchanged():
    df = _frame([('a', '2010', 100.0), ('b', '2010', np.nan), ('c', '2011', 3.0)])
    idx_before = df.index.copy()
    out = _normalise_sample_weights(df, country='T')
    assert len(out) == 3
    assert out.index.equals(idx_before)

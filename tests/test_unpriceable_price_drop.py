"""The 0 / inf / NaN Price drop in food_prices_from_acquired must be LOUD.

GH #591.  Before this landed, ``food_prices_from_acquired`` ended with

    v = v[['Price']].replace([0, np.inf, -np.inf], np.nan).dropna()

which is how six Nigeria waves shipped a food_prices table built on 0.5%
of the data without anyone noticing: a row whose Expenditure was recorded
against a ZERO Quantity produced ``Price = inf`` and was then *deleted*.
It did not come back as NaN (which a user would see) -- it ceased to exist.

These tests pin two things:
  1. the drop still removes exactly the rows it always did (no behavior
     change for the 15+ countries with a small, genuine unpriceable tail);
  2. it is now accounted for -- a per-call aggregated warning above
     PRICE_LOSS_WARN_THRESHOLD, plus a machine-readable tally on
     ``.attrs['price_rows_dropped']``.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    PRICE_LOSS_WARN_THRESHOLD,
    UnpriceableRowsWarning,
    food_prices_from_acquired,
)


def _fa(rows):
    """rows: (i, j, u, s, Quantity, Expenditure) -> canonical food_acquired."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', r[0], r[1], r[2], r[3]) for r in rows],
        names=['t', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': [r[4] for r in rows],
                         'Expenditure': [r[5] for r in rows]}, index=idx)


def _sentinel_frame(n_bad=9, n_good=1):
    """The Nigeria signature: Expenditure > 0 sitting on a ZERO Quantity.

    n_bad rows are unpriceable (Price = inf); n_good are fine.  Defaults give
    a 90% loss -- far above the 5% threshold.
    """
    rows = [(f'h{k}', 'rice', 'kg', 'purchased', 0.0, 500.0)
            for k in range(n_bad)]
    rows += [(f'g{k}', 'rice', 'kg', 'purchased', 10.0, 500.0)
             for k in range(n_good)]
    return _fa(rows)


class TestLoudness:
    def test_zero_quantity_with_expenditure_warns(self):
        """The exact defect from #591 must produce a warning, not silence."""
        with pytest.warns(UnpriceableRowsWarning, match=r'dropped 9 of 10'):
            food_prices_from_acquired(_sentinel_frame())

    def test_warning_names_inf_as_the_cause(self):
        with pytest.warns(UnpriceableRowsWarning) as rec:
            food_prices_from_acquired(_sentinel_frame())
        msg = str(rec[0].message)
        assert 'inf (9)' in msg
        assert 'ZERO Quantity' in msg

    def test_exactly_one_warning_per_call_not_one_per_row(self):
        """1,000 bad rows -> ONE warning.  Per-row warnings would be unusable
        noise and would be filtered away, which is how this stayed invisible."""
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter('always')
            food_prices_from_acquired(_sentinel_frame(n_bad=1000, n_good=100))
        assert sum(issubclass(w.category, UnpriceableRowsWarning)
                   for w in rec) == 1

    def test_tally_is_machine_readable(self):
        with pytest.warns(UnpriceableRowsWarning):
            out = food_prices_from_acquired(_sentinel_frame())
        tally = out.attrs['price_rows_dropped']
        assert tally['inf'] == 9
        assert tally['expected'] == 10
        assert tally['dropped_expected'] == 9
        assert tally['lost_fraction'] == pytest.approx(0.9)

    def test_small_genuine_tail_stays_quiet(self):
        """A 1-in-100 unpriceable row is the ordinary long tail (EHCVM,
        Uganda, Serbia ...) -- it must NOT warn, or the signal drowns."""
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter('always')
            food_prices_from_acquired(_sentinel_frame(n_bad=1, n_good=99))
        assert not any(issubclass(w.category, UnpriceableRowsWarning)
                       for w in rec)

    def test_threshold_is_five_percent(self):
        assert PRICE_LOSS_WARN_THRESHOLD == 0.05


class TestDropSemanticsUnchanged:
    """The set of rows dropped must be IDENTICAL to the pre-#591 expression,
    so no other country's food_prices shifts by a single row."""

    def test_same_rows_as_legacy_expression(self):
        fa = _fa([
            ('h1', 'rice',  'kg', 'purchased', 10.0, 500.0),   # fine
            ('h2', 'rice',  'kg', 'purchased',  0.0, 500.0),   # inf  -> drop
            ('h3', 'rice',  'kg', 'purchased', 10.0,   0.0),   # zero -> drop
            ('h4', 'rice',  'kg', 'purchased', np.nan, 500.0),  # NaN -> drop
            ('h5', 'maize', 'kg', 'produced',  10.0, np.nan),  # NaN -> drop
            ('h6', 'maize', 'kg', 'purchased',  4.0, 100.0),   # fine
        ])
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UnpriceableRowsWarning)
            out = food_prices_from_acquired(fa, units='unitvalue')

        legacy = (fa.assign(Price=fa['Expenditure'] / fa['Quantity'])[['Price']]
                  .replace([0, np.inf, -np.inf], np.nan).dropna()
                  .groupby(['t', 'i', 'j', 'u', 's']).median())
        pd.testing.assert_frame_equal(out.sort_index(), legacy.sort_index())

    def test_negative_inf_also_dropped(self):
        fa = _fa([('h1', 'rice', 'kg', 'purchased',  0.0, -500.0),  # -inf
                  ('h2', 'rice', 'kg', 'purchased', 10.0,  500.0)])
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UnpriceableRowsWarning)
            out = food_prices_from_acquired(fa, units='unitvalue')
        assert len(out) == 1
        assert np.isfinite(out['Price']).all()

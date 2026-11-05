"""A near-zero quantity and an impossible price are named, never changed (GH #1493).

Site Q reported only the upper tail of `food_acquired.Quantity`, so Senegal's
quantities near 1e-19 passed and put 1e22 in `food_prices`, which nothing screened.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.quantity_audit import (
    QUANTITY_LOW_REFERENCE_QUANTILE,
    QUANTITY_MIN_CELL,
    QUANTITY_OUTLIER_K,
    QuantityImplausibleError,
    QuantityImplausibleWarning,
    _clear_quantity_reports,
    audit_near_zero,
    check_quantities,
    quantity_reports,
)


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.delenv('LSMS_QUANTITY_STRICT', raising=False)
    _clear_quantity_reports()
    yield
    _clear_quantity_reports()


def _frame(values, column='Quantity', t='w1', u='Kg', j='Rice'):
    index = pd.MultiIndex.from_arrays(
        [[t] * len(values), [f'h{k}' for k in range(len(values))], [j] * len(values),
         [u] * len(values)], names=['t', 'i', 'j', 'u'])
    return pd.DataFrame({column: np.asarray(values, dtype=float)}, index=index)


def _typical(n=40, value=2.0):
    return [value] * n


class TestTheLowerTail:
    def test_a_float_residue_is_named(self):
        reports = audit_near_zero(_frame([4e-19] + _typical()), 'Quantity',
                                  country='Testland', table='food_acquired')
        assert len(reports) == 1
        report = reports[0]
        assert report['direction'] == 'low' and report['n_implausible'] == 1
        offender = report['offenders'][0]
        assert offender['i'] == 'h0' and offender['value'] == 4e-19
        assert offender['reference'] == 2.0
        assert offender['ratio'] == pytest.approx(2.0 / 4e-19, rel=1e-6)

    def test_a_value_inside_the_tolerance_is_not(self):
        inside = 2.0 / QUANTITY_OUTLIER_K * 1.01
        assert audit_near_zero(_frame([inside] + _typical()), 'Quantity',
                               country='T', table='food_acquired') == []

    def test_a_zero_records_none_not_almost_none(self):
        assert audit_near_zero(_frame([0.0] * 5 + _typical()), 'Quantity',
                               country='T', table='food_acquired') == []

    def test_a_zero_does_not_set_the_scale(self):
        """Forty zeros below forty twos would otherwise put p10 at zero."""
        values = [0.0] * 40 + _typical() + [1e-9]
        got = audit_near_zero(_frame(values), 'Quantity', country='T',
                              table='food_acquired')
        assert got[0]['offenders'][0]['reference'] == 2.0

    def test_a_negative_is_not_judged(self):
        assert audit_near_zero(_frame([-1.0] + _typical()), 'Quantity',
                               country='T', table='food_acquired') == []

    def test_a_thin_cell_is_not_judged(self):
        values = [1e-9] + _typical(QUANTITY_MIN_CELL - 2)
        assert audit_near_zero(_frame(values), 'Quantity', country='T',
                               table='food_acquired') == []

    def test_the_quantile_is_robust_to_a_few_contaminated_rows(self):
        """Two more residues in 42 rows leave p10 on the ordinary rows."""
        values = [1e-9, 1e-9, 1e-9] + _typical(40)
        got = audit_near_zero(_frame(values), 'Quantity', country='T',
                              table='food_acquired')
        assert got[0]['n_implausible'] == 3
        assert QUANTITY_LOW_REFERENCE_QUANTILE == 0.10

    def test_a_frame_without_the_column_is_silent(self):
        assert audit_near_zero(_frame(_typical()), 'Other', country='T',
                               table='food_acquired') == []


class TestCheckQuantities:
    def test_it_returns_its_input_untouched(self):
        df = _frame([4e-19] + _typical())
        before = df.copy()
        with pytest.warns(QuantityImplausibleWarning):
            got = check_quantities(df, country='T', table='food_acquired')
        assert got is df
        pd.testing.assert_frame_equal(df, before)

    @pytest.mark.parametrize('table', ['food_acquired', 'crop_production'])
    def test_a_near_zero_quantity_warns_and_files(self, table):
        with pytest.warns(QuantityImplausibleWarning, match='below the 10th percentile'):
            check_quantities(_frame([4e-19] + _typical()), country='T', table=table)
        got = quantity_reports('T', table)
        assert [r['direction'] for r in got] == ['low']

    def test_a_small_plot_is_a_plot(self):
        df = _frame([1e-6] + _typical(), column='Area')
        with warnings.catch_warnings():
            warnings.simplefilter('error', QuantityImplausibleWarning)
            check_quantities(df, country='T', table='plot_features')

    def test_an_unscreened_table_costs_a_lookup(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error', QuantityImplausibleWarning)
            check_quantities(_frame([4e-19] + _typical()), country='T', table='housing')

    def test_the_lever_is_the_quantity_lever(self, monkeypatch):
        monkeypatch.setenv('LSMS_QUANTITY_STRICT', '1')
        with pytest.raises(QuantityImplausibleError):
            check_quantities(_frame([4e-19] + _typical()), country='T',
                             table='food_acquired')

    def test_the_message_says_nothing_changed(self):
        with pytest.warns(QuantityImplausibleWarning, match='NOTHING HAS BEEN CHANGED'):
            check_quantities(_frame([4e-19] + _typical()), country='T',
                             table='food_acquired')

    def test_the_upper_tail_still_fires_alongside_it(self):
        with pytest.warns(QuantityImplausibleWarning):
            check_quantities(_frame([4e-19, 1e6] + _typical()), country='T',
                             table='food_acquired')
        assert {r['direction'] for r in quantity_reports('T', 'food_acquired')} == {
            'low', 'high'}


class TestFoodPrices:
    def test_an_impossible_price_is_named_under_its_own_table(self):
        with pytest.warns(QuantityImplausibleWarning, match="'Price'"):
            check_quantities(_frame([1.4e22] + _typical(40, 500.0), column='Price'),
                             country='T', table='food_prices')
        got = quantity_reports('T', 'food_prices')
        assert len(got) == 1 and got[0]['direction'] == 'high'
        assert got[0]['offenders'][0]['value'] == 1.4e22

    def test_a_low_price_is_not_screened(self):
        """It is a high quantity, and the upper rule names that."""
        df = _frame([1e-6] + _typical(40, 500.0), column='Price')
        with warnings.catch_warnings():
            warnings.simplefilter('error', QuantityImplausibleWarning)
            check_quantities(df, country='T', table='food_prices')


@pytest.mark.requires_s3
def test_senegal_names_its_residues_and_its_prices():
    import lsms_library as ll
    _clear_quantity_reports()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        served = ll.Country('Senegal').food_prices()
    assert served['Price'].astype(float).max() > 1e21
    prices = [o for r in quantity_reports('Senegal', 'food_prices') for o in r['offenders']]
    assert sum(o['value'] > 1e21 for o in prices) == 2
    low = [o for r in quantity_reports('Senegal', 'food_acquired')
           if r['direction'] == 'low' for o in r['offenders']]
    assert sum(o['value'] < 1e-9 for o in low) == 3

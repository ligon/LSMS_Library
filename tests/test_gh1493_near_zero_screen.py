"""A near-zero quantity and an impossible price are named, never changed (GH #1493).

Site Q reported only the upper tail of `food_acquired.Quantity`, so Senegal's
quantities near 1e-19 passed and put 1e22 in `food_prices`, which nothing screened.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library.local_tools import to_parquet
from lsms_library.paths import data_root
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

    def test_the_low_tail_message_does_not_blame_one_cause(self):
        """A float residue is 3 of 1,201 rows, so the message must not say it is
        the answer; and it must not call a small amount an error."""
        with pytest.warns(QuantityImplausibleWarning) as caught:
            check_quantities(_frame([1e-19] + _typical()), country='T', table='food_acquired')
        message = str(caught[0].message)
        assert "'Value' unit" in message and 'gram or millilitre' in message
        assert 'may be real' in message
        assert 'have each been the answer' not in message

    def test_the_upper_tail_message_keeps_its_causes(self):
        with pytest.warns(QuantityImplausibleWarning) as caught:
            check_quantities(_frame([1e6] + _typical()), country='T', table='food_acquired')
        assert 'have each been the answer' in str(caught[0].message)

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
def _food_frame(y, *, tiny=False, dear=False):
    """A wave of ordinary rice, with one quantity near zero or one huge outlay."""
    rows = [(y, f'{y}h{k}', 'Rice', 'Kg', 'purchased', 2.0, 100.0) for k in range(40)]
    rows += [(y, f'{y}t', 'Rice', 'Kg', 'purchased', 1e-19 if tiny else 2.0,
              1e12 if dear else 100.0)]
    out = pd.DataFrame(rows, columns=['t', 'i', 'j', 'u', 's', 'Quantity', 'Expenditure'])
    return out.set_index(['t', 'i', 'j', 'u', 's'])


class TestTheFoodDispatch:
    """The strict error is a RuntimeError, which the dispatch catches for its legacy
    path, so a bare `food_prices()` said "no wave-level build succeeded"."""

    @pytest.fixture
    def country(self, tmp_path, monkeypatch):
        """Malawi's registry over controlled frames; `_food.kw` sets the defect."""
        root = tmp_path / 'data'
        monkeypatch.setenv('LSMS_DATA_DIR', str(root))
        monkeypatch.setenv('LSMS_BUILD_BACKEND', 'dvc')
        monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
        monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
        monkeypatch.delenv('LSMS_GRAIN_STRICT', raising=False)
        data_root.cache_clear()
        monkeypatch.setattr(C.Country, '_join_v_from_sample', lambda self, df, *a, **k: df)
        defects = {}

        def grab(wave, request, *, _with_origin=False):
            out = _food_frame(wave.year, **defects)
            path = root / 'Malawi' / wave.year / '_' / 'food_acquired.parquet'
            path.parent.mkdir(parents=True, exist_ok=True)
            to_parquet(out, path, absolute_path=True)
            return (out, 'parquet') if _with_origin else out

        monkeypatch.setattr(C.Wave, 'grab_data', grab)
        c = C.Country('Malawi', preload_panel_ids=False)
        c._panel_ids_attempted = True
        c._updated_ids_cache = {}
        try:
            yield c, defects
        finally:
            data_root.cache_clear()

    @pytest.mark.parametrize('table', ['food_prices', 'food_quantities', 'food_expenditures'])
    def test_a_near_zero_quantity_fails_for_what_it_is(self, country, monkeypatch, table):
        c, defects = country
        defects['tiny'] = True
        monkeypatch.setenv('LSMS_QUANTITY_STRICT', '1')
        with pytest.raises(QuantityImplausibleError, match='food_acquired'):
            getattr(c, table)()

    def test_an_impossible_price_fails_for_what_it_is(self, country, monkeypatch):
        """`food_acquired` is clean here, so this is `food_prices`' own screen."""
        c, defects = country
        defects['dear'] = True
        with pytest.warns(QuantityImplausibleWarning, match='food_prices'):
            assert len(c.food_prices())
        monkeypatch.setenv('LSMS_QUANTITY_STRICT', '1')
        with pytest.raises(QuantityImplausibleError, match='food_prices'):
            C.Country('Malawi', preload_panel_ids=False).food_prices()

    def test_it_still_warns_and_serves_without_the_lever(self, country):
        c, defects = country
        defects['tiny'] = True
        with pytest.warns(QuantityImplausibleWarning):
            assert len(c.food_prices())

    def test_another_failure_keeps_the_legacy_route(self, country, monkeypatch):
        """Only this class leaves the catch.  A bare call to a table the legacy path
        can answer still gets that chance, and warns why."""
        c, _ = country
        # GH #1516: the fallback is only for a country whose legacy path can build the table.
        monkeypatch.setattr(C.Country, '_has_country_builder', lambda self, table: True)
        real = C.Country._aggregate_wave_data
        seen = []

        def boom(self, waves=None, method_name=None, **kw):
            seen.append(method_name)
            if method_name == 'food_acquired':
                raise RuntimeError('source unavailable')
            return real(self, waves, method_name, **kw)

        monkeypatch.setattr(C.Country, '_aggregate_wave_data', boom)
        with pytest.raises(Exception) as caught:
            c.food_prices()
        assert not isinstance(caught.value, QuantityImplausibleError)
        assert 'food_prices' in seen


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

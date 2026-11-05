"""The ``survey_kg`` rung of the crop ladder: ``crop_production.Quantity_kg``.

GH #1436, ruling @ligon 2026-11-04 (option A).  ``Quantity_kg`` is the survey's
kilograms for the LINE, a total; ``KgFactor`` is kilograms per ONE unit.  These
tests pin the rule on synthetic frames, so they run without data.  What each
country's column holds is tested against its source in
``test_niger_crop_survey_kg.py`` and ``test_mali_crop_survey_kg.py``.
"""
import numpy as np
import pandas as pd
import pytest

from lsms_library import quantity_audit
from lsms_library.transformations import (KG_FACTOR_LAYERS, harvest_kg,
                                          harvest_kg_factors)

INDEX = ['t', 'i', 'plot_id', 'j', 'u']


def _frame(rows, **columns):
    """``rows`` are ``(i, plot_id, j, u, Quantity, Quantity_kg)``; ``t`` is one wave."""
    df = pd.DataFrame(rows, columns=['i', 'plot_id', 'j', 'u', 'Quantity', 'Quantity_kg'])
    df.insert(0, 't', '2021-22')
    df = df.set_index(INDEX).astype('float64')
    for name, values in columns.items():
        df[name] = values
    return df


def _kg(frame):
    """``Harvest_kg`` as ``{(i, plot_id, j): kg}``."""
    return {key[1:]: value for key, value in harvest_kg(frame)['Harvest_kg'].items()}


def test_the_layer_is_first_and_the_partition_still_holds():
    assert KG_FACTOR_LAYERS[0] == 'survey_kg'
    df = _frame([('a', '1_1', 'Mil', 'Tiya', 10.0, 25.0),
                 ('b', '1_1', 'Mil', 'Tiya', 10.0, np.nan),
                 ('c', '1_1', 'Mil', 'Kg', 10.0, np.nan),
                 ('d', '1_1', 'Mil', 'Kg', 10.0, 0.0)])
    counts = harvest_kg_factors(df).attrs['kg_factor_sources']
    assert sum(counts[layer] for layer in KG_FACTOR_LAYERS) == len(df)
    assert counts['survey_kg'] == 1
    assert counts['metric'] == 2
    assert counts['none'] == 1
    assert counts['survey_kg_zero'] == 1


def test_a_frame_without_the_column_resolves_exactly_as_before():
    """The rung is inert without the column: every other layer's answer stands."""
    with_na = _frame([('a', '1_1', 'Mil', 'Kg', 10.0, np.nan),
                      ('b', '1_1', 'Mil', 'Tiya', 10.0, np.nan)],
                     KgFactor=[np.nan, 2.0])
    without = with_na.drop(columns='Quantity_kg')
    a = harvest_kg_factors(with_na)
    b = harvest_kg_factors(without)
    pd.testing.assert_frame_equal(a, b)
    assert a.attrs == b.attrs
    pd.testing.assert_frame_equal(harvest_kg(with_na), harvest_kg(without))


def test_the_total_is_served_and_the_implied_factor_is_disclosed():
    df = _frame([('a', '1_1', 'Mil', 'Tiya', 10.0, 25.0)])
    f = harvest_kg_factors(df)
    assert f['KgFactorSource'].iloc[0] == 'survey_kg'
    assert f['kg_per_unit'].iloc[0] == pytest.approx(2.5)
    assert f['kg_survey'].iloc[0] == pytest.approx(2.5)
    assert _kg(df) == {('a', '1_1', 'Mil'): 25.0}


def test_it_outranks_a_reported_factor_and_a_metric_label():
    """Read first, as ruled, including where the survey disagrees with its label."""
    df = _frame([('a', '1_1', 'Mil', 'Sac de 100kg', 2.0, 150.0),
                 ('b', '1_1', 'Mil', 'Sac de 100kg', 2.0, np.nan)],
                KgFactor=[90.0, 90.0])
    f = harvest_kg_factors(df)
    assert f['KgFactorSource'].tolist() == ['survey_kg', 'reported']
    assert _kg(df) == {('a', '1_1', 'Mil'): 150.0, ('b', '1_1', 'Mil'): 180.0}


@pytest.mark.parametrize('quantity', [np.nan, 0.0])
def test_a_total_beside_no_quantity_still_delivers(quantity):
    """Niger 3E has two lines stating kilograms beside a zero, and one beside a code."""
    df = _frame([('a', '1_1', 'Mil', 'Tiya', quantity, 40.0)])
    f = harvest_kg_factors(df)
    assert f['KgFactorSource'].iloc[0] == 'survey_kg'
    assert np.isnan(f['kg_survey'].iloc[0])
    assert _kg(df) == {('a', '1_1', 'Mil'): 40.0}


def test_the_missing_unit_sentinel_keeps_its_own_total():
    """A total needs no unit, and it pools with nothing."""
    df = _frame([('a', '1_1', 'Mil', 'Unknown', 10.0, 40.0)])
    assert harvest_kg_factors(df)['KgFactorSource'].iloc[0] == 'survey_kg'
    assert _kg(df) == {('a', '1_1', 'Mil'): 40.0}


@pytest.mark.parametrize('recorded', [0.0, -5.0, np.inf])
def test_an_unusable_total_falls_through(recorded):
    """A 0 beside a positive quantity states no mass.  Nothing rewrites the column."""
    df = _frame([('a', '1_1', 'Mil', 'Kg', 10.0, recorded)])
    f = harvest_kg_factors(df)
    assert f['KgFactorSource'].iloc[0] == 'metric'
    assert f['kg_per_unit'].iloc[0] == 1.0
    assert _kg(df) == {('a', '1_1', 'Mil'): 10.0}
    assert df['Quantity_kg'].iloc[0] == recorded


def test_the_counted_zero_is_only_a_finite_non_positive_total():
    df = _frame([('a', '1_1', 'Mil', 'Kg', 10.0, 0.0),
                 ('b', '1_1', 'Mil', 'Kg', 10.0, -1.0),
                 ('c', '1_1', 'Mil', 'Kg', 10.0, np.inf),
                 ('d', '1_1', 'Mil', 'Kg', 10.0, np.nan)])
    assert harvest_kg_factors(df).attrs['kg_factor_sources']['survey_kg_zero'] == 2


def test_an_implied_factor_is_not_a_reported_one():
    """It fills no sibling row, and the reported screen does not judge it."""
    rows = [(f'h{k}', '1_1', 'Mil', 'Tiya', 10.0, 25.0) for k in range(10)]
    rows.append(('z', '1_1', 'Mil', 'Tiya', 10.0, np.nan))
    f = harvest_kg_factors(_frame(rows))
    assert f['KgFactorSource'].iloc[-1] == 'none'
    assert f['kg_survey_median'].isna().all()
    assert np.isnan(f['kg_reported']).all()

    # A tonne in `Autres` implies 1,000 kg per unit.  KG_FACTOR_MAX would refuse
    # that as a reported factor; as a total it is what the survey says.
    tonne = _frame([('a', '1_1', 'Mil', 'Autres', 4.0, 4000.0)])
    assert _kg(tonne) == {('a', '1_1', 'Mil'): 4000.0}
    assert (harvest_kg_factors(tonne)
            .attrs['kg_factor_sources']['reported_implausible']) == 0


def test_it_moves_no_other_rows_factor():
    base = [('a', '1_1', 'Mil', 'Kg', 10.0, np.nan),
            ('b', '1_1', 'Mil', 'Sac de 50kg', 2.0, np.nan),
            ('c', '1_1', 'Mil', 'Tiya', 3.0, np.nan)]
    before = harvest_kg_factors(_frame(base))
    after = harvest_kg_factors(_frame(base + [('d', '1_1', 'Mil', 'Tiya', 3.0, 9.0),
                                              ('e', '1_1', 'Mil', 'Kg', 3.0, 6.0)]))
    pd.testing.assert_series_equal(before['kg_per_unit'],
                                   after['kg_per_unit'].iloc[:3], check_names=False)


def test_survey_kg_vs_metric_is_the_audit_of_the_label():
    df = _frame([('a', '1_1', 'Mil', 'Sac de 100kg', 2.0, 200.0),
                 ('b', '1_1', 'Mil', 'Sac de 100kg', 2.0, 40.0),
                 ('c', '1_1', 'Mil', 'Tiya', 2.0, 5.0)])
    d = harvest_kg_factors(df).attrs['kg_factor_disagreement']
    assert d['survey_kg_vs_metric'] == {'both': 2, 'disagree': 1, 'share': 0.5}


def test_a_column_of_strings_or_nullable_floats_reads_the_same():
    df = _frame([('a', '1_1', 'Mil', 'Tiya', 10.0, 25.0),
                 ('b', '1_1', 'Mil', 'Tiya', 10.0, np.nan)])
    nullable = df.assign(Quantity_kg=df['Quantity_kg'].astype('Float64'))
    assert _kg(nullable) == _kg(df) == {('a', '1_1', 'Mil'): 25.0}


def test_unknown_plot_rows_are_reported_in_kilograms_of_the_survey():
    df = _frame([('a', 'unknown_plot', 'Mil', 'Tiya', 10.0, 25.0),
                 ('a', '1_1', 'Mil', 'Tiya', 10.0, 30.0)])
    res = harvest_kg(df)
    assert res.attrs['unknown_plot_excluded'] == {'rows': 1, 'rows_with_kg': 1, 'kg': 25.0}
    assert res['Harvest_kg'].tolist() == [30.0]


def test_a_crop_level_named_crop_and_u_as_a_column_are_read():
    """Mali carries ``u`` as a column and Niger names its crop level ``crop``."""
    df = _frame([('a', '1_1', 'Mil', 'Tiya', 10.0, 25.0)]).reset_index('u')
    df.index = df.index.rename({'j': 'crop'})
    assert harvest_kg(df)['Harvest_kg'].tolist() == [25.0]


def test_a_carried_native_row_is_not_confused_with_a_total():
    df = _frame([('a', '1_1', 'Mil', 'Tiya', 10.0, 25.0),
                 ('b', '1_1', 'Mil', 'Tiya', 10.0, np.nan)])
    res = harvest_kg(df, carry_native=True)
    assert res['Harvest_kg'].tolist() == [25.0, 10.0]


def test_site_q_looks_at_the_column_and_changes_nothing():
    """Named at both ends, never clipped.  Reports say which column they judged."""
    assert 'Quantity_kg' in quantity_audit._SCREENED_COLUMNS['crop_production']
    assert 'Quantity_kg' in quantity_audit._NEAR_ZERO_COLUMNS['crop_production']
    rows = [(f'h{k}', '1_1', 'Mil', 'Tiya', 10.0, 25.0) for k in range(40)]
    rows.append(('high', '1_1', 'Mil', 'Tiya', 10.0, 999998.0))
    rows.append(('low', '1_1', 'Mil', 'Tiya', 10.0, 0.01))
    df = _frame(rows)
    before = df.copy()
    quantity_audit._clear_quantity_reports()
    with pytest.warns(quantity_audit.QuantityImplausibleWarning):
        out = quantity_audit.check_quantities(df, country='X', table='crop_production')
    pd.testing.assert_frame_equal(out, before)
    reports = [r for r in quantity_audit.quantity_reports(country='X', table='crop_production')
               if r['column'] == 'Quantity_kg']
    assert {r['direction'] for r in reports} == {'high', 'low'}
    quantity_audit._clear_quantity_reports()


def test_the_canonical_column_is_declared_optional_and_a_float():
    import yaml
    from importlib.resources import files
    doc = yaml.safe_load((files('lsms_library') / 'data_info.yml').read_text(encoding='utf-8'))
    declared = doc['Columns']['crop_production']['Quantity_kg']
    assert declared['type'] == 'float'
    assert declared['optional'] is True

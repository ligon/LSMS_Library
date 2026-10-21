"""Ethiopia #1118: missing units do not erase reported use observations."""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.transformations import U_UNKNOWN


@pytest.fixture
def eth(monkeypatch):
    path = (Path(__file__).resolve().parents[1] / 'lsms_library' / 'countries'
            / 'Ethiopia' / '_' / 'ethiopia.py')
    spec = importlib.util.spec_from_file_location('_eth_1118', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, '_eth_crop_label_map', lambda: {1: 'Maize', 2: 'Teff'})
    return module


@pytest.fixture
def ids():
    return dict(hhid='hh', holder_id='holder', parcel_id='parcel', field_id='field')


def test_real_organic_use_only_rows_survive_without_fabricated_amounts(eth, ids):
    raw = pd.DataFrame(dict(hh=['H'] * 3, holder=[1] * 3, parcel=[1] * 3,
                            field=[1, 2, 3], manure=[1, 2, None],
                            compost=[1, 2, None], other=[1, 2, None]))
    result = eth.plot_inputs_for_wave(
        '2011-12', raw, None, None,
        dict(ids, manure_used='manure', compost_used='compost', other_organic_used='other'))
    assert len(result) == 3
    assert set(result.index.get_level_values('input')) == {'Manure', 'Compost', 'Other Organic'}
    assert set(result.index.get_level_values('j')) == {eth.WHOLE_PLOT_SENTINEL}
    assert result.Applied.eq(True).all()
    assert result.u.tolist() == [U_UNKNOWN] * len(result)
    assert result[['Quantity', 'Purchased', 'Quantity_purchased', 'Improved']].isna().all().all()


def test_pesticide_use_gate_crop_and_duplicate_grain_are_preserved(eth, ids):
    raw = pd.DataFrame(dict(hh=['H'] * 5, holder=[1] * 5, parcel=[1] * 5,
                            field=[1, 1, 2, 3, 4], crop=[1, 1, 2, 1, 99],
                            used=[1, 1, 2, 1, 1], gate=[1, 1, 1, 2, 1]))
    result = eth.plot_inputs_for_wave(
        '2013-14', None, raw, None,
        dict(ids, pest_crop_code='crop', pest_used='used', pest_gate='gate'))
    assert len(result) == 1
    assert result.index.get_level_values('input').tolist() == ['Pesticide']
    assert result.index.get_level_values('j').tolist() == ['Maize']
    assert result.Applied.eq(True).all()
    assert result.u.tolist() == [U_UNKNOWN] * len(result)
    assert result[['Quantity', 'Purchased', 'Quantity_purchased', 'Improved']].isna().all().all()


def test_reported_kg_is_retained_when_only_use_or_improved_is_known(eth, ids):
    plots = pd.DataFrame(dict(hh=['H'], holder=[1], parcel=[1], field=[1],
                              urea_used=[1], urea_qty=[None]))
    crops = pd.DataFrame(dict(hh=['H'], holder=[1], parcel=[1], field=[1],
                              crop=[1], seed_qty=[None], improved=[2]))
    result = eth.plot_inputs_for_wave(
        '2015-16', plots, crops, None,
        dict(ids, urea_used='urea_used', urea_kg='urea_qty', seed_src='planting',
             seed_crop_code='crop', seed_qty='seed_qty', improved_code='improved'))
    assert len(result) == 2
    assert result.u.tolist() == ['Kg', 'Kg']
    assert result.Quantity.isna().all()
    assert result.xs('Seed', level='input').Improved.eq(True).all()
    assert result.xs('Urea', level='input').Applied.eq(True).all()


def test_holder_crop_purchase_does_not_fan_out_to_multiple_plots(eth, ids):
    crops = pd.DataFrame(dict(hh=['H'] * 3, holder=[1] * 3, parcel=[1] * 3,
                              field=[1, 2, 3], crop=[1, 1, 2], qty=[4, 5, 6]))
    purchases = pd.DataFrame(dict(holder=[1, 1], crop=[1, 2], bought=[1, 1], qty=[9, 7]))
    result = eth.plot_inputs_for_wave(
        '2018-19', None, crops, purchases,
        dict(ids, seed_src='planting', seed_crop_code='crop', seed_qty='qty',
             p_holder_id='holder', p_crop_code='crop', p_purch_flag='bought',
             p_purch_qty='qty'))
    assert len(result) == 3
    assert result.Quantity.tolist() == [4, 5, 6]
    assert result.xs('Maize', level='j')[['Purchased', 'Quantity_purchased']].isna().all().all()
    teff = result.xs('Teff', level='j')
    assert teff.Purchased.eq(True).all()
    assert teff.Quantity_purchased.tolist() == [7]
    assert result.u.eq('Kg').all()


@pytest.mark.parametrize('unit', [None, pd.NA, float('nan'), '', ' ', '\t\n'])
def test_final_boundary_normalizes_each_absent_unit_form(eth, ids, monkeypatch, unit):
    # Inject a producer's unit payload at the concat boundary. The held ESS
    # producers currently emit NA; future blank producers obey the same rule.
    concat = pd.concat

    def producer_payload(*args, **kwargs):
        result = concat(*args, **kwargs)
        result['u'] = unit
        return result

    monkeypatch.setattr(eth.pd, 'concat', producer_payload)
    raw = pd.DataFrame(dict(hh=['H'], holder=[1], parcel=[1], field=[1], used=[1]))
    result = eth.plot_inputs_for_wave('2021-22', raw, None, None, dict(ids, manure_used='used'))
    assert len(result) == 1
    assert result.u.tolist() == [U_UNKNOWN]
    assert result.Quantity.isna().all() and result.Applied.eq(True).all()


@pytest.mark.parametrize('unit', ['Kg', ' kg ', 'Unknown', '---'])
def test_final_boundary_preserves_every_nonblank_payload(eth, ids, monkeypatch, unit):
    concat = pd.concat

    def producer_payload(*args, **kwargs):
        result = concat(*args, **kwargs)
        result['u'] = unit
        return result

    monkeypatch.setattr(eth.pd, 'concat', producer_payload)
    raw = pd.DataFrame(dict(hh=['H'], holder=[1], parcel=[1], field=[1], used=[1]))
    result = eth.plot_inputs_for_wave('2021-22', raw, None, None, dict(ids, manure_used='used'))
    assert result.u.tolist() == [unit]

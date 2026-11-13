"""The ruled structural marker changes neither crop-specific data nor eligibility."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def eth(monkeypatch):
    path = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Ethiopia/_/ethiopia.py'
    spec = importlib.util.spec_from_file_location('_ethiopia1119', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, '_eth_crop_label_map_for_wave', lambda t: {1: 'Maize', 2: 'Teff'})
    return module


IDS = dict(hhid='hh', holder_id='holder', parcel_id='parcel', field_id='field')


def exact(a, b):
    pd.testing.assert_frame_equal(a, b, check_exact=True, rtol=0, atol=0)
    for col in a:
        pd.testing.assert_extension_array_equal(a[col].array, b[col].array,
                                               check_exact=True, rtol=0, atol=0)
        mask = a[col].notna().to_numpy()
        np.testing.assert_array_equal(mask, b[col].notna().to_numpy(), strict=True)
        np.testing.assert_array_equal(a[col][mask].to_numpy(), b[col][mask].to_numpy(), strict=True)
    assert a.attrs == b.attrs


@pytest.mark.parametrize('wave', ['2011-12', '2013-14', '2015-16', '2018-19', '2021-22'])
def test_actual_builder_changes_only_structural_crop_axis(eth, wave):
    plots = pd.DataFrame(dict(hh=['H', 'H'], holder=[7, 7], parcel=[1, 1],
                             field=[1, 2], used=[1, 2], kg=[3., None], manure=[1, 2]))
    crops = pd.DataFrame(dict(hh=['H'] * 4, holder=[7] * 4, parcel=[1] * 4,
                             field=[1, 2, 3, 4], crop=[1, 99, None, 2],
                             qty=[4., 5., 6., None], improved=[2] * 4, used=[1] * 4))
    snapshots = [f.copy(deep=True) for f in (plots, crops)]
    result = eth.plot_inputs_for_wave(wave, plots, crops, None,
        dict(IDS, urea_used='used', urea_kg='kg', manure_used='manure',
             seed_src='planting', seed_crop_code='crop', seed_qty='qty',
             improved_code='improved', pest_crop_code='crop', pest_used='used'))
    # Independent literal source expectations: unresolved crop fields 2/3 remain
    # excluded even with reported quantities; use-only observations survive.
    rows = [
        (wave, 'H', '7_1_1', 'Manure', '(not crop-specific)', None, 'Unknown', None, None, None, True),
        (wave, 'H', '7_1_1', 'Pesticide', 'Maize', None, 'Unknown', None, None, None, True),
        (wave, 'H', '7_1_1', 'Seed', 'Maize', 4., 'Kg', None, None, True, None),
        (wave, 'H', '7_1_1', 'Urea', '(not crop-specific)', 3., 'Kg', None, None, None, True),
        (wave, 'H', '7_1_4', 'Pesticide', 'Teff', None, 'Unknown', None, None, None, True),
        (wave, 'H', '7_1_4', 'Seed', 'Teff', None, 'Kg', None, None, True, None),
    ]
    expected = pd.DataFrame(rows, columns=['t', 'i', 'plot_id', 'input', 'j',
        'Quantity', 'u', 'Purchased', 'Quantity_purchased', 'Improved', 'Applied'])
    for col in ['Quantity', 'Quantity_purchased']:
        expected[col] = expected[col].astype('Float64')
    for col in ['Purchased', 'Improved', 'Applied']:
        expected[col] = expected[col].astype('boolean')
    # The mapped crop branch explicitly uses nullable string, retained by
    # concat with structural rows. State the expected NA policy, do not cast actual.
    expected['j'] = expected['j'].astype('string')
    expected['u'] = expected['u'].astype('string')
    expected = expected.set_index(['t', 'i', 'plot_id', 'input', 'j'])
    exact(result, expected)
    for original, current in zip(snapshots, (plots, crops)):
        pd.testing.assert_frame_equal(current, original, check_exact=True)


def test_crop_specific_label_equal_to_legacy_spelling_is_not_globally_replaced(eth, monkeypatch):
    monkeypatch.setattr(eth, '_eth_crop_label_map_for_wave', lambda t: {1: 'Whole Plot'})
    raw = pd.DataFrame(dict(hh=['H'], holder=[1], parcel=[1], field=[1], crop=[1], qty=[2.]))
    result = eth.plot_inputs_for_wave('2011-12', None, raw, None,
        dict(IDS, seed_src='planting', seed_crop_code='crop', seed_qty='qty'))
    assert result.index.get_level_values('j').tolist() == ['Whole Plot']
    np.testing.assert_array_equal(result.Quantity.to_numpy(), np.array([2.]))


def test_known_reducer_and_missing_household_exclusion_stay_unchanged(eth):
    raw = pd.DataFrame(dict(hh=['H', 'H', None], holder=[1] * 3, parcel=[1] * 3,
                            field=[1, 1, 2], used=[1] * 3, qty=[2., 5., 9.]))
    result = eth.plot_inputs_for_wave('2015-16', raw, None, None,
        dict(IDS, urea_used='used', urea_kg='qty'))
    assert result.index.tolist() == [('2015-16', 'H', '1_1_1', 'Urea', '(not crop-specific)')]
    # Existing first-duplicate policy is neither max nor a sum; GH1357's
    # anonymous household exclusion does not mint an identifier.
    np.testing.assert_array_equal(result.Quantity.to_numpy(), np.array([2.]))
    assert result.Applied.tolist() == [True] and result.u.tolist() == ['Kg']

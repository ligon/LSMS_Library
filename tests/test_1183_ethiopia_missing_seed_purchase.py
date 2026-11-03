"""GH #1183: a missing seed-purchase answer (section 5 Q3) stays missing.

Ruling (@ligon, 2026-11-03, option 2): a blank Q3 is not an observed "No".
Both boundaries keep it missing -- the decoder (``_yesno_bool_col`` instead
of ``_yesno_bool``, which mapped a <NA> code to False) and its caller (only
an EXPLICIT No zeroes the purchased quantity).  The existing retention rule
is unchanged, so a seed row kept only by the fabricated False / 0 payload
now carries nothing and is dropped like any other empty seed row.
Measured on the five held waves: -371 W1 and -3 W2 rows of 180,900, and
Purchased False -> <NA> (Quantity_purchased 0 -> <NA>) on 4,055 W1 + 57 W2
surviving rows; W3-W5 are byte-identical (they carry no missing Q3).
"""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def eth(monkeypatch):
    path = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Ethiopia/_/ethiopia.py'
    spec = importlib.util.spec_from_file_location('_eth1183', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, '_eth_crop_label_map_for_wave', lambda t: {1: 'Maize', 2: 'Teff'})
    return module


def build(eth, flags, amounts=None, quantity=None, direct=True, fields=(1,)):
    """One planting record per field; one section-5 record per flag."""
    if amounts is None:
        amounts = [None] * len(flags)
    planting = pd.DataFrame({'hh': ['H'] * len(fields), 'holder': [7] * len(fields),
                             'parcel_id': [1] * len(fields), 'field_id': list(fields),
                             'crop': [1] * len(fields),
                             'qty': pd.array([quantity] * len(fields), dtype='Float64')})
    seeds = pd.DataFrame({'hh': ['H'] * len(flags), 'holder': [7] * len(flags),
                          'parcel_id': [1] * len(flags), 'field_id': [1] * len(flags),
                          'crop': [1] * len(flags),
                          'bought': pd.array(flags, dtype='Float64'),
                          'amount': pd.array(amounts, dtype='Float64')})
    colmap = dict(hhid='hh', holder_id='holder', parcel_id='parcel_id', field_id='field_id',
                  seed_src='planting', seed_crop_code='crop', seed_qty='qty',
                  p_holder_id='holder', p_crop_code='crop', p_purch_flag='bought',
                  p_purch_qty='amount')
    if direct:
        # W1: section 5 carries field_id -> direct plot-crop join.
        colmap.update(p_field_id='field_id', p_parcel_id='parcel_id')
    t = '2011-12' if direct else '2013-14'
    return eth.plot_inputs_for_wave(t, None, planting, seeds, colmap)


@pytest.mark.parametrize('direct', [True, False])
def test_missing_answer_alone_no_longer_manufactures_a_seed_row(eth, direct):
    """No quantity, no improved flag, a blank Q3 and a blank amount: before
    #1183 the row survived on Purchased=False / Quantity_purchased=0."""
    result = build(eth, [None], direct=direct)
    assert len(result) == 0


@pytest.mark.parametrize('direct', [True, False])
def test_missing_answer_stays_missing_beside_a_reported_quantity(eth, direct):
    result = build(eth, [None], quantity=4., direct=direct)
    assert len(result) == 1
    assert result.Purchased.isna().all()
    assert result.Quantity_purchased.isna().all()
    np.testing.assert_array_equal(result.Quantity.to_numpy(), np.array([4.]))
    assert result.Purchased.dtype == pd.BooleanDtype()
    assert result.Quantity_purchased.dtype == pd.Float64Dtype()


@pytest.mark.parametrize('direct', [True, False])
def test_explicit_no_still_reads_false_and_zero(eth, direct):
    result = build(eth, [2.], direct=direct)
    assert result.Purchased.tolist() == [False]
    np.testing.assert_array_equal(result.Quantity_purchased.to_numpy(), np.array([0.]))


@pytest.mark.parametrize('direct', [True, False])
def test_explicit_yes_is_unchanged(eth, direct):
    result = build(eth, [1.], amounts=[3.], direct=direct)
    assert result.Purchased.tolist() == [True]
    np.testing.assert_array_equal(result.Quantity_purchased.to_numpy(), np.array([3.]))


def test_unrecognised_code_is_missing_and_no_longer_zeroes_the_amount(eth):
    """A code that is neither 1 nor 2 already decoded to <NA>, but the
    caller's ``mask(purch == False, 0.0)`` masked on the <NA> condition and
    served 0.  No such code occurs in the held W1-W5 files (measured: the
    only Quantity_purchased changes are the 4,055 + 57 missing-Q3 rows)."""
    result = build(eth, [3.], quantity=4.)
    assert result.Purchased.isna().all() and result.Quantity_purchased.isna().all()


def test_holder_crop_missing_beside_yes_keeps_the_yes(eth):
    result = build(eth, [None, 1.], amounts=[None, 2.], direct=False)
    assert result.Purchased.tolist() == [True]
    np.testing.assert_array_equal(result.Quantity_purchased.to_numpy(), np.array([2.]))


def test_misnamed_purchase_column_still_raises(eth):
    planting = pd.DataFrame({'hh': ['H'], 'holder': [7], 'parcel_id': [1], 'field_id': [1],
                             'crop': [1], 'qty': [4.]})
    seeds = planting.drop(columns='qty')
    colmap = dict(hhid='hh', holder_id='holder', parcel_id='parcel_id', field_id='field_id',
                  seed_src='planting', seed_crop_code='crop', seed_qty='qty',
                  p_holder_id='holder', p_crop_code='crop', p_purch_flag='no_such_col')
    with pytest.raises(KeyError):
        eth.plot_inputs_for_wave('2013-14', None, planting, seeds, colmap)

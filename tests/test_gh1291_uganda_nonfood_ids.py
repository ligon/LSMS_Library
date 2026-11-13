"""Execute both early nonfood scripts against synthetic helper output.

The household formatter itself is covered by test_format_id.py. These tests
exercise the shipped entry points that used to omit it, without reading data
or writing a parquet. The actual format_id remains in use.
"""
from pathlib import Path
import runpy
import sys
from types import ModuleType

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library import local_tools


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(params=['2009-10', '2010-11'])
def wave(request):
    return request.param


def _helper_frame(ids, items=None):
    if items is None:
        items = [f'Item {n}' for n in range(len(ids))]
    frame = pd.DataFrame(
        {'Expenditure': pd.array([1.25 + n for n in range(len(ids))], dtype='Float64')},
        index=pd.MultiIndex.from_arrays([ids, items], names=['i', 'j']),
    )
    frame.attrs = {'source': 'synthetic', 'metadata': {'reported': True}}
    return frame


def _execute(monkeypatch, wave, helper_result, writes):
    # Each real script appends its relative country directory to sys.path.
    # Restore that process-global list at teardown as well as the fake module.
    monkeypatch.setattr(sys, 'path', sys.path.copy())
    fake_country = ModuleType('uganda')

    def nonfood_expenditures(**kwargs):
        assert kwargs['HHID'] == 'hh'
        assert kwargs['fn'] == '../Data/GSEC15c.dta'
        return helper_result.copy(deep=True)

    fake_country.nonfood_expenditures = nonfood_expenditures
    monkeypatch.setitem(sys.modules, 'uganda', fake_country)

    def capture(frame, path):
        assert path == 'nonfood_expenditures.parquet'
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(local_tools, 'to_parquet', capture)
    runpy.run_path(str(ROOT / 'lsms_library/countries/Uganda' / wave /
                       '_/nonfood_expenditures.py'), run_name='__main__')


def _assert_payload(got, original, wave):
    assert got.index.names == ['t', 'i', 'j']
    assert got.index.get_level_values('t').tolist() == [wave] * len(original)
    assert got.index.get_level_values('j').tolist() == original.index.get_level_values('j').tolist()
    assert got.attrs == original.attrs
    assert_frame_equal(got.reset_index(drop=True), original.reset_index(drop=True))


def test_numeric_households_join_the_canonical_sample(monkeypatch, wave):
    original = _helper_frame([1230001.0, 1230001.0, 1230002.0],
                             ['Water', 'Rent', 'Water'])
    before = original.copy(deep=True)
    writes = []
    _execute(monkeypatch, wave, original, writes)
    assert len(writes) == 1
    got = writes[0]
    assert got.index.get_level_values('i').tolist() == ['1230001', '1230001', '1230002']
    sample = pd.DataFrame({'v': ['EA-A', 'EA-B']},
                          index=pd.Index(['1230001', '1230002'], name='i'))
    joined = got.join(sample)
    assert joined['v'].tolist() == ['EA-A', 'EA-A', 'EA-B']
    assert got.index.is_unique
    _assert_payload(got, before, wave)
    assert_frame_equal(original, before)


def test_canonical_strings_keep_leading_zeros(monkeypatch, wave):
    original = _helper_frame(['007', '7', 'HOUSE-A'])
    writes = []
    _execute(monkeypatch, wave, original, writes)
    got = writes[0]
    assert got.index.get_level_values('i').tolist() == ['007', '7', 'HOUSE-A']
    assert got.index.is_unique
    _assert_payload(got, original, wave)


def test_missing_household_stays_missing(monkeypatch, wave):
    original = _helper_frame([np.nan, 1230001.0])
    writes = []
    _execute(monkeypatch, wave, original, writes)
    got = writes[0]
    ids = got.index.get_level_values('i')
    assert ids.isna().tolist() == [True, False]
    assert ids[1] == '1230001'
    assert not {'None', 'nan', '<NA>'}.intersection(ids.dropna())
    _assert_payload(got, original, wave)


def test_fractional_numeric_uses_existing_formatter_policy(monkeypatch, wave):
    # format_id intentionally truncates numeric fractions. Held-source
    # acceptance must separately establish that actual hh values are integral.
    original = _helper_frame([123.75])
    writes = []
    _execute(monkeypatch, wave, original, writes)
    got = writes[0]
    assert got.index.get_level_values('i').tolist() == ['123']
    _assert_payload(got, original, wave)


def test_rekey_collision_refuses_to_write(monkeypatch, wave):
    original = _helper_frame([123.0, 123.5], ['Rent', 'Rent'])
    assert original.index.is_unique
    writes = []
    with pytest.raises(AssertionError, match=r'Non-unique \(t, i, j\)'):
        _execute(monkeypatch, wave, original, writes)
    assert writes == []

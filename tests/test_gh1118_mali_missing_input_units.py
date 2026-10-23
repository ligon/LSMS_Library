"""GH #1118 (Mali plot_inputs) under the #1041 missing-unit ruling.

``mali.plot_inputs_finalize`` must serve a retained record whose unit is
absent as ``U_UNKNOWN``, filled after the existing duplicate collapse, while
every other field, the content predicate and the existing unit mapping stay
unchanged.  The module is loaded from THIS checkout's source file.
"""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import lsms_library as ll
from lsms_library.transformations import U_UNKNOWN


@pytest.fixture
def mali(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    assert Path(ll.__file__).resolve().is_relative_to(root), ll.__file__
    source = root / 'lsms_library/countries/Mali/_/mali.py'
    spec = importlib.util.spec_from_file_location('mali_1118_actual_source', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert Path(module.__file__).resolve() == source
    # These existing helpers resolve categorical tables from native script cwd.
    monkeypatch.chdir(source.parent)
    return module


def row(unit=None, **changes):
    result = dict(t='2014-15', i='h1', plot_id='1_1', input='urea', crop=pd.NA,
                  u=unit, Quantity=2., Quantity_purchased=pd.NA,
                  Purchased=pd.NA, Improved=pd.NA)
    result.update(changes)
    return result


@pytest.mark.parametrize('unit', [None, pd.NA, np.nan, '', ' ', '\t'])
def test_true_missing_unit_on_retained_amount_is_unknown(mali, unit):
    got = mali.plot_inputs_finalize(pd.DataFrame([row(unit)]))
    assert len(got) == 1
    assert got['u'].tolist() == [U_UNKNOWN]
    assert got['Quantity'].tolist() == [2.]
    assert got.index.names == ['t', 'i', 'plot_id', 'input', 'crop']
    assert got.index.get_level_values('input').tolist() == ['Urea']
    assert got.index.get_level_values('crop').isna().all()


@pytest.mark.parametrize('flag', ['Purchased', 'Improved'])
def test_flag_without_amount_also_gets_unknown_unit(mali, flag):
    flags = {flag: True if flag == 'Purchased' else False}
    got = mali.plot_inputs_finalize(pd.DataFrame([row(None, Quantity=pd.NA, **flags)]))
    assert len(got) == 1
    assert got['Quantity'].isna().all()
    assert got['u'].tolist() == [U_UNKNOWN]
    assert bool(got[flag].iloc[0]) == bool(flags[flag])


@pytest.mark.parametrize('raw, expected', [('Kg', 'Kg'), ('Kilogramme', 'Kg'),
                                          ('Litres', 'Litre'), ('Sac', 'Sac')])
def test_existing_unit_mapping_and_amount_are_unchanged(mali, raw, expected):
    got = mali.plot_inputs_finalize(pd.DataFrame([row(raw, Quantity=-3.)]))
    assert got['u'].tolist() == [expected]
    assert got['Quantity'].tolist() == [-3.]


@pytest.mark.parametrize('reverse', [False, True])
def test_first_real_unit_survives_missing_duplicate(mali, reverse):
    rows = [row(None, Quantity=1.), row('Kg', Quantity=2.)]
    if reverse:
        rows.reverse()
    got = mali.plot_inputs_finalize(pd.DataFrame(rows))
    assert len(got) == 1
    assert got['u'].tolist() == ['Kg']
    assert got['Quantity'].tolist() == [3.]


def test_all_missing_duplicate_units_fill_after_existing_collapse(mali):
    got = mali.plot_inputs_finalize(pd.DataFrame([row(None, Quantity=1.),
                                                  row(' ', Quantity=2.)]))
    assert len(got) == 1
    assert got['u'].tolist() == [U_UNKNOWN]
    assert got['Quantity'].tolist() == [3.]
    assert got['Quantity_purchased'].isna().all()
    assert got['Purchased'].isna().all()
    assert got['Improved'].isna().all()


@pytest.mark.parametrize('quantity,purchased,improved,expected_rows', [
    (0., pd.NA, pd.NA, 1), (-2., pd.NA, pd.NA, 1),
    (pd.NA, True, pd.NA, 1), (pd.NA, False, pd.NA, 0),
    (pd.NA, pd.NA, False, 1), (pd.NA, pd.NA, pd.NA, 0),
    (999., pd.NA, pd.NA, 0), (9999., pd.NA, pd.NA, 0),
])
def test_existing_content_predicate_is_preserved(mali, quantity, purchased, improved,
                                                 expected_rows):
    got = mali.plot_inputs_finalize(pd.DataFrame([
        row('Kg', Quantity=quantity, Purchased=purchased, Improved=improved)]))
    assert len(got) == expected_rows
    if expected_rows:
        assert got['u'].tolist() == ['Kg']
        if pd.notna(quantity):
            assert got['Quantity'].tolist() == [quantity]


def test_missing_plot_crop_keys_and_source_frame_are_preserved(mali):
    source = pd.DataFrame([row(None, plot_id=pd.NA)])
    source.attrs['source_control'] = ('typed', 7, False)
    before = source.copy(deep=True)
    got = mali.plot_inputs_finalize(source)
    pd.testing.assert_frame_equal(source, before, check_exact=True)
    assert source.attrs == before.attrs
    assert len(got) == 1
    assert got.index.get_level_values('plot_id').isna().all()
    assert got.index.get_level_values('crop').isna().all()
    assert got['u'].tolist() == [U_UNKNOWN]


def test_purchased_quantity_only_is_genuine_content(mali):
    got = mali.plot_inputs_finalize(pd.DataFrame([
        row(None, Quantity=pd.NA, Quantity_purchased=2.)]))
    assert len(got) == 1
    assert got['Quantity'].isna().all()
    assert got['Quantity_purchased'].tolist() == [2.]
    assert got['u'].tolist() == [U_UNKNOWN]

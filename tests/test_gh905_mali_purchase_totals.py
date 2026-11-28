"""Preserve EACI household-quantity sums when batching only singleton keys."""
import ast

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root


def _original(purch):
    return purch.groupby(['i', 'input'], as_index=False).agg(
        Purchased=('bought', 'max'),
        Quantity_purchased=('qty', lambda s: s.sum(min_count=1)),
    )


@pytest.fixture(params=('2014-15', '2017-18'))
def reducer(request):
    path = countries_root() / 'Mali' / request.param / '_' / 'plot_inputs.py'
    module = ast.parse(path.read_text())
    fn, = [n for n in module.body
           if isinstance(n, ast.FunctionDef) and n.name == '_purchase_totals']
    namespace = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), 'exec'),
         {'pd': pd}, namespace)
    return namespace[fn.name]


def test_scalar_order_missingness_and_keys(reducer):
    df = pd.DataFrame({
        'i': ['b', 'a', 'b', 'b', 'b', 'a', 'q', 's', None, 'z', 'i', 'j'],
        'input': ['seed'] * 10 + ['manure', 'seed'],
        'qty': [1e16, np.nan, 1, 1, -1e16, np.nan, -0., 0., 7., np.nan,
                np.inf, -np.inf],
        'bought': pd.array([True, pd.NA, False, pd.NA, True, pd.NA, False,
                           True, True, pd.NA, True, True], dtype='boolean'),
    })
    old = _original(df)
    got = reducer(df)
    pd.testing.assert_frame_equal(old, got, check_exact=True)
    val = got.set_index('i').Quantity_purchased
    assert val['b'] == 0 and np.isnan(val['a']) and np.isnan(val['z'])


@pytest.mark.parametrize('dtype', ['Float64', 'float32', 'int16', 'Int64'])
def test_other_numeric_dtypes_follow_original(reducer, dtype):
    df = pd.DataFrame({'i': ['b', 'a', 'b'], 'input': ['seed'] * 3,
                       'bought': pd.array([True, pd.NA, False], dtype='boolean'),
                       'qty': pd.Series([2, 1, 4], dtype=dtype)})
    pd.testing.assert_frame_equal(_original(df), reducer(df), check_exact=True)


def test_empty_source_preserves_shape_and_dtypes(reducer):
    df = pd.DataFrame({'i': pd.Series(dtype='string'),
                       'input': pd.Series(dtype='string'),
                       'bought': pd.Series(dtype='boolean'),
                       'qty': pd.Series(dtype='float64')})
    pd.testing.assert_frame_equal(_original(df), reducer(df), check_exact=True)

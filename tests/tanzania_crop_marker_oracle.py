"""Frozen production source and strict comparisons for Tanzania #1119.

The fixture is the verbatim function at d66da398, not a mirror implementation.
Only expected crop labels are replaced; every other axis/domain is retained.
"""
import ast
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd


FIXTURE = Path(__file__).with_name('fixtures') / 'tanzania_plot_inputs_pre_1119.txt'
FUNCTION_SHA256 = 'e4bbe7a94cec936a973e77d483f5c43515d9cff2ff3de22103f386c0796a79ce'


def original_function():
    source = FIXTURE.read_text(encoding='utf-8')
    assert hashlib.sha256(source.encode()).hexdigest() == FUNCTION_SHA256
    return source


def restore_original_function(source):
    """Restore exact old production text; #905 still checks its whole digest."""
    function = next(n for n in ast.parse(source).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'plot_inputs_for_wave')
    lines = source.splitlines(keepends=True)
    return (''.join(lines[:function.lineno - 1]) + original_function()
            + ''.join(lines[function.end_lineno:]))


def relabel_expected(frame, crop_labels):
    """Explicit ruled labels at the frozen production output's ordered rows."""
    assert len(frame) == len(crop_labels)
    result = frame.copy(deep=True)
    # Keep all non-crop levels/codes, including unused domain members.
    arrays = pd.Series(crop_labels, dtype='string')
    replacement_codes, replacement_level = pd.factorize(arrays, sort=True)
    pos = frame.index.names.index('crop')
    result.index = pd.MultiIndex(
        levels=[replacement_level if j == pos else level
                for j, level in enumerate(frame.index.levels)],
        codes=[replacement_codes if j == pos else code
               for j, code in enumerate(frame.index.codes)], names=frame.index.names)
    return result


def assert_typed(actual, expected):
    assert type(actual) is type(expected)
    if isinstance(actual, dict):
        assert len(actual) == len(expected)
        for (ak, av), (ek, ev) in zip(actual.items(), expected.items()):
            assert_typed(ak, ek)
            assert_typed(av, ev)
    elif isinstance(actual, (list, tuple)):
        assert len(actual) == len(expected)
        for av, ev in zip(actual, expected):
            assert_typed(av, ev)
    elif isinstance(actual, np.ndarray):
        assert actual.dtype == expected.dtype
        np.testing.assert_array_equal(actual, expected)
    elif actual is pd.NA:
        assert expected is pd.NA
    elif isinstance(actual, float) and np.isnan(actual):
        assert np.isnan(expected)
    else:
        assert actual == expected


def assert_strict(actual, expected):
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    pd.testing.assert_index_equal(actual.index, expected.index, exact=True)
    pd.testing.assert_index_equal(actual.columns, expected.columns, exact=True)
    assert actual.dtypes.equals(expected.dtypes)
    if isinstance(actual.index, pd.MultiIndex):
        for al, el, ac, ec in zip(actual.index.levels, expected.index.levels,
                                  actual.index.codes, expected.index.codes):
            pd.testing.assert_index_equal(al, el, exact=True)
            np.testing.assert_array_equal(ac, ec)
    assert_typed(actual.attrs, expected.attrs)
    for col in actual:
        a, b = actual[col], expected[col]
        np.testing.assert_array_equal(a.isna().to_numpy(), b.isna().to_numpy())
        assert a.array.equals(b.array)
        # GH #1206: nullable Float64 equality alone can accept neighboring
        # doubles. Check nonmissing NumPy values in their own numeric dtype.
        if pd.api.types.is_numeric_dtype(a.dtype):
            dtype = getattr(a.dtype, 'numpy_dtype', a.dtype)
            np.testing.assert_array_equal(a.dropna().to_numpy(dtype=dtype),
                                          b.dropna().to_numpy(dtype=dtype))

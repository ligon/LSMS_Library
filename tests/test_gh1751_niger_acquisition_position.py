"""Niger EHCVM gift/purchase answers must follow their original input line."""
import ast
import importlib.util

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_index_equal

from lsms_library.paths import countries_root


@pytest.fixture(params=['2018-19', '2021-22'])
def driver(request):
    wave = request.param
    country = countries_root() / 'Niger'
    spec = importlib.util.spec_from_file_location('niger_1751_test', country / '_/niger.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    path = country / wave / '_/plot_inputs.py'
    source = ast.parse(path.read_text(), filename=str(path))
    # Extract only the pure wave wrapper. The shared tail below is the real
    # one, without executing the top-level source reads in either producer.
    source.body = [node for node in source.body if
                   (isinstance(node, ast.FunctionDef) and node.name == '_finish_with_acquisition')
                   or (isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                       and node.targets[0].id == 'ACQUISITION')]
    namespace = {'pd': pd, '_finish_plot_inputs': module._finish_plot_inputs}
    exec(compile(source, str(path), 'exec'), namespace)
    return wave, module._finish_plot_inputs, namespace


def _rows():
    # Rows 1 and 3 have the same (i, input). They differ in crop and u,
    # and row 3 has a substantive gift answer even if other values are NA.
    frame = pd.DataFrame({
        'i': ['E_test', 'E_skip', 'E_test'],
        'input': ['Seed', pd.NA, 'Seed'],
        'crop': ['Mil', pd.NA, pd.NA],
        'u': ['kg', 'kg', pd.NA],
        'Quantity': ['1.25', '404', pd.NA],
        'Purchased': [True, True, pd.NA],
        'Quantity_purchased': [0, 99, pd.NA],
        'Unit_purchased': ['Charrette Bovine', 'not kept', pd.NA],
        'Received_as_gift': [False, False, True],
        'Quantity_received_as_gift': [pd.NA, '99', '2.5'],
        'Unit_received_as_gift': [pd.NA, 'not kept', 'charrette asine'],
    }, index=[10, 4, 10])
    frame.attrs['witness'] = 'attrs are unchanged'
    return frame


@pytest.mark.parametrize('keep_rows', ['mixed', 'all_filtered', 'no_input_rows', 'many'])
def test_same_order_is_frame_exact_and_internal_positions_do_not_escape(driver, keep_rows):
    wave, tail, namespace = driver
    frame = _rows()
    if keep_rows == 'all_filtered':
        frame.loc[:, 'input'] = pd.NA
    elif keep_rows == 'no_input_rows':
        frame = frame.iloc[:0]
    elif keep_rows == 'many':
        frame = pd.concat([frame] * 150)

    # Exact pre-guard normal path: same filter, true tail without any input_obs
    # override, same public acquisition coercions in the same order.
    filtered = frame.loc[frame['input'].notna()].reset_index(drop=True)
    acquisitions = namespace['ACQUISITION']
    expected = tail(filtered.drop(columns=list(acquisitions)), wave)
    for col, dtype in acquisitions.items():
        values = filtered[col]
        if col == 'Quantity_received_as_gift':
            values = pd.to_numeric(values, errors='coerce')
        expected[col] = values.astype(dtype).array
    expected = expected[['Quantity', 'Purchased', 'Quantity_purchased', 'Unit_purchased',
                         'Received_as_gift', 'Quantity_received_as_gift', 'Unit_received_as_gift']]

    got = namespace['_finish_with_acquisition'](frame, wave)
    assert_frame_equal(got, expected, check_exact=True, check_index_type=True)
    for actual_level, expected_level in zip(got.index.levels, expected.index.levels):
        assert_index_equal(actual_level, expected_level, exact=True)
    assert got.attrs == expected.attrs == frame.attrs


@pytest.mark.parametrize('permutation', [[1, 0], [0, 0]])
@pytest.mark.parametrize('same_normalized_payload', [False, True])
def test_same_count_bad_order_fails_even_when_rows_share_identity(
        driver, permutation, same_normalized_payload):
    wave, tail, namespace = driver
    frame = _rows()
    if same_normalized_payload:
        # Make the two surviving rows indistinguishable by the full public
        # tail identity and shared payload, while acquisition answers differ.
        shared = ['crop', 'u', 'Quantity', 'Purchased', 'Quantity_purchased']
        frame.iloc[2, frame.columns.get_indexer(shared)] = frame.iloc[0][shared].tolist()
    namespace['_finish_plot_inputs'] = lambda data, t: tail(data, t).iloc[permutation]
    with pytest.raises(AssertionError, match=f'plot_inputs {wave}: shared tail changed the line order') as error:
        namespace['_finish_with_acquisition'](frame, wave)
    assert 'E_test' not in str(error.value)


def test_shorter_output_keeps_the_existing_count_error(driver):
    wave, tail, namespace = driver
    namespace['_finish_plot_inputs'] = lambda data, t: tail(data, t).iloc[:1]
    with pytest.raises(AssertionError, match=f'plot_inputs {wave}: shared tail changed the line count'):
        namespace['_finish_with_acquisition'](_rows(), wave)


def test_reported_observation_identity_is_never_overwritten(driver):
    wave, _, namespace = driver
    frame = _rows().assign(input_obs='reported')
    with pytest.raises(AssertionError, match=f'plot_inputs {wave}: expected no reported input_obs'):
        namespace['_finish_with_acquisition'](frame, wave)

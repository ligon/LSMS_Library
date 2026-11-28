"""The two acquisition units use the served vocabulary, not one another's card."""
import runpy
import sys

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import countries_root


@pytest.mark.parametrize('year', ['2018-19', '2021-22'])
def test_own_role_labels_in_the_served_vocabulary(year, monkeypatch):
    root = countries_root() / 'Burkina_Faso'
    wave = root / year / '_'
    source_name = f"../Data/s16b_me_bfa{year[:4]}.dta"
    numbers = pd.DataFrame({
        'grappe': [3] * 5, 'menage': [1, 2, 3, 4, 5], 'vague': [1] * 5,
        's16bq01': [3] * 5, 's16bq02': [1, 1, 1, 1, 2],
        's16bq03a': [10, 11, 12, None, 100], 's16bq03b': [5, 2, 5, None, 2],
        's16bq05': [1, 1, 2, None, 1],
        's16bq07a': [20, 21, 22, None, 200], 's16bq07b': [4, 2, 5, None, 2],
        's16bq08': [1, 2, 1, None, 1],
        's16bq09a': [30, 31, 32, None, 300], 's16bq09b': [2, 4, 5, None, 2],
        's16bq09c': [400, 410, 420, None, 4000],
    })
    numbers.attrs['invented'] = True
    labels = numbers.copy()
    # These three dictionaries emulate the separate 2018 value labels. In
    # 2021 the real adapter decodes the invented numbers from its own cards.
    labels['s16bq01'] = 'Engrais inorganiques - Ur\u00e9e'
    labels['s16bq03b'] = numbers['s16bq03b'].map({2: 'Kilogramme', 5: 'Charrett\u00e9e'})
    for name in ('s16bq07b', 's16bq09b'):
        labels[name] = numbers[name].map({2: 'Kilogramme', 4: 'Litre', 5: 'Charrette'})
    for name in ('s16bq05', 's16bq08'):
        labels[name] = numbers[name].map({1: 'Oui', 2: 'Non'})

    reads, writes = [], []

    def invented_reader(filename, *, convert_categoricals):
        assert str(filename) == source_name
        reads.append(convert_categoricals)
        return (labels if convert_categoricals else numbers).copy(deep=True)

    def writer(frame, filename):
        assert filename == 'plot_inputs.parquet'
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(local_tools, 'get_dataframe', invented_reader)
    monkeypatch.setattr(local_tools, 'to_parquet', writer)
    monkeypatch.chdir(wave)
    monkeypatch.syspath_prepend(str(root / '_'))
    for name in ('burkina_faso', 's16b_2021'):
        monkeypatch.delitem(sys.modules, name, raising=False)
    try:
        runpy.run_path(str(wave / 'plot_inputs.py'))
    finally:
        for name in ('burkina_faso', 's16b_2021'):
            sys.modules.pop(name, None)

    assert reads == ([True, False] if year == '2018-19' else [False])
    assert len(writes) == 1
    native = writes[0]
    assert native.index.is_unique
    cart = 'Charrett\u00e9e' if year == '2018-19' else 'Charrette'
    assert native.index.get_level_values('u').tolist() == [cart, 'Kilogramme', cart, 'Unknown']
    served = Country('Burkina_Faso')._apply_categorical_mappings(native)
    assert served.index.get_level_values('u').tolist() == [cart, 'Kg', cart, 'Unknown']

    expected = pd.DataFrame({
        'Quantity': pd.array([10, 11, 12, pd.NA], dtype='Float64'),
        'Purchased': pd.array([True, False, True, pd.NA], dtype='boolean'),
        'Quantity_purchased': pd.array([30, 31, 32, pd.NA], dtype='Float64'),
        'Unit_purchased': pd.array(['Kg', 'Litre', 'Charrette', pd.NA], dtype='string'),
        'Received_as_gift': pd.array([True, True, False, pd.NA], dtype='boolean'),
        'Quantity_received_as_gift': pd.array([20, 21, 22, pd.NA], dtype='Float64'),
        'Unit_received_as_gift': pd.array(['Litre', 'Kg', 'Charrette', pd.NA], dtype='string'),
    })
    for frame in (native, served):
        pd.testing.assert_frame_equal(frame[expected.columns].reset_index(drop=True),
                                      expected, check_exact=True)
    if year == '2021-22':
        pd.testing.assert_frame_equal(native[numbers.columns].reset_index(drop=True),
                                      numbers.iloc[:4], check_exact=True)
        assert native.attrs['invented'] is True
        assert native.attrs['s16b_2021_candidate']['excluded_explicit_nonuse'] == 1

"""GH #1144: food item codes are quarter-specific, without invented detail.

Expected meanings below are independent transcriptions of the held Stata
item dictionaries, including the opposite W2 planting/harvest meanings.
Groundnuts, Pepper and W2 shelled maize retain their recorded granularity.
"""
import importlib.util
import shutil
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

COUNTRY_DIR = countries_root() / 'Nigeria'
ORG = COUNTRY_DIR / '_' / 'categorical_mapping.org'


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def nigeria(monkeypatch, tmp_path):
    shutil.copyfile(ORG, tmp_path / 'categorical_mapping.org')
    monkeypatch.chdir(tmp_path)
    return _load(COUNTRY_DIR / '_' / 'nigeria.py', '_nigeria_1144')


SOURCE_FACTS = {
    '2010Q3': {20: 'Other grains and flour', 43: 'Groundnuts',
               44: 'Other nuts/seeds/pulses', 76: 'Pepper',
               77: 'Leaves (Cocoyam, Spinach, etc.)',
               78: 'Other vegetables (fresh or canned)',
               114: 'Other milk products'},
    '2011Q1': {20: 'Other grains and flour', 43: 'Groundnuts',
               44: 'Other nuts/seeds/pulses', 76: 'Pepper',
               77: 'Leaves (Cocoyam, Spinach, etc.)',
               78: 'Other vegetables (fresh or canned)',
               114: 'Other milk products'},
    '2012Q3': {20: 'Maize (unshelled, on the cob)', 21: 'Maize (shelled)',
               22: 'Other grains and flour', 43: 'Groundnuts',
               44: 'Other nuts/seeds/pulses', 76: 'Pepper',
               77: 'Leaves (Cocoyam, Spinach, etc.)',
               78: 'Other vegetables (fresh or canned)'},
    '2013Q1': {20: 'Maize (unshelled, on the cob)', 21: 'Maize (shelled)',
               22: 'Other grains and flour', 43: 'Groundnuts (unshelled)',
               44: 'Groundnuts (shelled)', 76: 'Fresh Pepper',
               77: 'Dry Pepper', 78: 'Leaves (Cocoyam, Spinach, etc.)',
               45: 'Other nuts/seeds/pulses',
               79: 'Other vegetables (fresh or canned)', 141: 'Salt',
               142: 'Unground Ogbono', 143: 'Ground Ogbono',
               144: 'Ground Pepper', 145: 'Melon (shelled)',
               146: 'Melon (unshelled)'},
    '2015Q3': {43: 'Groundnuts (unshelled)', 44: 'Groundnuts (shelled)',
               76: 'Fresh Pepper', 77: 'Dry Pepper',
               78: 'Leaves (Cocoyam, Spinach, etc.)'},
    '2016Q1': {43: 'Groundnuts (unshelled)', 44: 'Groundnuts (shelled)',
               76: 'Fresh Pepper', 77: 'Dry Pepper',
               78: 'Leaves (Cocoyam, Spinach, etc.)'},
    '2018Q3': {77: 'Dry Pepper', 148: 'Other Spices'},
    '2019Q1': {77: 'Dry Pepper', 148: 'Other Spices'},
}


@pytest.mark.parametrize('quarter', sorted(SOURCE_FACTS))
def test_round_food_labels_match_held_source_meanings(nigeria, quarter):
    labels = nigeria.harmonized_food_labels(quarter)
    for code, expected in SOURCE_FACTS[quarter].items():
        for spelling in (code, float(code), str(code), f'{code}.0'):
            assert labels[spelling] == expected, (quarter, spelling)


@pytest.mark.parametrize('quarter', sorted(SOURCE_FACTS))
def test_real_builder_decodes_quarter_and_preserves_source_values(
        nigeria, monkeypatch, quarter):
    from lsms_library import local_tools

    facts = SOURCE_FACTS[quarter]
    codes = list(facts)
    n = len(codes)
    raw = pd.DataFrame({
        'hhid': list(range(1, n + 1)), 'item_cd': codes,
        'q_buy': np.arange(n, dtype=float) + 1,
        'q_own': np.arange(n, dtype=float) + 0.5,
        'u': [1.] * n, 'spend': np.arange(n, dtype=float) + 100,
    })
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    result = nigeria.food_acquired_for_wave(
        'synthetic.csv', quarter,
        {'purchased': {'quantity': 'q_buy', 'unit': 'u'},
         'produced': {'quantity': 'q_own', 'unit': 'u'}}, 'spend')
    frame = result.reset_index().set_index(['i', 's'])
    assert len(frame) == 2 * n
    for i, (code, expected) in enumerate(facts.items(), 1):
        for source, qcol in [('purchased', 'q_buy'), ('produced', 'q_own')]:
            row = frame.loc[(str(i), source)]
            assert row['j'] == expected, (quarter, code, source)
            assert row['Quantity'] == raw.loc[i - 1, qcol]
            if source == 'purchased':
                assert row['Expenditure'] == raw.loc[i - 1, 'spend']
            else:
                assert pd.isna(row['Expenditure'])


def test_noargument_mapping_keeps_complete_global_code_contract(nigeria):
    from lsms_library.local_tools import df_from_orgfile

    tab = df_from_orgfile(ORG, name='harmonize_food')
    expected = {int(code): str(label).strip()
                for code, label in zip(tab['Code'], tab['Preferred Label'])
                if pd.notna(label) and str(label).strip() not in ('', '---')}
    assert nigeria.harmonized_food_labels() == expected
    assert nigeria._crop_labels() == expected


@pytest.mark.parametrize('quarter', sorted(SOURCE_FACTS))
def test_neutral_item_is_unchanged(nigeria, quarter):
    assert nigeria.harmonized_food_labels(quarter)[10] == 'Guinea corn/sorghum'


@pytest.mark.parametrize('quarter', ['2023Q3', '2024Q1', 'not-a-quarter',
                                    'Code', 'Preferred Label', '2010-11'])
def test_missing_quarter_never_uses_another_codebook(nigeria, quarter):
    with pytest.raises(KeyError, match=quarter):
        nigeria.harmonized_food_labels(quarter)


def _mock_food_table(monkeypatch, rows):
    from lsms_library import local_tools

    old = local_tools.df_from_orgfile
    table = pd.DataFrame(rows, columns=['Code', 'Preferred Label', '2010Q3'])

    def read(*args, **kwargs):
        if kwargs.get('name') == 'harmonize_food':
            return table.copy()
        return old(*args, **kwargs)
    monkeypatch.setattr(local_tools, 'df_from_orgfile', read)


def test_exact_preferred_wins_over_conflicting_historical_alias(nigeria, monkeypatch):
    _mock_food_table(monkeypatch, [
        (77, 'Dry Pepper', '  LEAVES  '),
        (78, 'Leaves', 'Leaves'),
    ])
    assert nigeria.harmonized_food_labels('2010Q3')[77] == 'Leaves'


def test_unreviewed_or_ambiguous_alias_preserves_native_and_reports(
        nigeria, monkeypatch):
    _mock_food_table(monkeypatch, [
        (1, 'Specific A', 'Unqualified'),
        (2, 'Specific B', 'Unqualified'),
        (3, 'Specific C', 'Another native'),
    ])
    with pytest.warns(nigeria.FoodItemDecodeWarning, match='2010Q3'):
        labels = nigeria.harmonized_food_labels('2010Q3', observed_codes=[1, 3])
    assert labels[1] == 'Unqualified'
    assert labels[3] == 'Another native'


def test_missing_known_curation_reports_and_preserves_raw_unknown_and_nan(
        nigeria, monkeypatch):
    from lsms_library import local_tools

    _mock_food_table(monkeypatch, [(20, 'Wrong global meaning', '---')])
    raw = pd.DataFrame({'hhid': [1, 2, 3], 'item_cd': [20., 98765., np.nan],
                        'q': [1., 2., 3.], 'u': [1.] * 3, 'e': [10., 20., 30.]})
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    with pytest.warns(nigeria.FoodItemDecodeWarning, match='20'):
        result = nigeria.food_acquired_for_wave(
            'synthetic.csv', '2010Q3',
            {'purchased': {'quantity': 'q', 'unit': 'u'}}, 'e')
    assert len(result) == 3
    j = result.reset_index().set_index('i')['j']
    assert float(j['1']) == 20
    assert float(j['2']) == 98765
    assert pd.isna(j['3'])
    assert 'Wrong global meaning' not in set(j.dropna())


def test_numeric_string_items_use_the_food_round(nigeria, monkeypatch):
    from lsms_library import local_tools

    raw = pd.DataFrame({'hhid': [1, 2, 3], 'item_cd': ['20', '20.0', '43.0'],
                        'q': [1., 2., 3.], 'u': [1.] * 3, 'e': [10., 20., 30.]})
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    result = nigeria.food_acquired_for_wave(
        'synthetic.csv', '2010Q3',
        {'purchased': {'quantity': 'q', 'unit': 'u'}}, 'e')
    j = result.reset_index().set_index('i')['j']
    assert list(j) == ['Other grains and flour', 'Other grains and flour',
                       'Groundnuts']


def test_card_weight_inputs_follow_the_food_round_decoder(nigeria, monkeypatch):
    from lsms_library import local_tools

    monkeypatch.setitem(sys.modules, 'nigeria', nigeria)
    mapping = _load(COUNTRY_DIR / '2010-11' / '_' / 'mapping.py', '_mapping_1144')
    raw = pd.DataFrame({
        'hhid': [1, 2], 'item_cd': [78, 43],
        's10bq3a': [.5, .5], 's10bq3b': [61., 61.],
        's10bq4': [100., 200.],
        's10bq5a': [np.nan, np.nan], 's10bq5b': [np.nan, np.nan],
        's10bq6a': [np.nan, np.nan], 's10bq6b': [np.nan, np.nan],
    })
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    result = mapping.inputs_food_acquired_w1_card_weight()
    assert len(result) == 2
    j = result.reset_index().set_index('i')['j']
    assert j['1'] == 'Other vegetables (fresh or canned)'
    assert j['2'] == 'Groundnuts'
    assert set(result.index.get_level_values('t')) == {'2011Q1'}

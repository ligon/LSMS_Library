"""Purchase/stock separation in the five pre-EHCVM releases and split hooks.

The 2017 Mali labels explicitly name stock d and purchases e. Older labels truncate
before naming the object; the independent GH1556 review's section 2 verifies their
order from answer distributions. These numeric-source controls read neither the
production YAML column selection nor country hooks to construct expectations.
"""
from collections import Counter
import importlib.util

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root()
CASES = [
    ('Mali', '2014-15', 'EACICHOC_p2.dta', 's11q01', 's11q04', 's11q05', False),
    ('Mali', '2017-18', 'eaci17_s05p2.dta', 'choc', 's5q04', 's5q05', False),
    ('Niger', '2011-12', 'NER_2011_ECVMA_v01_M_Stata8/ecvmachoc_p1_en.dta', 'ms11q01', 'ms11q04', 'ms11q05', False),
    ('Niger', '2014-15', 'NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_MS10P1.dta', 'MS10Q01', 'MS10Q04', 'MS10Q05', True),
    ('Burkina_Faso', '2014', 'emc2014_p3_chocs_27022015.dta', 'CS0', 'CS3', 'CS4', True),
]
EFFECTS = ['AffectedIncome', 'AffectedAssets', 'AffectedProduction', 'AffectedFoodStock', 'AffectedConsumption']


def _multiset(frame):
    return Counter(frame.astype(object).where(frame.notna(), None).itertuples(index=False, name=None))


@pytest.mark.parametrize('country,wave,file,shock,prefix,cope,upper', CASES)
def test_pre_ehcvm_labels_keep_source_specific_boolean_meanings(country, wave, file, shock, prefix, cope, upper):
    block = yaml.safe_load((ROOT / country / wave / '_' / 'data_info.yml').read_text())['shocks']['myvars']
    stock, purchases = block['AffectedFoodStock'], block['AffectedConsumption']
    assert stock[0] == prefix + ('D' if upper else 'd')
    assert purchases[0] == prefix + ('E' if upper else 'e')
    assert stock[1] == purchases[1]
    mapping = stock[1]['mapping']
    changed = 'drop' if (country, wave) == ('Niger', '2011-12') else ' Diminué' if wave == '2017-18' else 'Dimunié' if country in ('Niger', 'Mali') else 'Diminué'
    assert mapping[changed] is True
    assert mapping['Increase' if country == 'Niger' and wave == '2011-12' else 'Augmenté'] is True
    assert mapping['Unchanged' if country == 'Niger' and wave == '2011-12' else 'Inchangé'] is False
    assert pd.isna(pd.Series([np.nan, 'unrecognized answer']).map(mapping)).all()


@pytest.mark.requires_s3
@pytest.mark.parametrize('country,wave,file,shock,prefix,cope,upper', CASES)
def test_pre_ehcvm_food_effects_match_independent_source_answers(country, wave, file, shock, prefix, cope, upper):
    path = ROOT / country / wave / 'Data' / file
    raw = get_dataframe(str(path), convert_categoricals=False)
    labelled = get_dataframe(str(path))
    suffixes = 'ABCDE' if upper else 'abcde'
    columns = [prefix + letter for letter in suffixes]
    copes = [cope + letter for letter in ('ABC' if upper else 'abc')]
    # Preserve the established all-payload-empty exclusion, now with both answers.
    raw = raw.loc[raw[columns + copes].notna().any(axis=1)]
    truth = {1: True, 2: True, 3: False, 4: False, 9: False}
    expected = pd.DataFrame({name: raw[col].map(truth) for name, col in zip(EFFECTS, columns)})
    expected.insert(0, 'Shock', labelled.loc[raw.index, shock].astype(object))
    served = ll.Country(country).shocks(waves=[wave]).reset_index()
    cols = ['Shock', *EFFECTS]
    assert len(served) == len(raw)
    assert _multiset(served[cols]) == _multiset(expected[cols])
    wrong = expected.copy()
    wrong['AffectedConsumption'] = wrong['AffectedFoodStock']
    assert _multiset(served[cols]) != _multiset(wrong[cols])
    # This is the independent review's purchase signature, not a YAML assertion.
    assert raw[columns[4]].eq(1).mean() > 0.30
    assert raw[columns[3]].eq(1).mean() < 0.07


@pytest.mark.parametrize('country', ['Mali', 'Burkina_Faso', 'Senegal'])
@pytest.mark.parametrize('stock,purchases', [(np.nan, True), (False, np.nan), (np.nan, np.nan)])
def test_split_answers_preserve_independent_missingness(country, stock, purchases):
    path = ROOT / country / '2018-19' / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location(f'core_{country}_shocks', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Vague 2's d is livestock. Its value cannot fill either missing food answer.
    frame = pd.DataFrame({'vague': [2], 'AffectedFoodStock': [True],
                          'StockVague2': [stock], 'AffectedConsumption': [purchases],
                          'PurchasesVague2': ['unknown labelled answer'], 'Cope1': [0]})
    out = module.shocks(frame)
    for name, value in [('AffectedFoodStock', stock), ('AffectedConsumption', purchases)]:
        actual = out[name].iloc[0]
        assert pd.isna(actual) if pd.isna(value) else actual == value

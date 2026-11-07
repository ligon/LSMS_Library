"""Food purchases and food stocks are distinct answers in Module U / IHSES20.

Synthetic rows exercise the actual YAML extraction and Iraq's experience gate.
The source oracle uses numeric response codes and joins on household and shock,
so swapping the two answers cannot pass on aggregate counts alone.
"""
import json

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import local_tools
from lsms_library.data_access import get_data_file
from lsms_library.paths import countries_root

ROOT = countries_root()
CASES = [
    ('Malawi', '2010-11', 'Full_Sample/Household/hh_mod_u.dta', 'case_id', 17422),
    ('Malawi', '2013-14', 'HH_MOD_U_13.dta', 'y2_hhid', 9952),
    ('Malawi', '2016-17', 'Cross_Sectional/hh_mod_u.dta', 'case_id', 27904),
    ('Malawi', '2019-20', 'Cross_Sectional/HH_MOD_U.dta', 'case_id', 21024),
    ('Iraq', '2012', '2012ihses20_shocks.dta', 'questid', 8812),
]
PARAMS = pytest.mark.parametrize('country,wave,file,hhid,rows', CASES)


def _columns(country):
    if country == 'Malawi':
        return ('hh_u0a', {
            'AffectedIncome': 'hh_u03a', 'AffectedAssets': 'hh_u03b',
            'AffectedProduction': 'hh_u03c', 'AffectedConsumption': 'hh_u03e',
            'AffectedFoodStock': 'hh_u03d'},
            {f'HowCoped{n}': f'hh_u04{x}' for n, x in enumerate('abc')})
    return ('shock_id', {
        'AffectedIncome': 'q2002_1', 'AffectedAssets': 'q2002_2',
        'AffectedProduction': 'q2002_3', 'AffectedConsumption': 'q2002_5',
        'AffectedFoodStock': 'q2002_4'},
        {f'HowCoped{n}': f'q2003_{n + 1}' for n in range(3)})


@PARAMS
def test_distinct_answers_and_missingness_through_extraction(
        country, wave, file, hhid, rows, monkeypatch):
    c = ll.Country(country, preload_panel_ids=False)
    w = c[wave]
    info = w.resources['shocks']
    plan = w.column_mapping('shocks', info)
    edit = plan.pop('df_edit')
    plan.pop('datetime_specs', None)
    (mapping,) = plan.values()
    shock, effects, copes = _columns(country)
    labels = ('Increase', 'Decrease', 'Did not change') if wave == '2010-11' else (
        ('Increase', 'Decrease', 'Did Not Change') if country == 'Malawi' and
        wave != '2013-14' else ('INCREASE', 'DECREASE', 'DID NOT CHANGE'))
    inc, dec, same = labels
    raw = pd.DataFrame({hhid: ['a', 'b', 'c', 'd', 'e'],
                        shock: ['shock'] * 5})
    for field in effects.values():
        raw[field] = [inc] * 5
    raw[effects['AffectedConsumption']] = [inc, same, dec, np.nan, np.nan]
    raw[effects['AffectedFoodStock']] = [same, dec, np.nan, same, np.nan]
    for field in copes.values():
        raw[field] = ['coping answer'] * 5
    if country == 'Iraq':
        raw['q2001'] = ['yes'] * 4 + ['no']
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    out = local_tools.df_data_grabber('synthetic', mapping['idxvars'], **mapping['myvars'])
    if edit is not None:
        out = edit(out)
    n = 4 if country == 'Iraq' else 5
    assert len(out) == n
    for column, expected in [
        ('AffectedConsumption', [True, False, True, pd.NA, pd.NA]),
        ('AffectedFoodStock', [False, True, pd.NA, False, pd.NA]),
    ]:
        pd.testing.assert_series_equal(
            out[column].reset_index(drop=True).astype('boolean'),
            pd.Series(expected[:n], dtype='boolean', name=column))
    assert out['HowCoped0'].eq('coping answer').all()
    assert 'Experienced' not in out
    if wave in ('2016-17', '2019-20'):
        prefix = 'cs-17-' if wave == '2016-17' else 'cs-19-'
        assert out.index.get_level_values('i').str.startswith(prefix).all()


@pytest.mark.requires_s3
@PARAMS
def test_source_questions_and_every_served_answer(country, wave, file, hhid, rows):
    import pyreadstat

    path = ROOT / country / wave / 'Data' / file
    acquired = get_data_file(path)
    assert acquired is not None
    _, metadata = pyreadstat.read_dta(str(acquired), metadataonly=True)
    shock, effects, copes = _columns(country)
    for column, phrase in [('AffectedConsumption', 'food purchases'),
                           ('AffectedFoodStock', 'food stock')]:
        field = effects[column]
        assert phrase in metadata.column_names_to_labels[field].lower()
        assert {k: v.lower() for k, v in metadata.variable_value_labels[field].items()} == {
            1: 'increase', 2: 'decrease', 3: 'did not change'}

    raw = local_tools.get_dataframe(str(path), convert_categoricals=False)
    labelled = local_tools.get_dataframe(str(path))
    expected = pd.DataFrame({col: raw[field].map({1: True, 2: True, 3: False})
                             for col, field in effects.items()})
    for col, field in copes.items():
        expected[col] = labelled[field].astype(object)
    ids = raw[hhid].map(local_tools.format_id)
    if country == 'Malawi':
        if wave in ('2016-17', '2019-20'):
            ids = ('cs-17-' if wave == '2016-17' else 'cs-19-') + ids
        elif wave == '2013-14':
            walks = json.loads((ROOT / country / '_/updated_ids.json').read_text())
            ids = ids.map(lambda value: walks[wave].get(value, value))
    expected.index = pd.MultiIndex.from_arrays(
        [[wave] * len(raw), ids, labelled[shock].map(local_tools.format_id)],
        names=['t', 'i', 'Shock'])
    if country == 'Iraq':
        expected = expected.loc[raw['q2001'].eq(1).to_numpy()]
    expected = expected.dropna(how='all').sort_index()
    actual = ll.Country(country).shocks(waves=[wave]).reorder_levels(
        ['t', 'i', 'Shock']).sort_index()
    assert len(actual) == len(expected) == rows
    # Report counts/column names only: a failed keyed oracle must not print
    # household identifiers or individual answers into CI logs.
    if not actual.index.equals(expected.index):
        raise AssertionError(f'{country}/{wave}: source and served keys differ')
    for col in effects:
        if not actual[col].equals(expected[col].astype('boolean')):
            raise AssertionError(f'{country}/{wave}: {col} differs from source')
    for col in copes:
        if not actual[col].astype('string').equals(expected[col].astype('string')):
            raise AssertionError(f'{country}/{wave}: {col} differs from source')
    # Real answers distinguish the questions, not just their labels.
    assert (actual['AffectedConsumption'] != actual['AffectedFoodStock']).any()

"""Purchases and food stocks in the four extension EHCVM waves (GH #1561).

The source oracle names columns independently of the YAML/hooks.  Source labels
are stale: Benin/Togo switch from five columns to six between vagues; Guinea-
Bissau/CotedIvoire have six throughout.  The independent animal-shock control
checks that d behaves like livestock on six-column rows, including CIV vague 1
where Non-concerne is a Stata special missing and cannot identify that column.
"""
import importlib.util

import numpy as np
import pandas as pd
from pandas.io.stata import StataMissingValue
import pytest
import yaml

import lsms_library as ll
from lsms_library.data_access import get_data_file
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root()
FILES = {
    'Benin': ('2018-19', 'Data/s14_me_ben2018.dta'),
    'Togo': ('2018', 'Data1/s14_me_tgo2018.dta'),
    'Guinea-Bissau': ('2018-19', 'Data/s14_me_gnb2018.dta'),
    'CotedIvoire': ('2018-19', 'Data/Menage/s14_me_CIV2018.dta'),
}
SPLIT = ('Benin', 'Togo')
FOOD = ['AffectedConsumption', 'AffectedFoodStock']
# Independent own-source scope, not read from the production missing-tag map.
CIV_EFFECT_FIELDS = tuple(f's14q04{x}' for x in 'abcef')


def _block(country):
    wave, _ = FILES[country]
    return yaml.safe_load((ROOT / country / wave / '_' / 'data_info.yml').read_text())['shocks']['myvars']


def _module(country):
    wave, _ = FILES[country]
    spec = importlib.util.spec_from_file_location(
        f'extension_shocks_{country}', ROOT / country / wave / '_' / 'mapping.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('country', FILES)
def test_both_effects_keep_the_existing_answer_mapping(country):
    block = _block(country)
    expected_stock = 's14q04d' if country in SPLIT else 's14q04e'
    assert block['AffectedFoodStock'][0] == expected_stock
    assert block['AffectedConsumption'][0] == 's14q04f'
    for effect in FOOD:
        assert block[effect][1] == block['AffectedIncome'][1]
    if country in SPLIT:
        assert block['StockVague2'] == ['s14q04e', block['AffectedIncome'][1]]
        assert block['PurchasesVague2'] == 's14q04f'
        assert block['vague'] == 'vague'


def _frame(**overrides):
    # Opposite answers expose swaps.  Last two rows exercise missing selected
    # answers without using a true/false result as the raw layout sentinel.
    frame = pd.DataFrame({
        'vague': [1, 1, 2, 2, 1, 2],
        'AffectedFoodStock': [True, False, True, True, np.nan, np.nan],
        'StockVague2': [False, True, False, True, np.nan, np.nan],
        'AffectedConsumption': [np.nan, np.nan, True, False, np.nan, np.nan],
        'PurchasesVague2': [np.nan, np.nan, 'Diminué', 'Inchangé', np.nan, np.nan],
        'AffectedIncome': [False, True, False, True, np.nan, np.nan],
        'Cope1': [1, 0, 1, 1, np.nan, np.nan],
    }, index=pd.Index([4, 1, 5, 0, 3, 2], name='source_row'))
    for key, value in overrides.items():
        frame[key] = value
    return frame


@pytest.mark.parametrize('country', SPLIT)
def test_split_effects_are_selected_per_row_and_preserve_missingness(country):
    frame = _frame()
    out = _module(country).shocks(frame.copy())
    assert out.index.equals(frame.index)
    pd.testing.assert_series_equal(out.AffectedIncome, frame.AffectedIncome)
    expected = pd.DataFrame({
        'AffectedConsumption': [False, True, True, False, np.nan, np.nan],
        'AffectedFoodStock': [True, False, False, True, np.nan, np.nan],
    }, index=frame.index)
    pd.testing.assert_frame_equal(out[FOOD], expected)
    assert not {'StockVague2', 'PurchasesVague2', 'vague', 'Cope1'} & set(out)
    assert ('HowCoped2' in out) == (country != 'Benin')


@pytest.mark.parametrize('country', SPLIT)
@pytest.mark.parametrize('vague,d,e,f,raw_f,missing,other', [
    (1, True, np.nan, np.nan, np.nan, 'AffectedConsumption', 'AffectedFoodStock'),
    (2, True, np.nan, False, 'Inchangé', 'AffectedFoodStock', 'AffectedConsumption'),
    (2, True, False, np.nan, 'unmapped answer', 'AffectedConsumption', 'AffectedFoodStock'),
])
def test_one_missing_effect_does_not_fill_or_erase_the_other(
        country, vague, d, e, f, raw_f, missing, other):
    frame = pd.DataFrame({'vague': [vague], 'AffectedFoodStock': [d],
                          'StockVague2': [e], 'AffectedConsumption': [f],
                          'PurchasesVague2': [raw_f]})
    out = _module(country)._food_effects_by_vague(frame)
    assert pd.isna(out[missing].iloc[0])
    assert bool(out[other].iloc[0]) == (vague == 1)


@pytest.mark.parametrize('country', SPLIT)
@pytest.mark.parametrize('bad', [
    {'PurchasesVague2': ['Diminué', np.nan, 'Diminué', 'Inchangé', np.nan, np.nan]},
    {'PurchasesVague2': [np.nan, np.nan, np.nan, 'Inchangé', np.nan, np.nan]},
    {'vague': [1, 1, 2, 2, 3, 2]},
], ids=['f-filled-in-vague1', 'f-blank-in-vague2', 'unexpected-vague'])
def test_changed_split_layout_is_refused_loudly(country, bad):
    module = _module(country)
    assert not issubclass(module.ShockLayoutError, (ValueError, KeyError, RuntimeError))
    with pytest.raises(module.ShockLayoutError, match='s14q04f should be blank in vague 1'):
        module.shocks(_frame(**bad))


def _truth(values, *, country=None, tagged=None):
    # Independent finite-code oracle. Only CIV's five opted-in effects give
    # typed .d the existing Non-concerné -> False meaning (ledger 1687 §3–4).
    out = pd.to_numeric(values).map({1: True, 2: True, 3: False, 4: False}).astype('boolean')
    if country == 'CotedIvoire' and values.name in CIV_EFFECT_FIELDS:
        assert tagged is not None
        assert tagged.index.identical(values.index)
        is_d = tagged.map(lambda value: isinstance(value, StataMissingValue)
                          and value.string == '.d')
        assert values.loc[is_d].isna().all()
        out.loc[is_d] = False
    return out


def _effect_truth(raw, country, letter):
    field = f's14q04{letter}'
    return _truth(raw[field], country=country, tagged=raw.get(f'{field}__tag'))


def _tag(token):
    # Invented typed operands, using pandas' own representation table.
    value = next(value for value, label in StataMissingValue.MISSING_VALUES.items()
                 if label == token)
    return StataMissingValue(value)


@pytest.mark.parametrize('field', CIV_EFFECT_FIELDS)
def test_civ_oracle_maps_only_typed_d_and_preserves_other_missing(field):
    index = pd.Index([9, 2, 7, 1, 8, 0, 6, 3, 5, 4], name='source_row')
    values = pd.Series([1, 2, 3, 4, *([np.nan] * 6)], index=index, name=field)
    tagged = pd.Series([1, 2, 3, 4, _tag('.d'), _tag('.'), _tag('.a'),
                        _tag('.z'), '.d', np.nan], index=index, name=field)
    before = values.copy(deep=True)
    native_before = tagged.copy(deep=True)
    expected = pd.Series([True, True, False, False, False, *([pd.NA] * 5)],
                         index=index, name=field, dtype='boolean')
    pd.testing.assert_series_equal(_truth(values, country='CotedIvoire', tagged=tagged), expected)
    pd.testing.assert_series_equal(values, before)
    pd.testing.assert_series_equal(tagged, native_before)
    # The default oracle retains the original finite mapping and NA mask.
    pd.testing.assert_series_equal(_truth(values), expected.mask(tagged.map(
        lambda value: isinstance(value, StataMissingValue) and value.string == '.d')))


@pytest.mark.parametrize('country,field', [
    *[(country, field) for country in ('Benin', 'Togo', 'Guinea-Bissau')
      for field in CIV_EFFECT_FIELDS],
    *[('CotedIvoire', field) for field in ('s14q04d', 's14q01', 's14q05__1')],
])
def test_tagged_oracle_does_not_extend_country_or_field_scope(country, field):
    values = pd.Series([np.nan, 1, 2, 3, 4], name=field)
    tagged = pd.Series([_tag('.d'), 1, 2, 3, 4], name=field)
    pd.testing.assert_series_equal(_truth(values, country=country, tagged=tagged), _truth(values))


def test_civ_food_oracle_keeps_purchase_stock_and_livestock_distinct():
    raw = pd.DataFrame({
        's14q04d': [np.nan, 2, np.nan, 1],
        's14q04e': [np.nan, 1, np.nan, 3],
        's14q04f': [1, np.nan, np.nan, np.nan],
        's14q04e__tag': [_tag('.d'), 1, _tag('.'), 3],
        's14q04f__tag': [1, _tag('.d'), _tag('.'), _tag('.a')],
    }, index=pd.Index([4, 1, 3, 2], name='source_row'))
    before = raw.copy(deep=True)
    for letter, answers in [('e', [False, True, pd.NA, False]),
                            ('f', [True, False, pd.NA, pd.NA])]:
        expected = pd.Series(answers, index=raw.index, name=f's14q04{letter}', dtype='boolean')
        pd.testing.assert_series_equal(_effect_truth(raw, 'CotedIvoire', letter), expected)
    pd.testing.assert_series_equal(_effect_truth(raw, 'CotedIvoire', 'd'), _truth(raw.s14q04d))
    pd.testing.assert_frame_equal(raw, before)


def test_civ_oracle_requires_independent_aligned_missing_operands():
    values = pd.Series([np.nan, 1], index=[4, 2], name='s14q04e')
    with pytest.raises(AssertionError):
        _truth(values, country='CotedIvoire')
    with pytest.raises(AssertionError):
        _truth(values, country='CotedIvoire', tagged=pd.Series([_tag('.d'), 1]))
    with pytest.raises(AssertionError):
        _truth(values, country='CotedIvoire', tagged=pd.Series([_tag('.'), _tag('.d')], index=values.index))


@pytest.fixture(scope='module')
def source():
    cached = {}

    def read(country):
        if country not in cached:
            wave, filename = FILES[country]
            path = ROOT / country / wave / filename
            raw = get_dataframe(str(path), convert_categoricals=False)
            labels = get_dataframe(str(path))['s14q01'].astype(object)
            if country == 'CotedIvoire':
                # Resolve the managed source normally, then scan independently
                # of get_dataframe's production stata_missing_map overlay.
                managed = get_data_file(path)
                assert managed is not None
                with pd.io.stata.StataReader(managed) as reader:
                    tagged = reader.read(columns=list(CIV_EFFECT_FIELDS),
                                         convert_missing=True,
                                         convert_categoricals=False,
                                         convert_dates=False)
                assert tagged.index.identical(raw.index)
                assert list(tagged) == list(CIV_EFFECT_FIELDS)
            # Every row in the old module remains: use only its original fields
            # to identify rows, rather than the newly selected food effects.
            old_fields = [f's14q04{x}' for x in 'abcd'] + [
                c for c in raw if c.startswith('s14q05__')]
            raw = raw.loc[raw[old_fields].notna().any(axis=1)].copy()
            if country == 'CotedIvoire':
                for field in CIV_EFFECT_FIELDS:
                    raw[f'{field}__tag'] = tagged.loc[raw.index, field]
            raw['Shock'] = labels.loc[raw.index]
            raw['i'] = raw['grappe'].astype(int).astype(str) + raw['menage'].astype(int).astype(str).str.zfill(3)
            raw['t'] = wave
            cached[country] = raw
        return cached[country]
    return read


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', FILES)
def test_source_columns_match_served_rows_and_reject_wrong_columns(country, source):
    raw = source(country)
    wave, _ = FILES[country]
    expected = raw[['t', 'i', 'Shock']].copy()
    for name, letter in [('AffectedIncome', 'a'), ('AffectedAssets', 'b'), ('AffectedProduction', 'c')]:
        expected[name] = _effect_truth(raw, country, letter)
    expected['AffectedFoodStock'] = _effect_truth(raw, country, 'e')
    expected['AffectedConsumption'] = _effect_truth(raw, country, 'f')
    if country in SPLIT:
        first = raw.vague.eq(1)
        expected.loc[first, 'AffectedFoodStock'] = _truth(raw.loc[first, 's14q04d'])
        expected.loc[first, 'AffectedConsumption'] = _truth(raw.loc[first, 's14q04e'])
    expected = expected.set_index(['i', 't', 'Shock']).sort_index()
    served = ll.Country(country).shocks(waves=[wave]).sort_index()
    index_matches = served.index.equals(expected.index)
    assert index_matches, f'{country}: served row/index identity differs from source'
    pd.testing.assert_frame_equal(served[expected.columns].astype('boolean'), expected,
                                  check_index_type=False)
    # These controls fail for the old all-d selector and for exchanging the
    # two new columns, including their missingness rather than dropping NA.
    old = raw[['t', 'i', 'Shock']].assign(AffectedConsumption=_truth(raw.s14q04d))
    old = old.set_index(['i', 't', 'Shock']).sort_index().AffectedConsumption
    assert not expected.AffectedConsumption.equals(old)
    assert not expected.AffectedConsumption.equals(expected.AffectedFoodStock.rename('AffectedConsumption'))
    assert all(str(served[name].dtype) == 'boolean' for name in FOOD)


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', SPLIT)
def test_source_f_has_the_split_layout(country, source):
    raw = source(country)
    assert raw.loc[raw.vague.eq(1), 's14q04f'].isna().all()
    assert raw.loc[raw.vague.eq(2), 's14q04f'].notna().all()


@pytest.mark.requires_s3
@pytest.mark.parametrize('country,vague', [
    ('Benin', 2), ('Togo', 2), ('Guinea-Bissau', 1), ('Guinea-Bissau', 2),
    ('CotedIvoire', 1), ('CotedIvoire', 2),
])
def test_six_column_d_responds_to_animal_disease_not_food(country, vague, source):
    raw = source(country)
    rows = raw.loc[raw.vague.eq(vague)]
    animal_shock = rows.Shock.str.contains('maladies des animaux|doenças.*anima|animais', case=False, regex=True)
    assert animal_shock.sum() >= 50

    def decrease_contrast(letter):
        values = rows[f's14q04{letter}']
        valid = values.notna()
        return values[animal_shock & valid].eq(2).mean() - values[~animal_shock & valid].eq(2).mean()

    assert decrease_contrast('d') > 0.3
    assert all(decrease_contrast('d') - decrease_contrast(letter) > 0.3 for letter in 'ef')

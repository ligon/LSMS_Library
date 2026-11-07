"""EHCVM 2018-19 ``shocks``: section 14.04 is not what the ``.dta`` labels say (GH #1556).

The labels read d = stock, e = achat, f = achats again, with no livestock.  The
questionnaire prints six columns: revenus, avoirs, production agricole, effectif
du cheptel, stock, achats.  Both are true somewhere in the file:

* Niger asked the six columns in both vagues.
* Mali, Senegal and Burkina Faso asked five in vague 1 (d = stock, e = achats,
  f blank) and the six in vague 2.

So ``s14q04d``, which ``AffectedConsumption`` read throughout, is livestock on
those rows.  The oracle here identifies the columns by behaviour, not by label:
a household with no animals should answer Non-concerne on the livestock question.
"""
import collections
import importlib.util

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root()
WAVE = '2018-19'
FILES = {'Niger': 'ner', 'Mali': 'mli', 'Senegal': 'sen', 'Burkina_Faso': 'bfa'}
SPLIT = ['Mali', 'Senegal', 'Burkina_Faso']
#: Margin by which the livestock column must out-respond another column to owning no
#: animals, and the most any column may show in a vague that never asked about livestock.
OWNERSHIP_MARGIN = 0.10
NO_LIVESTOCK_QUESTION = 0.15


def _block(country):
    info = yaml.safe_load((ROOT / country / WAVE / '_' / 'data_info.yml').read_text(encoding='utf-8'))
    return info['shocks']['myvars']


def test_niger_reads_stock_e_and_purchases_f_in_both_vagues():
    assert _block('Niger')['AffectedFoodStock'][0] == 's14q04e'
    assert _block('Niger')['AffectedConsumption'][0] == 's14q04f'


@pytest.mark.parametrize('country', SPLIT)
def test_split_files_bring_both_columns_through_one_mapping(country):
    block = _block(country)
    assert block['AffectedFoodStock'][0] == 's14q04d'
    assert block['AffectedConsumption'][0] == 's14q04f'
    assert block['StockVague2'][0] == 's14q04e'
    assert block['AffectedFoodStock'][1] == block['StockVague2'][1] == block['AffectedConsumption'][1]
    assert block['PurchasesVague2'] == 's14q04f' and block['vague'] == 'vague'


def _module(country):
    path = ROOT / country / WAVE / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location(f'{country}_{WAVE}_gh1556', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _frame(**extra):
    base = {'vague': [1, 1, 2, 2, 2],
            'AffectedFoodStock': [True, False, True, True, np.nan],
            'AffectedConsumption': [np.nan, np.nan, True, False, np.nan],
            'StockVague2': [False, True, False, False, np.nan],
            'PurchasesVague2': [np.nan, np.nan, 'Diminué', 'Inchangé', np.nan],
            'Cope1': [1, 0, 1, 1, np.nan]}
    base.update(extra)
    return pd.DataFrame(base)


@pytest.mark.parametrize('country', SPLIT)
class TestStockByVague:
    def test_each_vague_serves_its_own_stock_column(self, country):
        out = _module(country).shocks(_frame())
        assert out['AffectedFoodStock'].tolist()[:4] == [True, False, False, False]
        assert out['AffectedConsumption'].tolist()[:4] == [False, True, True, False]
        assert pd.isna(out['AffectedConsumption'].iloc[4])
        assert pd.isna(out['AffectedFoodStock'].iloc[4])

    def test_helpers_do_not_reach_the_table(self, country):
        out = _module(country).shocks(_frame())
        assert not {'vague', 'StockVague2', 'PurchasesVague2', 'Cope1'} & set(out.columns)

    @pytest.mark.parametrize('bad', [
        {'PurchasesVague2': ['Diminué', np.nan, 'Diminué', 'Inchangé', np.nan]},
        {'PurchasesVague2': [np.nan, np.nan, np.nan, 'Inchangé', np.nan]},
        {'vague': [1, 1, 2, 2, 3]},
    ], ids=['f-filled-in-vague-1', 'f-blank-in-vague-2', 'unknown-vague'])
    def test_a_release_with_another_layout_is_refused(self, country, bad):
        module = _module(country)
        with pytest.raises(module.ShockLayoutError):
            module.shocks(_frame(**bad))

    def test_the_refusal_survives_the_fallback_catches(self, country):
        """The class survives the catches at one worker; the text survives everywhere.

        In an all-wave cold build at two workers the error reaches the caller as
        ``RuntimeError`` carrying this text, until the failure-signals PR lands (#1524).
        So the message has to say what is wrong on its own.  ``Feature`` drops the
        country with a warning that names it.  Measured by injection in the review of
        #1556; this test does not repeat that injection.
        """
        module = _module(country)
        assert not issubclass(module.ShockLayoutError, (ValueError, KeyError, RuntimeError))
        filled = {'PurchasesVague2': ['Diminué', np.nan, 'Diminué', 'Inchangé', np.nan]}
        with pytest.raises(module.ShockLayoutError) as refused:
            module.shocks(_frame(**filled))
        assert 's14q04f should be blank in vague 1' in str(refused.value)
        assert '1 filled in vague 1' in str(refused.value)


# ---------------------------------------------------------------------------- source-gated

def _source(country):
    data = ROOT / country / WAVE / 'Data'
    code = FILES[country]
    raw = get_dataframe(str(data / f's14_me_{code}2018.dta'), convert_categoricals=False)
    raw = raw.assign(Shock=get_dataframe(str(data / f's14_me_{code}2018.dta'))['s14q01'].astype(object).values)
    letters = [f's14q04{x}' for x in 'abcdef']
    copes = [c for c in raw.columns if c.startswith('s14q05__')]
    raw = raw[raw[letters + copes].notna().any(axis=1)].copy()
    animals = get_dataframe(str(data / f's17_me_{code}2018.dta'), convert_categoricals=False)
    owners = animals.loc[pd.to_numeric(animals['s17q03']) == 1, ['grappe', 'menage']].drop_duplicates()
    owners = owners.assign(owns=True)
    return raw.merge(owners, on=['grappe', 'menage'], how='left').assign(
        owns=lambda d: d['owns'].fillna(False).astype(bool))


@pytest.fixture(scope='module')
def source():
    cache = {}

    def read(country):
        if country not in cache:
            cache[country] = _source(country)
        return cache[country]
    return read


def _responds_to_owning_no_animals(frame, letter):
    """P(Non-concerne | no animals) - P(Non-concerne | animals); code 4 is Non-concerne."""
    column = pd.to_numeric(frame[f's14q04{letter}'])
    return (column[~frame['owns']] == 4).mean() - (column[frame['owns']] == 4).mean()


def _truth(series):
    return pd.to_numeric(series).map({1: True, 2: True, 3: False, 4: False})


def _multiset(frame):
    frame = frame.astype(object).where(frame.notna(), None)
    return collections.Counter(frame.itertuples(index=False, name=None))


def _expected(raw, rule, purchase_rule):
    stock = rule(raw)
    return pd.DataFrame({'Shock': raw['Shock'].values,
                         'AffectedIncome': _truth(raw['s14q04a']).values,
                         'AffectedAssets': _truth(raw['s14q04b']).values,
                         'AffectedProduction': _truth(raw['s14q04c']).values,
                         'AffectedFoodStock': stock.values,
                         'AffectedConsumption': purchase_rule(raw).values})


def _by_layout(raw):
    d, e = _truth(raw['s14q04d']), _truth(raw['s14q04e'])
    return d.where(pd.to_numeric(raw['vague']) == 1, e)


def _purchases_by_layout(raw):
    e, f = _truth(raw['s14q04e']), _truth(raw['s14q04f'])
    return e.where(pd.to_numeric(raw['vague']) == 1, f)


@pytest.mark.requires_s3
class TestAgainstTheSource:
    @pytest.mark.parametrize('country', SPLIT)
    def test_f_is_blank_exactly_in_vague_1(self, country, source):
        raw = source(country)
        vague = pd.to_numeric(raw['vague'])
        assert raw.loc[vague == 1, 's14q04f'].isna().all()
        assert raw.loc[vague == 2, 's14q04f'].notna().all()

    def test_niger_filled_f_in_both_vagues(self, source):
        assert source('Niger')['s14q04f'].notna().all()

    @pytest.mark.parametrize('country,vague', [('Niger', 1), ('Niger', 2), *((c, 2) for c in SPLIT)])
    def test_d_is_the_livestock_question_on_six_column_rows(self, country, vague, source):
        raw = source(country)
        rows = raw[pd.to_numeric(raw['vague']) == vague]
        d = _responds_to_owning_no_animals(rows, 'd')
        for other in 'ef':
            assert d - _responds_to_owning_no_animals(rows, other) > OWNERSHIP_MARGIN

    @pytest.mark.parametrize('country', SPLIT)
    def test_no_column_is_the_livestock_question_in_vague_1(self, country, source):
        raw = source(country)
        rows = raw[pd.to_numeric(raw['vague']) == 1]
        for letter in 'de':
            assert _responds_to_owning_no_animals(rows, letter) < NO_LIVESTOCK_QUESTION

    @pytest.mark.parametrize('country,rule,purchase_rule', [
        ('Niger', lambda raw: _truth(raw['s14q04e']), lambda raw: _truth(raw['s14q04f'])),
        *((c, _by_layout, _purchases_by_layout) for c in SPLIT)])
    def test_served_is_the_stock_and_purchases_of_each_layout(self, country, rule, purchase_rule, source):
        raw = source(country)
        served = ll.Country(country).shocks(waves=[WAVE]).reset_index()
        cols = ['Shock', 'AffectedIncome', 'AffectedAssets', 'AffectedProduction', 'AffectedConsumption', 'AffectedFoodStock']
        assert _multiset(served[cols]) == _multiset(_expected(raw, rule, purchase_rule)[cols])
        assert _multiset(served[cols]) != _multiset(_expected(raw, lambda r: _truth(r['s14q04d']), purchase_rule)[cols])
        assert _multiset(served[cols]) != _multiset(_expected(raw, rule, rule)[cols])

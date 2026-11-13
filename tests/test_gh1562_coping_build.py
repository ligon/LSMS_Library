"""The BUILD-time half of the GH #1562 coping contract.

Rulings (``isa-decisions.org``, 2026-11-12 and 2026-11-13):

* EHCVM 2018-19 (Benin, Burkina, CIV, GNB, Mali, Niger, Senegal, Togo): the
  hooks write ``HowCopedSet`` from the ``s14q05__N`` grid and no position;
  CIV's hook lives in its wave ``mapping.py``; GNB's codes 18-25 follow the
  Portuguese questionnaire.
* EHCVM 2021-22 option 25: the inferred position is kept and registered.
* Nigeria: W1-W3 rank four (``HowCoped3``), W4 fills the set only, and
  ``shocks`` (and ``assets``, the same unpinned shape) is served once per wave.
* Liberia: forest products are not coping strategies.

Static tier only (synthetic frames, the YAML, the hooks); the served tables are
measured in the stream report and in the ``requires_s3`` tests at the bottom.
"""
from __future__ import annotations

import importlib.util
import re
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import ehcvm
from lsms_library.ehcvm import (
    COPING_SET_COLUMN, CopingGridError, coping_autre_rank_key, coping_set_from_grid,
    derive_coping_autre_rank,
)
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

ROOT = countries_root()
EHCVM_2018 = {'Benin': '2018-19', 'Burkina_Faso': '2018-19', 'CotedIvoire': '2018-19',
              'Guinea-Bissau': '2018-19', 'Mali': '2018-19', 'Niger': '2018-19',
              'Senegal': '2018-19', 'Togo': '2018'}


def _module(country, wave, name='mapping.py'):
    path = ROOT / country / wave / '_' / name
    spec = importlib.util.spec_from_file_location(f'gh1562_{country}_{wave}_{name}', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------- the 2018-19 grid

LABELS = {1: 'a', 2: 'b', 25: 'z', 26: 'Aucune stratégie'}


def _grid(**cols):
    return pd.DataFrame({f'Cope{k}': v for k, v in cols.items()},
                        index=pd.Index(range(len(next(iter(cols.values())))), name='row'))


def test_positive_values_are_chosen_zero_is_not_na_is_not_asked():
    df = _grid(**{'1': [1, 0, 3, np.nan, 0], '2': [3, 0, 1, np.nan, 2],
                  '26': [0, 1, 0, np.nan, 0]})
    df['Other'] = 7
    out = coping_set_from_grid(df, LABELS, country='X')
    assert out[COPING_SET_COLUMN].tolist() == ['a+b', 'Aucune stratégie', 'a+b', pd.NA, 'b']
    assert list(out.columns) == ['Other', 'HowCoped0', 'HowCoped1', 'HowCoped2', COPING_SET_COLUMN]
    assert out[['HowCoped0', 'HowCoped1', 'HowCoped2']].isna().all().all()
    assert str(out[COPING_SET_COLUMN].dtype) == 'string'


@pytest.mark.parametrize('value', [4, -1, 0.5, 'Oui'])
def test_a_value_outside_the_reading_is_refused(value):
    df = _grid(**{'1': [1, value]})
    with pytest.raises(CopingGridError):
        coping_set_from_grid(df, LABELS, country='X')
    assert not issubclass(CopingGridError, (ValueError, KeyError, RuntimeError))


def test_a_chosen_code_without_a_label_is_refused():
    with pytest.raises(CopingGridError, match='no label'):
        coping_set_from_grid(_grid(**{'7': [1]}), LABELS, country='X')


def test_an_unchosen_code_without_a_label_is_fine():
    out = coping_set_from_grid(_grid(**{'1': [1], '7': [0]}), LABELS, country='X')
    assert out[COPING_SET_COLUMN].tolist() == ['a']


def test_a_label_with_the_separator_is_refused():
    with pytest.raises(CopingGridError, match='separator'):
        coping_set_from_grid(_grid(**{'1': [1]}), {1: 'a+b'}, country='X')


@pytest.mark.parametrize('country', EHCVM_2018)
def test_every_2018_hook_writes_the_set_and_na_positions(country):
    module = _module(country, EHCVM_2018[country])
    n = max(module.COPING_LABELS)
    frame = pd.DataFrame({f'Cope{k}': [0, 0] for k in range(1, n + 1)})
    frame.loc[0, 'Cope1'] = 1
    frame.loc[0, 'Cope2'] = 3
    if country in ('Benin', 'Togo', 'Burkina_Faso', 'Mali', 'Senegal'):
        # the per-vague 14.04 layout (GH #1556 / #1561): vague 1, f blank
        frame = frame.assign(vague=[1, 1], AffectedFoodStock=[True, False],
                             StockVague2=[np.nan, np.nan], PurchasesVague2=[np.nan, np.nan],
                             AffectedConsumption=[np.nan, np.nan])
    out = module.shocks(frame)
    positions = [c for c in out if str(c).startswith('HowCoped') and c != COPING_SET_COLUMN]
    assert positions == ['HowCoped0', 'HowCoped1', 'HowCoped2']
    assert out[positions].isna().all().all()
    assert not [c for c in out if str(c).startswith('Cope')]
    lab = module.COPING_LABELS
    assert out[COPING_SET_COLUMN].iloc[0] == '+'.join(sorted({lab[1], lab[2]}))
    assert pd.isna(out[COPING_SET_COLUMN].iloc[1])


def test_civ_2018_hook_lives_in_its_wave_module():
    """Audit §5: in the country module every edit re-warmed all 126 CIV wave hashes."""
    src = (ROOT / 'CotedIvoire' / '_' / 'cotedivoire.py').read_text(encoding='utf-8')
    assert 'def shocks(' not in src and '_COPING_LABELS' not in src
    module = _module('CotedIvoire', '2018-19')
    assert callable(module.shocks) and len(module.COPING_LABELS) == 26


# --------------------------------------------------------------- GNB 18-25

GNB_PORTUGUESE = {  # gnb_ehcvm1_qnr_household_excel_vague1.xlsx, S14_Chocs, column N
    18: 'Vente du stock de vivres',                   # 18. Venda das existencias de provisoes
    19: 'Pratique plus importante des activités de pêche',  # 19. ... atividades de pesca
    20: 'Vente de bétail',                            # 20. Venda de gado
    21: "Confiage des enfants à d'autres ménages",    # 21. Confiar os filhos a outros agregados
    22: 'Engagé dans des activités spirituelles',     # 22. Praticante de atividades espirituais
    25: 'Aucune stratégie',                           # 25. Nenhuma estrategia
}


def test_gnb_codes_follow_the_portuguese_questionnaire():
    labels = _module('Guinea-Bissau', '2018-19').COPING_LABELS
    for code, label in GNB_PORTUGUESE.items():
        assert labels[code] == label, code
    assert 'Louer/mettre ses terres en gages' not in labels.values()
    assert max(labels) == 25 and len(labels) == 25
    # the other EHCVM countries keep the French template's 26 codes
    benin = _module('Benin', '2018-19').COPING_LABELS
    assert all(labels[k] == benin[k] for k in range(1, 18))


def test_gnb_positions_are_optional_and_the_set_declared():
    shocks = load_yaml(ROOT / 'Guinea-Bissau' / '_' / 'data_scheme.yml')['Data Scheme']['shocks']
    for k in range(3):
        assert shocks[f'HowCoped{k}'].get('optional') is True
    assert shocks['HowCopedSet'] == 'str'


# --------------------------------------------------------------- 2021-22 option 25

@pytest.mark.parametrize('held,rank', [(set(), 1), ({1}, 2), ({2}, 1), ({1, 3}, 2), ({1, 2}, 3)])
def test_option_25_takes_the_least_rank_no_other_holds(held, rank):
    assert derive_coping_autre_rank(held) == rank


FIRST, SECOND = 'Première stratégie', 'Deuxième strategie'


@pytest.mark.parametrize('country', ['Benin', 'Niger'])
def test_option_25_beside_a_ranked_strategy_carries_the_key(country):
    module = _module(country, '2021-22')
    rows = [{'Cope1': FIRST, 'Cope25': 'Oui'},    # beside: inferred, keyed
            {'Cope25': 'Oui'},                    # alone: not in doubt, no key
            {'Cope1': SECOND, 'Cope25': 'Oui'},   # the lone Deuxieme: 25 is served first
            {'Cope1': FIRST}]                     # no option 25
    frame = pd.DataFrame([{**{f'Cope{n}': 'Non' for n in (1, 2, 25, 26)}, **r} for r in rows])
    out = module.shocks(frame)
    key = coping_autre_rank_key(country)
    assert out['Derivation'].tolist() == [key, pd.NA, key, pd.NA]
    lab = module.COPING_LABELS
    assert out['HowCoped0'].tolist() == [lab[1], lab[25], lab[25], lab[1]]


@pytest.mark.parametrize('country', ['Benin', 'Niger'])
def test_option_25_is_registered(country):
    from lsms_library.derivations import records_for, resolve_callable
    recs = records_for(country, 'shocks')
    key = coping_autre_rank_key(country)
    assert key in recs
    rec = recs[key]
    assert rec.function == 'lsms_library.ehcvm:derive_coping_autre_rank'
    assert callable(resolve_callable(rec.inputs))
    assert [str(w) for w in rec.waves] == ['2021-22']


def test_derivation_is_declared_under_shocks():
    canon = load_yaml(ROOT.parent / 'data_info.yml')['Columns']['shocks']
    for col in ('Derivation', 'HowCopedSet', 'HowCoped3'):
        assert canon[col].get('optional') is True, col


# --------------------------------------------------------------- Nigeria

NIGERIA = {'2010-11': ('2010Q3', '2011Q1'), '2012-13': ('2012Q3', '2013Q1'),
           '2015-16': ('2015Q3', '2016Q1'), '2018-19': ('2018Q3', '2019Q1')}


def _pinned(block):
    """The t each listed file is pinned to."""
    files = block['file']
    assert isinstance(files, list), block
    return [next(iter(f.values()))['t'] for f in files]


@pytest.mark.parametrize('wave', NIGERIA)
def test_nigeria_shocks_is_served_once_under_the_post_harvest_quarter(wave):
    info = load_yaml(ROOT / 'Nigeria' / wave / '_' / 'data_info.yml')
    pp, ph = NIGERIA[wave]
    assert _pinned(info['shocks']) == [ph]
    assert 't' in info['shocks']['idxvars']
    # the same shape as the individual_education pin it follows
    assert _pinned(info['individual_education'])[-1] == ph


@pytest.mark.parametrize('wave', NIGERIA)
def test_nigeria_assets_is_served_once_under_the_post_planting_quarter(wave):
    info = load_yaml(ROOT / 'Nigeria' / wave / '_' / 'data_info.yml')
    pp, _ = NIGERIA[wave]
    block = info['assets']
    subs = [block[name] for name in block['dfs']] if 'dfs' in block else [block]
    for sub in subs:
        assert _pinned(sub) == [pp]
        assert 't' in sub['idxvars']


@pytest.mark.parametrize('wave', ['2010-11', '2012-13', '2015-16'])
def test_nigeria_w1_w3_rank_four(wave):
    myvars = load_yaml(ROOT / 'Nigeria' / wave / '_' / 'data_info.yml')['shocks']['myvars']
    assert [myvars[f'HowCoped{k}'] for k in range(4)] == [f's15aq5{x}' for x in 'abcd']


def test_nigeria_w4_fills_the_set_only():
    myvars = load_yaml(ROOT / 'Nigeria' / '2018-19' / '_' / 'data_info.yml')['shocks']['myvars']
    assert not [k for k in myvars if k.startswith('HowCoped')]
    assert [myvars[f'Cope{k}'] for k in range(1, 5)] == [f's15aq7_{k}' for k in range(1, 5)]
    module = _module('Nigeria', '2018-19')
    frame = pd.DataFrame({'Cope1': pd.Categorical(['21. DID NOTHING', '16. RELIED ON SAVINGS', np.nan]),
                          'Cope2': [np.nan, '1. SALE OF LIVESTOCK', np.nan],
                          'Cope3': [np.nan, '16. RELIED ON SAVINGS', np.nan],
                          'Cope4': [np.nan, '12. DELAYED PAYMENT OBLIGATIONS', np.nan]})
    out = module.shocks(frame)
    assert list(out.columns) == ['HowCoped0', 'HowCoped1', 'HowCoped2', COPING_SET_COLUMN]
    assert out[['HowCoped0', 'HowCoped1', 'HowCoped2']].isna().all().all()
    assert out[COPING_SET_COLUMN].tolist() == [
        '21. DID NOTHING',
        '1. SALE OF LIVESTOCK+12. DELAYED PAYMENT OBLIGATIONS+16. RELIED ON SAVINGS',
        pd.NA]


# --------------------------------------------------------------- Liberia

def test_liberia_forest_products_are_not_coping_strategies():
    myvars = load_yaml(ROOT / 'Liberia' / '2018-19' / '_' / 'data_info.yml')['shocks']['myvars']
    assert not [k for k in myvars if k.startswith('HowCoped')]
    assert [myvars[f'ForestProduct{k}'] for k in range(3)] == ['S17_5_A', 'S17_5_B', 'S17_5_C']
    module = _module('Liberia', '2018-19')
    frame = pd.DataFrame({'Experienced': [True, True, False],
                          'ForestProduct0': ['bamboo', 'fish', 'honey'],
                          'ForestProduct1': ['no other product', 'honey', 'fish'],
                          'ForestProduct2': ['no other product', 'no other product', 'x']})
    out = module.shocks(frame)
    assert list(out.columns) == ['ForestProduct0', 'ForestProduct1', 'ForestProduct2']
    assert out['ForestProduct0'].tolist() == ['bamboo', 'fish']
    assert pd.isna(out['ForestProduct1'].iloc[0]) and out['ForestProduct1'].iloc[1] == 'honey'
    assert out['ForestProduct2'].isna().all()


# --------------------------------------------------------------- source tier

def _raw_grid(country, wave):
    from lsms_library.local_tools import get_dataframe
    rel = {'Togo': 'Data1/s14_me_tgo2018.dta', 'CotedIvoire': 'Data/Menage/s14_me_CIV2018.dta'}
    iso = {'Benin': 'ben', 'Burkina_Faso': 'bfa', 'Guinea-Bissau': 'gnb', 'Mali': 'mli',
           'Niger': 'ner', 'Senegal': 'sen'}
    path = ROOT / country / wave / (rel.get(country) or f'Data/s14_me_{iso[country]}2018.dta')
    return get_dataframe(str(path), convert_categoricals=False)


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', EHCVM_2018)
def test_the_served_2018_sets_are_the_grid_positives(country):
    """Oracle: an independent read of the raw grid (no hook imported for the
    counts).  The BUILT set (the parquet, before the read-time rules) has, per
    size, exactly as many rows as the raw file has rows with that many
    positive cells; and for every code k, exactly as many built sets contain
    code k's label as the raw column k has positive cells.  The served
    positions are all NA."""
    import lsms_library as ll
    from lsms_library.paths import data_root
    wave = EHCVM_2018[country]
    raw = _raw_grid(country, wave)
    cols = sorted((c for c in raw if re.fullmatch(r's14q05__\d+', c)), key=lambda c: int(c.split('__')[1]))
    grid = raw[cols].apply(pd.to_numeric, errors='coerce')
    positive = grid > 0
    n_pos = positive.sum(axis=1)
    expected_sizes = n_pos[n_pos > 0].value_counts().sort_index()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        served = ll.Country(country).shocks(waves=[wave])
    positions = [c for c in served if str(c).startswith('HowCoped') and c != COPING_SET_COLUMN]
    assert positions and served[positions].isna().all().all()
    built = pd.read_parquet(data_root() / country / 'var' / 'shocks.parquet')
    built = built[built.index.get_level_values('t').astype(str) == wave][COPING_SET_COLUMN].dropna()
    members = built.str.split('+')
    sizes = members.str.len().value_counts().sort_index()
    pd.testing.assert_series_equal(sizes, expected_sizes, check_names=False, check_dtype=False)
    labels = _module(country, wave).COPING_LABELS
    for col in cols:
        code = int(col.split('__')[1])
        n_raw = int(positive[col].sum())
        n_built = int(members.apply(lambda m, lab=labels[code]: lab in m).sum())
        assert n_built == n_raw, (country, code, labels[code], n_built, n_raw)

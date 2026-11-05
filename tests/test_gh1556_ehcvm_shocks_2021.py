"""EHCVM 2021-22 ``shocks``: the food-stock column and the coping ranks (GH #1556).

``s14bq04a..f`` are revenu, avoirs, production agricole, effectif du cheptel,
stock de produits alimentaires, achats.  ``AffectedConsumption`` read ``d``, the
livestock, in every wired wave.  Section 14.05 ranks each strategy (Premiere /
Deuxieme / Troisieme), except code 25, ``Autre strategie``, which is labelled
Oui/Non: the rank map never matched it, so the strategy was lost.

Two tiers: static (the YAML and the hooks on synthetic frames, no microdata), and
source-gated, with an oracle that reads the ``.dta`` through ``get_dataframe`` and
imports nothing from ``countries/``.
"""
import collections
import importlib.util
import re

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.data_access import get_data_file
from lsms_library.ehcvm import EHCVM_COUNTRIES
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root()
WAVE = '2021-22'
FIXED = {'Niger': 's14b_me_ner2021.dta', 'Mali': 's14b_me_mli2021.dta',
         'Senegal': 's14b_me_sen2021.dta', 'Burkina_Faso': 's14b_me_bfa2021.dta'}
#: The labelled files; Burkina's section 14.05 carries no value labels at all.
LABELLED = ['Niger', 'Mali', 'Senegal']
AFFECTED = {'AffectedIncome': 'a', 'AffectedAssets': 'b', 'AffectedProduction': 'c',
            'AffectedConsumption': 'e'}
KEPT_LABELS = {1: 'Utilisation de son épargne', 25: 'Autre stratégie', 26: 'Aucune stratégie'}


def _wired_waves():
    """Every EHCVM 2021-22 wave declaring ``shocks``, so a new wiring is checked on arrival."""
    wired = []
    for country in EHCVM_COUNTRIES:
        path = ROOT / country / WAVE / '_' / 'data_info.yml'
        info = yaml.safe_load(path.read_text(encoding='utf-8')) if path.exists() else {}
        if isinstance((info or {}).get('shocks'), dict):
            wired.append((country, info['shocks']))
    return wired


@pytest.mark.parametrize('country,block', _wired_waves(), ids=lambda x: x if isinstance(x, str) else '')
def test_affected_consumption_reads_the_stock_not_the_livestock(country, block):
    source = block['myvars']['AffectedConsumption']
    assert source[0] == 's14bq04e', f'{country}: {source[0]} is not the food stock'


def test_the_four_fixed_waves_are_all_wired():
    assert set(FIXED) <= {c for c, _ in _wired_waves()}


def _hook(country):
    path = ROOT / country / WAVE / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location(f'{country}_{WAVE}_gh1556', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FIRST, SECOND, THIRD = 'Première stratégie', 'Deuxième strategie', 'Troisième stratégie'


def _served(country, **cope):
    """One row through the wave's hook: the three served slots."""
    module = _hook(country)
    row = {f'Cope{n}': 'Non' for n in (1, 2, 3, 25, 26)}
    row.update({k.replace('c', 'Cope'): v for k, v in cope.items()})
    out = module.shocks(pd.DataFrame([row]))
    assert not [c for c in out.columns if c.startswith('Cope')]
    labels = module.COPING_LABELS
    return [out[f'HowCoped{k}'].iloc[0] for k in range(3)], labels


@pytest.mark.parametrize('country', LABELLED)
class TestLabelledRanks:
    def test_ranks_order_the_slots_not_the_codes(self, country):
        got, lab = _served(country, c1=SECOND, c2=FIRST)
        assert got[:2] == [lab[2], lab[1]] and pd.isna(got[2])

    def test_autre_alone_is_served(self, country):
        got, lab = _served(country, c25='Oui')
        assert got[0] == lab[25] and pd.isna(got[1])

    def test_autre_takes_the_rank_after_the_ranked_ones(self, country):
        got, lab = _served(country, c1=FIRST, c25='Oui')
        assert got[:2] == [lab[1], lab[25]] and pd.isna(got[2])
        got, lab = _served(country, c1=FIRST, c2=SECOND, c25='Oui')
        assert got == [lab[1], lab[2], lab[25]]

    def test_autre_fills_the_rank_no_other_strategy_holds(self, country):
        got, lab = _served(country, c1=SECOND, c25='Oui')
        assert got[0] == lab[25] and got[1] == lab[1]

    def test_non_and_blank_are_not_strategies(self, country):
        got, _ = _served(country, c1=np.nan, c25='Non')
        assert all(pd.isna(x) for x in got)


class TestBurkinaRawRanks:
    """No value labels here: the raw 1/2/3 are the ranks, and code 25 keeps its own."""

    def test_ranks_order_the_slots_not_the_codes(self):
        got, lab = _served('Burkina_Faso', c1=2.0, c2=1.0)
        assert got[:2] == [lab[2], lab[1]] and pd.isna(got[2])

    def test_code_25_keeps_its_rank(self):
        got, lab = _served('Burkina_Faso', c1=2.0, c2=3.0, c25=1.0)
        assert got == [lab[25], lab[1], lab[2]]

    def test_zero_and_blank_are_not_strategies(self):
        got, _ = _served('Burkina_Faso', c1=0.0, c2=np.nan, c3=0.0, c25=0.0, c26=0.0)
        assert all(pd.isna(x) for x in got)


# ---------------------------------------------------------------------------- source-gated

def _path(country):
    return ROOT / country / WAVE / 'Data' / FIXED[country]


@pytest.fixture(scope='module')
def source():
    cache = {}

    def read(country):
        if country not in cache:
            path = _path(country)
            raw = get_dataframe(str(path), convert_categoricals=False)
            shock = get_dataframe(str(path))['s14bq01']
            raw = raw.assign(Shock=shock.astype(object).values)
            letters = [f's14bq04{x}' for x in 'abcdef']
            copes = [c for c in raw.columns if re.fullmatch(r's14bq05__\d+', c)]
            cache[country] = (raw[raw[letters + copes].notna().any(axis=1)].copy(), copes)
        return cache[country]
    return read


@pytest.fixture(scope='module')
def served():
    cache = {}

    def read(country):
        if country not in cache:
            cache[country] = ll.Country(country).shocks(waves=[WAVE])
        return cache[country]
    return read


def _truth(series):
    """The wave's own mapping: Augmente and Diminue are affected, the other two are not."""
    return pd.to_numeric(series).map({1: True, 2: True, 3: False, 4: False})


def _multiset(frame):
    frame = frame.astype(object).where(frame.notna(), None)
    return collections.Counter(frame.itertuples(index=False, name=None))


def _expected(raw, stock_letter='e'):
    letters = dict(AFFECTED, AffectedConsumption=stock_letter)
    out = pd.DataFrame({name: _truth(raw[f's14bq04{x}']) for name, x in letters.items()})
    out.insert(0, 'Shock', raw['Shock'].values)
    return out


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', list(FIXED))
class TestAgainstTheSource:
    def test_the_labels_are_what_the_wiring_assumes(self, country):
        with pd.io.stata.StataReader(str(get_data_file(_path(country).relative_to(ROOT)))) as reader:
            labels = reader.variable_labels()
        assert 'cheptel' in labels['s14bq04d'].lower()
        assert 'stock' in labels['s14bq04e'].lower()
        assert 'achat' in labels['s14bq04f'].lower()

    def test_every_affected_column_is_the_source_column_it_names(self, country, source, served):
        raw, _ = source(country)
        cols = ['Shock', *AFFECTED]
        assert _multiset(served(country).reset_index()[cols]) == _multiset(_expected(raw)[cols])

    def test_the_livestock_wiring_would_not_pass(self, country, source, served):
        raw, _ = source(country)
        cols = ['Shock', *AFFECTED]
        assert _multiset(served(country).reset_index()[cols]) != _multiset(_expected(raw, 'd')[cols])

    def test_slots_follow_the_ranks_and_keep_code_25(self, country, source, served):
        raw, copes = source(country)
        ranks = raw[copes].apply(pd.to_numeric).fillna(0).to_numpy(dtype=int)
        codes = [int(c.split('__')[1]) for c in copes]
        at25 = codes.index(25)
        expected = collections.Counter()
        count = collections.Counter()
        for row in ranks:
            held = {r for k, r in enumerate(row) if r > 0 and (k != at25 or country == 'Burkina_Faso')}
            chosen = [(r, codes[k]) for k, r in enumerate(row) if r > 0 and (k != at25 or country == 'Burkina_Faso')]
            if row[at25] > 0 and country != 'Burkina_Faso':
                chosen.append((min(set(range(1, len(held) + 2)) - held), 25))
            for slot, (_, code) in enumerate(sorted(chosen)):
                count[slot] += 1
                if code in KEPT_LABELS:
                    expected[slot, KEPT_LABELS[code]] += 1
        frame = served(country)
        got = collections.Counter()
        for slot in range(3):
            column = frame[f'HowCoped{slot}']
            assert int(column.notna().sum()) == count[slot]
            for label in KEPT_LABELS.values():
                n = int((column == label).sum())
                if n:
                    got[slot, label] = n
        assert got == expected


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', LABELLED)
def test_only_code_25_has_the_oui_non_label_set(country):
    labels = get_dataframe(str(_path(country)), categories_only=True)
    sets = {int(k.split('__')[1]): {str(v) for v in dict(labels[k]).values() if pd.notna(v)}
            for k in labels if re.fullmatch(r's14bq05__\d+', k)}
    assert len(sets) == 26
    assert sets.pop(25) == {'Non', 'Oui'}
    assert all(values == {'Non', FIRST, SECOND, THIRD} for values in sets.values())

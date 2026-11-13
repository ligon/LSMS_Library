"""Coping strategies in ``shocks``: the read-time rules of GH #1562.

Rulings (``isa-decisions.org``): 2026-11-12 "Coping-strategy contract" and
2026-11-13 "Coping-strategy implementation".  The rules under test:

* a slot filler or missing code is served NA in its ``HowCoped<n>`` position
  and is never a member of ``HowCopedSet``;
* a "did nothing" answer is a set member only when it is the only answer;
  beside a real strategy it leaves the set and keeps its position;
* ``HowCopedSet`` is the distinct labels, sorted, joined with ``+``, taken from
  the build's own set where it wrote one and from the positions otherwise;
* Uganda's 96 "Other (specify)" stays a strategy, one member however often.

Static tier: synthetic frames and the committed ``coping.yml`` files, no
microdata.  Source tier (``requires_s3``): every registry label occurs in the
served table it is declared for, so a typo cannot leave a filler a strategy.
"""
from __future__ import annotations

import re
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library import _build_registry as R
from lsms_library import coping
from lsms_library.coping import (
    SET_COLUMN, CopingLabel, CopingLabelError, coping_set, normalise_coping,
    position_columns,
)
from lsms_library.paths import countries_root

ROOT = Path(countries_root())


def _labels(*entries):
    """Patchable stand-in for ``label_classes``: (label, klass, waves[, served_as])."""
    recs = tuple(CopingLabel(e[0], e[1], None if e[2] is None else frozenset(e[2]), "test",
                             e[3] if len(e) > 3 else None)
                 for e in entries)
    return lambda country: recs


def _frame(rows, waves=None, set_column=None):
    """rows: list of position tuples; one shock per row, household 'h<k>'."""
    n = max(len(r) for r in rows)
    waves = waves or ['2019-20'] * len(rows)
    idx = pd.MultiIndex.from_tuples(
        [(w, f'h{k}', 'Drought') for k, w in enumerate(waves)], names=['t', 'i', 'Shock'])
    data = {f'HowCoped{j}': pd.array([r[j] if j < len(r) else pd.NA for r in rows],
                                     dtype='string') for j in range(n)}
    data['AffectedIncome'] = pd.array([True] * len(rows), dtype='boolean')
    if set_column is not None:
        data[SET_COLUMN] = pd.array(set_column, dtype='string')
    return pd.DataFrame(data, index=idx)


# --------------------------------------------------------------------- coping_set

def test_positions_are_every_howcoped_n_in_order():
    assert position_columns(['HowCoped10', 'x', 'HowCoped3', 'HowCoped0', 'HowCopedSet']) == [
        'HowCoped0', 'HowCoped3', 'HowCoped10']


def test_set_is_distinct_sorted_plus_joined():
    assert coping_set(['Sold livestock', 'Relied on savings', 'Sold livestock']) == (
        'Relied on savings+Sold livestock')


def test_set_of_nothing_is_na():
    assert pd.isna(coping_set([pd.NA, None, float('nan')]))


def test_fillers_never_enter_the_set():
    got = coping_set(['Relied on savings', 'Nothing else', 'Nothing else'],
                     filler=frozenset({'Nothing else'}))
    assert got == 'Relied on savings'
    assert pd.isna(coping_set(['Nothing else'] * 3, filler=frozenset({'Nothing else'})))


def test_did_nothing_is_a_member_only_alone():
    dn = frozenset({'Did nothing'})
    assert coping_set(['Did nothing'], did_nothing=dn) == 'Did nothing'
    assert coping_set(['Did nothing', 'Did nothing'], did_nothing=dn) == 'Did nothing'
    assert coping_set(['Relied on savings', 'Did nothing'], did_nothing=dn) == 'Relied on savings'


def test_other_specify_is_a_strategy_and_deduplicated():
    """Uganda 96 stays a strategy (ruling), one member however often it repeats."""
    assert coping_set(['Relied on savings', 'Other (specify)', 'Other (specify)']) == (
        'Other (specify)+Relied on savings')


def test_a_label_with_the_separator_is_refused():
    with pytest.raises(CopingLabelError, match="separator"):
        coping_set(['Savings+credit'])
    assert not issubclass(CopingLabelError, (ValueError, KeyError, RuntimeError))


# --------------------------------------------------------------------- normalise_coping

def test_filler_is_na_in_its_position_and_absent_from_the_set(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(('Nothing else', 'filler', None)))
    df = _frame([('Relied on savings', 'Nothing else', 'Nothing else'),
                 ('Nothing else', 'Nothing else', 'Nothing else')])
    out = normalise_coping(df, 'X')
    assert out['HowCoped0'].tolist()[0] == 'Relied on savings'
    assert out['HowCoped1'].isna().all() and out['HowCoped2'].isna().all()
    assert pd.isna(out['HowCoped0'].iloc[1])
    assert out[SET_COLUMN].iloc[0] == 'Relied on savings'
    assert pd.isna(out[SET_COLUMN].iloc[1])
    # rows, index and the other columns are untouched; no row is dropped
    assert out.index.equals(df.index)
    pd.testing.assert_series_equal(out['AffectedIncome'], df['AffectedIncome'])


def test_did_nothing_keeps_its_position_and_leaves_the_set_beside_a_strategy(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(('Did nothing', 'did_nothing', None)))
    df = _frame([('Did nothing',), ('Did nothing', 'Obtained credit')])
    out = normalise_coping(df, 'X')
    assert out['HowCoped0'].tolist() == ['Did nothing', 'Did nothing']
    assert out['HowCoped1'].iloc[1] == 'Obtained credit'
    assert out[SET_COLUMN].tolist() == ['Did nothing', 'Obtained credit']


def test_a_wave_scoped_filler_applies_only_in_its_wave(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(('99.0', 'filler', ['2009-10'])))
    df = _frame([('99.0',), ('99.0',)], waves=['2009-10', '2010-11'])
    out = normalise_coping(df, 'X')
    assert pd.isna(out['HowCoped0'].iloc[0]) and out['HowCoped0'].iloc[1] == '99.0'
    assert pd.isna(out[SET_COLUMN].iloc[0]) and out[SET_COLUMN].iloc[1] == '99.0'


def test_a_build_written_set_is_kept_and_obeys_the_same_rules(monkeypatch):
    """EHCVM 2018-19: no positions, the set from the grid -- Mali's code 26
    beside another option leaves the set at read time."""
    monkeypatch.setattr(coping, 'label_classes',
                        _labels(('Aucune stratégie', 'did_nothing', None)))
    idx = pd.MultiIndex.from_tuples([('2018-19', 'h1', 'Sécheresse'),
                                     ('2018-19', 'h2', 'Sécheresse'),
                                     ('2018-19', 'h3', 'Sécheresse')],
                                    names=['t', 'i', 'Shock'])
    df = pd.DataFrame({SET_COLUMN: pd.array(
        ['Aucune stratégie+Vente de bétail', 'Aucune stratégie', pd.NA], dtype='string')},
        index=idx)
    out = normalise_coping(df, 'Mali')
    assert out[SET_COLUMN].tolist()[:2] == ['Vente de bétail', 'Aucune stratégie']
    assert pd.isna(out[SET_COLUMN].iloc[2])
    assert not position_columns(out.columns)


def test_the_set_is_derived_only_where_the_build_wrote_none(monkeypatch):
    """A multi-wave frame: the 2018-19 rows carry the build's set, the 2021-22
    rows only positions."""
    monkeypatch.setattr(coping, 'label_classes', _labels())
    df = _frame([(pd.NA,), ('Achat d\'aliments moins chers', 'Vente de bétail')],
                waves=['2018-19', '2021-22'],
                set_column=['Utilisation de son épargne', pd.NA])
    out = normalise_coping(df, 'X')
    assert out[SET_COLUMN].tolist() == ['Utilisation de son épargne',
                                        "Achat d'aliments moins chers+Vente de bétail"]


def test_the_fourth_position_counts(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels())
    out = normalise_coping(_frame([('a', 'b', 'c', 'd')]), 'Nigeria')
    assert out[SET_COLUMN].iloc[0] == 'a+b+c+d'


def test_normalisation_is_idempotent(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(
        ('Nothing else', 'filler', None), ('Did nothing', 'did_nothing', None)))
    df = _frame([('Did nothing', 'Relied on savings', 'Nothing else'), ('Did nothing',)])
    once = normalise_coping(df, 'X')
    pd.testing.assert_frame_equal(normalise_coping(once, 'X'), once)


def test_attrs_ride_through(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels())
    df = _frame([('a',)])
    df.attrs['id_converted'] = True
    assert normalise_coping(df, 'X').attrs['id_converted'] is True


def test_categorical_positions_are_handled(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(('No other response given', 'filler', None)))
    df = _frame([('Sold animals', 'No other response given')])
    df['HowCoped1'] = df['HowCoped1'].astype('category')
    out = normalise_coping(df, 'Malawi')
    assert pd.isna(out['HowCoped1'].iloc[0])
    assert out[SET_COLUMN].iloc[0] == 'Sold animals'


# ---------------------------------------------------------- slot-aware "did nothing"
# Ruling 2026-11-13, "Coping follow-ups": Uganda 2013-14 / 2015-16 code 40
# "Nothing else" in the FIRST slot with nothing else recorded is {Did nothing};
# in later slots it stays a filler.

UGANDA_LIKE = (('Nothing else', 'filler', ['2013-14']),
               ('Did nothing', 'did_nothing', None),
               ('Nothing else', 'did_nothing_when_alone_first', ['2013-14'], 'Did nothing'))


def test_lone_first_slot_is_served_as_did_nothing_in_position_and_set(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(*UGANDA_LIKE))
    df = _frame([('Nothing else', 'Nothing else', 'Nothing else'),
                 ('Nothing else', pd.NA, pd.NA)], waves=['2013-14', '2013-14'])
    out = normalise_coping(df, 'Uganda')
    assert out['HowCoped0'].tolist() == ['Did nothing', 'Did nothing']
    assert out['HowCoped1'].isna().all() and out['HowCoped2'].isna().all()
    assert out[SET_COLUMN].tolist() == ['Did nothing', 'Did nothing']


def test_first_slot_followed_by_a_strategy_stays_a_filler(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(*UGANDA_LIKE))
    df = _frame([('Nothing else', 'Relied on savings', 'Nothing else')], waves=['2013-14'])
    out = normalise_coping(df, 'Uganda')
    assert pd.isna(out['HowCoped0'].iloc[0]) and pd.isna(out['HowCoped2'].iloc[0])
    assert out['HowCoped1'].iloc[0] == 'Relied on savings'
    assert out[SET_COLUMN].iloc[0] == 'Relied on savings'


def test_later_slot_stays_a_filler_and_other_waves_are_untouched(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(*UGANDA_LIKE))
    df = _frame([('Relied on savings', 'Nothing else'), ('Nothing else',)],
                waves=['2013-14', '2010-11'])
    out = normalise_coping(df, 'Uganda')
    assert pd.isna(out['HowCoped1'].iloc[0]) and out[SET_COLUMN].iloc[0] == 'Relied on savings'
    # outside the entry's waves the label is neither a filler nor a lone answer
    assert out['HowCoped0'].iloc[1] == 'Nothing else' and out[SET_COLUMN].iloc[1] == 'Nothing else'


def test_the_slot_aware_rule_is_idempotent(monkeypatch):
    monkeypatch.setattr(coping, 'label_classes', _labels(*UGANDA_LIKE))
    df = _frame([('Nothing else', 'Nothing else', 'Nothing else'),
                 ('Nothing else', 'Obtained credit', pd.NA)], waves=['2013-14'] * 2)
    once = normalise_coping(df, 'Uganda')
    pd.testing.assert_frame_equal(normalise_coping(once, 'Uganda'), once)


@pytest.mark.parametrize('bad,match', [
    ({'did_nothing_when_alone_first': [{'label': 'x', 'evidence': 'e'}]}, 'needs a served_as'),
    ({'filler': [{'label': 'x', 'evidence': 'e', 'served_as': 'y'}]}, 'only for'),
    ({'did_nothing_when_alone_first': [{'label': 'x', 'evidence': 'e', 'served_as': 'y'}]},
     'not a did_nothing label'),
    ({'did_nothing': [{'label': 'y', 'evidence': 'e', 'waves': ['2010']}],
      'did_nothing_when_alone_first': [{'label': 'x', 'evidence': 'e', 'served_as': 'y',
                                        'waves': ['2011']}]}, 'not a did_nothing label'),
    ({'did_nothing': [{'label': 'x', 'evidence': 'e'}, {'label': 'y', 'evidence': 'e'}],
      'did_nothing_when_alone_first': [{'label': 'x', 'evidence': 'e', 'served_as': 'y'}]},
     'both'),
])
def test_a_malformed_slot_aware_entry_is_refused(bad, match, tmp_path):
    with pytest.raises(CopingLabelError, match=match):
        coping._parse_registry(bad, tmp_path / 'coping.yml')


def test_uganda_declares_the_slot_aware_rule():
    recs = [r for r in coping.label_classes('Uganda') if r.klass == coping.ALONE_FIRST]
    assert [(r.label, sorted(r.waves), r.served_as) for r in recs] == [
        ('Nothing else', ['2013-14', '2015-16'], 'Did nothing')]


def test_a_frame_with_no_coping_columns_is_returned_untouched():
    idx = pd.MultiIndex.from_tuples([('2012', 'h1', 'Drought')], names=['t', 'i', 'Shock'])
    df = pd.DataFrame({'Years': ['2012']}, index=idx)
    assert normalise_coping(df, 'Albania') is df


# --------------------------------------------------------------------- the registry files

REGISTRIES = sorted(ROOT.glob(f'*/_/{coping.REGISTRY_FILE}'))


@pytest.mark.parametrize('path', REGISTRIES, ids=lambda p: p.parent.parent.name)
def test_every_registry_loads(path):
    recs = coping._parse_registry(yaml.safe_load(path.read_text(encoding='utf-8')), path)
    assert recs, f'{path} declares no label'
    assert all(r.evidence for r in recs)


@pytest.mark.parametrize('bad,match', [
    ({'fillers': []}, 'unknown key'),
    ({'filler': [{'label': 'x'}]}, 'no evidence'),
    ({'filler': [{'label': 'a+b', 'evidence': 'e'}]}, 'set separator'),
    ({'filler': [{'label': 'x', 'evidence': 'e', 'wave': ['2010']}]}, 'unknown field'),
    ({'filler': [{'label': 'x', 'evidence': 'e', 'waves': '2010'}]}, 'non-empty list'),
    ({'filler': [{'label': 'x', 'evidence': 'e'}],
      'did_nothing': [{'label': 'x', 'evidence': 'e', 'waves': ['2010']}]}, 'both filler'),
])
def test_a_malformed_registry_is_refused(bad, match, tmp_path):
    with pytest.raises(CopingLabelError, match=match):
        coping._parse_registry(bad, tmp_path / 'coping.yml')


def test_one_label_may_change_class_between_disjoint_waves(tmp_path):
    recs = coping._parse_registry(
        {'filler': [{'label': 'x', 'evidence': 'e', 'waves': ['2010']}],
         'did_nothing': [{'label': 'x', 'evidence': 'e', 'waves': ['2011']}]},
        tmp_path / 'coping.yml')
    assert {r.klass for r in recs} == {'filler', 'did_nothing'}


_COPING_TOKEN = re.compile(r"""['"]HowCoped0['"]|^\s+HowCoped0:|coping_set_from_grid\(""", re.M)


def _coping_countries():
    """Countries whose config builds a coping column (scheme, YAML or script)."""
    out = set()
    for path in ROOT.glob('*/**/_/*'):
        if path.suffix not in {'.py', '.yml'} or path.name == coping.REGISTRY_FILE:
            continue
        if _COPING_TOKEN.search(path.read_text(encoding='utf-8', errors='replace')):
            out.add(path.relative_to(ROOT).parts[0])
    return out


def test_every_coping_country_has_a_registry():
    """A coping country without coping.yml serves fillers as strategies and
    "did nothing" beside real strategies in its set, silently."""
    countries = _coping_countries()
    assert len(countries) >= 15, countries
    missing = sorted(c for c in countries if not (ROOT / c / '_' / coping.REGISTRY_FILE).exists())
    assert not missing, missing


# --------------------------------------------------------------------- cache cost

def test_the_coping_module_is_not_folded_into_any_build_fingerprint():
    """The rules are read-path: editing coping.py or a coping.yml must move no
    hash.  Asserted structurally, as for the null-read audit."""
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    leaked = [p.split('=')[0] for p in parts if 'lsms_library.coping.' in p]
    assert not leaked, leaked
    assert 'lsms_library.coping.normalise_coping' in R._EXCLUDED_CALLABLES
    assert 'lsms_library.country.Country._finalize_result' in R._EXCLUDED_CALLABLES


def test_the_registry_is_not_a_build_input():
    from lsms_library.country import _BUILD_INPUT_SUFFIXES
    assert Path(coping.REGISTRY_FILE).suffix not in _BUILD_INPUT_SUFFIXES


# --------------------------------------------------------------------- source tier

def _registry_countries():
    return sorted(p.parent.parent.name for p in REGISTRIES)


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', _registry_countries())
def test_every_registry_label_occurs_where_it_is_declared(country):
    """Drift check: a mistyped or mis-scoped label would leave a filler a
    strategy forever.  Read from the RAW served positions and set (the rules
    themselves are bypassed), at the waves each entry names."""
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = ll.Country(country).shocks()
    flat = df.reset_index()
    cols = position_columns(flat.columns)
    long = flat.melt(id_vars=['t'], value_vars=cols, value_name='label')[['t', 'label']]
    if SET_COLUMN in flat.columns:
        parts = flat[['t', SET_COLUMN]].dropna()
        parts = parts.assign(label=parts[SET_COLUMN].str.split('+')).explode('label')
        long = pd.concat([long, parts[['t', 'label']]])
    long = long.dropna()
    missing = []
    for rec in coping.label_classes(country):
        where = long if rec.waves is None else long[long['t'].astype(str).isin(rec.waves)]
        if rec.klass in ('filler', coping.ALONE_FIRST):
            # a filler is served NA and a lone first-slot label is served as its
            # served_as, so both are looked for in the build's own frame
            continue
        if not (where['label'].astype(str) == rec.label).any():
            missing.append((rec.klass, rec.label, sorted(rec.waves or [])))
    assert not missing, missing


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', _registry_countries())
def test_every_filler_occurs_in_the_build_output(country):
    """Fillers are served NA, so the drift check for them reads the built
    parquet (pre-normalisation) rather than the served frame."""
    import lsms_library as ll
    from lsms_library.paths import data_root
    fillers = [r for r in coping.label_classes(country)
               if r.klass in ('filler', coping.ALONE_FIRST)]
    if not fillers:
        pytest.skip(f'{country} declares no filler')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        ll.Country(country).shocks()
    built = pd.read_parquet(Path(data_root()) / country / 'var' / 'shocks.parquet').reset_index()
    cols = position_columns(built.columns)
    long = built.melt(id_vars=['t'], value_vars=cols, value_name='label').dropna()
    missing = [(r.label, sorted(r.waves or [])) for r in fillers
               if not (long[long['t'].astype(str).isin(r.waves) if r.waves else long]
                       ['label'].astype(str) == r.label).any()]
    assert not missing, missing

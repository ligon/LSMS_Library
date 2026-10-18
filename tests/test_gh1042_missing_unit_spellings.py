"""GH #1042: two served unit spellings that mean "no unit recorded".

* GhanaLSS ``u='None'`` (GLSS4/5/6 Section 8H, own production).  Code 0 heads
  the GLSS5 and GLSS6 8H unit lists as ``None 00`` and is the zero-fill on every
  non-consumer row; neither of GLSS4's two (conflicting) printed 8H lists has a
  code 0.  GLSS5 prints no count unit at all; GLSS6 adds ``Single 37``.
  ``_/unit_labels.org`` maps it to the missing-unit sentinel, and own-produced
  Chicken in 2005-06 / 2012-13 -- whose code-0 rows price like one bird -- is
  then served on the count unit ``Single`` (``ghanalss.unrecorded_unit_to_count``).
* Malawi ``u='N/A'`` (IHS4/IHS5 Module G).  Every such row is unit code 23
  ``OTHER (SPECIFY)`` with the specify text ``N/A``.  ``_clean_freetext_unit``
  now emits the sentinel instead of the free text.
* Malawi code 23 ``OTHER (SPECIFY)`` with NO usable specify text (blank, a
  bare number, ``OTHER``, ``UNIT``) -- ruling @ligon 2026-10-18.  Served as
  ``Other (Specify)`` / ``Other`` / ``Unit`` before, now the sentinel.  A code-23
  row whose text names a unit (``Packet``, ``Whole Chicken``, ``Grams``) keeps
  it.  IHS2 (2004-05) prints the same answer as code 20 (label ``Other``) and
  its script reads no specify text, so all of its ``Other`` rows move.
* Malawi's ``u`` table used to park 13 codes on ``Other (Specify)``.  Six are
  the IHS3 Module G list -- 90 KG. BAG 3, BASKET (DENGU) (SHELLED) 12 /
  (UNSHELLED) 13, CUP 16, TIN 17, BASIN 21 -- and now decode to those labels
  (2016-17's code 21 is BASIN by its own ``hh_g07b`` value label).  IHPS 2013
  prints none of the six, so 2013-14 serves them as the sentinel; codes on no
  instrument's list (00 49 69 76 79 99 500) are the sentinel everywhere.

The remap has to happen at BUILD time: the kg inference runs on the raw ``u``,
so the delivered tier checks the property (no inferred kilograms on these
rows), not just the label.  Evidence: both countries' ``_/CONTENTS.org``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root, data_root
from lsms_library.transformations import U_UNKNOWN, food_kg_factors

_MALAWI = countries_root() / 'Malawi' / '_' / 'malawi.py'

#: Rows the remap moves, measured on the build at 3e3bd26ee (GH #1042).
_EXPECTED = {
    # 1,450 / 970 were the first pass; the count-unit follow-up serves the
    # Chicken rows among them (2005-06 184, 2012-13 203) on 'Single'.
    'GhanaLSS': ('None', {'1998-99': 88, '2005-06': 1266, '2012-13': 767}),
    # 24 / 19 were the first pass ('N/A' only); the code-23 rule adds
    # 2004-05 'Other' 892, 2010-11 'Other (Specify)' 17 + 'Unit' 8, 2013-14
    # 'Other (Specify)' 110, 2016-17 'Other (Specify)' 404 + 'Other' 1,
    # 2019-20 'Other (Specify)' 12 + 'Other' 1 (private cold rebuild,
    # 2026-10-18).
    # The decode residual adds 2010-11 23 off-list codes (00 14, 99 4, 49 2,
    # 69/79/500 1 each) and 2013-14 8 (03/12/13 6 not printed in IHPS 2013,
    # 00 1, 76 1): 25 -> 48 and 110 -> 118.
    'Malawi': ('N/A', {'2004-05': 892, '2010-11': 48, '2013-14': 118,
                       '2016-17': 429, '2019-20': 32}),
}

#: Malawi rows the IHS3 decode serves under their questionnaire labels.  They
#: were 'Other (Specify)' before (2010-11 5,422 incl. 23 off-list codes,
#: 2013-14 8, 2016-17 590); now no wave serves 'Other (Specify)' at all.
#: 2010-11 'Basin' 1,952 = 1,951 code 21 + 1 free-text row; 2016-17 'Basin'
#: 748 = 590 unlabelled code 21 + 158 already labelled 'basin'.
_MALAWI_IHS3_DECODE = {
    ('2010-11', 'Cup'): 3072, ('2010-11', 'Tin'): 281,
    ('2010-11', 'Basin'): 1952, ('2010-11', '90 kg Bag'): 5,
    ('2010-11', 'Basket (Dengu) (Shelled)'): 46,
    ('2010-11', 'Basket (Dengu) (Unshelled)'): 44,
    ('2016-17', 'Basin'): 748,
}

#: The IHS3 codes the shared table decodes, and the codes on no list.
_IHS3_CODES = {'03': '90 kg Bag', '12': 'Basket (Dengu) (Shelled)',
               '13': 'Basket (Dengu) (Unshelled)', '16': 'Cup', '17': 'Tin',
               '21': 'Basin'}
_OFF_LIST_CODES = ('00', '49', '69', '76', '79', '99', '500')

#: Rungs a missing-unit row may be served by: its own shipped factor, a metric
#: label, or nothing.  Never an inferred (pooled) rung.
_NON_INFERRED = {'survey_kg', 'metric', 'none'}


def _malawi():
    spec = importlib.util.spec_from_file_location('malawi_1042', _MALAWI)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover - environment-dependent imports
        pytest.skip(f'malawi.py not importable here: {exc}')
    return mod


# --------------------------------------------------------------- config tier
def test_ghanalss_unit_labels_map_none_to_the_sentinel():
    ul = df_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'unit_labels.org',
                         name='unit_label').dropna()
    umap = dict(zip(ul['u'].astype(str).str.strip(),
                    ul['Preferred Label'].astype(str).str.strip()))
    assert umap['None'] == U_UNKNOWN
    assert umap['none'] == U_UNKNOWN
    assert 'None' not in set(umap.values())


@pytest.mark.parametrize('text', ['N/A', 'n/a', ' N/A '])
def test_malawi_na_specify_text_is_the_sentinel(text):
    assert _malawi()._clean_freetext_unit(text) == U_UNKNOWN


def test_malawi_na_does_not_fall_back_to_other_specify():
    """Blanking 'N/A' would fall back to the code-23 label, which gets kg.

    Before the code-23 ruling the blank third row fell back to the label
    'Other (Specify)'; it is now the sentinel too.  A blank on a NON-other
    code still falls back to that code's label.
    """
    df = pd.DataFrame({'unitsdetail_bought': ['N/A', 'Basket', None, None],
                       'units_bought': ['OTHER (SPECIFY)'] * 3 + ['PIECE']})
    out = _malawi().handling_unusual_units(df, suffixes=['bought'])
    assert list(out['u_bought']) == [U_UNKNOWN, 'Basket', U_UNKNOWN, 'Piece']


# ---------------------------------------------- code 23, both sides (config)
@pytest.mark.parametrize('code,expected', [
    ('23', True), ('23A', True), ('23b', True), ('23.0', True), (' 23 ', True),
    ('2', False), ('22', False), ('230', False), ('21', False), (None, False),
])
def test_malawi_other_specify_code(code, expected):
    assert bool(_malawi().is_other_specify_code([code]).iloc[0]) is expected


@pytest.mark.parametrize('text', ['OTHER', 'Other', 'OTHER (SPECIFY)',
                                  'others', 'UNIT', 'Units', '', '1/4', '0'])
def test_malawi_non_informative_specify_text_is_blank(text):
    assert pd.isna(_malawi()._clean_freetext_unit(text))


def test_malawi_code23_boundary():
    """Code 23 + no usable text -> sentinel; code 23 + a unit -> that unit;
    any other code + no text -> the code's own label."""
    df = pd.DataFrame({
        'unitsdetail_bought': ['', 'OTHER', 'UNIT', '1/4', '1 PACKET',
                               'WHOLE CHICKEN', 'GRAMS', None, ''],
        'unitcode_bought': ['23', '23', '23', '23.0', '23', '23', '23A',
                            '9', '10'],
        'units_bought': ['OTHER (SPECIFY)'] * 7 + ['PIECE', 'HEAP'],
    })
    out = _malawi().handling_unusual_units(df, suffixes=['bought'])
    assert list(out['u_bought']) == [
        U_UNKNOWN, U_UNKNOWN, U_UNKNOWN, U_UNKNOWN,
        'Packet', 'Whole Chicken', 'Grams', 'Piece', 'Heap']


def test_malawi_other_specify_to_unknown_without_specify_text():
    """2013-14 ships no specify text: every code-23 row is the sentinel."""
    m = _malawi()
    u = pd.Series(['Other (Specify)', 'Kilogramme', 'Other (Specify)'])
    out = m.other_specify_to_unknown(
        u, m.is_other_specify_code(['23', '01', '23B']).values)
    assert list(out) == [U_UNKNOWN, 'Kilogramme', U_UNKNOWN]


def test_malawi_wave_scripts_route_through_the_helper():
    """Each fallback site (three scripts + handling_unusual_units) uses it."""
    root = countries_root() / 'Malawi'
    for wave in ('2004-05', '2010-11', '2013-14'):
        src = (root / wave / '_' / 'food_acquired.py').read_text()
        assert 'other_specify_to_unknown(' in src, wave
    for wave in ('2016-17', '2019-20'):
        src = (root / wave / '_' / 'food_acquired.py').read_text()
        assert 'handling_unusual_units(' in src, wave


def _malawi_u_table():
    path = countries_root() / 'Malawi' / '_' / 'categorical_mapping.org'
    tab = df_from_orgfile(str(path), name='u')
    return dict(zip(tab['Code'].astype(str), tab['Preferred Label'].astype(str)))


def test_malawi_u_table_decodes_ihs3_codes_per_the_questionnaire():
    table = _malawi_u_table()
    for code, label in _IHS3_CODES.items():
        assert table[code] == label, (code, table[code])
    for code in _OFF_LIST_CODES:
        assert table[code] == U_UNKNOWN, (code, table[code])
    # 'Other (Specify)' is reached only through code 23 (and its suffixes).
    other = sorted(c for c, lab in table.items() if lab == 'Other (Specify)')
    assert other == ['23', '23a', '23b', '23c', '23d', '23e', '23f'], other


def test_malawi_2013_14_does_not_decode_codes_ihps_does_not_print():
    src = (countries_root() / 'Malawi' / '2013-14' / '_' / 'food_acquired.py').read_text()
    assert "_IHPS13_UNPRINTED_CODES = ['03', '12', '13', '16', '17', '21']" in src
    assert '.isin(_IHPS13_UNPRINTED_CODES)' in src


# ------------------------------------------------------------ delivered tier
@pytest.mark.parametrize('country', sorted(_EXPECTED))
def test_delivered_rows_carry_the_sentinel_and_no_inferred_kg(country):
    if not (data_root() / country / 'var' / 'food_acquired.parquet').exists():
        pytest.skip(f'{country} food_acquired not cached')
    import lsms_library as ll

    spelling, by_wave = _EXPECTED[country]
    fa = ll.Country(country).food_acquired()
    u = fa.index.get_level_values('u').astype(str)
    assert not (u == spelling).any(), f'{country} still serves u={spelling!r}'

    t = fa.index.get_level_values('t')
    for wave, n in by_wave.items():
        assert int(((u == U_UNKNOWN) & (t == wave)).sum()) == n, (country, wave)

    sources = food_kg_factors(fa).loc[(u == U_UNKNOWN), 'KgFactorSource']
    assert set(sources.astype(str)) <= _NON_INFERRED, sources.value_counts()

    if country == 'Malawi':
        # Code 23 with no specification is gone under every spelling it had.
        for gone in ('Other', 'Unit'):
            assert not (u == gone).any(), f'Malawi still serves u={gone!r}'
        # The shipped conversion-table factor survives the relabel (5 code-23
        # rows + the first pass's 14 'N/A' rows, all 2019-20).
        assert int((sources.astype(str) == 'survey_kg').sum()) == 19
        # No wave serves 'Other (Specify)' any more: code 23 without text is
        # the sentinel, and the IHS3 codes carry their questionnaire labels.
        assert not (u == 'Other (Specify)').any(), 'Malawi still serves Other (Specify)'
        for (wave, label), n in _MALAWI_IHS3_DECODE.items():
            got = int(((u == label) & (t == wave)).sum())
            assert got == n, (wave, label, got)


# ------------------------------------------- GhanaLSS count units (follow-up)
def _ghanalss():
    path = countries_root() / 'GhanaLSS' / '_' / 'ghanalss.py'
    spec = importlib.util.spec_from_file_location('ghanalss_1042', path)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover - environment-dependent imports
        pytest.skip(f'ghanalss.py not importable here: {exc}')
    return mod


def test_ghanalss_count_unit_scope_is_chicken_in_glss5_and_glss6_only():
    """Eggs are deliberately NOT remapped: (Egg, Single) infers 0.50 kg/egg and
    the ladder has no item-level refusal.  Widening this dict is a decision
    that needs the kg check in CONTENTS.org repeated, not a one-line edit."""
    g = _ghanalss()
    assert g.COUNT_UNIT == 'Single'
    assert g.UNRECORDED_UNIT_IS_COUNT == {'2005-06': ('Chicken',),
                                         '2012-13': ('Chicken',)}


def test_ghanalss_count_unit_relabel_is_scoped_and_lossless():
    g = _ghanalss()
    idx = pd.MultiIndex.from_tuples(
        [('2005-06', 'h1', 'Chicken', U_UNKNOWN, 'produced', 0),
         ('2005-06', 'h1', 'Egg', U_UNKNOWN, 'produced', 0),
         ('2005-06', 'h2', 'Chicken', U_UNKNOWN, 'purchased', 0),
         ('2005-06', 'h3', 'Chicken', 'Kilogram', 'produced', 0)],
        names=['t', 'i', 'j', 'u', 's', 'visit'])
    df = pd.DataFrame({'Quantity': [1.0, 2.0, 3.0, 4.0]}, index=idx)
    out = g.unrecorded_unit_to_count(df, '2005-06')
    assert out.index.get_level_values('u').tolist() == [
        'Single', U_UNKNOWN, U_UNKNOWN, 'Kilogram']
    assert out['Quantity'].tolist() == df['Quantity'].tolist()
    # Not a wave in scope (GLSS4: no count unit served for chicken).
    df98 = df.rename(index={'2005-06': '1998-99'}, level='t')
    assert g.unrecorded_unit_to_count(df98, '1998-99').index.equals(df98.index)
    # A collision with a genuine Single row raises rather than reaching core.
    clash = pd.concat([df, pd.DataFrame(
        {'Quantity': [9.0]}, index=pd.MultiIndex.from_tuples(
            [('2005-06', 'h1', 'Chicken', 'Single', 'produced', 0)],
            names=idx.names))])
    with pytest.raises(AssertionError, match='collided'):
        g.unrecorded_unit_to_count(clash, '2005-06')


def test_ghanalss_chicken_count_rows_get_a_plausible_bird_weight():
    """The task's own acceptance check: the kg the ladder serves the counted
    chicken rows is a plausible bird (1-2.5 kg), measured 1.07 kg."""
    if not (data_root() / 'GhanaLSS' / 'var' / 'food_acquired.parquet').exists():
        pytest.skip('GhanaLSS food_acquired not cached')
    import lsms_library as ll

    fa = ll.Country('GhanaLSS').food_acquired()
    lv = fa.index.get_level_values
    t, j, u, s = lv('t'), lv('j').astype(str), lv('u').astype(str), lv('s')
    prod = (j == 'Chicken') & (s == 'produced')
    for wave, n_single in {'2005-06': 257, '2012-13': 2926}.items():
        assert int((prod & (t == wave) & (u == 'Single')).sum()) == n_single
        assert not (prod & (t == wave) & (u == U_UNKNOWN)).any(), wave
    kgf = food_kg_factors(fa).loc[(j == 'Chicken') & (u == 'Single')]
    assert set(kgf['KgFactorSource'].astype(str)) == {'item_unit'}
    assert kgf['kg_per_unit'].between(1.0, 2.5).all(), kgf['kg_per_unit'].describe()


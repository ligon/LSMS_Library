"""Malawi: code 48 lines whose own Module G text says cowpea get a cowpea label (GH #1228).

Seasonal Module G has no cowpea code, so respondents who grew it were coded 48
OTHER (SPECIFY) and served ``crop = 'Other'``.  2010-11 carries the name on the
row (``ag_g0d_os``).  The ruling (2026-11-04) is the smallest step: those lines,
and nothing else.  The Module D join halves of 2016-17 / 2019-20, Module I and
the local names stay deferred.

Static synthetic tier, plus a source-gated tier whose oracle reads Module G
directly.
"""
import ast
import importlib.util

import pandas as pd
import pytest

from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'
LABEL = 'Cowpea (Khobwe)'
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']


@pytest.fixture(scope='module')
def malawi():
    path = MALAWI / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh1228', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _g(codes, texts):
    n = len(codes)
    return pd.DataFrame({'hhid': [f'h{k}' for k in range(n)], 'plotkey': ['R1'] * n,
                         'code': codes, 'text': texts, 'q': [10.0] * n, 'unit': [1] * n,
                         'cond': [1] * n, 'seed': [2.0] * n, 'seed_u': [2] * n})


def _harvest(malawi, g, **kw):
    return malawi._harvest_block(g, hhid='hhid', plotkey='plotkey', cropcode='code',
                                 qty='q', unit='unit', condition='cond', t='2010-11', **kw)


def test_a_line_whose_own_text_says_cowpea_gets_the_label(malawi):
    out = _harvest(malawi, _g([48], ['COWPEAS']), crop_text='text')
    assert out['crop'].tolist() == [LABEL]
    assert out.attrs['crop_text_recovered'] == 1


def test_what_was_recorded_stays(malawi):
    """The code, the variety and the observation key still say 48."""
    plain = _harvest(malawi, _g([48], ['COWPEAS']))
    named = _harvest(malawi, _g([48], ['COWPEAS']), crop_text='text')
    assert plain['crop'].tolist() == ['Other']
    for column in ('_crop_code', 'production_obs', 'crop_variety', 'Quantity', 'u'):
        assert named[column].tolist() == plain[column].tolist(), column
    assert named['production_obs'].tolist() == ['reported:G:48']


@pytest.mark.parametrize('code, text, expected', [
    (48, 'NSEULA', 'Other'),      # no source says what this is
    (48, 'MSEWULA', 'Other'),
    (48, 'NKHOBWE', 'Other'),     # the food label attests khobwe, not this spelling
    (48, '', 'Other'),
    (48, None, 'Other'),
    (36, 'COWPEAS', 'Pigeon Pea (Nandolo)'),   # a coded crop is not a residue
])
def test_nothing_else_moves(malawi, code, text, expected):
    out = _harvest(malawi, _g([code], [text]), crop_text='text')
    assert out['crop'].tolist() == [expected]
    assert out.attrs['crop_text_recovered'] == 0


def test_the_text_is_normalised_before_it_is_matched(malawi):
    out = _harvest(malawi, _g([48], ['  cowpeas ']), crop_text='text')
    assert out['crop'].tolist() == [LABEL]


def test_a_sale_coded_48_stays_attached_to_the_line_it_attached_to(malawi):
    """The attach keys on the code, so naming the crop detaches nothing (the
    historical proposal, a synthetic code, would have)."""
    i_mod = pd.DataFrame({'hhid': ['h0'], 'code': [48], 'sold': [1], 'qs': [5.0],
                          'vs': [100.0], 'us': [1], 'cs': [1]})
    sale = [malawi._sale_block(i_mod, hhid='hhid', cropcode='code', sold_flag='sold',
                               qty_sold='qs', value_sold='vs', unit_sold='us',
                               condition='cs')]
    plain = malawi.assemble_crop_production('2010-11', [_harvest(malawi, _g([48], ['COWPEAS']))], sale)
    named = malawi.assemble_crop_production(
        '2010-11', [_harvest(malawi, _g([48], ['COWPEAS']), crop_text='text')], sale)
    assert named['Quantity_sold'].tolist() == plain['Quantity_sold'].tolist() == [5.0]
    assert named['Value_sold'].tolist() == plain['Value_sold'].tolist() == [100.0]
    assert named.index.get_level_values('crop').tolist() == [LABEL]
    assert named.attrs['crop_text_recovered'] == 1


def test_a_cowpea_line_and_an_nseula_line_are_not_summed_as_one_crop(malawi):
    g = _g([48, 48], ['COWPEAS', 'NSEULA'])
    g['hhid'] = 'h0'
    out = malawi.assemble_crop_production('2010-11', [_harvest(malawi, g, crop_text='text')], [])
    assert sorted(out.index.get_level_values('crop')) == [LABEL, 'Other']


def test_the_seed_row_of_the_same_line_carries_the_same_label(malawi):
    out = malawi._seed_block(_g([48, 48], ['COWPEAS', 'NSEULA']), hhid='hhid',
                             plotkey='plotkey', cropcode='code', qty='seed', unit='seed_u',
                             t='2010-11', crop_text='text')
    assert out['crop'].tolist() == [LABEL, 'Other']


def test_the_label_is_the_food_tables_and_reserves_no_code(malawi):
    from lsms_library.local_tools import all_dfs_from_orgfile
    tables = all_dfs_from_orgfile(MALAWI / '_' / 'categorical_mapping.org')
    food = tables['harmonize_food']
    assert set(malawi._CROP_TEXT_RECODE.values()) <= set(food['Preferred Label'])
    assert LABEL not in set(tables['harmonize_crop']['Preferred Label'])


def _block_calls(wave, script, block):
    tree = ast.parse((MALAWI / wave / '_' / f'{script}.py').read_text())
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)
            and getattr(n.func, 'id', None) == block]


@pytest.mark.parametrize('wave', WAVES)
@pytest.mark.parametrize('script, block', [('crop_production', '_harvest_block'),
                                           ('plot_inputs', '_seed_block')])
def test_only_2010_11_reads_a_crop_text(wave, script, block):
    """The ruling's scope, as a rule: the rest is deferred, not forgotten."""
    calls = _block_calls(wave, script, block)
    assert calls
    passes = [k.arg for call in calls for k in call.keywords if k.arg == 'crop_text']
    assert passes == (['crop_text'] if wave == '2010-11' else [])


# --- source-gated: an oracle reading Module G directly ----------------------

def _g_lines():
    from lsms_library.local_tools import format_id, get_dataframe
    g = get_dataframe(str(MALAWI / '2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta'),
                      convert_categoricals=False)
    g = g[~g.duplicated(keep='first')]
    code = pd.to_numeric(g['ag_g0d'], errors='coerce')
    text = g['ag_g0d_os'].astype('string').str.strip().str.upper()
    lines = g[code.eq(48).fillna(False) & text.eq('COWPEAS').fillna(False)].copy()
    lines['i'] = lines['case_id'].apply(format_id).astype(str)
    return lines


@pytest.mark.requires_s3
def test_the_served_rows_are_the_source_lines():
    from lsms_library.country import Country
    lines = _g_lines()
    assert len(lines) == 157
    cp = Country('Malawi').crop_production()
    named = cp[cp.index.get_level_values('crop') == LABEL]
    assert set(named.index.get_level_values('t')) == {'2010-11'}
    assert len(named) == len(lines)
    assert set(named.index.get_level_values('production_obs')) == {'reported:G:48'}
    assert sorted(set(named.index.get_level_values('i').astype(str))) == sorted(set(lines['i']))
    assert float(pd.to_numeric(named['Quantity'], errors='coerce').sum()) == pytest.approx(
        float(pd.to_numeric(lines['ag_g13a'], errors='coerce').sum()))


@pytest.mark.requires_s3
def test_seed_rows_carry_the_same_label():
    from lsms_library.country import Country
    lines = _g_lines()
    pi = Country('Malawi').plot_inputs().reset_index()
    named = pi[pi['crop'].astype(str).eq(LABEL)]
    assert set(named['t'].astype(str)) == {'2010-11'}
    assert set(named['input'].astype(str)) == {'Seed'}
    assert set(named['i'].astype(str)) <= set(lines['i'])


@pytest.mark.requires_s3
def test_the_label_joins_food_acquired_in_every_wave():
    from lsms_library.country import Country
    fa = Country('Malawi').food_acquired().reset_index()
    waves = set(fa.loc[fa['j'].astype(str).eq(LABEL), 't'].astype(str))
    assert waves == set(fa['t'].astype(str))
    assert len(waves) == 5

"""Malawi livestock: a blank head count stays blank (GH #1435).

``assemble_livestock`` reduced ``HeadCount`` / ``HeadAcquired`` / ``HeadSold``
with a bare ``sum`` (``min_count=0``), which turns an all-blank group into 0.
Module R has no repeated (household, species) line in any wave, so every group
is a singleton and that is all the reducer ever did: 14 / 38 / 46 cells the
survey left blank were served as a recorded zero.  The rule, for all four head
columns: a blank stays blank, a recorded 0 stays 0, two lines of one species
add.

Two tiers: static (synthetic rosters, no microdata) and source-gated (the six
Module R files read directly, with an oracle that never enters the reducer).
"""
import importlib.util

import pandas as pd
import pytest

from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

MALAWI = countries_root() / 'Malawi'
HEAD_COLUMNS = {'HeadCount': 'ag_r02', 'HeadAcquired': 'ag_r10', 'HeadSold': 'ag_r16',
                'HeadCount12MonthsAgo': 'ag_r07'}
COLUMN_ORDER = ['HeadCount', 'HeadAcquired', 'HeadSold', 'ValuePerAnimal',
                'HeadCount12MonthsAgo']

#: What each wave script reads: (wave, source relative to Malawi/, id column, id prefix).
MODULE_R = [
    ('2010-11', '2010-11/Data/Full_Sample/Agriculture/ag_mod_r1.dta', 'case_id', ''),
    ('2013-14', '2013-14/Data/AG_MOD_R1_13.dta', 'y2_hhid', ''),
    ('2016-17', '2016-17/Data/Cross_Sectional/ag_mod_r1.dta', 'case_id', 'cs-17-'),
    ('2016-17', '2016-17/Data/Panel/ag_mod_r1_16.dta', 'y3_hhid', ''),
    ('2019-20', '2019-20/Data/Cross_Sectional/ag_mod_r1.dta', 'case_id', ''),
    ('2019-20', '2019-20/Data/Panel/ag_mod_r1_19.dta', 'y4_hhid', ''),
]


@pytest.fixture(scope='module')
def malawi():
    path = MALAWI / '_' / 'malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_gh1435', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _assemble(malawi, **cols):
    base = {'hhid': ['1', '2'], 'ag_r0a': [301, 301], 'ag_r01': [1, 1],
            'ag_r02': [4.0, 4.0], 'ag_r10': [1.0, 1.0], 'ag_r16': [1.0, 1.0],
            'ag_r04': [5.0, 5.0], 'ag_r07': [3.0, 3.0]}
    base.update(cols)
    block = malawi._livestock_block(pd.DataFrame(base), hhid='hhid', animalcode='ag_r0a',
                                    t='2010-11')
    return malawi.assemble_livestock('2010-11', [block])


def _cell(frame, i, column):
    return frame.xs(i, level='i')[column].iloc[0]


@pytest.mark.parametrize('column,source', HEAD_COLUMNS.items())
def test_a_blank_is_served_blank(malawi, column, source):
    out = _assemble(malawi, **{source: [pd.NA, 4.0]})
    assert pd.isna(_cell(out, '1', column))
    assert _cell(out, '2', column) == 4.0


@pytest.mark.parametrize('column,source', HEAD_COLUMNS.items())
def test_a_recorded_zero_is_served_zero(malawi, column, source):
    out = _assemble(malawi, **{source: [0.0, pd.NA]})
    assert _cell(out, '1', column) == 0.0
    assert pd.isna(_cell(out, '2', column))


@pytest.mark.parametrize('column,source', HEAD_COLUMNS.items())
def test_two_lines_of_one_species_add_and_a_blank_adds_nothing(malawi, column, source):
    # No wave ships such a pair (measured 2026-11-03); the reducer's rule still holds.
    cols = {'hhid': ['1', '1', '2', '2', '3', '3'], 'ag_r0a': [301] * 6, 'ag_r01': [1] * 6,
            'ag_r02': [1.0] * 6, 'ag_r10': [1.0] * 6, 'ag_r16': [1.0] * 6, 'ag_r04': [5.0] * 6,
            'ag_r07': [1.0] * 6}
    cols[source] = [2.0, 3.0, pd.NA, 3.0, pd.NA, pd.NA]
    out = _assemble(malawi, **cols)
    assert _cell(out, '1', column) == 5.0
    assert _cell(out, '2', column) == 3.0
    assert pd.isna(_cell(out, '3', column))


def test_value_per_animal_is_not_additive_and_columns_keep_their_order(malawi):
    out = _assemble(malawi, hhid=['1', '1'], ag_r0a=[301, 301], ag_r04=[pd.NA, 7.0])
    assert _cell(out, '1', 'ValuePerAnimal') == 7.0
    assert list(out.columns) == COLUMN_ORDER
    assert list(out.index.names) == ['t', 'i', 'animal']
    assert (out.dtypes == pd.Float64Dtype()).all()


def test_blanks_do_not_decide_the_keep_filter(malawi):
    # ag_r01 No, nothing positive: not a holding, whatever is blank.
    out = _assemble(malawi, ag_r01=[2, 2], ag_r02=[pd.NA, 0.0], ag_r10=[pd.NA, 0.0],
                    ag_r16=[pd.NA, 2.0])
    assert sorted(out.index.get_level_values('i')) == ['2']


def _source(relative, id_column, prefix):
    try:
        raw = get_dataframe(str(MALAWI / relative), convert_categoricals=False)
    except Exception as exc:                         # no credentials / no blob
        pytest.skip(f'{relative} unavailable: {exc}')
    raw['hhid'] = prefix + raw[id_column].apply(format_id).astype('string')
    return raw


@pytest.mark.parametrize('wave', ['2010-11', '2013-14', '2016-17', '2019-20'])
def test_served_cells_equal_the_source_cells(malawi, wave):
    """Oracle: the wave's Module R rows, keep-filtered here, one row per key, no groupby."""
    species = malawi._malawi_code_map('harmonize_species')
    raws = [_source(rel, id_col, prefix)
            for w, rel, id_col, prefix in MODULE_R if w == wave]

    expected = []
    for raw in raws:
        num = {c: pd.to_numeric(raw[s], errors='coerce') for c, s in HEAD_COLUMNS.items()}
        kept = ((pd.to_numeric(raw['ag_r01'], errors='coerce') == 1)
                | (num['HeadCount'] > 0) | (num['HeadSold'] > 0) | (num['HeadAcquired'] > 0))
        frame = pd.DataFrame({'i': raw['hhid'].astype(str), **num})
        frame['animal'] = pd.to_numeric(raw['ag_r0a'], errors='coerce').map(species)
        expected.append(frame[kept & frame['animal'].notna()])
    expected = pd.concat(expected, ignore_index=True)
    # The premise of the fix: every group the reducer sees is a single source line.
    assert not expected.duplicated(['i', 'animal']).any()
    expected = expected.set_index(['i', 'animal']).sort_index()

    blocks = [malawi._livestock_block(raw, hhid='hhid', animalcode='ag_r0a', t=wave)
              for raw in raws]
    served = malawi.assemble_livestock(wave, blocks).droplevel('t')
    served.index = pd.MultiIndex.from_arrays(
        [served.index.get_level_values(n).astype(str) for n in expected.index.names],
        names=expected.index.names)
    served = served.sort_index()

    assert list(served.index) == list(expected.index)
    for column in HEAD_COLUMNS:
        pd.testing.assert_series_equal(
            pd.to_numeric(served[column]).astype('float64'),
            expected[column].astype('float64'),
            check_exact=True, check_names=False, check_index_type=False)

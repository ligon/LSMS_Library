"""Malawi livestock: a blank head count stays blank (GH #1435).

``assemble_livestock`` reduced ``HeadCount`` / ``HeadAcquired`` / ``HeadSold``
with a bare ``sum`` (``min_count=0``), which turns an all-blank group into 0.
Module R has no repeated (household, species) line in any wave, so every group
is a singleton and that is all the reducer ever did: 14 / 38 / 46 cells the
survey left blank were served as a recorded zero.  The rule, for all four head
columns: a blank stays blank, a recorded 0 stays 0, two lines of one species
add.

Serving blanks as blanks left 5 rows answering only the species gate ``ag_r01``
with no payload, so they stopped being served.  The maintainer's ruling
(2026-11-04) keeps them: ``ag_r01`` is served as ``Owned``, as answered, and the
deletion registered for a day is gone.  So the rows are 39,126 in the wave frames
and at the API alike.

Two tiers: static (synthetic rosters, no microdata) and source-gated (the six
Module R files read directly, with an oracle that never enters the reducer).
"""
import importlib.util
import json

import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.derivations import derivation_records
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root, data_root

MALAWI = countries_root() / 'Malawi'
HEAD_COLUMNS = {'HeadCount': 'ag_r02', 'HeadAcquired': 'ag_r10', 'HeadSold': 'ag_r16',
                'HeadCount12MonthsAgo': 'ag_r07'}
COLUMN_ORDER = ['HeadCount', 'HeadAcquired', 'HeadSold', 'ValuePerAnimal',
                'HeadCount12MonthsAgo', 'Owned']
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']
REMOVED_KEY = 'Malawi::livestock::owned-species-no-payload'

#: What each wave script reads: (wave, source relative to Malawi/, id column, id prefix).
MODULE_R = [
    ('2010-11', '2010-11/Data/Full_Sample/Agriculture/ag_mod_r1.dta', 'case_id', ''),
    ('2013-14', '2013-14/Data/AG_MOD_R1_13.dta', 'y2_hhid', ''),
    ('2016-17', '2016-17/Data/Cross_Sectional/ag_mod_r1.dta', 'case_id', 'cs-17-'),
    ('2016-17', '2016-17/Data/Panel/ag_mod_r1_16.dta', 'y3_hhid', ''),
    ('2019-20', '2019-20/Data/Cross_Sectional/ag_mod_r1.dta', 'case_id', 'cs-19-'),
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
    assert (out.drop(columns='Owned').dtypes == pd.Float64Dtype()).all()
    assert out['Owned'].dtype == pd.BooleanDtype()


def test_blanks_do_not_decide_the_keep_filter(malawi):
    # ag_r01 No, nothing positive: not a holding, whatever is blank.
    out = _assemble(malawi, ag_r01=[2, 2], ag_r02=[pd.NA, 0.0], ag_r10=[pd.NA, 0.0],
                    ag_r16=[pd.NA, 2.0])
    assert sorted(out.index.get_level_values('i')) == ['2']


def test_an_owned_species_with_no_answers_keeps_its_answer_and_mints_no_zero(malawi):
    # ag_r01 Yes keeps the row.  With none of the five questions answered they stay
    # blank, and the gate's own answer is the row's payload.
    out = _assemble(malawi, ag_r02=[pd.NA, 1.0], ag_r10=[pd.NA, 1.0], ag_r16=[pd.NA, 1.0],
                    ag_r04=[pd.NA, 1.0], ag_r07=[pd.NA, 1.0])
    assert sorted(out.index.get_level_values('i')) == ['1', '2']
    blank = out.xs('1', level='i')
    assert blank[list(HEAD_COLUMNS) + ['ValuePerAnimal']].isna().all(axis=None)
    assert bool(blank['Owned'].iloc[0]) is True


def test_owned_is_the_answer_as_given(malawi):
    # 1 Yes, 2 No (kept for a positive count elsewhere), anything else NA.
    out = _assemble(malawi, hhid=['1', '2', '3', '4'], ag_r0a=[301] * 4,
                    ag_r01=[1, 2, pd.NA, 9], ag_r02=[0.0, 3.0, 3.0, 3.0],
                    ag_r10=[0.0] * 4, ag_r16=[0.0] * 4, ag_r04=[5.0] * 4, ag_r07=[1.0] * 4)
    owned = out['Owned'].droplevel(['t', 'animal'])
    assert bool(owned['1']) is True
    assert bool(owned['2']) is False
    assert pd.isna(owned['3']) and pd.isna(owned['4'])


def test_a_wave_without_the_gate_serves_na(malawi):
    block = malawi._livestock_block(
        pd.DataFrame({'hhid': ['1'], 'ag_r0a': [301], 'ag_r02': [2.0]}),
        hhid='hhid', animalcode='ag_r0a', t='2010-11')
    assert pd.isna(malawi.assemble_livestock('2010-11', [block])['Owned'].iloc[0])


def test_the_deletion_is_gone_with_its_machinery(malawi):
    assert REMOVED_KEY not in derivation_records('Malawi')
    assert 'owned-species-no-payload' not in (MALAWI / '_' / 'derivations.yml').read_text()
    for name in ('derive_livestock_no_payload', 'inputs_livestock_no_payload',
                 'LIVESTOCK_NO_PAYLOAD', '_MODULE_R_SOURCES', '_LIVESTOCK_SERVED'):
        assert not hasattr(malawi, name), name


def test_owned_is_declared_and_says_how_the_wording_differs():
    canonical = yaml.safe_load((MALAWI.parents[1] / 'data_info.yml').read_text())
    declared = canonical['Columns']['livestock']['Owned']
    assert declared['type'] == 'bool'
    assert declared.get('optional') is True
    assert 'raised or' in declared['note'] and 'Did you own' in declared['note']
    scheme = yaml.safe_load((MALAWI / '_' / 'data_scheme.yml').read_text())['Data Scheme']
    assert scheme['livestock']['Owned'] == {'type': 'bool', 'optional': True}


def _source(relative, id_column, prefix):
    raw = get_dataframe(str(MALAWI / relative), convert_categoricals=False)
    raw['hhid'] = prefix + raw[id_column].apply(format_id).astype('string')
    return raw


@pytest.mark.requires_s3
def test_every_stored_row_is_served():
    """No row is lost between the parquet and the API any more, and the oracle's
    gate-only lines are among them."""
    country = ll.Country('Malawi')
    served = country.livestock()
    assert REMOVED_KEY not in served.attrs.get('derivations', {}).get('Malawi', {})

    def keys(frame):
        flat = frame.reset_index()[['t', 'i', 'animal']].astype(str)
        return set(map(tuple, flat.to_numpy()))

    stored = pd.read_parquet(data_root() / 'Malawi' / 'var' / 'livestock.parquet')
    assert keys(stored) == keys(served)
    assert served['Owned'].notna().all()


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave', WAVES)
def test_a_line_answering_only_the_gate_is_served(malawi, wave):
    """Oracle: found here from the raw files, with no reducer.  The API serves a
    panel household under its walked id, so the raw id goes through the crosswalk."""
    species = malawi._malawi_code_map('harmonize_species')
    walked = json.loads((MALAWI / '_' / 'updated_ids.json').read_text()).get(wave, {})
    expected = []
    for w, rel, id_col, prefix in MODULE_R:
        if w != wave:
            continue
        raw = _source(rel, id_col, prefix)
        answers = raw[['ag_r02', 'ag_r10', 'ag_r16', 'ag_r04', 'ag_r07']].apply(
            pd.to_numeric, errors='coerce')
        animal = pd.to_numeric(raw['ag_r0a'], errors='coerce').map(species)
        lines = ((pd.to_numeric(raw['ag_r01'], errors='coerce') == 1) & animal.notna()
                 & answers.isna().all(axis=1))
        ids = raw.loc[lines, 'hhid'].astype(str).map(lambda x: walked.get(x, x))
        expected += list(zip(ids, animal[lines].astype(str)))
    assert expected
    served = ll.Country('Malawi').livestock(waves=[wave]).droplevel('t')
    served.index = pd.MultiIndex.from_arrays(
        [served.index.get_level_values(n).astype(str) for n in served.index.names],
        names=served.index.names)
    got = served.loc[expected]
    assert got['Owned'].astype(bool).all()
    assert got[list(HEAD_COLUMNS) + ['ValuePerAnimal']].isna().all(axis=None)


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave', WAVES)
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
        frame['Owned'] = pd.to_numeric(raw['ag_r01'], errors='coerce').map(
            {1: True, 2: False}).astype('boolean')
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
    assert served['Owned'].tolist() == expected['Owned'].tolist()

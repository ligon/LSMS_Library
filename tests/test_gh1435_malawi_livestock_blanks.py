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

import lsms_library as ll
from lsms_library.derivations import derivation_records
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root, data_root

MALAWI = countries_root() / 'Malawi'
HEAD_COLUMNS = {'HeadCount': 'ag_r02', 'HeadAcquired': 'ag_r10', 'HeadSold': 'ag_r16',
                'HeadCount12MonthsAgo': 'ag_r07'}
COLUMN_ORDER = ['HeadCount', 'HeadAcquired', 'HeadSold', 'ValuePerAnimal',
                'HeadCount12MonthsAgo']
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']
DELETION = 'Malawi::livestock::owned-species-no-payload'

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


def test_an_owned_species_with_no_answers_reaches_the_wave_frame_blank(malawi):
    # ag_r01 Yes keeps the row at wave grain.  With none of the served questions answered
    # its payload is all NA, so _finalize_result's dropna(how='all') is what stops serving
    # it; the wave script is not the place that decides that, and it must not mint a 0.
    out = _assemble(malawi, ag_r02=[pd.NA, 1.0], ag_r10=[pd.NA, 1.0], ag_r16=[pd.NA, 1.0],
                    ag_r04=[pd.NA, 1.0], ag_r07=[pd.NA, 1.0])
    assert sorted(out.index.get_level_values('i')) == ['1', '2']
    assert out.xs('1', level='i')[list(HEAD_COLUMNS)].isna().all(axis=None)


def test_the_deletion_predicate_is_every_served_column_blank(malawi):
    out = _assemble(malawi, hhid=['1', '2', '3'], ag_r0a=[301] * 3, ag_r01=[1] * 3,
                    ag_r02=[pd.NA, 0.0, pd.NA], ag_r10=[pd.NA] * 3, ag_r16=[pd.NA] * 3,
                    ag_r04=[pd.NA, pd.NA, 5.0], ag_r07=[pd.NA] * 3)
    picked = malawi.derive_livestock_no_payload(out)
    # a recorded 0 is an answer, and so is a value
    assert list(out[picked].index.get_level_values('i')) == ['1']


def test_the_deletion_is_registered_and_resolves():
    record = derivation_records('Malawi')[DELETION]
    assert record['rows'] == 0
    assert record['table'] == 'livestock'
    assert list(record['waves']) == WAVES
    assert callable(record.resolve('function'))
    assert callable(record.resolve('inputs'))


def _source(relative, id_column, prefix):
    raw = get_dataframe(str(MALAWI / relative), convert_categoricals=False)
    raw['hhid'] = prefix + raw[id_column].apply(format_id).astype('string')
    return raw


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave', WAVES)
def test_the_registered_inputs_are_the_source_lines_with_no_answer(malawi, wave):
    """Oracle: keep-filtered here from the raw files, with no reducer and no predicate."""
    species = malawi._malawi_code_map('harmonize_species')
    expected = []
    for w, rel, id_col, prefix in MODULE_R:
        if w != wave:
            continue
        raw = _source(rel, id_col, prefix)
        answers = raw[['ag_r02', 'ag_r10', 'ag_r16', 'ag_r04', 'ag_r07']].apply(
            pd.to_numeric, errors='coerce')
        animal = pd.to_numeric(raw['ag_r0a'], errors='coerce').map(species)
        kept = (pd.to_numeric(raw['ag_r01'], errors='coerce') == 1) & animal.notna()
        lines = kept & answers.isna().all(axis=1)
        expected += list(zip(raw.loc[lines, 'hhid'].astype(str), animal[lines].astype(str)))
    got = malawi.inputs_livestock_no_payload(wave)
    assert list(got.index.names) == ['t', 'i', 'animal']
    assert sorted((i, str(a)) for _, i, a in got.index) == sorted(expected)
    assert (pd.to_numeric(got['ag_r01']) == 1).all()


@pytest.mark.requires_s3
def test_the_registered_rows_are_not_served_and_the_frame_says_so():
    country = ll.Country('Malawi')
    served = country.livestock()
    assert served.attrs['derivations']['Malawi'][DELETION]['rows'] == 0

    def keys(frame):
        flat = frame.reset_index()[['t', 'i', 'animal']].astype(str)
        return set(map(tuple, flat.to_numpy()))

    # Positive control: the parquet holds them under the served id namespace, so
    # "not served" below cannot pass just because the ids never could have matched.
    stored = keys(pd.read_parquet(data_root() / 'Malawi' / 'var' / 'livestock.parquet'))
    for wave in WAVES:
        gone = keys(country.derivation_inputs(DELETION, wave=wave))
        assert gone <= stored
        assert gone.isdisjoint(keys(served))


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

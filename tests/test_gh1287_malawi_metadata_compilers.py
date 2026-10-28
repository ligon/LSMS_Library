"""Country compilers use real metadata discovery and fail before partial writes.

Only get_dataframe/to_parquet are invented. Country, Wave, their metadata
properties and id_walk run unchanged. No native driver or data build is run.
Ledger: malawi-compiler-declarations-20261028.md §§2–5.
"""
import os
from pathlib import Path
import runpy

import pandas as pd
import pytest

from .malawi_compiler_metadata import write_metadata

from lsms_library import Country, local_tools, paths


ROOT = Path(__file__).resolve().parents[1]
TABLES = os.environ.get('MALAWI_COMPILER_TABLES', 'months_food_inadequate,livestock,plot_labor,food_coping,anthropometry,people_last7days,plot_features,crop_production,community_prices,plot_inputs').split(',')
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']


def _frame(table, wave, empty=False):
    """Non-sorted native identities with zero, fractional and missing payloads."""
    keys = [(wave, 'cs-17-0007'), (wave, '0012-003'), (wave, '000000000001')]
    names = ['t', 'i']
    extras = {
        'plot_inputs': (['plot_id', 'input', 'crop', 'u', 'input_obs'],
                        [('unknown_plot', 'Seed', 'unknown_crop', 'Unknown', f'row:synthetic:Malawi:{wave}:G:release-A:90'),
                         ('RG01_R01', 'Organic Fertilizer', '(not crop-specific)', 'Kilogramme', 'unresolved_observation'),
                         ('02', 'Seed', 'Unmapped crop code 4.5', 'Unknown', f'row:synthetic:Malawi:{wave}:G:release-A:3')]),
        'food_coping': (['Strategy'], [('BorrowFood',), ('LessPreferred',), ('LimitPortion',)]),
        'anthropometry': (['pid'], [('03',), ('01',), ('02',)]),
        'people_last7days': (['pid'], [('03',), ('01',), ('02',)]),
        'plot_features': (['plot_id'], [('RG02_R01',), ('01',), ('RG01_R02',)]),
        'crop_production': (['plot_id', 'crop'], [('unknown_plot', 'Maize'), ('RG01_R01', 'Groundnut'), ('02', 'Cassava')]),
        'livestock': (['animal'], [('Chicken',), ('Bull',), ('Goat',)]),
        'plot_labor': (['plot_id', 'source'], [('unknown_plot', 'Family'), ('RG01_R01', 'Hired'), ('02', 'Other')]),
    }
    if table in extras:
        axes, values = extras[table]
        names += axes
        keys = [key + value for key, value in zip(keys, values)]
    if table == 'community_prices':
        # The native helper deliberately carries i=v before j (malawi.py:4688).
        names = ['t', 'v', 'i', 'j', 'u']
        keys = [(wave, 'EA03', 'EA03', 'Beer', 'Bottle'),
                (wave, 'EA01', 'EA01', 'Maize', '50 kg Bag'),
                (wave, 'EA02', 'EA02', 'Soap', 'Unknown')]
    index = pd.MultiIndex.from_tuples(keys, names=names)
    number = lambda values: pd.array(values, dtype='Float64')
    if table == 'months_food_inadequate':
        columns = {'MonthsInadequate': pd.array([0, 12, pd.NA], dtype='Int64'),
                   'AnyInadequate': pd.array([False, True, pd.NA], dtype='boolean')}
    elif table == 'livestock':
        columns = {'HeadCount': number([0, 3.5, pd.NA]),
                   'HeadAcquired': number([pd.NA, 0, 2]),
                   'HeadSold': number([1, pd.NA, 0]),
                   'ValuePerAnimal': number([0, 2.25, pd.NA])}
    elif table == 'plot_labor':
        columns = {'PersonDays': number([0, 2.5, pd.NA]),
                   'Wage': number([pd.NA, 0, 7.25])}
    elif table == 'plot_inputs':
        columns = {'Quantity': number([0, 2.5, pd.NA]),
                   'Quantity_purchased': number([pd.NA, 0, pd.NA]),
                   'Purchased': pd.array([False, pd.NA, pd.NA], dtype='boolean'),
                   'Improved': pd.array([False, True, pd.NA], dtype='boolean'),
                   'Applied': pd.array([pd.NA, True, pd.NA], dtype='boolean')}
    elif table == 'food_coping':
        columns = {'Days': pd.array([0, 7, pd.NA], dtype='Int64')}
    elif table == 'anthropometry':
        columns = {'Weight': number([0, 2.5, pd.NA]),
                   'Height': number([pd.NA, 85.25, 0]),
                   'Age_months': number([12.5, 0, pd.NA]),
                   'Sex': pd.array(['F', pd.NA, 'M'], dtype='string')}
    elif table == 'people_last7days':
        flag = lambda values: pd.array(values, dtype='boolean')
        columns = {'farm_work': flag([False, True, pd.NA]),
                   'SOB_work': flag([pd.NA, False, True]),
                   'wage_work': flag([True, pd.NA, False]),
                   'farm_hrs': number([0, 2.5, pd.NA]),
                   'SB_hrs': number([pd.NA, 0, 1.25]),
                   'wage_hrs': number([1.5, pd.NA, 0]),
                   'wage_industry': pd.array(['Other', pd.NA, 'Agriculture'], dtype='string'),
                   'working_age': flag([True, pd.NA, False])}
    elif table == 'plot_features':
        columns = {'Area': number([0, 2.5, pd.NA]),
                   'AreaUnit': pd.array(['acres', pd.NA, 'acres'], dtype='string'),
                   'AreaSelfReported': number([pd.NA, 0, 1.25]),
                   'Tenure': pd.array(['Owned', pd.NA, 'Rented'], dtype='string'),
                   'TenureSystem': pd.array([pd.NA, 'Customary', 'Other'], dtype='string'),
                   'PlotOwned': pd.array([False, True, pd.NA], dtype='boolean'),
                   'SoilType': pd.array(['Clay', pd.NA, 'Sand'], dtype='string'),
                   'Irrigated': pd.array([False, pd.NA, True], dtype='boolean'),
                   'Fallow': pd.array([pd.NA, True, False], dtype='boolean'),
                   'ErosionProtection': pd.array([True, False, pd.NA], dtype='boolean')}
    elif table == 'crop_production':
        columns = {'Quantity': number([0, 2.5, pd.NA]),
                   'u': pd.array(['Unknown', '50 kg Bag', pd.NA], dtype='string'),
                   'condition': pd.array(['unknown_condition', 'shelled', 'unshelled'], dtype='string'),
                   'production_obs': pd.array(['reported:G:0', 'reported:G:4.5', 'reported:P:100'], dtype='string'),
                   'Quantity_sold': number([pd.NA, 0, 1.25]),
                   'Value_sold': number([0, 3.5, pd.NA]),
                   'Derivation': pd.array([pd.NA, 'synthetic-key', pd.NA], dtype='string'),
                   'planting_month': pd.array([0, 12, pd.NA], dtype='Int64'),
                   'harvest_month': pd.array([pd.NA, 1, 12], dtype='Int64'),
                   'intercropped': pd.array([False, True, pd.NA], dtype='boolean'),
                   'perennial': pd.array([False, pd.NA, True], dtype='boolean')}
    elif table == 'community_prices':
        columns = {'Price': number([0, 2.5, pd.NA]),
                   'NumberOfUnits': number([pd.NA, 50, 0]),
                   'Available': pd.array([False, True, pd.NA], dtype='boolean'),
                   'Derivation': pd.array([pd.NA, 'synthetic-key', pd.NA], dtype='string')}
    else:
        raise AssertionError(f'No invented frame for {table}')
    frame = pd.DataFrame(columns, index=index)
    frame.attrs = {'invented_compiler': {'release': 'synthetic-A', 'native_order': True}}
    return frame.iloc[:0].copy() if empty else frame


def _expected(table, frames):
    result = pd.concat(frames)
    # Existing crop compiler promotes native u; no other reduction is introduced.
    if table == 'crop_production' and 'u' in result.columns:
        result = result.set_index('u', append=True)
    return result


@pytest.fixture(params=TABLES)
def compiler(request, monkeypatch, tmp_path):
    table = request.param
    config_pack = Path(os.environ['MALAWI_COMPILER_CONFIG_PACK']) if 'MALAWI_COMPILER_CONFIG_PACK' in os.environ else tmp_path
    before = paths.countries_root()
    selected = []

    def select(variant='base'):
        config = config_pack / table / variant
        if 'MALAWI_COMPILER_CONFIG_PACK' not in os.environ:
            write_metadata(config, table, variant)
        assert config.is_dir()
        monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(config))
        paths.countries_root.cache_clear()
        assert paths.countries_root().resolve() == config.resolve()
        # CONFIG is separately hashed; these are actual metadata APIs.
        country = Country('Malawi')
        discovered = [(wave, country[wave]) for wave in country.waves]
        selected[:] = [str(config)]
        return country, discovered

    reads, writes = [], []
    frames, failure = {}, []

    def read(path):
        reads.append(path)
        assert path in frames, f'Unexpected compiler reader path {path}'
        if failure and path == failure[0]:
            raise failure[1]
        return frames[path].copy(deep=True)

    def write(frame, path):
        assert path == f'../var/{table}.parquet'
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    monkeypatch.setattr(local_tools, 'to_parquet', write)

    def seed(discovered, empty=()):
        frames.clear()
        for number, (wave, obj) in enumerate(discovered):
            frames[f'../{obj.wave_folder}/_/{table}.parquet'] = _frame(table, wave, number in empty)
        # Old files remain available in memory even when metadata retires them.
        for wave in WAVES:
            frames.setdefault(f'../{wave}/_/{table}.parquet', _frame(table, wave))

    def run():
        assert selected
        return runpy.run_path(str(ROOT / f'lsms_library/countries/Malawi/_/{table}.py'), run_name='__main__')

    yield table, select, seed, run, frames, failure, reads, writes
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(before))
    paths.countries_root.cache_clear()


@pytest.mark.parametrize('variant', ['native_only', 'yaml_only'])
@pytest.mark.parametrize('failed_wave', [0, 2])
@pytest.mark.parametrize('error_type', [FileNotFoundError, PermissionError, ValueError, KeyError])
def test_declared_error_keeps_identity_cause_and_never_writes(compiler, variant, failed_wave, error_type):
    table, select, seed, run, frames, failure, reads, writes = compiler
    country, discovered = select(variant)
    assert country.waves == WAVES
    assert all(table in obj.data_scheme for _, obj in discovered)
    seed(discovered)
    expected_paths = [f'../{obj.wave_folder}/_/{table}.parquet' for _, obj in discovered]
    cause = RuntimeError('invented decoder cause')
    error = error_type('invented declared failure')
    error.__cause__ = cause
    failure[:] = [expected_paths[failed_wave], error]
    with pytest.raises(error_type) as caught:
        run()
    assert caught.value is error
    assert caught.value.__cause__ is cause
    assert reads == expected_paths[:failed_wave + 1]
    assert writes == []


@pytest.mark.parametrize('empty', [(), (0, 2), (0, 1, 2, 3)])
def test_healthy_and_declared_empty_frames_conserve_exact_native_output(compiler, empty):
    table, select, seed, run, frames, failure, reads, writes = compiler
    _, discovered = select()
    seed(discovered, empty)
    expected_paths = [f'../{obj.wave_folder}/_/{table}.parquet' for _, obj in discovered]
    expected = _expected(table, [frames[path] for path in expected_paths])
    run()
    assert reads == expected_paths
    assert len(writes) == 1
    pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs
    assert writes[0].index.is_unique
    # Exercise actual identity classification/substitution once on invented maps.
    # The compiler itself must keep raw IDs and never walk them on its own.
    if len(expected) and table != 'community_prices':
        maps = {wave: {'cs-17-0007': 'canonical-7', 'canonical-7': 'earlier-7', '0012-003': 'canonical-12'} for wave in WAVES}
        walked = local_tools.id_walk(writes[0], maps)
        assert walked.index.get_level_values('i').tolist() == [
            {'cs-17-0007': 'canonical-7', '0012-003': 'canonical-12'}.get(value, value)
            for value in expected.index.get_level_values('i')]
        assert walked.attrs['id_converted'] is True
        pd.testing.assert_frame_equal(local_tools.id_walk(walked, maps), walked, check_exact=True)
        pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)


@pytest.mark.parametrize('variant', ['unwired', 'retired_wave', 'missing_wave_dir'])
def test_unwired_or_unregistered_wave_skips_before_stale_output_reader(compiler, variant):
    table, select, seed, run, frames, failure, reads, writes = compiler
    _, discovered = select(variant)
    seed(discovered)
    # A successful stale read would contaminate the output; it is not an error mock.
    eligible = [obj for _, obj in discovered if table in obj.data_scheme]
    expected_paths = [f'../{obj.wave_folder}/_/{table}.parquet' for obj in eligible]
    expected = _expected(table, [frames[path] for path in expected_paths])
    run()
    assert reads == expected_paths
    assert len(writes) == 1
    pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs


@pytest.mark.parametrize('variant', ['mapped', 'extra_wave', 'yaml_removed_native_retained'])
def test_actual_folder_mapping_and_union_declarations_govern_reads(compiler, variant):
    table, select, seed, run, frames, failure, reads, writes = compiler
    _, discovered = select(variant)
    seed(discovered)
    expected_paths = [f'../{obj.wave_folder}/_/{table}.parquet'
                      for _, obj in discovered if table in obj.data_scheme]
    expected = _expected(table, [frames[path] for path in expected_paths])
    run()
    assert reads == expected_paths
    assert len(writes) == 1
    pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs


@pytest.mark.parametrize('variant', ['none_declared', 'country_retired'])
def test_no_declared_inputs_refuses_without_read_or_write(compiler, variant):
    table, select, seed, run, frames, failure, reads, writes = compiler
    _, discovered = select(variant)
    seed(discovered)
    with pytest.raises(AssertionError):
        run()
    assert reads == []
    assert writes == []


@pytest.mark.parametrize('compiler', ['community_prices'], indirect=True)
def test_current_community_three_wave_declarations_keep_native_cluster_axis(compiler):
    table, select, seed, run, frames, failure, reads, writes = compiler
    _, discovered = select('current_three_wave')
    seed(discovered)
    expected_paths = [f'../{obj.wave_folder}/_/{table}.parquet'
                      for _, obj in discovered if table in obj.data_scheme]
    assert expected_paths == [f'../{wave}/_/community_prices.parquet'
                              for wave in ['2010-11', '2013-14', '2019-20']]
    expected = _expected(table, [frames[path] for path in expected_paths])
    run()
    assert reads == expected_paths
    assert len(writes) == 1
    pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
    assert writes[0].attrs == expected.attrs
    assert writes[0].index.names == ['t', 'v', 'i', 'j', 'u']
    assert writes[0].index.get_level_values('v').equals(writes[0].index.get_level_values('i'))

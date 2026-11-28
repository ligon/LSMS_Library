"""W1-W3 keep logical rounds and cover outcomes, including dated attempts."""

import runpy

import pandas as pd
import pytest

from lsms_library import Country, datetime_parse, local_tools
from lsms_library.derivations import records_for
from lsms_library.paths import countries_root


NIGERIA = countries_root() / 'Nigeria'
TABLE = 'interview_date'
KEY = '::interview_date::date-day-imputed'
ROUND_FILES = {
    '2010-11': (
        ('2011Q1', 'Post Harvest Wave 1/Household/secta_harvestw1.dta', 'sq1', 0),
        ('2010Q3', 'Post Planting Wave 1/Household/secta_plantingw1.dta', 'saq22a', 0),
    ),
    '2012-13': (
        ('2013Q1', 'Post Harvest Wave 2/Household/secta_harvestw2.dta', 'saq1', 0),
        ('2012Q3', 'Post Planting Wave 2/Household/secta_plantingw2.dta', 'saq22a', 0),
    ),
    '2015-16': (
        ('2015Q3', 'secta_plantingw3.dta', 'saq22a', 2),
        ('2016Q1', 'secta_harvestw3.dta', 'saq22a', 1),
    ),
}


@pytest.mark.parametrize('folder', ROUND_FILES)
def test_paper_date_scripts_keep_reported_and_mark_only_partial_visits(folder, tmp_path, monkeypatch):
    from lsms_library.datetime_parse import DatetimeParseWarning

    pairs = ROUND_FILES[folder]
    sources = {part[1]: part for part in pairs}
    reads = []
    sample_keys = []

    def read(source, *, convert_categoricals=True, **kwargs):
        rel = str(source).removeprefix('../Data/')
        t, _, status, partial_count = sources[rel]
        assert convert_categoricals is (folder != '2015-16')
        reads.append(rel)
        year = int(t[:4])
        names = ['direct', 'undated', 'impossible', *[f'partial-{k}' for k in range(partial_count)]]
        sample_keys.extend((t, i) for i in names)
        status_values = [4, 1, 1, *([1] * partial_count)]
        if folder != '2015-16':
            status_values = ['4. Refused' if k == 4 else '1. Completed' for k in status_values]
        return pd.DataFrame({
            'hhid': names, 'sector': [1] * len(names),
            'saq13y': [year, None, year, *([year] * partial_count)],
            'saq13m': [2, None, 9, *([3] * partial_count)],
            'saq13d': [20, None, 31, *([None] * partial_count)],
            status: status_values,
        })

    original_writer = local_tools.to_parquet
    target = tmp_path / f'{TABLE}.parquet'

    def write(df, filename, **kwargs):
        assert filename == target.name
        return original_writer(df, str(target), **kwargs)

    monkeypatch.syspath_prepend(str(NIGERIA / '_'))
    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    monkeypatch.setattr(local_tools, 'to_parquet', write)
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})
    with pytest.warns(DatetimeParseWarning):
        runpy.run_path(str(NIGERIA / folder / '_' / f'{TABLE}.py'))

    assert reads == [part[1] for part in pairs]
    stored = pd.read_parquet(target)
    assert stored.index.names == ['i', 't']
    assert stored.index.is_unique
    assert set(stored.index.get_level_values('t')) == {part[0] for part in pairs}
    by_round = stored.reorder_levels(['t', 'i'])
    # The read order and the written order are deliberately different for W3.
    assert list(stored.index.get_level_values('t').unique()) == sorted(
        (part[0] for part in pairs), reverse=True)
    assert stored['int_t'].notna().all()
    for t, _, _, partial_count in pairs:
        frame = by_round.xs(t, level='t')
        assert list(frame.index) == ['direct', *[f'partial-{k}' for k in range(partial_count)]]
        assert frame.loc['direct', 'int_t'] == pd.Timestamp(f'{t[:4]}-02-20')
        assert frame.loc['direct', 'InterviewResult'] == 'Refused'
        if partial_count:
            assert pd.isna(frame.loc['direct', 'Derivation'])
            assert frame['Derivation'].dropna().eq(KEY).all()
            assert len(frame['Derivation'].dropna()) == partial_count
            assert frame['int_t'].iloc[1:].eq(pd.Timestamp(f'{t[:4]}-03-01')).all()
            assert frame['InterviewResult'].iloc[1:].eq('Completed').all()
        else:
            assert 'Derivation' not in frame
    stamp = local_tools.read_parquet_datetime_audit(target)
    assert {r['wave'] for r in stamp} == {p[0] for p in pairs}
    assert {r['wave_folder'] for r in stamp} == {folder}
    assert {r['wave']: (r['missing'], r['out_of_range'], r['imputed_day']) for r in stamp} == {
        t: (1, 1, n) for t, _, _, n in pairs
    }
    wanted = pairs[0][0]
    datetime_parse._clear_datetime_reports()
    with pytest.warns(DatetimeParseWarning, match='replayed'):
        datetime_parse.replay_datetime_audit(stamp, 'Nigeria', TABLE, waves=[wanted])
    assert [r['wave'] for r in datetime_parse.datetime_reports('Nigeria', TABLE)] == [wanted]

    if folder == '2015-16':
        country = Country('Nigeria')
        country._panel_ids_attempted = True
        country._sample_v_cache = pd.DataFrame(
            {'v': 'synthetic'}, index=pd.MultiIndex.from_tuples(sample_keys, names=['t', 'i']))
        data_scheme = country.resources['Data Scheme'][TABLE]
        served = country._finalize_result(stored, data_scheme, TABLE)
        assert records_for('Nigeria', TABLE)[KEY].columns == ['Int_t']
        assert served.attrs['derivations']['Nigeria'][KEY]['rows'] == 3
        assert len(served) == 5


def test_paper_dates_strict_mode_is_distinct(monkeypatch):
    source = pd.DataFrame({'y': [2015], 'm': [9], 'd': [31]})
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})
    monkeypatch.setenv('LSMS_DATE_STRICT', '1')
    with pytest.raises(datetime_parse.DatetimeParseError):
        datetime_parse.assign_datetime(
            source, 'int_t', source, calendar='gregorian', country='Nigeria',
            table=TABLE, wave='2015Q3', wave_folder='2015-16')

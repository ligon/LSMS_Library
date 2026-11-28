"""GH #1045: preserve Niger's reported EHCVM visit clocks and audit the parse."""
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library import Country, datetime_parse, local_tools

ROOT = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Niger'
COVERS = {'2018-19': 's00_me_ner2018.dta', '2021-22': 's00_me_ner2021.dta'}
OPERANDS = {
    'int_start': 's00q23a', 'int_end': 's00q23b',
    'int_start_v2': 's00q24a', 'int_end_v2': 's00q24b',
    'int_start_v3': 's00q25a', 'int_end_v3': 's00q25b',
}


def _config(wave):
    with (ROOT / wave / '_/data_info.yml').open() as fh:
        return yaml.safe_load(fh)


@pytest.mark.parametrize('wave', COVERS)
def test_own_iso_operands_and_placeholders(wave):
    parent = _config(wave)
    spec = parent['interview_date']
    assert spec['file'] == COVERS[wave] and spec['idxvars'] == {'i': ['grappe', 'menage']}
    wanted = {
        target: {'datetime': source, 'calendar': 'gregorian', 'format': '%Y-%m-%dT%H:%M:%S'}
        for target, source in OPERANDS.items()
    }
    if wave == '2018-19':
        for value in wanted.values():
            value['sentinels'] = ['##N/A##']
    assert spec['myvars'] == wanted


@pytest.mark.parametrize('wave', ('2011-12', '2014-15'))
def test_pre_ehcvm_has_no_interview_date_producer(wave):
    assert 'interview_date' not in _config(wave)
    assert not (ROOT / wave / '_/interview_date.py').exists()


@pytest.fixture
def invented(monkeypatch, tmp_path):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('LSMS_DATE_STRICT', raising=False)
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})

    def setup(wave, malformed=False):
        year = wave[:4]
        sent = '##N/A##' if wave == '2018-19' else ''
        data = pd.DataFrame({
            'grappe': [9, 9], 'menage': [1, 2],
            's00q23a': [f'{year}-11-02T15:27:04', f'{year}-11-06T14:27:19'],
            # A reversed start/end pair stays reversed. Neither clock is corrected.
            's00q23b': [f'{year}-11-02T15:07:08', sent],
            's00q24a': [f'{year}-10-30T08:01:01', sent],
            's00q24b': [f'{year}-10-30T12:00:00', ''],
            # End-only stays present, while the entirely absent visit is not made.
            's00q25a': ['', ''],
            's00q25b': [f'{year}-11-05T12:00:00' if not malformed else 'discard me', ''],
        })
        reads = []

        def reader(path, **kw):
            assert Path(path).name == COVERS[wave]
            reads.append(path)
            return data.copy(deep=True)

        monkeypatch.setattr(local_tools, 'get_dataframe', reader)
        return reads, Country('Niger', preload_panel_ids=False)[wave]

    return setup


@pytest.mark.parametrize('wave', COVERS)
def test_grab_data_keeps_clock_and_ordinal_and_replays_reports(invented, monkeypatch, wave):
    reads, obj = invented(wave)
    if wave == '2018-19':
        with pytest.warns(datetime_parse.DatetimeParseWarning, match='sentinel=1'):
            cold = obj.grab_data('interview_date')
    else:
        cold = obj.grab_data('interview_date')
    assert len(reads) == 1
    assert cold.index.names == ['t', 'i', 'visit']
    assert not cold.index.duplicated().any()
    assert sorted(cold.groupby(level='i').size()) == [1, 3]
    assert cold.groupby(level='visit').size().to_dict() == {1: 2, 2: 1, 3: 1}
    assert set(map(str, cold.dtypes)) == {'datetime64[us]'}
    assert cold.xs(3, level='visit')['Interview start'].isna().all()
    row = cold.xs(1, level='visit').iloc[0]
    assert row['Interview start'] == pd.Timestamp(f'{wave[:4]}-11-02T15:27:04')
    assert row['Interview end'] < row['Interview start']

    path = local_tools.data_root('Niger') / wave / '_/interview_date.parquet'
    ledger = local_tools.read_parquet_datetime_audit(path)
    assert len(ledger) == 6 and {r['wave'] for r in ledger} == {wave}
    reports = {r['column']: r for r in ledger}
    assert reports['int_end']['sentinel'] == int(wave == '2018-19')
    assert reports['int_end']['missing'] == int(wave == '2021-22')
    assert reports['int_start_v3']['missing'] == 2

    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})
    if wave == '2018-19':
        with pytest.warns(datetime_parse.DatetimeParseWarning, match='replayed from'):
            warm = obj.grab_data('interview_date')
    else:
        warm = obj.grab_data('interview_date')
    assert len(reads) == 1
    assert warm.index.names == cold.index.names
    pd.testing.assert_frame_equal(
        cold.reset_index().astype({'visit': 'int64'}),
        warm.reset_index().astype({'visit': 'int64'}),
    )
    assert datetime_parse.datetime_reports('Niger', 'interview_date') == [
        dict(r, from_cache=True) for r in ledger
    ]


def test_strict_mode_reports_an_unrecognized_niger_stamp(invented, monkeypatch):
    _, obj = invented('2021-22', malformed=True)
    monkeypatch.setenv('LSMS_DATE_STRICT', '1')
    with pytest.raises(datetime_parse.DatetimeParseError, match='int_end_v3.*unparsed=1'):
        obj.grab_data('interview_date')

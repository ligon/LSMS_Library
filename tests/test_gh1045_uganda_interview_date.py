"""Exercise all eight Uganda date scripts and its native compiler without raw data."""
import importlib.util
from pathlib import Path
import runpy
import sys

import pandas as pd
import pytest

from lsms_library import datetime_parse, local_tools
from lsms_library.paths import data_root

UGANDA = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Uganda'
WAVES = {
    '2005-06': ('Data/GSEC1.dta', 'HHID', 'region'),
    '2009-10': ('Data/GSEC1.dta', 'HHID', 'region'),
    '2010-11': ('Data/GSEC1.dta', 'HHID', 'region'),
    '2011-12': ('Data/GSEC1.dta', 'HHID', 'region'),
    '2013-14': ('Data/GSEC1.dta', 'HHID', 'rotate'),
    '2015-16': ('Data/gsec1.dta', 'HHID', 'district'),
    '2018-19': ('Data/GSEC1.dta', 'hhid', 'batch'),
    '2019-20': ('Data1/HH/gsec1.dta', 'hhid', 'batch'),
}
COUNTRY, TABLE = 'Uganda', 'interview_date'
DAY = '::interview_date::date-day-imputed'
MONTH = '::interview_date::date-month-day-imputed'


@pytest.fixture
def run_wave(monkeypatch, tmp_path):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    monkeypatch.setattr(sys, 'path', list(sys.path))
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})
    monkeypatch.delenv('LSMS_DATE_STRICT', raising=False)

    def run(wave, incomplete=()):
        # Match the native Make invocation: each wave runs in a fresh process.
        local_tools._resolve_data_path.cache_clear()
        cover, hh, t_input = WAVES[wave]
        year = int(wave[:4])
        rows = [(year, '02', '05'), *incomplete]
        i = ['fixture-date-' + str(n) for n in range(len(rows))]
        idx = pd.MultiIndex.from_product([i, [wave]], names=['j', 't'])

        def reader(path, idxvars, **myvars):
            assert path == '../' + cover
            assert idxvars['j'] == hh
            name, transform = idxvars['t']
            assert name == t_input and transform('invented input') == wave
            native = ('h1bq2c', 'h1bq2b', 'h1bq2a') if year <= 2009 else ('year', 'month', 'day')
            assert myvars == dict(zip(('year', 'month', 'day'), native))
            return pd.DataFrame(rows, columns=['year', 'month', 'day'], index=idx)

        monkeypatch.setattr(local_tools, 'df_data_grabber', reader)
        driver = UGANDA / wave / '_/interview_date.py'
        monkeypatch.chdir(driver.parent)
        runpy.run_path(str(driver), run_name='__main__')
        path = data_root(COUNTRY) / wave / '_/interview_date.parquet'
        return pd.read_parquet(path), local_tools.read_parquet_datetime_audit(path)

    yield run
    local_tools._resolve_data_path.cache_clear()


@pytest.mark.parametrize('wave', WAVES)
def test_native_wave_roles_and_date_report(run_wave, wave):
    out, reports = run_wave(wave)
    assert list(out.columns) == ['Int_t']
    assert out.index.names == ['j', 't']
    assert out['Int_t'].iat[0] == pd.Timestamp(f'{wave[:4]}-02-05')
    assert str(out['Int_t'].dtype) == 'datetime64[us]'
    assert len(reports) == 1
    report = reports[0]
    assert [
        report['wave'], report['wave_folder'], report['column'], report['calendar'],
        report['parsed'], report['n'],
    ] == [wave, wave, 'Int_t', 'gregorian', 1, 1]


def test_national_compiler_keeps_exact_and_padded_dates_and_replays_footers(run_wave, monkeypatch):
    reports = []
    for wave in WAVES:
        incomplete = {
            '2005-06': [(2005, None, None)],
            '2010-11': [(2010, '06', None), (2010, '04', '31'), (None, None, None)],
        }.get(wave, [])
        if wave == '2010-11':
            with pytest.warns(datetime_parse.DatetimeParseWarning, match='out_of_range=1'):
                _, wave_reports = run_wave(wave, incomplete)
        else:
            _, wave_reports = run_wave(wave, incomplete)
        reports.extend(wave_reports)

    compiler = UGANDA / '_/interview_date.py'
    monkeypatch.chdir(compiler.parent)
    spec = importlib.util.spec_from_file_location('uganda', compiler.parent / 'uganda.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'uganda', module)
    spec.loader.exec_module(module)
    runpy.run_path(str(compiler), run_name='__main__')
    path = data_root(COUNTRY) / 'var/interview_date.parquet'
    out = pd.read_parquet(path)
    assert out.index.names == ['i', 't']
    assert len(out) == 10
    assert out['Derivation'].value_counts().to_dict() == {MONTH: 1, DAY: 1}
    exact = out['Derivation'].isna()
    assert out.loc[exact].index.get_level_values('t').tolist() == list(WAVES)
    assert (out.loc[exact, 'Int_t'].dt.day == 5).all()
    assert out.loc[('fixture-date-1', '2005-06'), 'Int_t'] == pd.Timestamp('2005-01-01')
    assert out.loc[('fixture-date-1', '2010-11'), 'Int_t'] == pd.Timestamp('2010-06-01')
    footer = local_tools.read_parquet_datetime_audit(path)
    assert footer == reports

    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})
    with pytest.warns(datetime_parse.DatetimeParseWarning, match='out_of_range=1'):
        datetime_parse.replay_datetime_audit(footer, COUNTRY, TABLE, waves=['2010-11'])
    assert datetime_parse.datetime_reports(COUNTRY, TABLE) == [
        dict(footer[2], from_cache=True)
    ]


def test_impossible_day_obeys_shared_strict_mode(run_wave, monkeypatch):
    monkeypatch.setenv('LSMS_DATE_STRICT', '1')
    with pytest.raises(datetime_parse.DatetimeParseError, match='out_of_range=1'):
        run_wave('2010-11', [(2010, '04', '31')])
    assert not (data_root(COUNTRY) / '2010-11/_/interview_date.parquet').exists()

"""GhanaLSS calendar visits: real YAML/formatters over synthetic source rows.

The source choices and omission semantics are recorded in
.coder/ledger/ghanalss-visit-dates.md. No microdata or shared L2 is used.
"""
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import local_tools as tools
from lsms_library.paths import data_root


WAVES = ('1987-88', '1988-89', '1991-92', '1998-99', '2005-06', '2012-13', '2016-17')


def _sources(wave):
    """Distinct date pages deliberately disagree; missing dates stay missing."""
    year = int(wave[:4])
    if wave in ('1987-88', '1988-89'):
        return {'y00a.dat': pd.DataFrame({
            'HID': [100101, 100102], 'DAY1': [10, 11], 'MO1': [10, 10],
            'YR1': [year - 1900] * 2, 'DAY2': [24, None], 'MO2': [10, None],
            'YR2': [year - 1900, None]})}
    if wave in ('1991-92', '1998-99'):
        first = pd.DataFrame({'clust': [3001] * 3, 'nh': [1, 2, 3],
                              'dd': [10, 11, 31], 'mm': [10, 10, 2],
                              'yy': [year - 1900] * 3})
        if wave == '1991-92':
            later = pd.DataFrame({'clust': [3001, 3001], 'nh': [1, 3]})
            for visit in range(1, 12):
                later[f'vd{visit}'] = [9 if visit == 1 else 10 + visit,
                                       9 if visit == 1 else None]
                later[f'vm{visit}'] = [10, 10 if visit == 1 else None]
                later[f'vy{visit}'] = [91, 91 if visit == 1 else None]
            later.loc[0, ['vd2', 'vm2']] = [31, 2]
            return {'s0a.dta': first, 's0b.dta': later}
        return {'sec0a.dta': first,
                'sec0b.dta': pd.DataFrame({'clust': [3001], 'nh': [1],
                    'visitcd': [1], 's0dy_sum': [10], 's0mt_sum': [10], 's0yr_sum': [98]}),
                'sec0c.dta': pd.DataFrame({
            'clust': [3001] * 6, 'nh': [1, 1, 1, 3, 3, 3],
            'visitcd': [1, 2, 3, 1, 2, 3],
            's0vday': [9, 31, 25, 9, None, None],
            's0vmonth': [10, 2, 10, 10, None, None],
            's0vyear': [98, 98, 98, 98, None, None]})}
    first = pd.DataFrame({'ddate': [10, 31], 'mdate': [10, 2],
                          'ydate': [year] * 2})
    if wave == '2005-06':
        first['hhid'] = [100101, 100102]
        return {'sec0.dta': first,
                'secb0.dta': pd.DataFrame({'hhid': [100101], 'day': [10],
                    'month': [10], 'year': [year]}),
                'secb1.dta': pd.DataFrame({'hhid': [100101], 'visitno': [2],
                    'intday': [None], 'intmon': [None], 'intyr': [None]}),
                'secb2.dta': pd.DataFrame({'hhid': [100101], 'visitcd': [2],
                    'vday': [None], 'vmon': [None], 'vyr': [None]})}
    if wave == '2012-13':
        first['HID'] = [100101, 100102]
        return {'sec0.dta': first}
    first['clust'], first['nh'] = [70001] * 2, [1, 2]
    first['mdate'] = ['October', 'February']
    return {'g7sec0.dta': first}


@pytest.fixture
def build_wave(monkeypatch, tmp_path):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    data_root.cache_clear()
    original = tools.get_dataframe

    def build(wave):
        sources = _sources(wave)

        def read(fn, *args, **kwargs):
            name = Path(fn).name.lower()
            if name in sources:
                return sources[name].copy()
            if Path(fn).suffix.lower() in ('.dta', '.dat'):
                raise AssertionError(f'Unexpected source read: {fn}')
            return original(fn, *args, **kwargs)

        monkeypatch.setattr(tools, 'get_dataframe', read)
        if wave in ('1998-99', '2005-06'):
            import sys
            from lsms_library.derivations import resolve_callable
            builder = resolve_callable('lsms_library.countries.GhanaLSS._.visit_dates:build_visit_dates')
            monkeypatch.setattr(sys.modules[builder.__module__], 'get_dataframe', read)
        return ll.Country('GhanaLSS')[wave].interview_date()

    yield build
    data_root.cache_clear()


@pytest.mark.parametrize('wave', WAVES)
def test_wave_dates_have_unique_integer_calendar_visits(build_wave, wave):
    result = build_wave(wave)
    assert set(result.index.names) == {'t', 'i', 'visit'}
    assert result.index.is_unique
    assert pd.api.types.is_integer_dtype(result.index.get_level_values('visit').dtype)
    assert list(result.columns) == (['Int_t', 'Derivation']
                                    if wave in ('1998-99', '2005-06') else ['Int_t'])
    if 'Derivation' in result:
        assert result.Derivation.isna().all()
    assert result['Int_t'].notna().all()
    assert pd.api.types.is_datetime64_any_dtype(result['Int_t'])


@pytest.mark.parametrize('wave', ('1987-88', '1988-89'))
def test_early_second_contact_is_present_without_inventing_missing_date(build_wave, wave):
    result = build_wave(wave).reset_index()
    assert result.groupby('visit').size().to_dict() == {1: 2, 2: 1}
    assert result.loc[result.visit == 2, 'Int_t'].iloc[0] == pd.Timestamp(f'{wave[:4]}-10-24')


@pytest.mark.parametrize('wave', ('1991-92', '1998-99'))
def test_intake_page_wins_and_intake_only_households_survive(build_wave, wave):
    result = build_wave(wave).reset_index()
    intake = result[result.visit == 1].set_index('i')['Int_t']
    assert intake.to_dict() == {
        '300101': pd.Timestamp(f'{wave[:4]}-10-10'),
        '300102': pd.Timestamp(f'{wave[:4]}-10-11'),
    }
    # A competing valid first date does not replace impossible intake date.
    assert '300103' not in set(result.i)
    # Visit 2 is impossible, but visit 3 remains visit 3 (not renumbered).
    assert 2 not in set(result.visit)
    assert 3 in set(result.visit)
    assert set(result.visit) == ({1, *range(3, 12)} if wave == '1991-92' else {1, 3})


@pytest.mark.parametrize('wave', ('2005-06', '2012-13', '2016-17'))
def test_later_waves_keep_only_existing_intake_and_drop_invalid_slot(build_wave, wave):
    result = build_wave(wave)
    assert result.index.get_level_values('visit').tolist() == [1]
    assert result.Int_t.tolist() == [pd.Timestamp(f'{wave[:4]}-10-10')]


def test_shared_hook_preserves_reported_years_and_equal_or_negative_gaps():
    hook = ll.Country('GhanaLSS').formatting_functions['interview_date']
    idx = pd.MultiIndex.from_tuples([('1987-88', 'a'), ('1987-88', 'b')], names=['t', 'i'])
    first = pd.Timestamp('1987-10-10')
    data = pd.DataFrame({'Int_t': [first, first],
                         'Int_t_v2': [first, pd.Timestamp('1927-11-09')]}, index=idx)
    result = hook(data)
    assert result.loc[('1987-88', 'a', 2), 'Int_t'] == first
    assert result.loc[('1987-88', 'b', 2), 'Int_t'] == pd.Timestamp('1927-11-09')

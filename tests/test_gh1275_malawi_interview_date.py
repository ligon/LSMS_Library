"""IHS6 and the retained IHPS interview contribute their own Gregorian day."""
from pathlib import Path
import sys

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.datetime_parse import DatetimeParseError


WAVE = '2024-25'
XS = 'Cross_Sectional/hh_mod_a_filt.dta'
PANEL = 'Panel/hh_mod_a_filt_24.dta'


@pytest.fixture
def covers(monkeypatch):
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    monkeypatch.delitem(sys.modules, 'malawi', raising=False)
    frames = {
        XS: pd.DataFrame({
            'case_id': [11, 12],
            'interviewDate': ['2024-02-29', '2024-07-11'],
        }),
        PANEL: pd.DataFrame({
            'case_id': [11, 118, 119],
            'y5_hhid': ['refresh', 'retained', 'undated'],
            'y4_hhid': ['', 'old-18', 'old-19'],
            'interviewDate': ['2024-02-29', '2024-08-15', ''],
            'consumption_date': ['2024-03-03', '2024-08-18', '2024-09-20'],
        }),
    }

    def invented_reader(path, **kwargs):
        key = Path(path).parts[-2:]
        assert '/'.join(key) in frames
        return frames['/'.join(key)].copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', invented_reader)
    return frames


def _native_dates():
    return Country('Malawi')[WAVE].grab_data('interview_date')


def test_date_spec_and_hook_keep_one_dated_row_per_interview(covers):
    got = _native_dates()
    assert got.index.names == ['t', 'i', 'visit']
    assert set(got.index.get_level_values('t')) == {WAVE}
    assert set(got.index.get_level_values('visit')) == {1}
    assert got.index.is_unique
    assert list(got.columns) == ['Int_t']
    assert str(got['Int_t'].dtype) == 'datetime64[us]'
    assert got.droplevel(['t', 'visit'])['Int_t'].to_dict() == {
        'cs-24-11': pd.Timestamp('2024-02-29'),
        'cs-24-12': pd.Timestamp('2024-07-11'),
        'retained': pd.Timestamp('2024-08-15'),
    }


def test_gregorian_day_rejects_invalid_date(covers, monkeypatch):
    monkeypatch.setenv('LSMS_DATE_STRICT', '1')
    covers[XS].loc[1, 'interviewDate'] = '2023-02-29'
    with pytest.raises(DatetimeParseError):
        _native_dates()


def test_two_undated_copies_do_not_manufacture_a_visit(covers):
    covers[XS].loc[0, 'interviewDate'] = ''
    covers[PANEL].loc[0, 'interviewDate'] = ''
    got = _native_dates()
    assert set(got.index.get_level_values('i')) == {'cs-24-12', 'retained'}


def test_conflicting_refresh_dates_refuse_selection(covers):
    covers[PANEL].loc[0, 'interviewDate'] = '2024-03-01'
    with pytest.raises(ValueError, match='refresh dates disagree'):
        _native_dates()


def test_refresh_without_a_current_ihs6_interview_refuses_selection(covers):
    covers[PANEL].loc[0, 'case_id'] = 13
    with pytest.raises(ValueError, match='refresh counterparts'):
        _native_dates()


def test_duplicate_ihps_cover_ids_refuse_selection(covers):
    covers[PANEL].loc[1, 'y5_hhid'] = 'refresh'
    with pytest.raises(ValueError, match='missing or duplicate cover IDs'):
        _native_dates()

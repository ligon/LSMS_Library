"""Malawi's historical civil dates retain the original cover and visit roles."""
from hashlib import sha256

import pandas as pd
import pytest

from lsms_library import Country, datetime_parse, local_tools as lt
from lsms_library.countries.Malawi._._malawi_interview_dates import parsed_interview_dates

KEY = '::interview_date::date-day-imputed'
TABLE = 'interview_date'
COUNTRY = 'Malawi'


@pytest.fixture
def wave(monkeypatch, tmp_path):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('LSMS_DATE_STRICT', raising=False)
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})

    def setup(wave_name, providers):
        reads = []

        def get(path, **kwargs):
            key = next(k for k in providers if str(path).endswith(k))
            reads.append(key)
            return providers[key].copy(deep=True)

        monkeypatch.setattr(lt, 'get_dataframe', get)
        return Country(COUNTRY, preload_panel_ids=False)[wave_name], reads

    return setup


def test_ihs2_keeps_numeric_day_and_month_in_their_source_roles(wave):
    cover = pd.DataFrame({'case_id': ['a', 'b'], 'a14a': [5, 12],
                          'a14b': [9, 3], 'a14c': [2004, 2005]})
    obj, reads = wave('2004-05', {'sec_a.dta': cover})
    result = obj.grab_data(TABLE)
    assert reads == ['sec_a.dta']
    assert list(result.columns) == ['Int_t']
    assert result.index.get_level_values('visit').tolist() == [1, 1]
    assert list(result['Int_t']) == [pd.Timestamp('2004-09-05'), pd.Timestamp('2005-03-12')]


def test_ihs3_applies_parser_to_decoded_stata_dates_not_epoch_counts(wave):
    cover = pd.DataFrame({'case_id': ['a', 'b'],
                          'hh_a23_1': pd.to_datetime(['2010-08-31', '2010-09-02']),
                          'hh_a23_2': pd.to_datetime([None, '2011-09-03'])})
    obj, reads = wave('2010-11', {'Full_Sample/Household/hh_mod_a_filt.dta': cover})
    result = obj.grab_data(TABLE)
    assert len(reads) == 1
    assert sorted(result.index.get_level_values('visit')) == [1, 1, 2]
    assert result['Int_t'].tolist() == list(pd.to_datetime(['2010-08-31', '2010-09-02', '2011-09-03']))
    assert len(datetime_parse.datetime_reports(COUNTRY, TABLE)) == 2


def test_ihps_keeps_attempt_one_of_two_visits_and_tags_only_the_padded_visit(wave):
    cover = pd.DataFrame({
        'y2_hhid': ['a', 'b'],
        'hh_a23a_1': [None, 3], 'hh_a23a_2': pd.Categorical(['MAY', 'JUNE']),
        'hh_a23a_3': [2013, 2013],
        'hh_a23b_1': [17, 18],  # Retry of visit 1, deliberately unrelated.
        'hh_a37a_1': [4, None], 'hh_a37a_2': pd.Categorical(['AUGUST', 'OCTOBER']),
        'hh_a37a_3': [2013, 2013],
    })
    obj, reads = wave('2013-14', {'HH_MOD_A_FILT_13.dta': cover})
    result = obj.grab_data(TABLE).reset_index().set_index(['i', 'visit'])
    assert len(reads) == 1
    assert result['Int_t'].to_dict() == {
        ('a', 1): pd.Timestamp('2013-05-01'), ('b', 1): pd.Timestamp('2013-06-03'),
        ('a', 2): pd.Timestamp('2013-08-04'), ('b', 2): pd.Timestamp('2013-10-01'),
    }
    assert result.loc[result.Derivation.eq(KEY)].index.tolist() == [('a', 1), ('b', 2)]


def test_provenance_follows_source_row_not_just_a_repeated_household(wave):
    frame = pd.DataFrame({
        'int_t_y': [2013, 2013], 'int_t_m': [6, 6], 'int_t_d': [None, 2],
        'int_t_v2_y': [2013, 2013], 'int_t_v2_m': [9, 9], 'int_t_v2_d': [3, None],
    }, index=pd.MultiIndex.from_tuples([('2013-14', 'same')] * 2, names=['t', 'i']))
    frame_before = frame.copy()
    out = parsed_interview_dates(frame, '2013-14', components=True)
    assert out['Derivation'].eq(KEY).tolist() == [True, False, False, True]
    assert out['Int_t'].dt.day.tolist() == [1, 2, 3, 1]
    pd.testing.assert_frame_equal(frame, frame_before)


@pytest.mark.parametrize('w,i,source_marker,tagged', [
    ('2016-17', 'y3_hhid', '16', True), ('2019-20', 'y4_hhid', '19', False),
])
def test_split_files_keep_identity_and_do_not_count_cross_section_visit_two_as_missing(
        wave, monkeypatch, w, i, source_marker, tagged):
    y = w[:4]
    providers = {
        'Cross_Sectional/hh_mod_a_filt.dta': pd.DataFrame({
            'case_id': ['cs-a'], 'interviewDate': [f'{y}-08-13']}),
        f'Panel/hh_mod_a_filt_{source_marker}.dta': pd.DataFrame({
            i: ['p1', 'p2'], 'interviewdate_v1': [f'{y}-09-17', ''],
            'interviewdate_v2': [f'{y}-10-19', '##N/A##' if tagged else ''],
        }),
    }
    obj, reads = wave(w, providers)
    if tagged:
        with pytest.warns(datetime_parse.DatetimeParseWarning, match='sentinel=1'):
            cold = obj.grab_data(TABLE)
    else:
        cold = obj.grab_data(TABLE)
    assert reads == list(providers)
    assert set(cold.index.get_level_values('i')) == {('cs-17-cs-a' if w == '2016-17' else 'cs-19-cs-a'), 'p1'}
    assert list(cold.columns) == ['Int_t']
    assert cold.groupby(level='visit').size().to_dict() == {1: 2, 2: 1}

    path = lt.data_root(COUNTRY) / w / '_/interview_date.parquet'
    footer = lt.read_parquet_datetime_audit(path)
    r = {r['column']: r for r in footer}
    assert r['int_t']['n'] == 3
    assert r['int_t_v2']['n'] == 2 and r['int_t_v2']['parsed'] == 1
    assert r['int_t_v2']['sentinel'] == int(tagged)
    assert r['int_t_v2']['missing'] == int(not tagged)

    same = sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(datetime_parse, '_DATETIME_LEDGER', {})
    if tagged:
        with pytest.warns(datetime_parse.DatetimeParseWarning, match='replayed from'):
            warm = obj.grab_data(TABLE)
    else:
        warm = obj.grab_data(TABLE)
    assert len(reads) == 2 and sha256(path.read_bytes()).hexdigest() == same
    pd.testing.assert_frame_equal(
        warm.reset_index().astype({'visit': 'int64'}),
        cold.reset_index().astype({'visit': 'int64'}))
    assert datetime_parse.datetime_reports(COUNTRY, TABLE) == [dict(r, from_cache=True) for r in footer]


def test_a_recorded_visit_two_cannot_be_assigned_outside_the_panel_file(wave):
    frame = pd.DataFrame({'int_t': ['2016-05-02'], 'int_t_v2': ['2016-09-04'],
                          'date_panel_role': [None]},
                         index=pd.MultiIndex.from_tuples([('2016-17', 'a')], names=['t', 'i']))
    with pytest.raises(ValueError, match='visit 2 outside the panel cover'):
        parsed_interview_dates(frame, '2016-17', panel_visit=True)



def test_panel_source_flag_is_independent_of_a_missing_identifier(wave):
    providers = {
        'Cross_Sectional/hh_mod_a_filt.dta': pd.DataFrame({
            'case_id': ['cs-a'], 'interviewDate': ['2019-08-13']}),
        'Panel/hh_mod_a_filt_19.dta': pd.DataFrame({
            'y4_hhid': [None], 'interviewdate_v1': ['2019-09-05'],
            'interviewdate_v2': ['2019-10-07'],
        }),
    }
    obj, _ = wave('2019-20', providers)
    result = obj.grab_data(TABLE)
    assert result.xs(2, level='visit')['Int_t'].tolist() == [pd.Timestamp('2019-10-07')]
    reports = {r['column']: r for r in datetime_parse.datetime_reports(COUNTRY, TABLE)}
    assert reports['int_t_v2']['n'] == 1 and reports['int_t_v2']['parsed'] == 1

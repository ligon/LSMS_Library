"""GH #798: a declared datetime column with mixed string formats.

``pd.to_datetime(col, errors='coerce')`` infers ONE format from the first
parseable value and nulls every value spelled another way.  A warm L2-country
parquet concatenates waves before ``_finalize_result`` casts, so Mali (EAC-I
``YYYY-MM-DD HH:MM:SS`` + EHCVM ``YYYY-MM-DD``) served 12,194 of 24,939 rows and
Tanzania (UPD space-separated + ISO ``T``) 16,534 of 22,427.  Both casts now go
through ``country._coerce_datetime_column``: ``format='mixed'``, and a
``DatetimeCoercionWarning`` for any non-blank string that still comes back NaT.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.country import (
    DatetimeAmbiguityWarning,
    DatetimeCoercionWarning,
    _coerce_datetime_column,
    _enforce_canonical_dtypes,
    _enforce_declared_dtypes,
)

# The three spellings measured in the corpus: EAC-I / UPD, EHCVM, ISO 'T'.
THREE_FORMATS = ['2014-07-18 00:00:00', '2019-01-05', '2021-08-17T11:19:11']
EXPECTED = [pd.Timestamp('2014-07-18'), pd.Timestamp('2019-01-05'),
            pd.Timestamp('2021-08-17 11:19:11')]


def _record(fn, *args, **kwargs):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        out = fn(*args, **kwargs)
    return out, [x.message for x in w
                 if isinstance(x.message, DatetimeCoercionWarning)]


@pytest.mark.parametrize('dtype', [object, 'string'])
def test_three_formats_all_parse_without_warning(dtype):
    s = pd.Series(THREE_FORMATS, dtype=dtype)
    out, warned = _record(_coerce_datetime_column, s, column='Int_t')
    assert out.notna().all()
    assert list(out) == EXPECTED
    assert warned == []


def test_whole_column_inference_is_what_lost_the_rows():
    """Pin the pandas behaviour the fix exists for, so a future pandas that
    stops inferring a single format makes this visible rather than silent."""
    s = pd.Series(THREE_FORMATS, dtype='string')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        old = pd.to_datetime(s, errors='coerce')
    assert old.isna().sum() >= 1


def test_unparseable_string_is_counted_and_named():
    s = pd.Series(THREE_FORMATS + ['##N/A##', 'garbage', None, '', '  '],
                  dtype=object)
    out, warned = _record(_coerce_datetime_column, s, country='X',
                          table='interview_date', column='Int_t')
    assert out.notna().sum() == 3
    assert len(warned) == 1
    w = warned[0]
    assert (w.country, w.table, w.column, w.count) == ('X', 'interview_date',
                                                      'Int_t', 2)
    assert set(w.examples) == {'##N/A##', 'garbage'}
    assert 'X/interview_date/Int_t' in str(w)


def test_genuine_nulls_do_not_warn():
    s = pd.Series(['2019-01-05', None, pd.NA], dtype='string')
    out, warned = _record(_coerce_datetime_column, s)
    assert out.isna().sum() == 2
    assert warned == []


def test_datetime_column_passes_through_with_its_resolution():
    """Liberia's warm parquet is datetime64[ms]; the cast must not change it."""
    s = pd.Series(pd.to_datetime(['2018-09-22', None])).astype('datetime64[ms]')
    out, warned = _record(_coerce_datetime_column, s)
    assert out.dtype == s.dtype
    assert out.equals(s)
    assert warned == []


def test_both_enforcers_use_the_helper():
    df = pd.DataFrame({'Int_t': pd.Series(THREE_FORMATS, dtype='string')})
    _enforce_declared_dtypes(df, {'Int_t': 'datetime'}, country='X',
                             table='interview_date')
    assert list(df['Int_t']) == EXPECTED

    df = pd.DataFrame({'Int_t': pd.Series(THREE_FORMATS, dtype='string')})
    _enforce_canonical_dtypes(df, 'interview_date', country='X')
    assert list(df['Int_t']) == EXPECTED


def test_enforcers_warn_with_country_and_table():
    df = pd.DataFrame({'Int_t': pd.Series(['2019-01-05', 'nope'],
                                          dtype='string')})
    _, warned = _record(_enforce_canonical_dtypes, df, 'interview_date',
                        country='X')
    assert [(w.country, w.table, w.column, w.count) for w in warned] == [
        ('X', 'interview_date', 'Int_t', 1)]



@pytest.mark.parametrize('dtype', [object, 'string'])
def test_year_last_dayfirst_is_settled_by_the_data(dtype):
    """``13/01/2019`` can only be day-first, so the whole group is read so.

    Under bare ``format='mixed'`` ``05/01/2019`` came back **1 May** next to
    13 January -- two conventions, no NaT, no warning (the risk the red team
    named; @ligon chose this guard on 2026-10-18).  The pre-#798 whole-column
    call read both day-first, which is the contrast pinned at the end.
    """
    s = pd.Series(['13/01/2019', '05/01/2019'], dtype=dtype)
    out, warned = _record(_coerce_datetime_column, s, column='Int_t')
    assert list(out) == [pd.Timestamp('2019-01-13'), pd.Timestamp('2019-01-05')]
    assert warned == []

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        old = pd.to_datetime(s, errors='coerce')
    assert list(old) == [pd.Timestamp('2019-01-13'), pd.Timestamp('2019-01-05')]


@pytest.mark.parametrize('dtype', [object, 'string'])
def test_year_last_monthfirst_is_settled_by_the_data(dtype):
    s = pd.Series(['01/13/2019', '01/05/2019 10:30'], dtype=dtype)
    out, warned = _record(_coerce_datetime_column, s, column='Int_t')
    assert list(out) == [pd.Timestamp('2019-01-13'),
                         pd.Timestamp('2019-01-05 10:30')]
    assert warned == []


@pytest.mark.parametrize('vals,reason', [
    (['05/01/2019', '06/02/2019'], 'ambiguous'),     # no field above 12
    (['13/01/2019', '01/13/2019'], 'conflicting'),   # evidence both ways
])
def test_undecidable_year_last_is_NaT_and_warns_never_guessed(vals, reason):
    s = pd.Series(vals, dtype=object)
    out, warned = _record(_coerce_datetime_column, s, country='X',
                          table='interview_date', column='Int_t')
    assert out.isna().all()
    assert len(warned) == 1, warned          # one warning, not two
    w = warned[0]
    assert isinstance(w, DatetimeAmbiguityWarning)
    assert (w.reason, w.count, w.wave) == (reason, 2, None)
    assert 'X/interview_date/Int_t' in str(w)


def test_year_last_order_is_settled_per_wave():
    """Two waves may use opposite conventions; each is settled on its own."""
    idx = pd.MultiIndex.from_tuples(
        [('w1', 'a'), ('w1', 'b'), ('w2', 'c'), ('w2', 'd'), ('w3', 'e')],
        names=['t', 'i'])
    s = pd.Series(['13/01/2019', '05/01/2019', '01/13/2019', '01/05/2019',
                   '03/04/2019'], index=idx, dtype=object)
    out, warned = _record(_coerce_datetime_column, s, column='Int_t')
    assert list(out[:4]) == [pd.Timestamp('2019-01-13'), pd.Timestamp('2019-01-05')] * 2
    assert pd.isna(out.iloc[4])
    assert [(type(w), w.reason, w.wave) for w in warned] == \
        [(DatetimeAmbiguityWarning, 'ambiguous', 'w3')]


def test_year_first_strings_never_reach_the_guard():
    """Every stored string in the corpus is year-first; none may be touched."""
    s = pd.Series(['2019-01-05', '2019/01/05', '2019-05-01 08:00:00'], dtype=object)
    out, warned = _record(_coerce_datetime_column, s, column='Int_t')
    assert list(out) == [pd.Timestamp('2019-01-05'), pd.Timestamp('2019-01-05'),
                         pd.Timestamp('2019-05-01 08:00')]
    assert warned == []


def test_mixed_timezones_warn_and_are_served_unconverted():
    df = pd.DataFrame({'Int_t': ['2021-08-17T10:00:00+01:00',
                                 '2021-08-17 10:00:00']})
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        _enforce_declared_dtypes(df, {'Int_t': 'datetime'},
                                 country='X', table='interview_date')
    msgs = [x.message for x in w if isinstance(x.message, DatetimeCoercionWarning)]
    assert len(msgs) == 1 and 'timezone' in str(msgs[0])
    assert msgs[0].country == 'X' and msgs[0].column == 'Int_t'
    assert list(df['Int_t']) == ['2021-08-17T10:00:00+01:00', '2021-08-17 10:00:00']


# --- warm regression (needs the built corpus) --------------------------------

WARM = {
    'Mali': (24939, {'2014-15': 3804, '2017-18': 8390,
                     '2018-19': 6602, '2021-22': 6143}),
    'Tanzania': (22427, {'2008-09': 3263, '2010-11': 3923, '2012-13': 5010,
                         '2014-15': 4338, '2019-20': 1184, '2020-21': 4709}),
}


@pytest.mark.parametrize('country', sorted(WARM))
def test_warm_interview_date_keeps_every_wave(country):
    import lsms_library as ll
    from lsms_library.paths import data_root

    if not (data_root() / country / 'var' / 'interview_date.parquet').exists():
        pytest.skip(f'{country} interview_date not cached here')
    rows, per_wave = WARM[country]
    c = ll.Country(country)
    df, warned = _record(c.interview_date)
    assert len(df) == rows
    assert df.index.get_level_values('t').value_counts().to_dict() == per_wave
    assert df['Int_t'].notna().all()
    assert warned == []

    single = ll.Country(country, preload_panel_ids=False)
    parts = [single.interview_date(waves=[w]) for w in c.waves]
    cat = pd.concat([p for p in parts if len(p)])
    assert df.sort_index().equals(cat.sort_index())

"""GH #1038: Ethiopia ``interview_date`` carried the placeholder ``'##N/A##'``.

``'##N/A##'`` is the Survey Solutions export marker for an unrecorded
timestamp.  It sat in the source ``.dta`` (8 of 6,770 ``InterviewStart`` in
2018-19's household cover; 112 of 1,596 ``InterviewDate`` -- 107 households --
in 2021-22's post-harvest cover), was cached as a string, and was rejected by
the datetime cast on every read with a ``DatetimeCoercionWarning`` naming 115
values (GH #798).  Each wave's ``data_info.yml`` now maps it to null at the
point the column is read.

The households are real: every one of the 8 / 107 is present in ``sample``,
``household_roster``, ``food_acquired`` (104 of the 107 in 2021-22; the other
three are absent from the household cover itself), so only the timestamp is
missing.  Mapping it to null also lets the grain collapse of 2021-22's
holder-grain cover pick a *dated* holder row over a placeholder one: household
``060105088801701096`` was served nothing before and is now dated
2022-04-18 14:27:53.

GH #1056 moved both waves onto the GH #1045 build-time parser: the
placeholder is now a declared ``sentinels:`` entry (still served NaT, now
counted), and a cited fieldwork ``window:`` COUNTS -- and keeps -- the
dates years outside fieldwork (W4 15 rows, W5 18 holder rows; 7 of the W5
rows may be Ethiopian-calendar dates, see CONTENTS.org),
which used to be served with nothing anywhere noticing them.  Served values
are unchanged.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.country import DatetimeCoercionWarning, _coerce_datetime_column
from lsms_library.datetime_parse import (DatetimeParseWarning,
                                         apply_datetime_specs, datetime_reports)
from lsms_library.local_tools import df_data_grabber

PLACEHOLDER = '##N/A##'
WAVES = {'2018-19': ('sect_cover_hh_w4.dta', 'InterviewStart'),
         '2021-22': ('sect_cover_ph_w5.dta', 'InterviewDate')}
# GH #1056: the DDI's fieldwork months for the event each wave dates (W4
# household questionnaire Jun-Aug 2019; W5 post-harvest Apr-Jun 2022), widened
# by one month each side.  Pinned: a change to the window changes the count.
WINDOWS = {'2018-19': ('2019-05-01', '2019-09-30'),
           '2021-22': ('2022-03-01', '2022-07-31')}
IN_WINDOW = {'2018-19': '2019-06-19T09:15:28', '2021-22': '2022-04-18T14:27:53'}
# Measured on this branch (base 3a33f2869; cold private root), per wave
# build-time report -- SOURCE-row grain (W5: holder rows of the PH cover):
# (n, parsed, sentinel, outside_window).
REPORTS = {'2018-19': (6770, 6762, 8, 15), '2021-22': (1596, 1484, 112, 18)}
# Served household rows outside WINDOWS, after the (t, i) collapse.  A
# different quantity from REPORTS' outside_window: W5's 18 of 1,475 served
# households equals the 18 of 1,596 holder rows only because no out-of-window
# holder row is lost in the collapse.
SERVED_OUTSIDE = {'2018-19': 15, '2021-22': 18}


def _record(fn, *args, **kwargs):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        out = fn(*args, **kwargs)
    return out, [x.message for x in w
                 if isinstance(x.message, DatetimeCoercionWarning)]


@pytest.fixture(scope='module')
def ethiopia():
    import lsms_library as ll
    return ll.Country('Ethiopia', preload_panel_ids=False)


# --- config tier: no data needed ---------------------------------------------

@pytest.mark.parametrize('wave', sorted(WAVES))
def test_wave_config_declares_the_placeholder_a_sentinel(ethiopia, wave):
    fn, col = WAVES[wave]
    info = ethiopia[wave].resources['interview_date']
    assert info['file'] == fn
    spec = info['myvars']['int_t']
    assert spec['datetime'] == col
    assert spec['calendar'] == 'gregorian'
    assert spec['format'] == '%Y-%m-%dT%H:%M:%S'
    assert spec['sentinels'] == [PLACEHOLDER]
    # GH #1056: a fieldwork window, so dates years outside it are counted.
    assert [pd.Timestamp(x) for x in spec['window']] == \
        [pd.Timestamp(x) for x in WINDOWS[wave]]
    # No resolution: the served value keeps its time of day, as before.
    assert 'resolution' not in spec
    mapping = ethiopia[wave].column_mapping('interview_date', info)
    assert mapping['datetime_specs'] == {'int_t': spec}


@pytest.mark.parametrize('wave', sorted(WAVES))
def test_wired_spec_parses_without_a_placeholder_and_counts_the_window(
        ethiopia, wave, tmp_path):
    """Run the wave's own resolved mapping through ``df_data_grabber`` and the
    build-time parser on a synthetic cover file: the placeholder is NaT (a
    counted sentinel), an in-window date parses, and a 2014 date
    is counted ``outside_window`` and KEPT (GH #1056)."""
    fn, col = WAVES[wave]
    info = ethiopia[wave].resources['interview_date']
    mapping = ethiopia[wave].column_mapping('interview_date', info)
    src = tmp_path / 'cover.csv'
    pd.DataFrame({'household_id': ['a', 'b', 'c'],
                  col: [IN_WINDOW[wave], PLACEHOLDER,
                        '2014-01-02T10:04:03']}).to_csv(src, index=False)
    fm = mapping[fn]
    df = df_data_grabber(src, fm['idxvars'], **fm['myvars'])
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        df = apply_datetime_specs(df, mapping['datetime_specs'],
                                  country='Ethiopia-test-1056',
                                  table='interview_date', wave=wave)
    assert str(df['int_t'].dtype) == 'datetime64[us]'
    assert df.loc['a', 'int_t'] == pd.Timestamp(IN_WINDOW[wave])
    assert pd.isna(df.loc['b', 'int_t'])
    assert df.loc['c', 'int_t'] == pd.Timestamp('2014-01-02 10:04:03')
    (rep,) = [r for r in datetime_reports('Ethiopia-test-1056', 'interview_date')
              if r['wave'] == wave]
    assert (rep['parsed'], rep['sentinel'], rep['outside_window'],
            rep['unparsed']) == (2, 1, 1, 0)
    assert any(isinstance(x.message, DatetimeParseWarning) for x in w)
    # The read-time cast has nothing left to coerce.
    out, warned = _record(_coerce_datetime_column, df['int_t'],
                          country='Ethiopia', table='interview_date',
                          column='Int_t')
    assert warned == []
    assert out.notna().tolist() == [True, False, True]


# --- warm tier: needs the built corpus ---------------------------------------

def test_warm_interview_date_has_no_placeholder(ethiopia):
    from lsms_library.paths import data_root

    if not (data_root() / 'Ethiopia' / 'var' / 'interview_date.parquet').exists():
        pytest.skip('Ethiopia interview_date not cached here')
    df, warned = _record(ethiopia.interview_date)
    assert warned == []
    assert df['Int_t'].notna().all()
    assert df.index.get_level_values('t').value_counts().to_dict() == \
        {'2018-19': 6762, '2021-22': 1475}
    recovered = df.xs(('060105088801701096', '2021-22'), level=['i', 't'])
    assert list(recovered['Int_t']) == [pd.Timestamp('2022-04-18 14:27:53')]

    # GH #1056: the out-of-window dates are served as recorded AND counted --
    # by the build-time report (replayed from the parquet stamp when warm).
    t = df.index.get_level_values('t')
    for wave, (lo, hi) in WINDOWS.items():
        x = df.loc[t == wave, 'Int_t']
        served_out = int(((x < pd.Timestamp(lo)) |
                          (x >= pd.Timestamp(hi) + pd.Timedelta(days=1))).sum())
        assert served_out == SERVED_OUTSIDE[wave], (wave, served_out)
        assert int((x.dt.year == 2014).sum()) >= 13
        reps = [r for r in datetime_reports('Ethiopia', 'interview_date')
                if r['wave'] == wave]
        assert reps, wave
        rep = reps[-1]
        assert (rep['n'], rep['parsed'], rep['sentinel'],
                rep['outside_window']) == REPORTS[wave], (wave, rep)

    # The cache tiers hold pre-cast data: the string must not be there either.
    paths = [data_root() / 'Ethiopia' / 'var' / 'interview_date.parquet']
    paths += [data_root() / 'Ethiopia' / w / '_' / 'interview_date.parquet'
              for w in WAVES]
    for p in [p for p in paths if p.exists()]:
        cached = pd.read_parquet(p)
        assert not (cached.astype(object) == PLACEHOLDER).any().any(), p

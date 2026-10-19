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
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.country import DatetimeCoercionWarning, _coerce_datetime_column
from lsms_library.local_tools import df_data_grabber

PLACEHOLDER = '##N/A##'
WAVES = {'2018-19': ('sect_cover_hh_w4.dta', 'InterviewStart'),
         '2021-22': ('sect_cover_ph_w5.dta', 'InterviewDate')}


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
def test_wave_config_maps_the_placeholder_to_null(ethiopia, wave):
    fn, col = WAVES[wave]
    info = ethiopia[wave].resources['interview_date']
    assert info['file'] == fn
    mapping = ethiopia[wave].column_mapping('interview_date', info)[fn]
    assert mapping['myvars']['int_t'] == ([col], {PLACEHOLDER: None})


@pytest.mark.parametrize('wave', sorted(WAVES))
def test_wired_mapping_reaches_the_cast_without_a_placeholder(ethiopia, wave,
                                                             tmp_path):
    """Run the wave's own resolved mapping through ``df_data_grabber`` on a
    synthetic cover file, then through the declared-datetime cast."""
    fn, col = WAVES[wave]
    info = ethiopia[wave].resources['interview_date']
    mapping = ethiopia[wave].column_mapping('interview_date', info)[fn]
    src = tmp_path / 'cover.csv'
    pd.DataFrame({'household_id': ['a', 'b', 'c'],
                  col: ['2019-06-19T09:15:28', PLACEHOLDER,
                        '2019-06-18T15:20:10']}).to_csv(src, index=False)
    df = df_data_grabber(src, mapping['idxvars'], **mapping['myvars'])
    assert pd.isna(df.loc['b', 'int_t'])
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

    # The cache tiers hold pre-cast data: the string must not be there either.
    paths = [data_root() / 'Ethiopia' / 'var' / 'interview_date.parquet']
    paths += [data_root() / 'Ethiopia' / w / '_' / 'interview_date.parquet'
              for w in WAVES]
    for p in [p for p in paths if p.exists()]:
        cached = pd.read_parquet(p)
        assert not (cached.astype(object) == PLACEHOLDER).any().any(), p

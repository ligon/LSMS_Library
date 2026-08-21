"""GH #323: India `employment` (SECT02AD) is ACTIVITY-level, not person-level.

One row = one JOB.  A person holds 1-14 activities (`act`, the per-person slot
A..T), each carrying its OWN wage.  Declaring the index as (t, i, pid) made it
non-unique (6,194 duplicate person tuples), and `_normalize_dataframe_index`
collapsed it with a **skipna** ``groupby().first()``.  That is a class-1
(silently WRONG) failure, not merely class-2 (silently MISSING): `.first()`
fills each column INDEPENDENTLY from that column's first non-null value, so it
MANUFACTURES rows present in no source record.

Measured on the raw SECT02AD.DTA (16,089 rows, 3-key exactly unique):
  * wage observations : 5,627 -> 2,136 non-null   (62.0% destroyed)
  * reported cash     : 92,116 -> 55,498          (39.8% silently lost)
  * 107 persons received Cash_per_day and Rural from two DIFFERENT rows.

The guard is the widened index (t, i, pid, act).  These tests pin it.

Note on row counts: the API returns 6,158 rows, not the 16,089 source rows,
because `country.py`'s universal ``dropna(how='all')`` safety-net drops the
9,931 activity rows where BOTH Cash_per_day and Rural are NaN.  That drop is
pre-existing and applies identically before and after the fix; every row
carrying a wage survives it (5,627 of 6,158).
"""
from __future__ import annotations

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.country import _normalize_dataframe_index


# --- Unit test: no real data needed.  Pins the reducer's fabrication directly. --

def test_skipna_first_fabricates_cross_row_values_when_act_is_dropped():
    """The mechanism, in miniature: hhcode=1011 idcode=1 from the real source.

    Row A carries the wage (15.0) but no workplace; rows B..F carry a workplace
    but a 0.0 wage.  Collapsing to (t, i, pid) yields (15.0, 'Rural') -- a pair
    that exists in NO source row.  Declaring `act` must prevent that.
    """
    rows = [  # (act, Cash_per_day, Rural)  -- verbatim from SECT02AD.DTA
        ('A', 15.0, None), ('B', 0.0, 'Rural'), ('C', 0.0, 'Rural'),
        ('D', 0.0, 'Rural'), ('E', 0.0, 'Rural'), ('F', None, 'Rural'),
    ]
    idx = pd.MultiIndex.from_tuples(
        [('1997-98', '1011', '1', a) for a, _, _ in rows],
        names=['t', 'i', 'pid', 'act'],
    )
    df = pd.DataFrame(
        {'Cash_per_day': [c for _, c, _ in rows],
         'Rural': [r for _, _, r in rows]},
        index=idx,
    )

    # PERSON-level index (the bug): collapses 6 jobs into 1 fabricated row.
    with pytest.warns(RuntimeWarning, match='GH #323'):
        bad = _normalize_dataframe_index(df, {'index': '(t, i, pid)'}, None)
    assert len(bad) == 1
    # The fabrication: wage from row A, workplace from row B.
    assert bad['Cash_per_day'].iloc[0] == 15.0
    assert bad['Rural'].iloc[0] == 'Rural'   # <- present in no source row
    assert bad['Cash_per_day'].sum() == 15.0  # 4 further wage rows destroyed

    # ACTIVITY-level index (the fix): all six jobs survive, unfabricated.
    out = _normalize_dataframe_index(df, {'index': '(t, i, pid, act)'}, None)
    assert list(out.index.names) == ['t', 'i', 'pid', 'act']
    assert len(out) == 6
    assert out.index.is_unique
    # Row A keeps its OWN (wage, workplace) pair -- NaN workplace, not 'Rural'.
    a = out.xs('A', level='act').iloc[0]
    assert a['Cash_per_day'] == 15.0
    assert pd.isna(a['Rural'])


# --- Integration tests against the real India table. -------------------------

@pytest.fixture(scope='module')
def emp() -> pd.DataFrame:
    try:
        return ll.Country('India').employment()
    except Exception as e:                                    # pragma: no cover
        pytest.skip(f'India employment unavailable: {e}')


def test_act_level_is_declared(emp):
    """`act` must be part of the canonical index -- the guard against re-collapse."""
    assert 'act' in emp.index.names, (
        'employment lost its activity level; the person-level index silently '
        'collapses 6,194 duplicate tuples (GH #323)')


def test_no_gh323_collapse_warning_on_a_COLD_build(monkeypatch):
    """The warning fires ONLY on a cold build -- so this test MUST force one.

    In warm operation the collapse is already baked into the L2-country parquet
    and `_normalize_dataframe_index` is never re-entered, so a naive version of
    this test passes even on the buggy config: the bug hides behind the cache
    that the bug poisoned.  `LSMS_NO_CACHE=1` bypasses the L2 reads and forces
    the rebuild that actually exercises the collapse.  Without this the guard is
    VACUOUS (verified: it passed against the pre-fix config).
    """
    import warnings
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = ll.Country('India').employment()
    gh323 = [str(w.message) for w in caught if 'GH #323' in str(w.message)]
    assert not gh323, f'employment still collapses on a cold build: {gh323}'
    assert df.index.is_unique
    assert int(df.index.duplicated().sum()) == 0


def test_wage_observations_are_not_destroyed(emp):
    """62% of wage observations were being discarded by groupby().first()."""
    assert int(emp['Cash_per_day'].notna().sum()) == 5627   # was 2136
    assert float(emp['Cash_per_day'].sum()) == 92116.0      # was 55498.0


def test_row_count_recovers(emp):
    """6,158 = 16,089 source rows - 9,931 all-NaN rows dropped by the
    pre-existing dropna(how='all') safety-net in country.py."""
    assert len(emp) == 6158                                 # was 2593


def test_frankenstein_person_is_not_fabricated(emp):
    """VALIDATE WHERE VALIDATION IS NEEDED: the ambiguous rows, not the free ones.

    hhcode=1011 idcode=1 is one of the 107 persons whose collapsed row took
    Cash_per_day and Rural from DIFFERENT activity rows.  Post-fix it must
    expose its six real jobs, with A = (15.0, NaN) -- NOT (15.0, 'Rural').
    """
    i = emp.index.get_level_values('i').astype(str)
    p = emp.index.get_level_values('pid').astype(str)
    person = emp[(i == '1011') & (p == '1')]

    acts = list(person.index.get_level_values('act'))
    assert acts == ['A', 'B', 'C', 'D', 'E', 'F'], acts

    a = person.xs('A', level='act').iloc[0]
    assert a['Cash_per_day'] == 15.0
    assert pd.isna(a['Rural']), (
        "row A's workplace is NaN in the source; a non-null value here means "
        'the skipna groupby().first() fabricated it from another activity row')

    # ...and the wage genuinely belongs to A alone.
    assert float(person['Cash_per_day'].sum()) == 15.0


def test_every_person_activity_pair_is_distinct(emp):
    """`act` is a per-person slot: (i, pid, act) is exactly unique."""
    f = emp.index.to_frame()
    assert not f[['t', 'i', 'pid', 'act']].duplicated().any()

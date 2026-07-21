"""GH #323 -- the grain contract, tested as BEHAVIOUR rather than as names.

``tests/test_gh323_explicit_reducers.py`` guards the *wiring*: core must not
import or call the country-facing reducers.  A static scan can prove that and
nothing more -- a locally-defined helper with any name at all could reduce grain
silently and pass it.  (That instrument's limits are not hypothetical: it was a
substring grep, and it turned ``development`` red for a week by flagging
``country._collapse_to_cluster_grain``, a legitimate private helper that merely
shared a suffix with a banned name.)

This file guards the *behaviour*, and therefore states the contract as
properties of what core returns.  The contract is NOT "core never reduces rows"
-- Site 2's household->cluster projection legitimately reduces.  It is:

    P1  CONSERVATION    every row that leaves is accounted for.
    P2  NO INVENTION    every cell holds an OBSERVED value, or NA.
    P3  ASYMMETRY       destructive collapse is loud; lossless collapse is silent.
    P4  ACCURACY        the number it reports is the number it destroyed.

P2 is deliberately weaker than the "output rows are a subset of input rows" form
this file was first written with.  That stronger version is WRONG here, and the
mistake is worth recording because it is an attractive one: ``groupby().first()``
skips NA per column, so complementary rows yield a combination that appears
nowhere in the source -- which LOOKS like fabrication and is in fact the
intended COMPLETION.  ``NaN`` is absence, not contradiction; ``reduce_to_agreed``
returns the very same composite on purpose.  Chasing the stronger property leads
straight to ``.first(skipna=False)``, which returns ``<NA>`` for values the
survey actually recorded -- a regression dressed as a fix.

The composite is wrong only when the rows describe DIFFERENT REAL ENTITIES, i.e.
when the key is unique only within some coarser unit and two real clusters have
been merged.  That is a broken IDENTIFIER, and D1 says fix the identifier.  It
is not something a reducer can detect, which is why no property here tries to.

P3's silent half matters as much as its loud half.  Of ~7.5M rows sitting on a
duplicated declared index corpus-wide, 6.46M are exact duplicates of a surviving
row -- collapsing those loses nothing.  Warning on the raw duplicate count would
bury the ~542k real losses under 6.5M false alarms, and a warning nobody reads is
how this bug survived to begin with.

Design note: ``slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org``.
"""
import os
import warnings

import pandas as pd
import pytest

from lsms_library.country import (
    GrainCollapseError,
    GrainCollapseWarning,
    _normalize_dataframe_index,
)

SCHEMA = {'index': '(t, i)'}


def _frame(rows):
    """Build a (t, i)-indexed frame from ``(t, i, A, B)`` tuples."""
    idx = pd.MultiIndex.from_tuples([(t, i) for t, i, *_ in rows], names=['t', 'i'])
    return pd.DataFrame({'A': [r[2] for r in rows],
                         'B': [r[3] for r in rows]}, index=idx)


def _collapse(df, table_name='household_roster'):
    """Run the real core path, returning ``(out, warnings)``."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = _normalize_dataframe_index(
            df, SCHEMA, wave='2018', table_name=table_name, country='Testland')
    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    return out, grain


def _rowset(df):
    """Every row as a comparable tuple, index included."""
    return set(map(tuple, df.reset_index().astype(str).itertuples(index=False)))


def _oracle_destroyed(df, levels=('t', 'i')):
    """Independently computed truth: rows dropped from DISAGREEING groups.

    Deliberately not a reimplementation of core's counter -- it is the
    definition from the design note, so that P4 cannot pass by agreeing with a
    bug in the thing it is checking.
    """
    flat = df.reset_index().astype(str)
    size = flat.groupby(list(levels), dropna=False).size()
    distinct = flat.drop_duplicates().groupby(list(levels), dropna=False).size()
    conflicting = distinct.index[distinct > 1]
    return int((size.loc[conflicting] - 1).sum()) if len(conflicting) else 0


# ---------------------------------------------------------------------------
# P1 -- conservation
# ---------------------------------------------------------------------------

def test_p1_output_is_exactly_the_distinct_declared_keys():
    """No reduction beyond collapsing the declared index may occur.

    This is general in a way a name-check cannot be: it does not care WHERE a
    row was dropped.  Any future site that quietly drops an extra row breaks the
    equality, whatever that site is called.
    """
    df = _frame([('2018', 'h1', 'a', 'b'),
                 ('2018', 'h1', 'a', 'b'),
                 ('2018', 'h2', 'c', 'd'),
                 ('2019', 'h1', 'e', 'f')])
    out, _ = _collapse(df)
    expected = len(df.index.unique())
    assert len(out) == expected, (
        f'core returned {len(out)} rows for {expected} distinct declared keys')


def test_p1_a_unique_index_is_not_touched_at_all():
    df = _frame([('2018', 'h1', 'a', 'b'), ('2018', 'h2', 'c', 'd')])
    out, grain = _collapse(df)
    assert len(out) == len(df)
    assert _rowset(out) == _rowset(df)
    assert not grain, 'a unique index must not produce a grain report'


# ---------------------------------------------------------------------------
# P2 -- no fabrication
# ---------------------------------------------------------------------------

def test_p2_lossless_collapse_returns_a_real_row():
    """Exact duplicates: the survivor must be the row that was there."""
    df = _frame([('2018', 'h1', 'a', 'b'), ('2018', 'h1', 'a', 'b')])
    out, _ = _collapse(df)
    assert _rowset(out) <= _rowset(df)


def test_p2_every_output_cell_is_an_observed_value_or_na():
    """No cell may hold a value that was never observed in its group.

    This is the correct form of "no fabrication".  An earlier draft of this file
    asserted the stronger `output rows subset of input rows`, and that was WRONG
    -- see the completion test below for why.  This weaker property is the one
    that actually encodes the contract, and it is still not free: it fails for
    any reducer that computes a new value (a mean, a midpoint) rather than
    selecting an observed one.
    """
    df = _frame([('2018', 'h1', 'a1', 'b1'),
                 ('2018', 'h1', 'a2', 'b2')])
    out, _ = _collapse(df)
    for col in out.columns:
        observed = set(df[col].dropna().astype(str))
        for val in out[col]:
            assert pd.isna(val) or str(val) in observed, (
                f'{col}={val!r} was never observed in the input')


def test_p2_complementary_missingness_is_COMPLETION_not_fabrication():
    """(a1, <NA>) + (<NA>, b2) -> (a1, b2) is CORRECT, and must stay that way.

    This pins a doctrine that is easy to "fix" into a regression.  ``NaN`` is
    ABSENCE, not contradiction: if one row reports Region and the other reports
    Rural, the cluster has both, and keeping both discards no observed value.
    The repo's own reducer implements exactly this -- see
    ``test_nan_is_absence_not_contradiction`` in
    ``tests/test_gh323_explicit_reducers.py`` -- and ``reduce_to_agreed``
    returns the SAME composite that ``groupby().first()`` does here.

    So the composite is not the bug, and the tempting one-word "fix"
    (``.first(skipna=False)``) is a REGRESSION: it would return ``<NA>`` for a
    value the survey actually recorded.  Verified on pandas 3.0.2.

    Where a composite IS wrong is when the rows describe DIFFERENT REAL
    ENTITIES -- two clusters merged by a key that is unique only within a
    district.  That is a broken identifier, and the fix is the identifier
    (GH #323 D1), not the reducer.
    """
    df = _frame([('2018', 'h1', 'a1', pd.NA),
                 ('2018', 'h1', pd.NA, 'b2')])
    out, _ = _collapse(df)
    assert len(out) == 1
    assert out['A'].iloc[0] == 'a1'
    assert out['B'].iloc[0] == 'b2', (
        'core dropped an observed value instead of completing from it -- has '
        'someone set skipna=False?')


# ---------------------------------------------------------------------------
# P3 -- the loud/silent asymmetry
# ---------------------------------------------------------------------------

def test_p3_a_destructive_collapse_is_loud():
    df = _frame([('2018', 'h1', 'a', 'b'),
                 ('2018', 'h1', 'DIFFERENT', 'b')])
    _, grain = _collapse(df)
    assert grain, 'rows that disagree were collapsed with no warning'


def test_p3_a_lossless_collapse_is_silent():
    """The half that is usually forgotten.

    6.46M of the corpus's 7.5M duplicate rows are exact duplicates. Warning on
    those would bury the ~542k real losses, and a warning nobody reads is how
    #323 survived being closed once already.
    """
    df = _frame([('2018', 'h1', 'a', 'b')] * 3)
    _, grain = _collapse(df)
    assert not grain, (
        'a provably lossless collapse warned; that noise is what buries the '
        'real signal')


def test_p3_strict_mode_escalates_destruction_to_an_exception(monkeypatch):
    monkeypatch.setenv('LSMS_GRAIN_STRICT', '1')
    df = _frame([('2018', 'h1', 'a', 'b'),
                 ('2018', 'h1', 'DIFFERENT', 'b')])
    with pytest.raises(GrainCollapseError):
        _normalize_dataframe_index(df, SCHEMA, wave='2018',
                                   table_name='household_roster',
                                   country='Testland')


def test_p3_strict_mode_stays_quiet_when_nothing_is_destroyed(monkeypatch):
    """Strict mode must not become 'raise on any duplicate' -- that is the
    known-bad allowlist failure mode in a different costume."""
    monkeypatch.setenv('LSMS_GRAIN_STRICT', '1')
    df = _frame([('2018', 'h1', 'a', 'b')] * 2)
    out = _normalize_dataframe_index(df, SCHEMA, wave='2018',
                                     table_name='household_roster',
                                     country='Testland')
    assert len(out) == 1


# ---------------------------------------------------------------------------
# P4 -- the reported number is the true number
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('rows,expected', [
    # one disagreeing pair -> 1 destroyed
    ([('2018', 'h1', 'a', 'b'), ('2018', 'h1', 'X', 'b')], 1),
    # three rows, all disagree -> 2 destroyed
    ([('2018', 'h1', 'a', 'b'), ('2018', 'h1', 'X', 'b'),
      ('2018', 'h1', 'Y', 'b')], 2),
    # exact duplicates -> 0 destroyed (lossless)
    ([('2018', 'h1', 'a', 'b'), ('2018', 'h1', 'a', 'b')], 0),
    # two identical rows INSIDE a disagreeing group still count: two identical
    # roster rows are two distinct PEOPLE, and household size is wrong if you
    # drop one.
    ([('2018', 'h1', 'a', 'b'), ('2018', 'h1', 'a', 'b'),
      ('2018', 'h1', 'X', 'b')], 2),
])
def test_p4_reported_destroyed_matches_an_independent_oracle(rows, expected):
    df = _frame(rows)
    assert _oracle_destroyed(df) == expected, 'the oracle itself is wrong'

    _, grain = _collapse(df)
    if expected == 0:
        assert not grain
        return
    assert grain, 'destruction went unreported'
    msg = str(grain[0].message)
    assert f'DESTROYED {expected} ' in msg, (
        f'report disagrees with the oracle ({expected} destroyed): {msg}')


def test_p4_a_vacuous_report_would_not_satisfy_this_file():
    """Guard the guard: P1/P4 pass trivially if core stops reporting at all,
    so pin that a known-destructive frame really does produce a report naming
    the cell.
    """
    df = _frame([('2018', 'h1', 'a', 'b'), ('2018', 'h1', 'X', 'b')])
    _, grain = _collapse(df)
    assert grain
    msg = str(grain[0].message)
    assert 'Testland' in msg and 'household_roster' in msg, (
        f'the report does not identify the cell it is about: {msg}')


# ---------------------------------------------------------------------------
# The sanctioned exception: additive tables
# ---------------------------------------------------------------------------

def test_additive_tables_conserve_the_total_instead_of_selecting():
    """``_ADDITIVE_MEASURE_COLUMNS`` (food_acquired) SUMS rather than selects.

    P2 does not apply there -- a summed row is legitimately new -- so the
    property that replaces it is conservation of the total. Without this, the
    additive path would be the one place in core with no stated contract.
    """
    idx = pd.MultiIndex.from_tuples([('2018', 'h1'), ('2018', 'h1')],
                                    names=['t', 'i'])
    df = pd.DataFrame({'Quantity': [2.0, 3.0], 'Expenditure': [10.0, 15.0]},
                      index=idx)
    out = _normalize_dataframe_index(df, SCHEMA, wave='2018',
                                     table_name='food_acquired',
                                     country='Testland')
    assert len(out) == 1
    assert out['Quantity'].sum() == pytest.approx(5.0)
    assert out['Expenditure'].sum() == pytest.approx(25.0)

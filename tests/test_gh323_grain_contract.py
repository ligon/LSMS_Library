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
    P2  NO INVENTION    the served row is a SUBSET OF AN OBSERVED ROW on every
                        column core does not reduce -- and every cell holds an
                        observed value, or NA.
    P3  ASYMMETRY       destructive collapse is loud; lossless collapse is silent.
    P4  ACCURACY        the number it reports is the number it destroyed.

**P2 was deliberately WEAKER than this until 2026-09-12, and the reversal is the
part worth reading.**  The weaker form ("every cell holds an observed value or
NA") was chosen on 2026-07-13 because ``groupby().first()`` skips NA per column,
so complementary rows yielded a combination appearing nowhere in the source --
argued then to be not fabrication but intended COMPLETION: ``NaN`` is absence,
not contradiction, and ``reduce_to_agreed`` returns the very same composite on
purpose.

That doctrine was formulated against the WRONG ALTERNATIVE.  The alternative on
the table was ``first(skipna=False)``, which returns ``<NA>`` even for a group
that AGREES and where only one row reports -- a regression dressed as a fix, and
rightly rejected.  Row-coherent SELECTION is not that: it returns a whole
OBSERVED row (``country.most_complete_row`` -- the group's most complete row,
ties on original order).  @ligon reversed the doctrine on that basis, and two
independent reasons make it coherent rather than merely requested:

(a) **Core already calls these groups destroyed.**  ``_audit_index_collapse``'s
    own docstring: "Missing values count as values: two rows that differ only in
    whether a field is recorded are different rows."  Core used to REPORT the
    group as destructive and then SERVE the completion -- the instrument and the
    reducer contradicting each other.  Selection makes them agree.

(b) **Completion has a legitimate home, and it is not core.**
    ``build_transforms.reduce_to_agreed`` (``na_is_conflict=False``) is the
    country-facing helper a maintainer invokes IN WRITING at the call site,
    exactly as Benin/Togo do.  Per "NO AGGREGATION IN CORE", completing across
    rows is the author's call, not the access path's.

So completion stays in ``reduce_to_agreed``, and core selects.  The stronger P2
-- served row is a subset of an observed row on every reducer-free column -- was
right for the selection branch after all.  It is still not free: it fails for any
reducer that computes a new value (a mean, a midpoint) OR assembles one
per-column.  GH #871.

A selection is still wrong when the rows describe DIFFERENT REAL ENTITIES, i.e.
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

    The weak half of "no fabrication": necessary, and not free (it fails for any
    reducer that computes a new value -- a mean, a midpoint -- rather than
    selecting an observed one), but NOT sufficient.  A per-column composite
    passes it.  The strong half is the next test.
    """
    df = _frame([('2018', 'h1', 'a1', 'b1'),
                 ('2018', 'h1', 'a2', 'b2')])
    out, _ = _collapse(df)
    for col in out.columns:
        observed = set(df[col].dropna().astype(str))
        for val in out[col]:
            assert pd.isna(val) or str(val) in observed, (
                f'{col}={val!r} was never observed in the input')


def test_p2_complementary_missingness_is_NOT_completed_by_core():
    """(a1, <NA>) + (<NA>, b2) -> (a1, <NA>).  The 2026-09-12 reversal, pinned.

    Core SELECTS an observed row; it does not assemble one.  Both rows here are
    equally complete (one non-NA cell each), so the tie breaks on original order
    and row 0 is served -- ``B`` stays NA, which is what row 0 said.
    ``groupby().first()`` used to serve ``(a1, b2)``: a record neither row
    reported.

    Completion across rows is still available, and still correct where a
    maintainer signs for it: ``build_transforms.reduce_to_agreed``
    (``na_is_conflict=False``), invoked in writing at the call site, as
    Benin/Togo do (``tests/test_gh323_explicit_reducers.py``
    ``test_nan_is_absence_not_contradiction``).  What changed is that the ACCESS
    PATH no longer does it behind the caller.

    Note what this is NOT: ``.first(skipna=False)``, which would return ``<NA>``
    even for a group that agrees and where only one row reports.  Core returns a
    whole observed row.  GH #871.
    """
    df = _frame([('2018', 'h1', 'a1', pd.NA),
                 ('2018', 'h1', pd.NA, 'b2')])
    out, _ = _collapse(df)
    assert len(out) == 1
    assert out['A'].iloc[0] == 'a1'
    assert pd.isna(out['B'].iloc[0]), (
        'core completed a row from two different rows -- is the per-column '
        'first() composite back? (GH #871)')


def test_p2_the_served_row_is_a_subset_of_an_observed_row():
    """The STRONG half of no-fabrication: row coherence, not just cell coherence.

    Every column core does not reduce must come from ONE row of the group.  A
    per-column composite satisfies "every cell was observed" and fails this.
    """
    df = _frame([('2018', 'h1', 'a1', pd.NA),
                 ('2018', 'h1', pd.NA, 'b2'),
                 ('2018', 'h2', 'a3', 'b3')])
    out, _ = _collapse(df)

    def _norm(row):
        return tuple(None if pd.isna(v) else v for v in row)
    inputs = {_norm(row) for row in df.itertuples(index=False)}
    for _key, row in out.iterrows():
        assert _norm(row[c] for c in out.columns) in inputs, (
            f'served {tuple(row)!r} is no observed row of the group')


def test_p2_selection_prefers_the_MOST_COMPLETE_observed_row():
    """Row coherence alone would permit serving the emptiest row.  It does not.

    This is the difference between the plan's ``nth(0)`` and what landed: given a
    blank row and a populated one on the same broken key -- Niger 2014-15
    ``household_roster`` ``('2014-15', '101008', '1')``, whose ``pid`` is a
    household id stamped on every member -- the served row is the POPULATED one,
    and it is an observed row rather than a composite.  GH #871.
    """
    df = _frame([('2018', 'h1', pd.NA, pd.NA),
                 ('2018', 'h1', 'a2', 'b2')])
    out, _ = _collapse(df)
    assert len(out) == 1
    assert (out['A'].iloc[0], out['B'].iloc[0]) == ('a2', 'b2'), (
        'core served the blank row over a strictly more complete one')


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

"""Declared index-collapse policy: `aggregation: unique` (GH #323).

`_normalize_dataframe_index` used to collapse ANY non-unique declared index
with a silent ``groupby().first()``.  Where the source is legitimately finer
than the declared index -- e.g. ``cluster_features`` (index ``(t, v)``) read
off a household-level cover page -- that collapse is CORRECT but was
UNDECLARED, and it silently resolved genuine within-group conflicts by
whichever row happened to sort first.

`aggregation: unique` declares the projection AND enforces the constancy
invariant it depends on: a column that is not constant within the declared
group is not determinable from the source, so the cell becomes ``pd.NA`` with
a loud warning (class-2, missing) rather than a first-row guess (class-1,
wrong).

Iraq is the reference case.  2006-07 is exactly lossless (0/2961 conflicts).
2012 has exactly ONE conflicted cluster -- 964 (ERBIL), 7 URBAN / 2 RURAL --
which the old ``.first()`` silently called RURAL, the 2-of-9 MINORITY.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import Country, _normalize_dataframe_index


def _frame(rows):
    return pd.DataFrame(rows).set_index(['t', 'v'])


class TestUniqueReducer:
    """Unit-level contract for the scalar `aggregation: unique` policy."""

    def test_constant_group_collapses_losslessly(self):
        """A column constant within the group survives the projection."""
        df = _frame([
            {'t': '2012', 'v': '1', 'Region': 'ERBIL', 'Rural': 'Urban'},
            {'t': '2012', 'v': '1', 'Region': 'ERBIL', 'Rural': 'Urban'},
            {'t': '2012', 'v': '2', 'Region': 'DUHOK', 'Rural': 'Rural'},
        ])
        entry = {'index': '(t, v)', 'aggregation': 'unique',
                 'Region': 'str', 'Rural': 'str'}
        with warnings.catch_warnings():
            warnings.simplefilter('error')  # a lossless collapse must be SILENT
            out = _normalize_dataframe_index(df, entry, None, 'cluster_features')

        assert len(out) == 2
        assert out.loc[('2012', '1'), 'Region'] == 'ERBIL'
        assert out.loc[('2012', '1'), 'Rural'] == 'Urban'

    def test_conflict_becomes_NA_and_warns(self):
        """A non-constant column -> pd.NA + a loud, actionable warning.

        This is the core of the fix: source order is not evidence.  Pre-fix,
        groupby().first() returned 'Rural' here (the first row) with no signal.
        """
        df = _frame([
            {'t': '2012', 'v': '964', 'Region': 'ERBIL', 'Rural': 'Rural'},
            {'t': '2012', 'v': '964', 'Region': 'ERBIL', 'Rural': 'Urban'},
            {'t': '2012', 'v': '964', 'Region': 'ERBIL', 'Rural': 'Urban'},
        ])
        entry = {'index': '(t, v)', 'aggregation': 'unique',
                 'Region': 'str', 'Rural': 'str'}
        with pytest.warns(RuntimeWarning, match=r"not constant within"):
            out = _normalize_dataframe_index(df, entry, None, 'cluster_features')

        assert len(out) == 1
        # The ambiguous cell is MISSING, not guessed -- and specifically it is
        # NOT the first row's value.
        assert pd.isna(out.loc[('2012', '964'), 'Rural'])
        # ...while the UNAMBIGUOUS column in the same group is preserved: the
        # check is per-COLUMN, not per-row.
        assert out.loc[('2012', '964'), 'Region'] == 'ERBIL'

    def test_conflict_warning_names_key_and_values(self):
        """The warning must be actionable: which key, which competing values."""
        df = _frame([
            {'t': '2012', 'v': '964', 'Rural': 'Rural'},
            {'t': '2012', 'v': '964', 'Rural': 'Urban'},
        ])
        entry = {'index': '(t, v)', 'aggregation': 'unique', 'Rural': 'str'}
        with pytest.warns(RuntimeWarning) as rec:
            _normalize_dataframe_index(df, entry, None, 'cluster_features')
        msg = str(rec[0].message)
        assert '964' in msg and 'Rural' in msg and 'Urban' in msg

    def test_NA_does_not_contradict_an_observed_value(self):
        """{'Urban', <NA>} is not a conflict -- a missing value is not evidence."""
        df = _frame([
            {'t': '2012', 'v': '5', 'Rural': 'Urban'},
            {'t': '2012', 'v': '5', 'Rural': pd.NA},
        ])
        entry = {'index': '(t, v)', 'aggregation': 'unique', 'Rural': 'str'}
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            out = _normalize_dataframe_index(df, entry, None, 'cluster_features')
        assert out.loc[('2012', '5'), 'Rural'] == 'Urban'

    def test_undeclared_collapse_still_warns(self):
        """No `aggregation:` -> the historical .first() + GH #323 warning.

        Regression guard: the new branch must not silence the undeclared case,
        which is still a real bug in the ~13 other affected countries.
        """
        df = _frame([
            {'t': '2012', 'v': '1', 'Rural': 'Urban'},
            {'t': '2012', 'v': '1', 'Rural': 'Rural'},
        ])
        entry = {'index': '(t, v)', 'Rural': 'str'}  # no aggregation key
        with pytest.warns(RuntimeWarning, match=r"GH #323"):
            out = _normalize_dataframe_index(df, entry, None, 'cluster_features')
        assert len(out) == 1

    def test_unknown_reducer_raises(self):
        """An unrecognised scalar reducer must fail loudly, not fall through."""
        df = _frame([
            {'t': '2012', 'v': '1', 'Rural': 'Urban'},
            {'t': '2012', 'v': '1', 'Rural': 'Rural'},
        ])
        entry = {'index': '(t, v)', 'aggregation': 'bogus', 'Rural': 'str'}
        with pytest.raises(ValueError, match=r"unknown aggregation reducer"):
            _normalize_dataframe_index(df, entry, None, 'cluster_features')

    def test_mapping_form_is_not_a_collapse_reducer(self):
        """`aggregation: {visit: first}` (the grain policy) must NOT be treated
        as a duplicate-collapse reducer -- 9 countries declare it on
        interview_date and must keep the historical path."""
        df = _frame([
            {'t': '2012', 'v': '1', 'Rural': 'Urban'},
            {'t': '2012', 'v': '1', 'Rural': 'Rural'},
        ])
        entry = {'index': '(t, v)', 'aggregation': {'visit': 'first'},
                 'Rural': 'str'}
        with pytest.warns(RuntimeWarning, match=r"GH #323"):  # old path
            out = _normalize_dataframe_index(df, entry, None, 'interview_date')
        assert out.loc[('2012', '1'), 'Rural'] == 'Urban'  # .first(), unchanged


class TestIraqClusterFeatures:
    """End-to-end: the real Iraq tables."""

    @pytest.fixture(scope='class')
    def cf(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return Country('Iraq').cluster_features()

    def test_row_counts_are_the_cluster_counts(self, cf):
        """The projection is household -> cluster; no rows are 'recovered'."""
        n = cf.groupby(level='t', observed=True).size()
        assert n['2006-07'] == 2961
        assert n['2012'] == 2828
        assert cf.index.is_unique

    def test_cluster_964_urbanicity_is_not_silently_wrong(self, cf):
        """THE fix.  Pre-fix this cell was 'Rural' -- the 2-of-9 minority,
        chosen purely by source order.  The 2012 `stratum` does not encode
        urbanicity, so nothing in the source adjudicates it: the honest value
        is missing."""
        row = cf.xs(('2012', '964'), level=('t', 'v'))
        assert pd.isna(row['Rural'].iloc[0]), "cluster 964 Rural must not be guessed"
        assert row['Region'].iloc[0] == 'ERBIL', "unambiguous Region must survive"

    def test_conflict_is_confined_to_that_one_cell(self, cf):
        """Everything else is a clean, lossless projection -- the fix must not
        blow NA holes anywhere else."""
        assert int(cf['Rural'].isna().sum()) == 1
        assert int(cf['Region'].isna().sum()) == 0

    def test_2006_07_is_exactly_lossless(self, cf):
        """2006-07 derives Rural from the DESIGN STRATUM label, so it is
        constant within cluster by construction: zero conflicts."""
        w = cf.xs('2006-07', level='t')
        assert int(w['Rural'].isna().sum()) == 0
        assert int(w['Region'].isna().sum()) == 0

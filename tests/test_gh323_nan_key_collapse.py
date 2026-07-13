"""GH #323 (reopened): the duplicate-index collapse must not silently lose rows.

Two defect classes are covered here, both of which were live in
``_normalize_dataframe_index``:

1. **NaN-key annihilation.**  The collapse groups with pandas' ``dropna=True``
   default, which DELETES every group whose index key contains NaN on any
   level -- not just the duplicate rows, the whole group.  It is invisible to
   the existing duplicate accounting (``index.duplicated()`` counts repeats,
   not NaN keys) and is *gated* on the index being non-unique, so a table only
   loses its NaN-key rows once some OTHER defect makes the index non-unique.
   Burkina Faso 2014 ``food_acquired`` lost 460,438 rows / 261.8M CFA this way.

2. **Non-additive tables reduced with ``first()``.**  ``plot_inputs`` records one
   row per reported input application; two source rows can legitimately land on
   one ``(t, i, input, crop, u)``.  ``Quantity`` is additive, so ``first()``
   silently discarded the rest (2,778.98 kg of seed on Burkina 2018-19 alone).

These tests run against ``_normalize_dataframe_index`` directly -- no country
data, no cache, no network -- so they are fast and deterministic.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import _normalize_dataframe_index
from lsms_library.feature import _ADDITIVE_MEASURE_COLUMNS


def _frame(rows, index_names):
    df = pd.DataFrame(rows)
    return df.set_index(index_names)


class TestNaNKeyRowsSurviveTheCollapse:
    """A NaN in a declared index level must not delete the row."""

    def test_nan_key_group_is_retained_when_index_is_non_unique(self):
        # Two rows share (t=2014, i=A, u=kg) -> index is NOT unique, so the
        # collapse fires.  The third row has u=NaN: pre-fix it was deleted
        # outright by groupby's dropna=True default.
        df = _frame(
            [
                {'t': '2014', 'i': 'A', 'u': 'kg', 'Expenditure': 10.0},
                {'t': '2014', 'i': 'A', 'u': 'kg', 'Expenditure': 5.0},
                {'t': '2014', 'i': 'B', 'u': None, 'Expenditure': 100.0},
            ],
            ['t', 'i', 'u'],
        )
        assert not df.index.is_unique  # precondition: the collapse must fire

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = _normalize_dataframe_index(
                df, {'index': '(t, i, u)'}, '2014', 'food_acquired')

        # The NaN-u row must still be there, with its money intact.
        assert out['Expenditure'].sum() == pytest.approx(115.0), (
            'the NaN-key row was deleted by the collapse (GH #323)')
        assert len(out) == 2, f'expected 2 groups, got {len(out)}:\n{out}'

    def test_nan_key_deletion_is_reported(self):
        df = _frame(
            [
                {'t': '2014', 'i': 'A', 'u': 'kg', 'Expenditure': 10.0},
                {'t': '2014', 'i': 'A', 'u': 'kg', 'Expenditure': 5.0},
                {'t': '2014', 'i': 'B', 'u': None, 'Expenditure': 100.0},
            ],
            ['t', 'i', 'u'],
        )
        with pytest.warns(RuntimeWarning, match='NaN index key'):
            _normalize_dataframe_index(
                df, {'index': '(t, i, u)'}, '2014', 'food_acquired')

    def test_unique_index_is_untouched(self):
        # No duplicates -> the collapse never runs -> NaN keys are irrelevant.
        df = _frame(
            [
                {'t': '2014', 'i': 'A', 'u': 'kg', 'Expenditure': 10.0},
                {'t': '2014', 'i': 'B', 'u': None, 'Expenditure': 100.0},
            ],
            ['t', 'i', 'u'],
        )
        out = _normalize_dataframe_index(
            df, {'index': '(t, i, u)'}, '2014', 'food_acquired')
        assert len(out) == 2
        assert out['Expenditure'].sum() == pytest.approx(110.0)

    def test_non_nan_groups_are_unaffected_by_the_fix(self):
        """dropna=False must only ADD groups -- never regroup existing ones."""
        df = _frame(
            [
                {'t': '2014', 'i': 'A', 'u': 'kg', 'Expenditure': 10.0},
                {'t': '2014', 'i': 'A', 'u': 'kg', 'Expenditure': 5.0},
                {'t': '2014', 'i': 'B', 'u': None, 'Expenditure': 100.0},
            ],
            ['t', 'i', 'u'],
        )
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = _normalize_dataframe_index(
                df, {'index': '(t, i, u)'}, '2014', 'food_acquired')
        # The (2014, A, kg) group still sums its two additive rows to 15.
        assert out.loc[('2014', 'A', 'kg'), 'Expenditure'] == pytest.approx(15.0)


class TestPlotInputsIsAdditive:
    """plot_inputs Quantity must be SUMMED, not first()'d."""

    def test_registered_as_additive(self):
        assert 'plot_inputs' in _ADDITIVE_MEASURE_COLUMNS
        assert 'Quantity' in _ADDITIVE_MEASURE_COLUMNS['plot_inputs']

    def test_duplicate_input_rows_are_summed(self):
        # Two seed rows collapsing onto one (t, i, input, crop, u): pre-fix
        # first() kept 1.0 and threw away 2.0.
        df = _frame(
            [
                {'t': '2018-19', 'i': 'A', 'input': 'Seed', 'crop': 'X',
                 'u': 'Kilogramme', 'Quantity': 1.0},
                {'t': '2018-19', 'i': 'A', 'input': 'Seed', 'crop': 'X',
                 'u': 'Kilogramme', 'Quantity': 2.0},
            ],
            ['t', 'i', 'input', 'crop', 'u'],
        )
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = _normalize_dataframe_index(
                df, {'index': '(t, i, input, crop, u)'}, '2018-19', 'plot_inputs')
        assert len(out) == 1
        assert out['Quantity'].iloc[0] == pytest.approx(3.0), (
            'plot_inputs Quantity was reduced with first(), discarding seed '
            '(GH #323)')


class TestReduceToAgreed:
    """The honest reducer for a source coarser than its declared grain."""

    def test_agreeing_rows_collapse_to_the_agreed_value(self):
        from lsms_library.transformations import reduce_to_agreed
        df = _frame(
            [
                {'t': '2014', 'v': '9', 'Region': 'BdM', 'District': 'BALE'},
                {'t': '2014', 'v': '9', 'Region': 'BdM', 'District': 'BALE'},
            ],
            ['t', 'v'],
        )
        out = reduce_to_agreed(df)
        assert len(out) == 1
        assert out['Region'].iloc[0] == 'BdM'
        assert out['District'].iloc[0] == 'BALE'

    def test_disagreeing_rows_go_to_NA_not_an_arbitrary_winner(self):
        from lsms_library.transformations import reduce_to_agreed
        # zd=9 really does straddle BALE and TUY in Burkina 2014.
        df = _frame(
            [
                {'t': '2014', 'v': '9', 'Region': 'BdM', 'District': 'BALE'},
                {'t': '2014', 'v': '9', 'Region': 'BdM', 'District': 'TUY'},
            ],
            ['t', 'v'],
        )
        out = reduce_to_agreed(df)
        assert len(out) == 1
        assert out['Region'].iloc[0] == 'BdM'      # agrees -> kept
        assert pd.isna(out['District'].iloc[0]), (
            'an ambiguous District must be NA, not an arbitrary winner')

    def test_nulls_do_not_count_as_disagreement(self):
        from lsms_library.transformations import reduce_to_agreed
        # One all-null row + one populated row -> the populated value stands.
        df = _frame(
            [
                {'t': '2014', 'i': 'A', 'AffectedAssets': None},
                {'t': '2014', 'i': 'A', 'AffectedAssets': True},
            ],
            ['t', 'i'],
        )
        out = reduce_to_agreed(df)
        assert len(out) == 1
        assert bool(out['AffectedAssets'].iloc[0]) is True


class TestAddVisitLevel:
    def test_appends_a_constant_visit_level(self):
        from lsms_library.transformations import add_visit_level
        df = _frame(
            [{'t': '2018-19', 'i': 'A', 'Quantity': 1.0}],
            ['t', 'i'],
        )
        out = add_visit_level(df, visit=1)
        assert 'visit' in out.index.names
        assert out.index.get_level_values('visit').unique().tolist() == [1]
        assert len(out) == len(df)

"""`unknown_plot` is not a plot: `harvest_kg` and `crop_diversity` leave it out (GH #1460).

Ruling, @ligon 2026-11-04, per the canonical CONSUMER RULE: exclude it from per-plot
quantities and report how many rows were excluded.
"""
import math
import warnings

import pandas as pd
import pytest

from lsms_library.transformations import (
    _PLOT_UNKNOWN,
    crop_diversity,
    gross_crop_revenue,
    harvest_kg,
    yield_kg,
)

UNKNOWN = 'unknown_plot'


def _frame(rows, crop_level='j', plot_level='plot_id'):
    """Each row is ``(t, i, plot, crop, u, Quantity, Value_sold)``."""
    index = pd.MultiIndex.from_tuples(
        [r[:5] for r in rows], names=['t', 'i', plot_level, crop_level, 'u'])
    return pd.DataFrame({'Quantity': [r[5] for r in rows],
                         'Value_sold': [r[6] for r in rows]},
                        index=index, dtype=float)


ROWS = [
    ('w1', 'h1', 'p1', 'Maize', 'Kg', 10.0, None),
    ('w1', 'h1', UNKNOWN, 'Maize', 'Kg', 4.0, 8.0),
    ('w1', 'h1', UNKNOWN, 'Maize', 'Gram', 3000.0, None),
    ('w1', 'h1', UNKNOWN, 'Beans', 'Kg', 0.0, None),
    ('w1', 'h2', UNKNOWN, 'Beans', 'Kg', 5.0, 2.0),
]


class TestHarvestKg:
    def test_the_rows_leave_and_the_rest_stay(self):
        out = harvest_kg(_frame(ROWS))
        assert UNKNOWN not in out.index.get_level_values('plot_id')
        assert out.loc[('w1', 'h1', 'p1', 'Maize'), 'Harvest_kg'] == 10.0
        assert len(out) == 1

    def test_the_exclusion_is_reported_with_its_kilograms(self):
        got = harvest_kg(_frame(ROWS)).attrs['unknown_plot_excluded']
        assert got == {'rows': 4, 'rows_with_kg': 3, 'kg': 12.0}

    def test_kept_plus_reported_is_every_kilogram(self):
        out = harvest_kg(_frame(ROWS))
        assert out['Harvest_kg'].sum() + out.attrs[
            'unknown_plot_excluded']['kg'] == 22.0

    def test_a_frame_with_none_reports_zero(self):
        got = harvest_kg(_frame(ROWS[:1])).attrs['unknown_plot_excluded']
        assert got == {'rows': 0, 'rows_with_kg': 0, 'kg': 0.0}

    def test_a_frame_with_no_plot_level_reduces_and_reports_zero(self):
        out = harvest_kg(_frame(ROWS).droplevel('plot_id'))
        assert out.attrs['unknown_plot_excluded']['rows'] == 0
        assert out['Harvest_kg'].sum() == 22.0

    def test_the_legacy_plot_spelling_is_covered(self):
        out = harvest_kg(_frame(ROWS, plot_level='plot'))
        assert out.attrs['unknown_plot_excluded']['rows'] == 4

    def test_its_factors_still_inform_other_rows(self):
        """Evidence, not a row served: a reported factor on an `unknown_plot`
        row still counts toward the survey median of a real plot's unit."""
        rows = [('w1', f'h{k}', UNKNOWN, 'Maize', 'Sack', 1.0, None) for k in range(4)]
        rows.append(('w1', 'h9', 'p1', 'Maize', 'Sack', 2.0, None))
        cp = _frame(rows)
        cp['KgFactor'] = [50.0] * 4 + [float('nan')]
        out = harvest_kg(cp, min_reports=3)
        assert out.loc[('w1', 'h9', 'p1', 'Maize'), 'Harvest_kg'] == 100.0

    def test_yield_never_saw_them(self):
        pf = pd.DataFrame({'Area': [2.0]}, index=pd.MultiIndex.from_tuples(
            [('w1', 'h1', 'p1')], names=['t', 'i', 'plot_id']))
        out = yield_kg(_frame(ROWS), pf, on='plot')
        assert out['Yield_kg'].tolist() == [5.0]


class TestCropDiversity:
    def _rows(self):
        return [
            ('w1', 'h1', 'p1', 'Maize', 'Kg', 1.0, None),
            ('w1', 'h1', 'p2', 'Beans', 'Kg', 1.0, None),
            ('w1', 'h1', UNKNOWN, 'Beans', 'Kg', 1.0, None),
            ('w1', 'h2', UNKNOWN, 'Beans', 'Kg', 1.0, None),
        ]

    def test_it_is_not_an_occurrence(self):
        out = crop_diversity(_frame(self._rows()))
        assert out.loc[('w1', 'h1'), 'Crop_diversity'] == pytest.approx(math.log(2))

    def test_a_household_with_nothing_else_has_no_score(self):
        out = crop_diversity(_frame(self._rows()))
        assert ('w1', 'h2') not in out.index

    def test_the_exclusion_is_reported(self):
        got = crop_diversity(_frame(self._rows())).attrs['unknown_plot_excluded']
        assert got == {'rows': 2, 'households_lost': 1}

    def test_a_real_plot_alone_makes_a_farm_level_row_redundant(self):
        """`unknown_plot` goes first, so the Farm-level row is the crop's record."""
        rows = [('w1', 'h1', UNKNOWN, 'Beans', 'Kg', 1.0, None),
                ('w1', 'h1', 'Farm-level', 'Beans', 'Kg', None, 5.0),
                ('w1', 'h1', 'p1', 'Maize', 'Kg', 1.0, None)]
        out = crop_diversity(_frame(rows))
        assert out.loc[('w1', 'h1'), 'Crop_diversity'] == pytest.approx(math.log(2))

    def test_the_value_basis_keeps_them(self):
        """A sale share has no land key."""
        out = crop_diversity(_frame(ROWS), weight='value')
        assert ('w1', 'h2') in out.index
        assert 'unknown_plot_excluded' not in out.attrs

    def test_revenue_keeps_every_sale(self):
        got = gross_crop_revenue(_frame(ROWS))
        assert got.loc[('w1', 'h1'), 'Gross_crop_revenue'] == 8.0


def test_the_sentinel_is_the_one_data_info_names():
    assert _PLOT_UNKNOWN == UNKNOWN


@pytest.mark.requires_s3
@pytest.mark.parametrize('country', ['Uganda', 'Malawi'])
def test_real_frames(country):
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = ll.Country(country).crop_production()
    plot = 'plot_id' if 'plot_id' in cp.index.names else 'plot'
    rows = int((cp.index.get_level_values(plot).astype(object) == UNKNOWN).sum())
    out = harvest_kg(cp)
    assert rows > 0 and out.attrs['unknown_plot_excluded']['rows'] == rows
    assert UNKNOWN not in out.index.get_level_values('plot_id')
    assert crop_diversity(cp).attrs['unknown_plot_excluded']['rows'] == rows

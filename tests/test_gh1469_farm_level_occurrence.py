"""`crop_diversity(weight='count')` and the `Farm-level` sentinel (GH #1469).

A `plot_id = 'Farm-level'` row on `crop_production` holds a sale no plot can own.
It is not a further occurrence of a crop the household reports on a plot, and it
is the only record of a crop the household does not.  Synthetic, except the
cold-gated real-data checks at the end.
"""
import math
import warnings

import pandas as pd
import pytest

from lsms_library.feature import _missing_level_sentinels
from lsms_library.transformations import (
    _PLOT_FARM_LEVEL,
    _resolve_crop_level,
    crop_diversity,
    gross_crop_revenue,
)

FARM = "Farm-level"


def _frame(rows, crop_level="crop"):
    """Each row is ``(t, i, plot_id, crop, Value_sold)``."""
    index = pd.MultiIndex.from_tuples(
        [r[:4] for r in rows], names=["t", "i", "plot_id", crop_level])
    return pd.DataFrame({"Value_sold": [r[4] for r in rows]}, index=index,
                        dtype=float)


def _h(df, i="h1", t="w1"):
    return crop_diversity(df).loc[(t, i), "Crop_diversity"]


def test_the_sentinel_is_the_one_data_info_declares():
    assert _missing_level_sentinels("plot_inputs")["plot_id"] == _PLOT_FARM_LEVEL


@pytest.mark.parametrize("crop_level", ["crop", "j"])
def test_a_farm_row_repeating_a_plotted_crop_is_not_an_occurrence(crop_level):
    with_sale = _frame([("w1", "h1", "p1", "Maize", None),
                        ("w1", "h1", "p2", "Beans", None),
                        ("w1", "h1", FARM, "Maize", 50.0)], crop_level)
    without = with_sale[with_sale.index.get_level_values("plot_id") != FARM]
    pd.testing.assert_frame_equal(crop_diversity(with_sale),
                                  crop_diversity(without))
    assert _h(with_sale) == pytest.approx(math.log(2))


def test_a_farm_row_is_kept_where_it_is_the_only_record_of_the_crop():
    df = _frame([("w1", "h1", "p1", "Maize", None),
                 ("w1", "h1", FARM, "Beans", 50.0)])
    assert _h(df) == pytest.approx(math.log(2))


def test_a_household_with_only_farm_rows_is_unchanged():
    """GhanaLSS's shape: a blanket filter would delete every household."""
    df = _frame([("w1", "h1", FARM, "Maize", 10.0),
                 ("w1", "h1", FARM, "Beans", 20.0),
                 ("w1", "h2", FARM, "Maize", 5.0)])
    out = crop_diversity(df)
    assert len(out) == 2
    assert _h(df) == pytest.approx(math.log(2))
    assert _h(df, "h2") == 0.0


def test_the_rule_is_per_household_and_per_wave():
    df = _frame([("w1", "h1", "p1", "Maize", None),
                 ("w1", "h1", "p1", "Beans", None),
                 ("w1", "h2", FARM, "Maize", 5.0),     # another household's plot
                 ("w1", "h2", "p1", "Beans", None),
                 ("w2", "h1", FARM, "Maize", 5.0),     # another wave's plot
                 ("w2", "h1", "p1", "Beans", None)])
    assert _h(df, "h2") == pytest.approx(math.log(2))
    assert _h(df, "h1", "w2") == pytest.approx(math.log(2))


def test_a_frame_with_no_farm_rows_is_untouched():
    df = _frame([("w1", "h1", "p1", "Maize", None),
                 ("w1", "h1", "p2", "Maize", None),
                 ("w1", "h1", "p1", "Beans", None)])
    expected = -(2 / 3 * math.log(2 / 3) + 1 / 3 * math.log(1 / 3))
    assert _h(df) == pytest.approx(expected)


def test_a_null_plot_id_is_not_the_sentinel():
    """Mali's tree rows carry a null `plot_id`: not Farm-level, and still a
    record of the crop on some plot."""
    df = _frame([("w1", "h1", None, "Mango", None),
                 ("w1", "h1", FARM, "Mango", 5.0),
                 ("w1", "h1", "p1", "Beans", None)])
    df.index = df.index.set_levels(
        df.index.levels[df.index.names.index("plot_id")].astype("string"),
        level="plot_id")
    assert _h(df) == pytest.approx(math.log(2))


def test_a_frame_with_no_plot_level_still_counts():
    df = _frame([("w1", "h1", "p1", "Maize", None),
                 ("w1", "h1", "p2", "Beans", None)]).droplevel("plot_id")
    assert _h(df) == pytest.approx(math.log(2))


def test_the_value_basis_keeps_the_farm_row():
    """The sale is on the Farm-level row, so a value share needs it."""
    df = _frame([("w1", "h1", "p1", "Maize", None),
                 ("w1", "h1", "p2", "Beans", 50.0),
                 ("w1", "h1", FARM, "Maize", 50.0)])
    out = crop_diversity(df, weight="value")
    assert out.loc[("w1", "h1"), "Crop_diversity"] == pytest.approx(math.log(2))
    assert gross_crop_revenue(df).loc[("w1", "h1"),
                                      "Gross_crop_revenue"] == 100.0


@pytest.mark.requires_s3
class TestDelivered:
    def _plot_rows(self, df):
        return df.index.get_level_values("plot_id").astype(object)

    def test_ghanalss_is_all_farm_level_and_unchanged(self):
        import lsms_library as ll

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cp = ll.Country("GhanaLSS").crop_production()
        farm = self._plot_rows(cp) == FARM
        assert farm.all()
        out = crop_diversity(cp)
        # Every household with a crop stays, on the count of its distinct crops.
        hh = cp.index.droplevel(
            [n for n in cp.index.names if n not in ("t", "i")]).unique()
        assert len(out) == len(hh)

    def test_mali_farm_rows_all_repeat_a_plotted_crop_or_stand_alone(self):
        import lsms_library as ll

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cp = ll.Country("Mali").crop_production()
        farm = self._plot_rows(cp) == FARM
        if not farm.any():
            pytest.skip("this Mali build serves no Farm-level row")
        # The result never depends on whether a redundant row is present.
        flat = cp.index.to_frame(index=False)
        keys = ["t", "i", _resolve_crop_level(cp.index.names)]
        plotted = pd.MultiIndex.from_frame(flat.loc[~farm, keys])
        redundant = farm & pd.MultiIndex.from_frame(flat[keys]).isin(plotted)
        pd.testing.assert_frame_equal(crop_diversity(cp),
                                      crop_diversity(cp[~redundant]))

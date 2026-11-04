"""`harvest_kg` keeps the crop axis whichever name the country gives it (GH #1460).

Ten countries declare `crop_production`'s crop level as `crop`, the rest as `j`,
and `level_aliases` renames `crop` onto `j` on the way into a `Feature` frame.
`harvest_kg` used to look for `j` alone, so a `Country` frame whose level was
`crop` was grouped to `(t, i, plot_id)` and its kilograms summed across the crops
of a plot, while a `Feature` frame of the same country kept them apart.

Synthetic throughout, except the one cold-gated real-data check at the end.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.feature import _harmonize_country_frame, _level_aliases
from lsms_library.transformations import (
    _CROP_CANONICAL,
    _CROP_LEVELS,
    _canonicalise_crop_level,
    PlotGrainMismatchWarning,
    harvest_kg,
    harvest_kg_factors,
    yield_kg,
)

ROWS = [
    # t, i, plot, crop, u, Quantity
    ("w1", "h1", "p1", "Maize", "Kg", 10.0),
    ("w1", "h1", "p1", "Beans", "Kg", 3.0),
    ("w1", "h1", "p2", "Maize", "Kg", 5.0),
    ("w1", "h2", "p1", "Maize", "Kg", 7.0),
    ("w1", "h2", "p1", "Maize", "Gram", 2000.0),
]


def _crop_production(crop_level, plot_level="plot_id"):
    index = pd.MultiIndex.from_tuples(
        [r[:5] for r in ROWS], names=["t", "i", plot_level, crop_level, "u"])
    return pd.DataFrame({"Quantity": [r[5] for r in ROWS]}, index=index)


def _plot_features():
    index = pd.MultiIndex.from_tuples(
        [("w1", "h1", "p1"), ("w1", "h1", "p2"), ("w1", "h2", "p1")],
        names=["t", "i", "plot_id"])
    return pd.DataFrame({"Area": [2.0, 1.0, 4.0]}, index=index)


class TestCropLevelIsResolved:
    def test_each_crop_on_a_plot_is_its_own_row(self):
        out = harvest_kg(_crop_production("crop"))
        assert out.loc[("w1", "h1", "p1", "Maize"), "Harvest_kg"] == 10.0
        assert out.loc[("w1", "h1", "p1", "Beans"), "Harvest_kg"] == 3.0
        assert len(out) == 4

    def test_output_level_is_canonical_whichever_spelling_came_in(self):
        for spelling in _CROP_LEVELS:
            out = harvest_kg(_crop_production(spelling))
            assert list(out.index.names) == ["t", "i", "plot_id", "j"]

    def test_both_spellings_give_the_same_frame(self):
        a = harvest_kg(_crop_production("crop"))
        b = harvest_kg(_crop_production("j"))
        pd.testing.assert_frame_equal(a, b)

    def test_units_still_sum_within_a_plot_crop(self):
        out = harvest_kg(_crop_production("crop"))
        assert out.loc[("w1", "h2", "p1", "Maize"), "Harvest_kg"] == 9.0

    def test_total_equals_the_row_level_kilograms(self):
        """No kilogram moves: only the grain does."""
        cp = _crop_production("crop")
        direct = (cp["Quantity"] * harvest_kg_factors(cp)["kg_per_unit"]).sum()
        assert harvest_kg(cp)["Harvest_kg"].sum() == pytest.approx(direct)
        assert direct == 27.0

    def test_legacy_plot_spelling_is_still_canonicalised_alongside(self):
        out = harvest_kg(_crop_production("crop", plot_level="plot"))
        assert list(out.index.names) == ["t", "i", "plot_id", "j"]

    def test_a_frame_with_no_crop_level_still_reduces(self):
        cp = _crop_production("crop").droplevel("crop")
        out = harvest_kg(cp)
        assert list(out.index.names) == ["t", "i", "plot_id"]
        assert out["Harvest_kg"].sum() == 27.0

    def test_attrs_tallies_survive(self):
        out = harvest_kg(_crop_production("crop"))
        assert sum(out.attrs["kg_factor_sources"].values()) == len(ROWS)


class TestCountryAndFeatureAgree:
    def test_the_alias_target_is_the_canonical_level(self):
        assert _level_aliases("crop_production")["crop"] == _CROP_CANONICAL

    def test_a_harmonized_frame_gives_the_same_rows(self):
        cp = _crop_production("crop")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            harmonized = _harmonize_country_frame(
                cp, ["t", "i", "plot_id", "j", "u"], "Fixtureland",
                "crop_production", False,
                aliases=_level_aliases("crop_production"), sentinels={},
                alignment_report={})
        assert "crop" not in harmonized.index.names
        pd.testing.assert_frame_equal(harvest_kg(cp),
                                      harvest_kg(harmonized).sort_index())


class TestYieldIsUnchanged:
    """`yield_kg` sums across crops on purpose (the WB construct), so resolving
    the crop level inside `harvest_kg` must not move a single yield."""

    @pytest.mark.parametrize("on", ["plot", "parcel"])
    def test_spelling_does_not_move_a_yield(self, on):
        a = yield_kg(_crop_production("crop"), _plot_features(), on=on)
        b = yield_kg(_crop_production("j"), _plot_features(), on=on)
        pd.testing.assert_frame_equal(a, b)

    def test_yield_sums_the_crops_of_a_plot(self):
        out = yield_kg(_crop_production("crop"), _plot_features(), on="plot")
        assert out.loc[("w1", "h1", "p1"), "Yield_kg"] == (10.0 + 3.0) / 2.0


class TestEmptyYieldWarns:
    """`fertilizer_rate` warns when both sides have rows and nothing joins, and
    its twin did not: `yield_kg`'s default returned an empty frame in silence."""

    def _crop(self, plot="1_2", t="w1"):
        index = pd.MultiIndex.from_tuples(
            [(t, "h1", plot, "Maize", "Kg")],
            names=["t", "i", "plot_id", "crop", "u"])
        return pd.DataFrame({"Quantity": [10.0]}, index=index)

    def _area(self, plot="1_2", t="w1"):
        index = pd.MultiIndex.from_tuples(
            [(t, "h1", plot)], names=["t", "i", "plot_id"])
        return pd.DataFrame({"Area": [2.0]}, index=index)

    def test_a_shared_field_parcel_key_warns_at_the_parcel_grain(self):
        """The EHCVM shape: `{field}_{parcel}` on both sides."""
        with pytest.warns(PlotGrainMismatchWarning, match="on='plot'"):
            out = yield_kg(self._crop(), self._area())
        assert out.empty
        with warnings.catch_warnings():
            warnings.simplefilter("error", PlotGrainMismatchWarning)
            assert yield_kg(self._crop(), self._area(), on="plot")[
                "Yield_kg"].tolist() == [5.0]

    def test_rounds_that_share_no_wave_say_so(self):
        """The Nigeria shape: harvest on the post-harvest quarter, area on the
        post-planting one."""
        with pytest.warns(PlotGrainMismatchWarning, match="share no wave"):
            out = yield_kg(self._crop(plot="1", t="2011Q1"),
                           self._area(plot="1", t="2010Q3"), on="plot")
        assert out.empty

    def test_a_join_that_finds_something_does_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", PlotGrainMismatchWarning)
            assert len(yield_kg(self._crop(plot="1"), self._area(plot="1"))) == 1

    def test_no_harvest_to_join_does_not_warn(self):
        crop = self._crop().iloc[0:0]
        with warnings.catch_warnings():
            warnings.simplefilter("error", PlotGrainMismatchWarning)
            assert yield_kg(crop, self._area()).empty


def test_canonicalise_is_a_noop_on_the_canonical_name_and_on_none():
    res = pd.DataFrame({"x": [1.0]}, index=pd.MultiIndex.from_tuples(
        [("w1", "Maize")], names=["t", "j"]))
    assert list(_canonicalise_crop_level(res, "j").index.names) == ["t", "j"]
    assert list(_canonicalise_crop_level(res, None).index.names) == ["t", "j"]


@pytest.mark.requires_s3
def test_niger_country_and_feature_frames_agree():
    """The real pair the issue measured.  A `crop`-level country whose kilograms
    do not depend on any later edit to the frame: same rows, same total."""
    import lsms_library as ll

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country = harvest_kg(ll.Country("Niger").crop_production())
        feature = harvest_kg(ll.Feature("crop_production")(["Niger"]))
    assert list(country.index.names) == list(feature.index.names)
    pd.testing.assert_frame_equal(country.sort_index(), feature.sort_index())

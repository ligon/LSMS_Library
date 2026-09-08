"""Tests for lsms_library.visualizations.

The pure-logic paths run on a synthetic roster so they need no microdata; the
one data-gated test is skipped when Uganda is unavailable.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")

from lsms_library.visualizations import (  # noqa: E402
    PALETTE, coordinate_map, population_pyramid,
)


@pytest.fixture(autouse=True)
def _close_figures():
    """Close figures between tests; matplotlib keeps them open otherwise."""
    yield
    import matplotlib.pyplot as plt
    plt.close("all")


def _roster(n=400):
    """A synthetic household_roster with the index the real one carries."""
    rows = []
    for k in range(n):
        rows.append({
            "i": f"h{k // 4}", "t": "2020", "v": f"c{k // 40}", "pid": str(k),
            "Sex": "M" if k % 2 else "F",
            "Age": float(k % 70),
            "Rural": "Rural" if (k // 4) % 3 else "Urban",
            "w2": 1.0 + (k % 5),
        })
    return pd.DataFrame(rows).set_index(["i", "t", "v", "pid"])


def test_returns_axes_and_draws_bars():
    ax = population_pyramid(_roster(), weights=False)
    assert ax.__class__.__name__.startswith("Axes")
    assert ax.patches, "no bars drawn"


def test_weighting_changes_the_picture_but_not_the_people():
    """A weight column must move bar widths without inventing rows."""
    r = _roster()
    plain = population_pyramid(r, weights=False)
    wtd = population_pyramid(r, weights="w2")
    widths_plain = sorted(abs(p.get_width()) for p in plain.patches)
    widths_wtd = sorted(abs(p.get_width()) for p in wtd.patches)
    assert widths_plain != widths_wtd
    assert sum(widths_wtd) > sum(widths_plain)


def test_by_splits_into_two_series_and_legend_omits_redundant_title():
    """`by='Rural'` with values Rural/Urban must not title the legend 'Rural'."""
    ax = population_pyramid(_roster(), weights=False, by="Rural")
    labels = {t.get_text() for t in ax.get_legend().get_texts()}
    assert labels == {"Rural", "Urban"}
    title = ax.get_legend().get_title()
    assert not title.get_text(), "legend title repeats a group value"


def test_unknown_by_raises_actionably():
    with pytest.raises(KeyError, match="neither a roster column nor"):
        population_pyramid(_roster(), weights=False, by="NotAColumn")


def test_single_year_bands_label_one_year_not_a_range():
    """bin_width=1 must read '30', never '30-30'."""
    ax = population_pyramid(_roster(), weights=False, bin_width=1, max_age=40)
    texts = {t.get_text() for t in ax.texts}
    assert not any("–" in s and s.split("–")[0] == s.split("–")[-1] for s in texts)
    assert "40+" in texts, "the open-ended top band must always be labelled"


def test_missing_weights_warns_rather_than_failing():
    r = _roster().drop(columns=["w2"])
    with pytest.warns(UserWarning, match="no sampling weights"):
        population_pyramid(r, weights=True)


def test_rejects_a_non_frame():
    with pytest.raises(TypeError, match="Country, a country name"):
        population_pyramid(42)


def test_palette_does_not_encode_sex_by_colour():
    """Design invariant: one ink unconditioned, two only when `by=` is given.

    Colour is reserved for the conditioning variable because the mirrored
    layout already encodes sex.  If someone adds a per-sex colour, this fails.
    """
    ax = population_pyramid(_roster(), weights=False)
    colours = {p.get_facecolor() for p in ax.patches}
    assert len(colours) == 1, "unconditioned pyramid must use a single ink"


@pytest.mark.slow
def test_country_pyramid_carries_its_universe_caption():
    """A real country states what population it represents."""
    ll = pytest.importorskip("lsms_library")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ax = population_pyramid("Uganda", wave="2009-10")
    except Exception as exc:                      # pragma: no cover
        pytest.skip(f"Uganda unavailable: {exc}")
    caption = " ".join(t.get_text() for t in ax.figure.texts)
    assert "Represents:" in caption


def _tailed_roster(tail_age=115, tail_n=1, n=8000, span=30):
    """A dense age structure plus a few implausibly old people.

    The density matters: the visibility rule is a ratio against the largest
    band, so a fixture with only a few dozen people per band makes three
    centenarians a legitimately *visible* 4.5% and the rule correctly keeps
    them.  Real surveys have a much wider dynamic range -- Uganda's largest
    band holds ~2,900 people and its 4 centenarians are 0.14% of it -- so the
    fixture reproduces that ratio rather than a token tail.
    """
    rows = [{"i": f"h{k // 4}", "t": "2020", "v": "c0", "pid": str(k),
             "Sex": "M" if k % 2 else "F", "Age": float(k % span)}
            for k in range(n)]
    rows += [{"i": "hz", "t": "2020", "v": "c0", "pid": f"z{k}",
              "Sex": "F", "Age": float(tail_age)} for k in range(tail_n)]
    return pd.DataFrame(rows).set_index(["i", "t", "v", "pid"])


def _closing_band(ax):
    bands = [s.get_text() for s in ax.texts if s.get_text().endswith("+")]
    assert len(bands) == 1, f"expected one closing band, got {bands}"
    return int(bands[0].rstrip("+"))


def _all_text(ax):
    return " ".join([s.get_text() for s in ax.texts]
                    + [s.get_text() for s in ax.figure.texts])


def test_auto_max_age_ignores_an_invisible_tail():
    """One centenarian must not stretch the scale by seventeen empty bands."""
    ax = population_pyramid(_tailed_roster(tail_age=115, tail_n=1),
                            weights=False)
    assert _closing_band(ax) < 115, "the closing band was dragged up by the tail"


def test_truncation_is_disclosed_not_silent():
    """Hiding the tail is fine; implying nobody is older is not."""
    ax = population_pyramid(_tailed_roster(tail_age=115, tail_n=1),
                            weights=False)
    assert "oldest recorded 115" in _all_text(ax)


def test_auto_max_age_is_geometric_not_a_fixed_number():
    """A tail that is genuinely visible must be kept, on the same rule.

    Same shape, same oldest age -- only the tail's weight differs.  A rule
    that returned a fixed number could not tell these apart.
    """
    thin = population_pyramid(_tailed_roster(tail_age=115, tail_n=1),
                              weights=False)
    dense = population_pyramid(_tailed_roster(tail_age=115, tail_n=400),
                               weights=False)
    assert _closing_band(dense) > _closing_band(thin), (
        f"thin={_closing_band(thin)} dense={_closing_band(dense)}"
    )


def test_explicit_max_age_still_honoured():
    ax = population_pyramid(_tailed_roster(), weights=False, max_age=60)
    assert _closing_band(ax) == 60


def test_bad_max_age_string_rejected():
    with pytest.raises(ValueError, match="max_age must be an int or 'auto'"):
        population_pyramid(_tailed_roster(), weights=False, max_age="tall")


def test_ghost_draws_a_second_unfilled_series():
    """`ghost=` overlays a comparison weighting as an outline, not a fill."""
    r = _roster()
    ax = population_pyramid(r, weights="w2", ghost=False)
    filled = [p for p in ax.patches if p.get_facecolor()[3] > 0]
    outline = [p for p in ax.patches if p.get_facecolor()[3] == 0]
    assert filled and outline, "expected both a filled and an outlined series"
    assert len(outline) == len(filled)


def test_ghost_is_absent_unless_asked():
    ax = population_pyramid(_roster(), weights=False)
    assert not [p for p in ax.patches if p.get_facecolor()[3] == 0]


def test_ghost_coincides_when_the_weight_is_constant():
    """A self-weighting wave must show outline and fill on top of each other.

    GhanaLSS GLSS1-3 are self-weighting -- weight is a genuine constant 1.0 --
    so `weights=True, ghost=False` should draw two identical series.  If the
    ghost silently used the wrong column this would diverge.
    """
    r = _roster()
    r = r.assign(const=1.0)
    ax = population_pyramid(r, weights="const", ghost=False)
    filled = sorted(abs(p.get_width()) for p in ax.patches
                    if p.get_facecolor()[3] > 0)
    outline = sorted(abs(p.get_width()) for p in ax.patches
                     if p.get_facecolor()[3] == 0)
    assert filled == pytest.approx(outline)


def test_ghost_basis_is_named_in_the_subtitle():
    ax = population_pyramid(_roster(), weights="w2", ghost=False)
    assert "outline: unweighted" in _all_text(ax)


def test_ghost_rejects_a_bad_spec():
    with pytest.raises(TypeError, match="ghost= takes"):
        population_pyramid(_roster(), weights=False, ghost=3.7)
    with pytest.raises(KeyError, match="ghost="):
        population_pyramid(_roster(), weights=False, ghost="nope")


# --- coordinate_map --------------------------------------------------------

def _geo(n=40, lat0=1.0, lon0=32.0):
    """Cluster-shaped frame: one row per `v`, with coordinates and a weight."""
    rows = [{"t": "2020", "v": f"c{k}",
             "Latitude": lat0 + (k % 8) * 0.25,
             "Longitude": lon0 + (k // 8) * 0.25,
             "weight": 1.0 + (k % 5)} for k in range(n)]
    return pd.DataFrame(rows)


def test_static_map_plots_every_point():
    ax = coordinate_map(_geo(), size="weight", interactive=False)
    assert len(ax.collections[0].get_offsets()) == 40


def test_marker_area_not_radius_is_proportional():
    """A 4x weight must be a 4x AREA, i.e. a 2x radius -- not a 4x radius.

    Radius-proportional symbols are the classic lie of this chart type: on
    Uganda 2013-14 the cluster weights span 197x, which as radius would read
    as ~38,000x by area.
    """
    from lsms_library.visualizations import _radius_by_area

    r, n_floored = _radius_by_area([1.0, 4.0], r_max=10.0, r_floor=0.0)
    assert r[1] == pytest.approx(10.0)
    assert r[1] / r[0] == pytest.approx(2.0), "radius should go as sqrt(value)"
    assert n_floored == 0


def test_aspect_is_corrected_for_latitude():
    """`equal` aspect is only right on the equator."""
    import numpy as np

    eq = coordinate_map(_geo(lat0=0.0), interactive=False)
    far = coordinate_map(_geo(lat0=45.0), interactive=False)
    assert eq.get_aspect() == pytest.approx(1.0, abs=0.02)
    assert far.get_aspect() == pytest.approx(1 / np.cos(np.radians(45.9)), rel=0.05)
    assert far.get_aspect() > eq.get_aspect()


def test_rows_without_coordinates_are_counted_not_silently_dropped():
    df = _geo(10)
    df.loc[0:2, "Latitude"] = pd.NA
    ax = coordinate_map(df, interactive=False)
    assert len(ax.collections[0].get_offsets()) == 7
    assert "7 points" in _all_text(ax)


def test_missing_coordinate_column_raises():
    with pytest.raises(KeyError, match="Latitude"):
        coordinate_map(pd.DataFrame({"x": [1]}), interactive=False)


def test_interactive_map_carries_a_basemap_and_markers():
    """The interactive path is the real map: tiles give shapes and place names."""
    folium = pytest.importorskip("folium")
    m = coordinate_map(_geo(), size="weight")
    html = m.get_root().render()
    assert "tile.openstreetmap.org" in html, "no basemap tile layer"
    assert "circlemarker" in html.lower()
    assert "positions approximate" in html, "provenance caption missing"


def test_interactive_without_folium_says_how_to_fix_it(monkeypatch):
    import builtins
    real = builtins.__import__

    def no_folium(name, *a, **k):
        if name == "folium":
            raise ImportError("no folium")
        return real(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", no_folium)
    with pytest.raises(ImportError, match="pip install folium"):
        coordinate_map(_geo(), interactive=True)


def test_the_size_floor_is_counted_and_disclosed():
    """The floor breaks the proportionality the caption promises, so say so.

    Measured on Uganda 2013-14: weights span 197x, and within a 14px budget
    188 of 620 markers (30%) fall below the 2px floor, overstating their area
    by up to 4x.  A caption reading "area proportional to weight" while that
    is untrue for a third of the markers is exactly the quiet falsehood this
    module exists to avoid.
    """
    from lsms_library.visualizations import _radius_by_area

    _, n = _radius_by_area([1.0, 400.0], r_max=14.0, r_floor=2.0)
    assert n == 1, "a 400x span must floor the small marker"

    df = _geo(12)
    df.loc[:5, "weight"] = 0.01
    df.loc[6:, "weight"] = 100.0
    ax = coordinate_map(df, size="weight", interactive=False)
    assert "at the minimum size" in _all_text(ax)


def test_log_scale_says_it_is_log_and_needs_no_floor():
    from lsms_library.visualizations import _radius_by_area

    _, n = _radius_by_area([1.0, 400.0], r_max=14.0, r_floor=2.0, scale="log")
    assert n == 0, "log compresses enough that nothing needs flooring"

    df = _geo(10)
    ax = coordinate_map(df, size="weight", interactive=False, scale="log")
    txt = _all_text(ax)
    assert "log(weight)" in txt
    assert "at the minimum size" not in txt


def test_bad_scale_rejected():
    from lsms_library.visualizations import _radius_by_area

    with pytest.raises(ValueError, match="scale must be"):
        _radius_by_area([1.0], scale="sqrt")


# --- coordinate_map(where=) ------------------------------------------------

def _clusters():
    return pd.DataFrame({
        "v": ["c1", "c2", "c3", "c4", "c5"],
        "Region": ["bafata", "bafata", "oio", "oio", "sab"],
        "Rural": ["Rural", "Urban", "Rural", "Rural", "Urban"],
        "Latitude": [12.1, 12.2, 12.0, None, 11.9],
        "Longitude": [-14.6, -14.7, -15.0, -15.1, -15.6],
        "weight": [10.0, 20.0, 5.0, 7.0, 40.0],
    })


def test_where_dict_restricts_titles_and_counts_dropped_within_the_selection():
    from lsms_library.visualizations import coordinate_map
    ax = coordinate_map(_clusters(), size="weight", interactive=False,
                        where={"Region": "oio"})
    assert "Region = oio" in ax.get_title(loc="left")
    # c3 drawn, c4 has no latitude: one point, one dropped -- both stated.
    texts = " ".join(t.get_text() for t in ax.texts)
    assert "1 points" in texts and "1 cluster(s) without coordinates" in texts
    assert len(ax.collections[0].get_offsets()) == 1


def test_where_accepts_a_list_and_a_callable():
    from lsms_library.visualizations import coordinate_map
    ax = coordinate_map(_clusters(), interactive=False,
                        where={"Region": ["bafata", "sab"]})
    assert len(ax.collections[0].get_offsets()) == 3
    assert "Region in {bafata, sab}" in ax.get_title(loc="left")
    ax = coordinate_map(_clusters(), interactive=False,
                        where=lambda df: df.Rural == "Urban")
    assert len(ax.collections[0].get_offsets()) == 2


def test_where_that_matches_nothing_names_the_values_the_column_holds():
    from lsms_library.visualizations import coordinate_map
    with pytest.raises(ValueError, match=r"Region in \['Bafata'\].*bafata, oio, sab"):
        coordinate_map(_clusters(), interactive=False, where={"Region": "Bafata"})
    with pytest.raises(KeyError, match="not a column"):
        coordinate_map(_clusters(), interactive=False, where={"strata": "x"})
    with pytest.raises(TypeError, match="where= takes"):
        coordinate_map(_clusters(), interactive=False, where="Region == 'oio'")


# --- lorenz_curve ----------------------------------------------------------

import re  # noqa: E402

import numpy as np  # noqa: E402
from matplotlib.collections import PolyCollection  # noqa: E402

from lsms_library.visualizations import _MISSING_MPL, _lorenz, lorenz_curve  # noqa: E402


def _households(n=300, t="2020", seed=0):
    """A household-grain frame with the columns the frame path reads."""
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "i": [f"h{k}" for k in range(n)], "t": t,
        "value": np.round(rng.lognormal(3, 0.8, n), 2),
        "size": rng.integers(1, 9, n).astype(float),
        "weight": rng.uniform(0.5, 2.0, n),
        "Rural": np.where(np.arange(n) % 3 == 0, "Urban", "Rural"),
        "Region": np.array(["N", "S", "E", "W"])[np.arange(n) % 4],
    }).set_index(["i", "t"])


def _long(n=50, t="2020"):
    """The same households as expenditure-shaped rows: (i, t, v, j, s) x Expenditure."""
    h = _households(n, t).reset_index()
    rows = [{"i": r.i, "t": r.t, "v": "c0", "j": f"item{j}", "s": "purchased",
             "Expenditure": r.value / 3}
            for _, r in h.iterrows() for j in range(3)]
    return pd.DataFrame(rows).set_index(["i", "t", "v", "j", "s"])


def _fills(ax):
    return [c for c in ax.collections if isinstance(c, PolyCollection)]


def _curve(ax, k=0):
    """The k-th drawn curve; ``ax.lines[0]`` is always the diagonal."""
    return ax.lines[1 + k]


def test_lorenz_equal_values_lie_on_the_diagonal():
    F, L, gini = _lorenz([3.0] * 5, [1.0] * 5)
    assert gini == pytest.approx(0.0)
    assert np.allclose(F, L)
    assert F[0] == 0 and F[-1] == pytest.approx(1.0) and L[-1] == pytest.approx(1.0)


def test_lorenz_one_household_with_everything():
    """Gini is 1 - w_that / sum(w): the trapezoid rule gives it exactly."""
    F, L, gini = _lorenz([0, 0, 0, 5.0], [1, 1, 1, 3.0])
    assert gini == pytest.approx(1 - 3 / 6)
    assert np.allclose(L[:-1], 0) and L[-1] == pytest.approx(1.0)


def test_lorenz_duplicate_row_equals_doubled_weight():
    a = _lorenz([1, 2, 2, 3], [1, 1, 1, 1])
    b = _lorenz([1, 2, 3], [1, 2, 1])
    assert np.array_equal(a[0], b[0]) and np.array_equal(a[1], b[1])
    assert a[2] == pytest.approx(b[2])


def test_lorenz_rejects_negative_and_all_zero():
    with pytest.raises(ValueError, match="negative value"):
        _lorenz([1.0, -1.0], [1.0, 1.0])
    with pytest.raises(ValueError, match="every value is zero"):
        _lorenz([0.0, 0.0], [1.0, 1.0])
    with pytest.raises(ValueError, match="negative weight"):
        _lorenz([1.0, 1.0], [1.0, -1.0])


def test_lorenz_takes_nullable_floats_and_drops_zero_weights():
    """sample() weights are pandas Float64; a zero weight cannot move the curve."""
    w = pd.Series([1.0, 0.0, 1.0], dtype="Float64")
    F, L, gini = _lorenz(pd.Series([1.0, 100.0, 3.0]), w)
    assert len(F) == 3            # 0 + two positive-weight households
    assert gini == pytest.approx(_lorenz([1.0, 3.0], [1.0, 1.0])[2])


def test_household_frame_draws_curve_diagonal_and_fill():
    ax = lorenz_curve(_households(), value="value", weights="weight")
    assert ax.__class__.__name__.startswith("Axes")
    assert len(ax.lines) == 2, "one curve plus the diagonal, nothing else"
    assert len(_fills(ax)) == 1, "the single-series gap is shaded"
    assert ax.get_xlim() == (0.0, 1.0) and ax.get_ylim() == (0.0, 1.0)
    assert ax.get_xlabel() == "share of people, poorest first"
    assert ax.get_ylabel() == "share of value"
    assert ax.get_legend() is None, "a lone series needs no legend"


def test_two_series_draw_no_fill():
    """Overlapping translucent fills make a third colour; two curves get none."""
    ax = lorenz_curve(_households(), value="value", weights="weight", by="Rural")
    assert len(ax.lines) == 3
    assert not _fills(ax)


def test_per_person_and_per_household_differ_unless_sizes_are_one():
    h = _households()
    person = lorenz_curve(h, value="value", weights="weight", per="person")
    household = lorenz_curve(h, value="value", weights="weight", per="household")
    assert not np.array_equal(_curve(person).get_ydata(), _curve(household).get_ydata())
    assert person.get_xlabel().startswith("share of people")
    assert household.get_xlabel().startswith("share of households")

    ones = h.assign(size=1.0)
    a = lorenz_curve(ones, value="value", weights="weight", per="person")
    b = lorenz_curve(ones, value="value", weights="weight", per="household")
    assert np.allclose(_curve(a).get_ydata(), _curve(b).get_ydata())


def test_per_person_without_a_size_column_raises():
    with pytest.raises(ValueError, match="needs a household-size column"):
        lorenz_curve(_households().drop(columns="size"), value="value", weights=False)
    # per='household' needs no size at all
    lorenz_curve(_households().drop(columns="size"), value="value", weights=False,
                 per="household")


def test_item_level_frame_with_per_person_raises_early():
    """A long frame cannot carry a size; say so instead of dying on KeyError."""
    with pytest.raises(ValueError, match="item-level frame .* has none; pass per='household'"):
        lorenz_curve(_long(50), weights=False)


def test_expenditure_shaped_frame_is_summed_to_household_grain():
    a = lorenz_curve(_long(50), weights=False, per="household")
    b = lorenz_curve(_households(50), value="value", weights=False, per="household")
    assert np.allclose(_curve(a).get_xdata(), _curve(b).get_xdata())
    assert np.allclose(_curve(a).get_ydata(), _curve(b).get_ydata())
    assert a.get_ylabel() == "share of spending"


def test_household_frame_without_expenditure_asks_for_value():
    with pytest.raises(ValueError, match="pass value="):
        lorenz_curve(_households(), weights=False)


def test_by_draws_bar_and_alt_with_gini_in_each_legend_entry():
    ax = lorenz_curve(_households(), value="value", weights="weight", by="Rural")
    assert [_curve(ax, k).get_color() for k in range(2)] == [PALETTE["bar"], PALETTE["alt"]]
    labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert all(re.search(r", Gini \d\.\d\d$", s) for s in labels), labels
    assert {s.split(",")[0] for s in labels} == {"Rural", "Urban"}
    assert not ax.get_legend().get_title().get_text(), "legend title repeats a value"
    assert "Gini" not in _all_text(ax), "with a legend, the Gini lives there, not in the subtitle"


def test_by_with_three_or_more_values_raises_naming_them():
    with pytest.raises(ValueError, match=r"holds 4 values \(N, S, E, W\)"):
        lorenz_curve(_households(), value="value", weights=False, by="Region")


def test_by_tuple_picks_two_titles_the_legend_and_counts_the_rest():
    ax = lorenz_curve(_households(), value="value", weights=False, by=("Region", ["N", "S"]))
    labels = [t.get_text().split(",")[0] for t in ax.get_legend().get_texts()]
    assert labels == ["N", "S"]
    assert ax.get_legend().get_title().get_text() == "Region"
    assert "150 outside the two Region values picked, not drawn" in _all_text(ax)
    with pytest.raises(ValueError, match="has no value"):
        lorenz_curve(_households(), value="value", weights=False, by=("Region", ["N", "Z"]))


def test_wave_list_is_an_ordered_ramp_darkening_toward_the_most_recent():
    from matplotlib.colors import to_rgb

    multi = pd.concat([_households(100, "2010"), _households(100, "2015", seed=1),
                       _households(100, "2020", seed=2)])
    ax = lorenz_curve(multi, wave=["2010", "2015", "2020"], value="value", weights="weight")
    assert len(ax.lines) == 4 and not _fills(ax)
    light = [sum(to_rgb(_curve(ax, k).get_color())) for k in range(3)]
    assert light[0] > light[1] > light[2], "earlier waves must be lighter"
    assert to_rgb(_curve(ax, 2).get_color()) == pytest.approx(to_rgb(PALETTE["bar"]))
    labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert [s.split(",")[0] for s in labels] == ["2010", "2015", "2020"]
    assert all("Gini" in s for s in labels)
    assert ax.get_title(loc="left") == "households"


def test_wave_list_with_by_and_too_many_waves_raise():
    multi = pd.concat([_households(20, str(y)) for y in range(2000, 2006)])
    with pytest.raises(ValueError, match="cannot be combined"):
        lorenz_curve(multi, wave=["2000", "2001"], value="value", weights=False, by="Rural")
    with pytest.raises(ValueError, match="at most 5"):
        lorenz_curve(multi, wave=[str(y) for y in range(2000, 2006)], value="value",
                     weights=False)
    with pytest.raises(ValueError, match="has no wave"):
        lorenz_curve(multi, wave=["2000", "2099"], value="value", weights=False)


def test_multi_wave_frame_defaults_to_the_most_recent_with_a_warning():
    multi = pd.concat([_households(50, "2010"), _households(50, "2020")])
    with pytest.warns(UserWarning, match="drawing the most recent"):
        ax = lorenz_curve(multi, value="value", weights=False)
    assert ax.get_title(loc="left") == "2020"


def test_frame_path_rejects_zeros_and_basis():
    with pytest.raises(TypeError, match="zeros= needs a Country"):
        lorenz_curve(_households(), value="value", weights=False, zeros="include")
    with pytest.raises(TypeError, match="basis= needs a Country"):
        # value=None (the frame carries Expenditure) so the frame check is reached
        lorenz_curve(_households().rename(columns={"value": "Expenditure"}),
                     weights=False, basis="total")
    with pytest.raises(TypeError, match="basis= applies only"):
        # checked before the frame/Country split: basis has no meaning off food
        lorenz_curve(_households(), value="value", weights=False, basis="purchased")


def test_subtitle_states_basis_and_weighting_and_the_half_label_matches_the_curve():
    ax = lorenz_curve(_households(), value="value", weights="weight")
    text = _all_text(ax)
    assert "value per person" in text and "weighted by weight" in text
    assert re.search(r"Gini \d\.\d\d", text)
    assert "300 households" in text
    F, L = _curve(ax).get_xdata(), _curve(ax).get_ydata()
    half = float(np.interp(0.5, F, L))
    label = next(t.get_text() for t in ax.texts if "poorest half" in t.get_text())
    assert f"{half:.0%} of value" in label
    assert "poorest half of people" in label

    unweighted = lorenz_curve(_households(), value="value", weights=False, per="household")
    assert "unweighted" in _all_text(unweighted)
    assert "poorest half of households" in _all_text(unweighted)


def test_lorenz_missing_weights_warn_rather_than_fail():
    """Same fallback as the pyramid, but a Lorenz curve draws no 'counts'."""
    with pytest.warns(UserWarning, match="no sampling weights available; proceeding unweighted"):
        lorenz_curve(_households().drop(columns="weight"), value="value")


def test_lorenz_unknown_by_names_the_frame_not_a_roster():
    with pytest.raises(KeyError, match="neither a household-frame column nor"):
        lorenz_curve(_households(), value="value", weights=False, by="NotAColumn")


def test_lorenz_rejects_a_non_frame():
    with pytest.raises(TypeError, match="Country, a country name"):
        lorenz_curve(42)


def test_matplotlib_error_names_its_caller():
    """The shared message is a template now; each chart fills in its own name."""
    assert _MISSING_MPL.format(caller="lorenz_curve").startswith("lorenz_curve() needs")
    assert "population_pyramid" not in _MISSING_MPL


def test_nan_values_are_counted_not_silently_dropped():
    h = _households(30)
    h.iloc[:5, h.columns.get_loc("value")] = np.nan
    ax = lorenz_curve(h, value="value", weights=False)
    text = _all_text(ax)
    assert "25 households" in text and "5 with no usable value, not drawn" in text

    # An all-NaN household in an item-level frame must stay NaN (min_count=1),
    # so it lands in the same disclosure rather than becoming a silent zero.
    long = _long(20)
    hh0 = long.index.get_level_values("i") == "h0"
    long.loc[hh0, "Expenditure"] = np.nan
    ax = lorenz_curve(long, weights=False, per="household")
    assert "19 households" in _all_text(ax) and "1 with no usable value" in _all_text(ax)


def test_duplicate_household_keys_raise_rather_than_double_count():
    h = _households(30)
    dup = pd.concat([h, h])
    with pytest.raises(ValueError, match=r"30 duplicated \('i', 't'\) key"):
        lorenz_curve(dup, value="value", weights=False)


def test_by_with_a_single_value_is_the_single_series_case():
    h = _households(30).assign(Rural="Rural")
    ax = lorenz_curve(h, value="value", weights=False, by="Rural")
    assert len(ax.lines) == 2 and len(_fills(ax)) == 1
    assert ax.get_legend() is None
    text = _all_text(ax)
    assert re.search(r"Gini \d\.\d\d", text) and "by=Rural had one value (Rural)" in text
    assert any("poorest half" in t.get_text() for t in ax.texts)


def test_by_column_with_no_values_raises_actionably():
    h = _households(30).assign(Rural=pd.NA)
    with pytest.raises(ValueError, match="has no non-null value"):
        lorenz_curve(h, value="value", weights=False, by="Rural")


def test_per_person_weighting_is_exactly_size_times_weight():
    """per='person' draws _lorenz(x/size, size*w); per='household' _lorenz(x, w)."""
    h = _households(80)
    x, size, w = (h[c].to_numpy(dtype=float) for c in ("value", "size", "weight"))
    person = lorenz_curve(h, value="value", weights="weight", per="person")
    F, L, gini = _lorenz(x / size, size * w)
    assert np.array_equal(_curve(person).get_xdata(), F)
    assert np.array_equal(_curve(person).get_ydata(), L)
    assert f"Gini {gini:.2f}" in _all_text(person)
    household = lorenz_curve(h, value="value", weights="weight", per="household")
    F, L, gini = _lorenz(x, w)
    assert np.array_equal(_curve(household).get_xdata(), F)
    assert np.array_equal(_curve(household).get_ydata(), L)
    assert f"Gini {gini:.2f}" in _all_text(household)


def _gini_of_line(line):
    """Trapezoid Gini of a drawn polyline -- exact, so two charts can be compared
    below the two-decimal rounding of the printed number."""
    F, L = np.asarray(line.get_xdata(), float), np.asarray(line.get_ydata(), float)
    return 1.0 - float(np.sum(np.diff(F) * (L[1:] + L[:-1])))


@pytest.mark.slow
def test_uganda_lorenz_gini_and_zero_count_are_recomputed_not_pinned():
    """Uganda 2013-14 defaults, checked against an independent computation.

    The Gini is recomputed from ``food_expenditures``,
    ``household_characteristics`` and ``sample`` with the mean-absolute-
    difference formula -- a different formula from the trapezoid rule the
    chart uses, on the same data -- and the no-purchase count from the two
    household sets.  A pinned 0.50 would test the cache, not the wiring.
    """
    ll = pytest.importorskip("lsms_library")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c = ll.Country("Uganda")
            fe = c.food_expenditures(waves=["2013-14"])
            hc = c.household_characteristics(waves=["2013-14"])
            smp = c.sample()
            ax = lorenz_curve(c, wave="2013-14")
    except Exception as exc:                      # pragma: no cover
        pytest.skip(f"Uganda unavailable: {exc}")

    x = fe["Expenditure"].groupby(level=["i", "t"]).sum()
    size = np.exp(hc["log HSize"]).round()
    size.index = size.index.droplevel("v")
    size = size.reorder_levels(["i", "t"])
    wave = smp[smp.index.get_level_values("t") == "2013-14"]
    # in sample, no expenditure row -- over ALL sample households of the wave,
    # null weights included, as the chart counts them
    k = len(wave.index.difference(x.index))
    w = pd.to_numeric(wave["weight"], errors="coerce").dropna()

    df = pd.concat([x.rename("x"), size.rename("size"), w.rename("w")],
                   axis=1, join="inner")
    df = df[(df["size"] > 0) & (df["w"] > 0)]
    y = (df["x"] / df["size"]).to_numpy(dtype=float)
    ww = (df["w"] * df["size"]).to_numpy(dtype=float)
    W, mu = ww.sum(), (ww * y).sum() / ww.sum()
    gini = (ww[:, None] * ww[None, :] * np.abs(y[:, None] - y[None, :])).sum() / (2 * W ** 2 * mu)

    text = _all_text(ax)
    printed = float(re.search(r"Gini (\d\.\d\d)", text).group(1))
    assert printed == pytest.approx(gini, abs=0.01)
    assert f"{k:,} with no recorded purchase, not drawn" in text
    assert "food purchases per person" in text
    assert "weighted by the survey's sampling weights" in text
    assert "Represents:" in " ".join(t.get_text() for t in ax.figure.texts)
    n_drawn = int(re.search(r"([\d,]+) households", text).group(1).replace(",", ""))

    # zeros='include': the k households join at zero, the curve sags, Gini rises.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inc = lorenz_curve(c, wave="2013-14", zeros="include")
    itext = _all_text(inc)
    assert f"{k:,} with no recorded purchase, drawn at zero" in itext
    assert f"{n_drawn + k:,} households" in itext
    assert _gini_of_line(_curve(inc)) > _gini_of_line(_curve(ax))
    assert _curve(inc).get_ydata()[1] == 0.0, "the first vertex after the origin sits at zero"


def test_a_wide_item_by_household_table_raises_citing_gh817():
    """A one-column-per-item table must not be summed into a plausible curve.

    This is the guard itself, on a synthetic wide frame, so it keeps working
    whatever the corpus does.  It used to be exercised through Uganda's real
    `nonfood_expenditures`, which WAS wide (41-96 item columns, `m` baked into
    the index).  GH #817 made every declarer long, so Uganda no longer
    triggers it -- see the test below -- but the guard must stay: the next
    country to land a pivoted table would otherwise get a correct-looking
    curve drawn over a broken shape.
    """
    from lsms_library.visualizations import _country_measure

    wide = pd.DataFrame(
        {"Soap": [10.0, 20.0], "Charcoal": [5.0, 7.0]},
        index=pd.MultiIndex.from_tuples([("h1", "2020"), ("h2", "2020")],
                                        names=["i", "t"]),
    )

    class _FakeCountry:
        name = "Nowhere"
        data_scheme = ["nonfood_expenditures"]

        @staticmethod
        def nonfood_expenditures(waves=None):
            return wide

    with pytest.raises(ValueError) as excinfo:
        _country_measure(_FakeCountry(), None, "nonfood_expenditures", "person")
    msg = str(excinfo.value)
    assert "#817" in msg and "Expenditure" in msg and "columns" in msg


@pytest.mark.slow
def test_uganda_nonfood_table_is_long_and_draws_since_gh817():
    """The other half of #817: Uganda's own table is no longer the wide one.

    `nonfood_expenditures` is now `(t, i, j) x Expenditure` for every declarer,
    so the chart that used to refuse it draws it.  Asserting this here is what
    stops a re-pivot from being silently reintroduced -- the guard above would
    start firing again and only this test would notice.
    """
    pytest.importorskip("lsms_library")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ax = lorenz_curve("Uganda", wave="2013-14",
                              value="nonfood_expenditures")
    except ValueError as exc:                     # pragma: no cover
        if "#817" in str(exc):
            pytest.fail(
                "Uganda.nonfood_expenditures is wide again -- GH #817 fixed "
                f"it to (t, i, j) x Expenditure.  {exc}")
        raise
    except Exception as exc:                      # pragma: no cover
        pytest.skip(f"Uganda unavailable: {exc}")
    assert "nonfood_expenditures" in _all_text(ax)


@pytest.mark.slow
def test_nigeria_default_wave_is_the_most_recent_the_measure_holds():
    """Country.waves ends in 2024Q1, which has no food rows; the default must
    come from the measure's own `t` (as the pyramid reads the roster's).

    Runs only against a WARM Nigeria cache.  The credentialed CI data job
    pre-builds Uganda alone before the full suite, so without this gate the
    test would cold-build eight rounds of Nigeria ``food_acquired`` on every
    push to master -- a cost nobody asked this test to incur.  The Uganda
    tests above need no such gate for the same reason.
    """
    ll = pytest.importorskip("lsms_library")
    from lsms_library.paths import data_root
    var = data_root("Nigeria") / "var"
    warm = all((var / f"{t}.parquet").exists()
               for t in ("food_acquired", "household_roster", "sample"))
    if not warm:
        pytest.skip("needs a warm Nigeria cache (food_acquired, household_roster, "
                    "sample); this test never triggers a cold build")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c = ll.Country("Nigeria")
            held = set(map(str, pd.unique(c.food_expenditures().index.get_level_values("t"))))
            ax = lorenz_curve(c)
    except Exception as exc:                      # pragma: no cover
        pytest.skip(f"Nigeria unavailable: {exc}")
    wave = ax.get_title(loc="left").split()[-1]
    assert wave in held, (wave, sorted(held))
    assert wave == max(held)
    assert wave != "2024Q1" and "2024Q1" in c.waves

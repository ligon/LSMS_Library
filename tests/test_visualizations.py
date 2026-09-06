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

from lsms_library.visualizations import PALETTE, population_pyramid  # noqa: E402


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

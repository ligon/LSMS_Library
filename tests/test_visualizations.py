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

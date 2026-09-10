"""`plot_id` is the canonical plot-axis level name (@ligon, 2026-09-10).

`plot_features` already declared `plot_id` in all 23 countries that have it.
This pins the other half of the decision:

* `lsms_library/data_info.yml` registers `crop_production` on `plot_id`, and
  `level_aliases` maps the legacy `plot` ONTO it (not the other way round);
* `Feature('crop_production')` therefore emits a `plot_id` level whichever
  spelling a country's own frame carries;
* every `transformations.py` plot-grain transform ACCEPTS both spellings and
  EMITS the canonical one, with byte-identical values either way.

The last point is the one worth a test: the transforms used to normalise to
`plot`, so a silent regression here would be a schema change nobody sees until
a downstream join comes back empty.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.feature import (
    _canonical_index_levels,
    _harmonize_country_frame,
    _level_aliases,
    _missing_level_sentinels,
)
from lsms_library.transformations import (
    _PLOT_CANONICAL,
    _PLOT_LEVELS,
    _resolve_plot_level,
    fertilizer_rate,
    harvest_kg,
    nitrogen_kg,
    seed_kg,
    yield_kg,
)


# ---------------------------------------------------------------------------
# The declarations
# ---------------------------------------------------------------------------

class TestDeclaredCanonicalName:
    def test_crop_production_is_registered_on_plot_id(self):
        levels = _canonical_index_levels("crop_production")
        assert "plot_id" in levels
        assert "plot" not in levels

    def test_plot_features_already_agreed(self):
        assert "plot_id" in _canonical_index_levels("plot_features")

    def test_the_alias_points_AT_plot_id_not_away_from_it(self):
        """Direction is `{name a country emits: canonical name}`.  Reversing it
        is the whole content of this change, and reads identically at a
        glance -- so it is asserted, not eyeballed."""
        assert _level_aliases("crop_production")["plot"] == "plot_id"

    def test_transform_preference_order_is_canonical_first(self):
        assert _PLOT_CANONICAL == "plot_id"
        assert _PLOT_LEVELS[0] == "plot_id"
        assert set(_PLOT_LEVELS) == {"plot_id", "plot"}

    def test_resolve_accepts_either_spelling_and_a_pandas_index(self):
        assert _resolve_plot_level(["t", "i", "plot"]) == "plot"
        assert _resolve_plot_level(["t", "i", "plot_id"]) == "plot_id"
        assert _resolve_plot_level(pd.Index(["t", "i", "plot"])) == "plot"
        assert _resolve_plot_level(["t", "i"]) is None
        assert _resolve_plot_level(None) is None


# ---------------------------------------------------------------------------
# Feature assembly
# ---------------------------------------------------------------------------

def _frame(names, tuples, **cols):
    idx = pd.MultiIndex.from_tuples(tuples, names=names)
    return pd.DataFrame(cols or {"Quantity": [1.0] * len(tuples)}, index=idx)


CANON = _canonical_index_levels("crop_production")


@pytest.mark.parametrize("spelling", ["plot", "plot_id"])
def test_feature_alignment_emits_plot_id_from_either_spelling(spelling):
    df = _frame(["t", "v", "i", spelling, "j", "u"],
                [("2011", "c1", "h1", "p1", "Maize", "kg")])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = _harmonize_country_frame(
            df, CANON, "X", "crop_production", False,
            aliases=_level_aliases("crop_production"),
            sentinels=_missing_level_sentinels("crop_production"))
    assert list(out.index.names) == CANON
    assert "plot" not in out.index.names
    assert out.index.get_level_values("plot_id").tolist() == ["p1"]


# ---------------------------------------------------------------------------
# The transforms: same numbers, canonical name, from either spelling
# ---------------------------------------------------------------------------

def _crop_production(plot_level):
    return _frame(
        ["t", "i", plot_level, "j", "u"],
        [("2019-20", "hh1", "p1", "Maize", "kg"),
         ("2019-20", "hh1", "p1", "Maize", "Kg"),
         ("2019-20", "hh1", "p2", "Maize", "kg")],
        Quantity=[10.0, 5.0, 3.0])


def _plot_inputs(plot_level):
    return _frame(
        ["t", "i", plot_level, "input", "j", "u"],
        [("2019-20", "hh1", "p1", "Urea", "Maize", "kg"),
         ("2019-20", "hh1", "p2", "Urea", "Maize", "kg")],
        Quantity=[100.0, 50.0])


def _seed_inputs(plot_level):
    return _frame(
        ["t", "i", plot_level, "input", "j", "u"],
        [("2019-20", "hh1", "p1", "Seed", "Maize", "kg")],
        Quantity=[4.0])


def _plot_features(key):
    return _frame(["t", "i", key],
                  [("2019-20", "hh1", "p1"), ("2019-20", "hh1", "p2")],
                  Area=[2.0, 1.0])


def _both(fn):
    """Run ``fn`` on the `plot`-keyed and `plot_id`-keyed inputs."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return fn("plot"), fn("plot_id")


@pytest.mark.parametrize("transform, builder", [
    (harvest_kg, _crop_production),
    (nitrogen_kg, _plot_inputs),
    (seed_kg, _seed_inputs),
])
def test_plot_grain_transforms_emit_plot_id_from_either_spelling(transform,
                                                                 builder):
    legacy, canon = _both(lambda lvl: transform(builder(lvl)))
    for out in (legacy, canon):
        assert "plot_id" in out.index.names
        assert "plot" not in out.index.names
    # Same numbers, not just the same shape.
    pd.testing.assert_frame_equal(legacy, canon)


@pytest.mark.parametrize("on", ["plot", "parcel"])
def test_yield_kg_emits_plot_id_or_parcel_never_plot(on):
    legacy, canon = _both(
        lambda lvl: yield_kg(_crop_production(lvl), _plot_features("plot_id"),
                             on=on))
    expected = "parcel" if on == "parcel" else "plot_id"
    for out in (legacy, canon):
        assert expected in out.index.names
        assert "plot" not in out.index.names
    pd.testing.assert_frame_equal(legacy, canon)


@pytest.mark.parametrize("on", ["plot", "parcel"])
def test_fertilizer_rate_emits_plot_id_or_parcel_never_plot(on):
    legacy, canon = _both(
        lambda lvl: fertilizer_rate(_plot_inputs(lvl),
                                    _plot_features("plot_id"), on=on))
    expected = "parcel" if on == "parcel" else "plot_id"
    for out in (legacy, canon):
        assert expected in out.index.names
        assert "plot" not in out.index.names
    pd.testing.assert_frame_equal(legacy, canon)


def test_plot_features_may_still_be_keyed_either_way():
    """The area side is resolved with the same helper, so a `plot_features`
    that (hypothetically) spelled the axis `plot` still joins."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        a = fertilizer_rate(_plot_inputs("plot_id"), _plot_features("plot_id"),
                            on="plot")
        b = fertilizer_rate(_plot_inputs("plot_id"), _plot_features("plot"),
                            on="plot")
    pd.testing.assert_frame_equal(a, b)


def test_on_is_a_mode_string_not_a_level_name():
    """`on='plot'` must keep working -- it names the JOIN GRAIN, not the level.
    Renaming it would be a gratuitous API break."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = yield_kg(_crop_production("plot_id"), _plot_features("plot_id"),
                       on="plot")
    assert list(out.index.names)[-1] == "plot_id"
    with pytest.raises(ValueError, match="must be 'parcel' or 'plot'"):
        yield_kg(_crop_production("plot_id"), _plot_features("plot_id"),
                 on="plot_id")


# ---------------------------------------------------------------------------
# The corpus-wide config invariant (commit B)
# ---------------------------------------------------------------------------

def test_no_country_declares_a_plot_level_any_more():
    """Every `index:` in every country's `data_scheme.yml` spells the plot
    axis `plot_id`.

    This is the invariant the per-country rename bought, and it is the one a
    future wave or country can silently break -- copying a sibling country's
    script is how `plot` spread in the first place.  Reading the YAML as TEXT
    (rather than through `Country`) keeps it a pure config check with no data
    and no cache.
    """
    import re
    from lsms_library.paths import countries_root

    offenders = []
    for ds in sorted(countries_root().glob("*/_/data_scheme.yml")):
        for k, line in enumerate(ds.read_text().split("\n"), 1):
            m = re.match(r"^\s*index:\s*\((.*)\)\s*$", line)
            if m and "plot" in [x.strip() for x in m.group(1).split(",")]:
                offenders.append(f"{ds.parts[-3]}/_/data_scheme.yml:{k}: {line.strip()}")
    assert not offenders, (
        "these declared indexes still spell the plot axis `plot`; the "
        "canonical name is `plot_id` (@ligon, 2026-09-10):\n  "
        + "\n  ".join(offenders))

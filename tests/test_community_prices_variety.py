"""``community_prices`` serves one row per SURVEY-RECORDED price -- GH #840.

A community price form prices a (product, VARIETY) pair, in a field VISIT, as
one of several within-form OBSERVATIONS.  The old grain ``(t, v, j, u)`` named
none of the three, so Mali, Niger and Tanzania each kept ONE row per key --
by questionnaire row order, or by a "post-harvest first" passage preference --
and dropped the rest without a trace.  Ruling (@ligon, 2026-10-18): Option 1,
add the level so no row is selected; precedent GhanaLSS ``obs`` and the #871
"core selects nothing silently" ruling.

Three tiers, deliberately separate:

* ``TestDeclaration`` -- config only, no microdata.  The canonical index, its
  sentinels, the per-country declarations, and that none of the three
  builders still selects (read with ``ast``, never imported: a country module
  runs data reads at import).
* ``TestAssembly`` -- the ``Feature()`` alignment on synthetic frames shaped
  like the real ones (GhanaLSS ``obs``, Niger ``variety/visit/obs``, a
  ``(t, v, j, u)`` country), so the cross-country contract is pinned on a
  laptop with no S3 credentials.
* ``TestBuilt`` -- the real tables, skipped when they cannot be built.

MEASURED NUMBERS (private cold rebuild, 2026-10-19, on the commit that
introduced them).  Served rows before -> after:

    Mali      2014-15    23,646 -> 40,218
    Niger     2011-12     8,923 -> 32,369
              2014-15     6,500 -> 13,808
    Tanzania  2019-20     2,769 ->  3,076
              2020-21    13,062 -> 14,623

The number of (t, v, j, u) CELLS is unchanged in every wave -- the old table
is a projection of the new one, and every row it served is one of the new
table's rows for that cell.
"""
from __future__ import annotations

import ast
import warnings
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

DATA_INFO = Path(__file__).resolve().parents[1] / "lsms_library" / "data_info.yml"

CANONICAL = ["t", "v", "j", "u", "variety", "visit", "obs"]

# What each country's own table carries (data_scheme.yml `index:`).
DECLARED = {
    "Mali": ["t", "v", "j", "u", "variety", "visit"],
    "Niger": ["t", "v", "j", "u", "variety", "visit", "obs"],
    "Tanzania": ["t", "v", "j", "u", "variety"],
}

# (file, function) that used to select one row per (t, v, j, u).
BUILDERS = {
    "Mali": ("Mali/_/mali.py", "community_prices_finalize"),
    "Niger": ("Niger/_/niger.py", "_finish_community_prices"),
    "Tanzania": ("Tanzania/_/tanzania.py", "community_prices_for_wave"),
}

# Measured served rows by wave (see the module docstring), and the (t, v, j, u)
# cell counts the pre-#840 tables served -- which the new tables must keep.
EXPECTED_ROWS = {
    "Mali": {"2014-15": 40218},
    "Niger": {"2011-12": 32369, "2014-15": 13808},
    "Tanzania": {"2019-20": 3076, "2020-21": 14623},
}
EXPECTED_CELLS = {
    "Mali": {"2014-15": 23646},
    "Niger": {"2011-12": 8923, "2014-15": 6500},
    "Tanzania": {"2019-20": 2769, "2020-21": 13062},
}


def _index_info():
    with open(DATA_INFO, encoding="utf-8") as f:
        return yaml.safe_load(f)["Index Info"]


def _scheme_index(country):
    scheme = load_yaml(countries_root() / country / "_" / "data_scheme.yml")
    spec = scheme["Data Scheme"]["community_prices"]["index"]
    return [tok.strip() for tok in spec.strip("() ").split(",")]


def _function_source(relpath, name):
    path = countries_root() / relpath
    src = path.read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(src, node)
    raise AssertionError(f"{name} not found in {relpath}")


class TestDeclaration:
    def test_canonical_index_registers_the_three_levels(self):
        spec = _index_info()["index_info"]["community_prices"]
        assert [t.strip() for t in spec.strip("()").split(",")] == CANONICAL

    def test_every_added_level_has_a_sentinel(self):
        sentinels = _index_info()["missing_level_sentinels"]["community_prices"]
        assert set(sentinels) == {"variety", "visit", "obs"}
        # Integers, so the assembled level stays integer-typed beside the
        # countries that carry a native visit/obs (GhanaLSS, Niger, Mali).
        assert sentinels["visit"] == 1 and isinstance(sentinels["visit"], int)
        assert sentinels["obs"] == 1 and isinstance(sentinels["obs"], int)
        # The condition/season spelling, NOT the unit sentinel `Unknown`.
        assert sentinels["variety"] == "unknown_variety"

    def test_not_a_nan_fabrication(self):
        # A NULL on a declared index level is a deferred silent deletion.
        assert "community_prices" not in (_index_info().get("fabricate_missing_levels") or [])

    @pytest.mark.parametrize("country", sorted(DECLARED))
    def test_country_declares_its_levels(self, country):
        assert _scheme_index(country) == DECLARED[country]

    @pytest.mark.parametrize("country", sorted(BUILDERS))
    def test_builder_selects_nothing(self, country):
        src = _function_source(*BUILDERS[country])
        for selector in ("drop_duplicates(", ".first()", ".head(", ".nth("):
            assert selector not in src, (
                f"{country}'s community_prices builder calls {selector!r}: "
                "GH #840 says no row is selected -- a duplicate key is an error")


class TestAssembly:
    """`Feature()`'s per-country alignment against the declared sentinels."""

    @staticmethod
    def _frame(levels, rows):
        idx = pd.MultiIndex.from_tuples([r[:-1] for r in rows], names=levels)
        return pd.DataFrame({"Price": [r[-1] for r in rows]}, index=idx)

    def _align(self, df, country):
        from lsms_library.feature import (_canonical_index_levels,
                                          _harmonize_country_frame,
                                          _missing_level_sentinels)
        levels = _canonical_index_levels("community_prices")
        assert levels == CANONICAL
        report = {}
        out = _harmonize_country_frame(
            df, levels, country, "community_prices",
            sentinels=_missing_level_sentinels("community_prices"),
            alignment_report=report)
        return out, report

    def test_ghanalss_obs_is_kept_not_collapsed(self):
        # Before #840 GhanaLSS was the modal-excluded minority; registering a
        # canonical index WITHOUT obs would have dropped and collapsed its three
        # vendor observations instead.
        df = self._frame(["t", "v", "j", "u", "obs"], [
            ("2016-17", "1", "Maize", "Kg", 1, 3.0),
            ("2016-17", "1", "Maize", "Kg", 2, 3.5),
            ("2016-17", "1", "Maize", "Kg", 3, 4.0),
        ])
        out, report = self._align(df, "GhanaLSS")
        assert list(out.index.names) == CANONICAL
        assert len(out) == 3 and out.index.is_unique
        assert report["fabricated"]["GhanaLSS"] == {"variety": "unknown_variety", "visit": 1}

    def test_reduced_country_gets_sentinels(self):
        df = self._frame(["t", "v", "j", "u"], [("2019-20", "1", "Maize", "Kg", 9.0)])
        out, _ = self._align(df, "Malawi")
        assert list(out.index.names) == CANONICAL
        row = out.index[0]
        assert row[4:] == ("unknown_variety", 1, 1)

    def test_assembled_levels_stay_integer(self):
        from lsms_library.feature import _select_kept_shape
        ghana, _ = self._align(self._frame(["t", "v", "j", "u", "obs"], [
            ("2016-17", "1", "Maize", "Kg", 2, 3.0)]), "GhanaLSS")
        niger, _ = self._align(self._frame(CANONICAL, [
            ("2011-12", "1", "Mil", "Kg", "7061", 2, 4, 250.0)]), "Niger")
        malawi, _ = self._align(self._frame(["t", "v", "j", "u"], [
            ("2019-20", "1", "Maize", "Kg", 9.0)]), "Malawi")
        frames = [pd.concat({c: f}, names=["country"])
                  for c, f in (("GhanaLSS", ghana), ("Niger", niger), ("Malawi", malawi))]
        kept, dropped, _ = _select_kept_shape(frames, CANONICAL)
        assert not dropped
        out = pd.concat(kept)
        for lvl in ("visit", "obs"):
            assert pd.api.types.is_integer_dtype(out.index.get_level_values(lvl)), lvl


def _build(country):
    import lsms_library as ll
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country(country).community_prices()
    except Exception as e:  # pragma: no cover - data not available
        pytest.skip(f"{country} community_prices not buildable here: {e}")
    if df is None or len(df) == 0:
        pytest.skip(f"{country} community_prices is empty here")
    return df


@pytest.mark.requires_s3
class TestBuilt:
    @pytest.mark.parametrize("country", sorted(DECLARED))
    def test_index_is_the_declared_one_and_unique(self, country):
        df = _build(country)
        assert list(df.index.names) == DECLARED[country]
        assert df.index.is_unique
        assert "Description" in df.columns

    @pytest.mark.parametrize("country", sorted(DECLARED))
    def test_no_null_on_an_added_level(self, country):
        df = _build(country)
        for lvl in DECLARED[country][4:]:
            assert not df.index.get_level_values(lvl).isna().any(), lvl

    @pytest.mark.parametrize("country", sorted(DECLARED))
    def test_variety_is_a_code_not_a_float_string(self, country):
        v = _build(country).index.get_level_values("variety")
        assert v.str.fullmatch(r"\d+").all(), v[~v.str.fullmatch(r"\d+")].unique()[:5]

    @pytest.mark.parametrize("country", sorted(DECLARED))
    def test_rows_and_cells_are_the_measured_ones(self, country):
        df = _build(country)
        t = df.index.get_level_values("t")
        assert pd.Series(1, index=t).groupby(level=0).size().to_dict() == EXPECTED_ROWS[country]
        cells = (df.reset_index()[["t", "v", "j", "u"]].drop_duplicates()
                   .groupby("t").size().to_dict())
        assert cells == EXPECTED_CELLS[country], (
            "the (t, v, j, u) cells the pre-#840 table served must all still be "
            "present -- the old table is a projection of this one")

    def test_mali_tomato_paste_varieties_survive(self):
        # CONTENTS.org's instance: three varieties under u='Boite' at very
        # different prices; the old build kept whichever came first.
        x = _build("Mali").xs(("Concentré de tomate", "Boite"), level=("j", "u"))
        med = x.groupby(level="variety")["Price"].median()
        assert {"5451", "5452"} <= set(med.index)
        assert med["5452"] > 3 * med["5451"]   # 220 g tin vs 60 g tin
        assert (x.groupby(level=["t", "v", "visit"]).size() == 3).any()

    def test_mali_carries_both_passages(self):
        visits = set(_build("Mali").index.get_level_values("visit"))
        assert visits == {1, 2}

    def test_tanzania_lumped_codes_keep_their_own_prices(self):
        # The #637 comment's instance: one j, two source codes, two prices.
        df = _build("Tanzania")
        cell = df.loc[("2020-21", "01-06-47-85", "Millet & Sorghum (grain)", "Kg")]
        assert cell["Price"].to_dict() == {"6": 2000.0, "7": 16000.0}

    def test_niger_second_records_are_slots_four_to_six(self):
        df = _build("Niger")
        obs = df.index.get_level_values("obs")
        assert set(obs) <= {1, 2, 3, 4, 5, 6}
        late = df[obs > 3]
        assert late.groupby(level="t").size().to_dict() == {"2011-12": 32, "2014-15": 132}
        # 2014-15's second records are one grappe's second questionnaire.
        assert set(late.xs("2014-15", level="t").index.get_level_values("v")) == {"212"}

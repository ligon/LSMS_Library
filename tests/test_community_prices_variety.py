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

    Mali      2014-15    23,646 -> 40,218 (GH #840), then 155,931 (GH #822)
    Niger     2011-12     8,923 -> 32,352   (32,369 before the r1 fix below)
              2014-15     6,500 -> 13,691   (13,808 before the r1 fix below)
    Tanzania  2019-20     2,769 ->  3,076
              2020-21    13,062 -> 14,623

Niger's second source records are duplicate ENTRIES of one questionnaire
(2011-12 passage 2 re-ships 150 grappes; 2014-15 grappe 212 has two
``numquest``), not second readings.  The first cut enumerated every one of
their slots as ``obs`` 4-6, so 134 served rows (2011-12 17, 2014-15 117)
repeated an already-served row in every column but ``obs`` (red team r1);
``_community_price_triples`` now drops those, and the 30 slots that repeat
nothing stay as ``obs`` 4-6.

GH #840 kept the number of (t, v, j, u) CELLS unchanged. GH #822 restores
Mali readings 1/2 and expands those cells 23,646 -> 39,245 while retaining
every old cell and every reading-3 value (now obs=3). Its source census is
recorded in slurm_logs/isa_backlog_2026-10-21/Mali/822/.

GH #823 adds Niger 2018-19 (EHCVM s05, 84,444 rows, 42,087 cells) without
touching a 2011-12 / 2014-15 row.  s05 has no variety field (the canonical
``unknown_variety``) and TWO readings per record, so its second records are
``obs`` 3-4, not 4-6; the CS07 tests below are scoped to the CS07 waves and
``tests/test_gh823_niger_s05_community_prices.py`` pins the s05 rules.
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
    "Mali": ["t", "v", "j", "u", "variety", "visit", "obs"],
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
# cell counts after the source-accounted fixes (Mali expanded again in #822).
EXPECTED_ROWS = {
    "Mali": {"2014-15": 155931},
    # 32,369 / 13,808 before the r1 duplicate-entry drop (-17 / -117).
    "Niger": {"2011-12": 32352, "2014-15": 13691, "2018-19": 84444},
    "Tanzania": {"2019-20": 3076, "2020-21": 14623},
}
EXPECTED_CELLS = {
    "Mali": {"2014-15": 39245},
    "Niger": {"2011-12": 8923, "2014-15": 6500, "2018-19": 42087},
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


@pytest.fixture(scope="module")
def niger_module():
    """``Niger/_/niger.py``, imported the way the wave scripts import it (no
    data read at import)."""
    import sys
    sys.path.insert(0, str(countries_root() / "Niger" / "_"))
    try:
        import niger
    finally:
        sys.path.pop(0)
    return niger


class TestNigerDuplicateEntries:
    """``_community_price_triples`` on synthetic CS07 grids (2014-15 layout:
    one unit column shared by three (price, quantity) slots).

    A second source record for one (grappe, variety) is a duplicate ENTRY of
    the questionnaire (red team r1): a slot of it that repeats a served row is
    dropped, a slot that repeats nothing is served as ``obs = slot + 3``."""

    TRIPLES = [("cs07q04", "cs07q05", "cs07q03"),
               ("cs07q06", "cs07q07", "cs07q03"),
               ("cs07q08", "cs07q09", "cs07q03")]

    @staticmethod
    def _grids(records):
        codes = pd.DataFrame(records, columns=[
            "grappe", "numquest", "cs07q01", "cs07q02", "cs07q03",
            "cs07q04", "cs07q05", "cs07q06", "cs07q07", "cs07q08", "cs07q09"])
        labels = codes.copy()
        labels["cs07q01"] = "Mil"
        labels["cs07q02"] = "mil vendu au kilo"
        labels["cs07q03"] = labels["cs07q03"].map({1: "Kg", 2: "Gramme"})
        return labels, codes

    def _triples(self, niger, records):
        labels, codes = self._grids(records)
        out = niger._community_price_triples(
            labels, {"Mil": "Millet"}, {"Kg": "Kg", "Gramme": "Gramme"},
            self.TRIPLES, codes=codes)
        return out.set_index("obs")[["u", "Price", "Quantity"]]

    def test_a_repeated_slot_is_dropped_and_a_new_one_kept(self, niger_module):
        nan = float("nan")
        out = self._triples(niger_module, [
            # numquest 1: slots 1, 2 priced, slot 3 empty
            (212, 1, 701, 7011, 1, 100.0, 2.0, 150.0, 3.0, nan, nan),
            # numquest 2: slot 1 repeats, slot 2 disagrees, slot 3 is new
            (212, 2, 701, 7011, 1, 100.0, 2.0, 175.0, 3.0, 200.0, 4.0),
        ])
        assert sorted(out.index) == [1, 2, 5, 6]
        assert out.loc[5, "Price"] == 175.0 and out.loc[6, "Price"] == 200.0

    def test_a_conflicting_unit_is_not_a_repeat(self, niger_module):
        # Same Price and Quantity under another unit code: one of the two is a
        # keying error and nothing here can say which, so both are served.
        nan = float("nan")
        out = self._triples(niger_module, [
            (61, 1, 701, 7011, 2, 250.0, 192.0, nan, nan, nan, nan),
            (61, 2, 701, 7011, 1, 250.0, 192.0, nan, nan, nan, nan),
        ])
        assert out["u"].to_dict() == {1: "Gramme", 4: "Kg"}

    def test_a_byte_identical_row_does_not_count_as_a_record(self, niger_module):
        # 2011-12 p2 re-ships whole grappe blocks.  The byte-identical copy is
        # dropped before ranking, so a genuinely different record after it is
        # the SECOND record (obs 4-6), not the third (obs 7-9).
        nan = float("nan")
        out = self._triples(niger_module, [
            (53, 1, 701, 7011, 1, 100.0, 2.0, nan, nan, nan, nan),
            (53, 1, 701, 7011, 1, 100.0, 2.0, nan, nan, nan, nan),
            (53, 1, 701, 7011, 1, 125.0, 2.0, nan, nan, nan, nan),
        ])
        assert sorted(out.index) == [1, 4]

    def test_the_tail_accepts_the_result(self, niger_module):
        nan = float("nan")
        labels, codes = self._grids([
            (212, 1, 701, 7011, 1, 100.0, 2.0, 150.0, 3.0, nan, nan),
            (212, 2, 701, 7011, 1, 100.0, 2.0, 175.0, 3.0, 200.0, 4.0),
        ])
        long = niger_module._community_price_triples(
            labels, {"Mil": "Millet"}, {"Kg": "Kg"}, self.TRIPLES, codes=codes)
        out = niger_module._finish_community_prices(long, "2014-15")
        assert out.index.is_unique and len(out) == 4
        assert "slot" not in out.columns


class TestNigerQuantitySentinels:
    """GH #1088: recognize the two exact 9999.9 representations only in
    Quantity, retaining the price and the existing recorded-price grain."""

    @staticmethod
    def _long(niger, quantities, prices=(100.0, 150.0, 200.0)):
        records = [
            (1, 1, 701, 7011, 1,
             prices[0], quantities[0], prices[1], quantities[1],
             prices[2], quantities[2]),
        ]
        labels, codes = TestNigerDuplicateEntries._grids(records)
        return niger._community_price_triples(
            labels, {"Mil": "Millet"}, {"Kg": "Kg"},
            TestNigerDuplicateEntries.TRIPLES, codes=codes)

    @pytest.mark.parametrize("quantity", [9999.9, 9999.900390625])
    @pytest.mark.parametrize("slot", [1, 2, 3])
    def test_exact_missing_code_keeps_price_through_finalize(
            self, niger_module, quantity, slot):
        from lsms_library import Country

        quantities = [1.0, 2.0, 3.0]
        quantities[slot - 1] = quantity
        long = self._long(niger_module, quantities)
        wave = niger_module._finish_community_prices(long, "2014-15")
        country = Country("Niger")
        served = country._finalize_result(
            wave, country._materialization_entry("community_prices"),
            "community_prices")
        assert len(long) == len(served) == 3
        assert served.index.is_unique
        assert list(served.index.names) == CANONICAL
        assert served.index.get_level_values("obs").tolist() == [1, 2, 3]
        assert served["Price"].tolist() == [100.0, 150.0, 200.0]
        assert pd.isna(long.set_index("obs").loc[slot, "Quantity"])
        assert pd.isna(served.xs(slot, level="obs")["Quantity"].iloc[0])

    @pytest.mark.parametrize("quantity", [
        9997.0, 9998.0, 9999.8, 9999.89, 9999.91,
        # The adjacent float32 values: a tolerance/float32 coercion must not
        # turn a distinct reported weight into the missing code.
        9999.8994140625, 9999.9013671875,
        # Adjacent doubles around each exact missing-code representation.
        9999.899999999998, 9999.900000000001,
        9999.900390624998, 9999.900390625002,
        10000.0, 99998.0, 100000.0,
    ])
    def test_nearby_reported_quantity_is_unchanged(self, niger_module, quantity):
        out = self._long(niger_module, [quantity, 2.0, 3.0]).set_index("obs")
        assert out.loc[1, "Quantity"] == quantity
        assert out.loc[1, "Price"] == 100.0

    @pytest.mark.parametrize("quantity", [0.0, -1.0, None, 9999.0, 99999.0])
    @pytest.mark.parametrize("slot", [1, 2, 3])
    def test_existing_missing_quantity_policy_is_unchanged(
            self, niger_module, quantity, slot):
        quantities = [1.0, 2.0, 3.0]
        quantities[slot - 1] = quantity
        out = self._long(niger_module, quantities).set_index("obs")
        assert len(out) == 3
        assert pd.isna(out.loc[slot, "Quantity"])
        assert out["Price"].tolist() == [100.0, 150.0, 200.0]

    @pytest.mark.parametrize("price", [9999.9, 9999.900390625])
    def test_quantity_missing_codes_do_not_expand_price_filter(
            self, niger_module, price):
        out = self._long(niger_module, [1.0, 2.0, 3.0],
                         prices=[price, 150.0, 200.0]).set_index("obs")
        assert len(out) == 3
        assert out.loc[1, "Price"] == price
        assert out.loc[1, "Quantity"] == 1.0

    @pytest.mark.parametrize("second_quantity, expected_obs", [
        (1.0, [1, 4]),  # The held-source tail: a reported weight is distinct.
        (9999.9, [1]),  # Exact normalized content repeats are still copies.
    ])
    def test_second_record_uses_existing_content_equality(
            self, niger_module, second_quantity, expected_obs):
        nan = float("nan")
        labels, codes = TestNigerDuplicateEntries._grids([
            (1, 1, 701, 7011, 1, 100.0, 9999.900390625, nan, nan, nan, nan),
            (1, 2, 701, 7011, 1, 100.0, second_quantity, nan, nan, nan, nan),
        ])
        out = niger_module._community_price_triples(
            labels, {"Mil": "Millet"}, {"Kg": "Kg"},
            TestNigerDuplicateEntries.TRIPLES, codes=codes).set_index("obs")
        assert out.index.tolist() == expected_obs
        assert out["Price"].eq(100.0).all()
        assert pd.isna(out.loc[1, "Quantity"])
        if second_quantity == 1.0:
            assert out.loc[4, "Quantity"] == 1.0


# The CS07 waves (three slots per record); 2018-19 is s05 (two slots).
NIGER_CS07 = ["2011-12", "2014-15"]


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
        # Niger 2018-19 (s05) names no variety: the canonical sentinel, which
        # is a declared value, not a mis-formatted code (GH #823).
        v = v[v != "unknown_variety"]
        assert v.str.fullmatch(r"\d+").all(), v[~v.str.fullmatch(r"\d+")].unique()[:5]

    @pytest.mark.parametrize("country", sorted(DECLARED))
    def test_rows_and_cells_are_the_measured_ones(self, country):
        df = _build(country)
        t = df.index.get_level_values("t")
        assert pd.Series(1, index=t).groupby(level=0).size().to_dict() == EXPECTED_ROWS[country]
        cells = (df.reset_index()[["t", "v", "j", "u"]].drop_duplicates()
                   .groupby("t").size().to_dict())
        assert cells == EXPECTED_CELLS[country], (
            "the (t, v, j, u) cell counts must match the source-accounted "
            "coverage; all old cells are separately verified as retained")

    def test_mali_tomato_paste_varieties_survive(self):
        # CONTENTS.org's instance: three varieties under u='Boite' at very
        # different prices; the old build kept whichever came first.
        x = _build("Mali").xs(("Concentré de tomate", "Boite"), level=("j", "u"))
        med = x.groupby(level="variety")["Price"].median()
        assert {"5451", "5452"} <= set(med.index)
        assert med["5452"] > 3 * med["5451"]   # 220 g tin vs 60 g tin
        varieties = x.reset_index().groupby(["t", "v", "visit"])["variety"].nunique()
        assert (varieties == 3).any()

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
        df = df[df.index.get_level_values("t").isin(NIGER_CS07)]
        obs = df.index.get_level_values("obs")
        assert set(obs) <= {1, 2, 3, 4, 5, 6}
        late = df[obs > 3]
        # {2011-12: 32, 2014-15: 132} before the r1 duplicate-entry drop: 134
        # of those 164 rows repeated a served row (the test below).
        assert late.groupby(level="t").size().to_dict() == {"2011-12": 15, "2014-15": 15}
        # 2014-15's second records are one grappe's second questionnaire.
        assert set(late.xs("2014-15", level="t").index.get_level_values("v")) == {"212"}

    def test_niger_no_second_record_repeats_a_served_row(self):
        # Red team r1: a second source record is a duplicate ENTRY, so a slot
        # of it that matches a first-record row in every served column is a
        # copy, not a reading -- serving it double-weights that price in any
        # cluster mean, median or count.  134 such rows before the fix.
        x = _build("Niger").reset_index()
        x = x[x["t"].isin(NIGER_CS07)].copy()
        x["slot"] = (x["obs"] - 1) % 3 + 1
        content = ["t", "v", "j", "u", "variety", "visit", "slot",
                   "Price", "Quantity", "Description"]
        first = pd.MultiIndex.from_frame(x.loc[x["obs"] <= 3, content])
        later = pd.MultiIndex.from_frame(x.loc[x["obs"] > 3, content])
        assert int(later.isin(first).sum()) == 0

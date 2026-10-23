"""Mali tree-crop ``crop_production`` rows: a 12-month harvest, the sale as recorded -- GH #1267 + PR #1272.

Both EAC-I tree rosters ask, per tree crop,

* the number of months harvested in the last 12 (2014-15 3B.09 ``s3bq09``;
  2017-18 11F.10 ``s11fq10``);
* the harvest "en moyenne chaque mois" (3B.10 ``s3bq10a`` / 11F.11 ``s11fq11a``);
* the quantity sold "en moyenne chaque mois" (3B.13 ``s3bq13a`` / 11F.14
  ``s11fq14a``);
* "le montant total de cette vente" (3B.14 ``s3bq14`` / 11F.15 ``s11fq15`` --
  printed identically in both questionnaires; the 2017-18 Stata label says
  "mensuelle moyenne").

History.  Until #1267 the 2014-15 tree rows served ``Quantity = s3bq09`` --
the month COUNT -- a misreading inherited from the WB ``MLI_EACI1.do:259``.
#1267 served the per-month answers as recorded under two identity keys.  The
rulings on PR #1272 (@ligon, 2026-10-23) serve the 12-MONTH HARVEST,
``Quantity`` = per-month harvest x months harvested, under the registered
derivation ``Mali::crop_production::tree-twelve-month-total`` for both
waves (edge rule: the 99 / 9999 / 9999.9 codes masked before the product; 0
a month is 0; months NA, or months 0 beside a positive answer, is NA).
Rounds 1-3 also multiplied ``Quantity_sold`` (and, in round 3,
``Value_sold``) by the month count.  The REVISED ruling of the same day,
after the document search (both enumerator manuals ask the value as a
TOTAL; the price levels put it on the per-month quantity's footing), serves
the sale AS RECORDED, following the Ethiopia #1257 precedent: an identity
derivation ``Mali::crop_production::tree-sale-window``, on tree rows with a
positive sale, names the unsettled window.  The codes are still masked (99 /
9999 / 9999.9 on the quantity, 999999 on the value), the month count no
longer touches the sale, and ``Value_sold / Quantity_sold`` is the same
price it was (cashew sold in Kg: 500 FCFA/kg in 2017-18).  The raw answers
come back from ``Country.derivation_inputs``.

``TestDeclared`` and ``TestDerive`` need no microdata; ``TestServed`` builds
the real table and compares it with the raw roster through
``Country.derivation_inputs`` (needs S3).
"""
from __future__ import annotations

import importlib.util
import re
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.paths import countries_root

COUNTRY = "Mali"
TABLE = "crop_production"
KEY = "Mali::crop_production::tree-twelve-month-total"
SALE_KEY = "Mali::crop_production::tree-sale-window"
#: The value a tree row with a positive sale carries: both keys, sorted, '+'.
BOTH = "+".join(sorted((KEY, SALE_KEY)))
#: The #1267 identity keys this ruling replaces -- they must be gone.
RETIRED_KEYS = ("Mali::crop_production::3b-per-harvest-month",
                "Mali::crop_production::11f-per-harvest-month")
#: Served tree rows per wave (2014-15: 358 producing roster rows, two of which
#: -- Mandarinier and Tangerine, both 'Mandarine', both all-zero -- share a key).
TREE_ROWS = {"2014-15": 357, "2017-18": 270}
#: 2014-15 tree rows on which the served Quantity EQUALS the month count (both
#: non-null): 88 are 0 months / 0 harvested and 38 a per-month answer of
#: exactly 1 (so the 12-month total is the month count).  Before #1267 it was
#: every one of the 351 rows with a count, because Quantity WAS the count; at
#: e2836e5d2 (per-month answers as recorded) it was 115.
STILL_EQUAL_2014 = 126
#: Served tree rows whose value is NA, by wave and column, measured
#: 2026-10-23 on a cold build.  The HARVEST (the product): 2014-15 Quantity 7
#: = 3 with 0 months beside a positive answer + 3 with a missing month count
#: (one of them the 9999.9 code) + 1 missing code beside one month; 2017-18
#: Quantity 86 = 85 'Aucune recolte' rows with no answer + 1 beside a
#: positive harvest.  THE SALE (as recorded, revised ruling): 2014-15
#: Quantity_sold 2 = the two missing codes, and Value_sold 156 = 153 unsold or
#: unrecorded + the 3 rows carrying the 999999 code -- the 0 / missing month
#: count no longer blanks a sale (it did in rounds 1-3: 8 and 162 at
#: development 57d255585); 2017-18 85 and 143, unchanged.
EDGE_NA = {("2014-15", "Quantity"): 7, ("2014-15", "Quantity_sold"): 2,
           ("2014-15", "Value_sold"): 156,
           ("2017-18", "Quantity"): 86, ("2017-18", "Quantity_sold"): 85,
           ("2017-18", "Value_sold"): 143}
#: Tree rows with a positive served sale (Quantity_sold > 0 or Value_sold >
#: 0), which carry SALE_KEY beside KEY: 2014-15 203 = 196 priced + 4 with a
#: zero value + 2 whose value is the 999999 code + 1 with a value and no
#: positive quantity; 2017-18 128 = 127 priced + 1 with no value recorded.
SALE_ROWS = {"2014-15": 203, "2017-18": 128}
#: Priced tree rows (Quantity_sold > 0 and Value_sold > 0) by wave: 2014-15
#: 190 -> 196 against development 57d255585, the six whose month count is 0
#: (3) or missing (3) now serving their recorded sale.
PRICED_TREES = {"2014-15": 196, "2017-18": 127}
#: Served price (Value_sold / Quantity_sold) of tree rows SOLD IN Kg, median
#: FCFA/kg, (wave, crop) -> (n, median), crops with n >= 5.  On every row
#: priced both before and after the revised ruling the price is identical
#: (both columns were multiplied by the same month count); the 2014-15
#: cashew cell gains one row, a sale beside a 0 or missing month count, and
#: its median moves 275 (n=12, development 57d255585) -> 300 (n=13).
KG_PRICES = {("2014-15", "Noix de cajou"): (13, 300.0), ("2014-15", "Mangue"): (9, 300.0),
             ("2014-15", "Orange"): (5, 500.0),
             ("2017-18", "Noix de cajou"): (21, 500.0), ("2017-18", "Mangue"): (9, 125.0)}


def _root() -> Path:
    return Path(countries_root()) / COUNTRY


@pytest.fixture
def mali(monkeypatch):
    repo = Path(__file__).resolve().parents[1]
    assert Path(ll.__file__).resolve().is_relative_to(repo), ll.__file__
    source = repo / "lsms_library/countries/Mali/_/mali.py"
    spec = importlib.util.spec_from_file_location("mali_1267_actual_source", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(source.parent)
    return module


def _recorded(answer: pd.Series, *, value: bool = False) -> pd.Series:
    """A recorded answer, written independently of mali.py: a quantity loses
    the 99 / 9999 / 9999.9 codes, a value the 999999 code."""
    x = pd.to_numeric(answer, errors="coerce").astype("float64")
    if value:
        return x.mask(x == 999999)
    return x.mask(x.isin([99, 9999]) | np.isclose(x.to_numpy(dtype="float64"), 9999.9))


def _expected_total(per_month: pd.Series, months: pd.Series) -> pd.Series:
    """The harvest product, written independently of mali.py: codes masked
    first; 0 a month is 0; months NA, or 0 beside a positive answer, is NA."""
    x = _recorded(per_month)
    m = pd.to_numeric(months, errors="coerce").astype("float64")
    out = np.where(x == 0, 0.0, np.where((m == 0) & (x > 0), np.nan, x * m))
    return pd.Series(out, index=per_month.index, dtype="float64")


def _months_2017(labels: pd.Series) -> pd.Series:
    def conv(v):
        if pd.isna(v):
            return np.nan
        s = str(v).strip()
        if s.lower().startswith("aucune"):
            return 0.0
        m = re.match(r"^(\d+)\s*mois", s)
        return float(m.group(1)) if m else np.nan
    return labels.map(conv).astype("float64")


class TestDeclared:

    def test_months_harvested_is_declared_optional(self):
        class _Loader(yaml.SafeLoader):
            pass
        _Loader.add_constructor("!make", lambda loader, node: {"__make__": True})
        with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
            entry = yaml.load(f, Loader=_Loader)["Data Scheme"][TABLE]
        assert entry["months_harvested"] == {"type": "int", "optional": True}

    def test_one_twelve_month_total_spans_both_waves(self):
        """The harvest only, since the revised ruling: the sale left this key."""
        recs = derivation_records(COUNTRY)
        rec = recs[KEY]
        assert list(rec.waves) == ["2014-15", "2017-18"]
        assert list(rec.columns) == ["Quantity"]
        assert rec.rows == {"perennial": True}
        assert callable(resolve_callable(rec.function))
        assert callable(resolve_callable(rec.inputs))
        assert not set(RETIRED_KEYS) & set(recs)

    def test_the_sale_has_its_own_window_key(self):
        """Revised ruling (Ethiopia #1257 precedent): the tree sale is served as
        recorded under an identity key that names the window conflict, with
        the manual quotes on a ``questionnaire`` basis, the 2017-18 label on a
        ``variable-label`` basis and serving-as-recorded a ``modelling-choice``."""
        rec = derivation_records(COUNTRY)[SALE_KEY]
        assert list(rec.waves) == ["2014-15", "2017-18"]
        assert list(rec.columns) == ["Quantity_sold", "Value_sold"]
        assert set(rec.bases) == {"questionnaire", "variable-label", "modelling-choice"}
        ev = " ".join(a.get("evidence", "") for a in rec.assumptions)
        assert "determiner le montant total de la vente" in ev           # 2014-15 3B.14
        assert "la valeur en F CFA de la" in ev and "quantite totale" in ev   # 2017-18 11F.15
        assert "mensuelle moyenne" in ev
        assert callable(resolve_callable(rec.function))
        assert callable(resolve_callable(rec.inputs))
        # the two keys' raw variables partition the roster's measured answers
        harvest = set(derivation_records(COUNTRY)[KEY].raw_variables)
        sale = set(rec.raw_variables)
        assert {"s3bq13a", "s3bq14", "s11fq14a", "s11fq15"} <= sale - harvest
        assert {"s3bq10a", "s11fq11a"} <= harvest - sale

    @pytest.mark.parametrize("wave", ["2014-15", "2017-18"])
    def test_wave_scripts_build_trees_through_the_derivation(self, wave):
        src = (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "'Quantity': pd.to_numeric(s3b['s3bq09']" not in src
        assert "tree_crop_rows(" in src and "stamp_tree_derivations(df)" in src
        assert "per_harvest_month" not in src


class TestDerive:

    def test_2014_15_harvest_is_per_month_times_months_and_the_sale_as_recorded(self, mali):
        raw = pd.DataFrame({"s3bq09": [3.0, 0.0, 99.0, 0.0, 2.0],
                            "s3bq10a": [12.0, 0.0, 4.0, 10.0, 5.0],
                            "s3bq10b": ["Sac", "Manquant", "Kilogramme", "Sac", "Sac"],
                            "s3bq13a": [5.0, 0.0, 1.0, 10.0, 0.0],
                            "s3bq13b": ["Sac", pd.NA, "Kilogramme", "Sac", pd.NA],
                            "s3bq14": [60000.0, np.nan, 900.0, 75000.0, np.nan]})
        got = mali.tree_crop_rows(raw, "2014-15")
        assert got["Quantity"].iloc[0] == 36.0                  # 12 a month x 3 months
        assert got["Quantity"].iloc[1] == 0.0                   # 0 months, 0 a month
        assert got["Quantity"].iloc[[2, 3]].isna().all()       # months NA; 0 months beside 10
        assert got["Quantity"].iloc[4] == 10.0
        assert got["months_harvested"].tolist()[:2] == [3, 0]
        assert pd.isna(got["months_harvested"].iloc[2])        # 99 = Manquant
        # revised ruling: the sale is served as recorded, whatever the month count
        assert got["Quantity_sold"].tolist() == [5.0, 0.0, 1.0, 10.0, 0.0]
        assert got["Value_sold"].iloc[[0, 2, 3]].tolist() == [60000.0, 900.0, 75000.0]
        assert got["Value_sold"].iloc[[1, 4]].isna().all()     # no value recorded
        price = got["Value_sold"].iloc[0] / got["Quantity_sold"].iloc[0]
        assert price == 60000.0 / 5.0                           # the per-month ratio

    def test_2017_18_scales_the_harvest_only(self, mali):
        raw = pd.DataFrame({
            "s11fq10": ["3 mois", "12 mois",
                        "Aucune récolte (Arbre fruitier n?a pas commencé à produire)", pd.NA],
            "s11fq11a": [2.0, 1.0, 200.0, np.nan], "s11fq11b": ["Kg"] * 4,
            "s11fq14a": [1.0, 0.0, np.nan, np.nan], "s11fq14b": ["Kg"] * 4,
            "s11fq15": [500.0, np.nan, np.nan, np.nan]})
        got = mali.tree_crop_rows(raw, "2017-18")
        assert got["months_harvested"].tolist()[:3] == [3, 12, 0]
        assert pd.isna(got["months_harvested"].iloc[3])
        assert got["Quantity"].tolist()[:2] == [6.0, 12.0]
        assert pd.isna(got["Quantity"].iloc[2])                 # 'Aucune recolte' beside 200
        assert got["Quantity_sold"].tolist()[:2] == [1.0, 0.0]  # as recorded
        assert got["Value_sold"].iloc[0] == 500.0                # as recorded, not x 3

    def test_missing_codes_are_masked_and_the_sale_is_not_multiplied(self, mali):
        """99 / 9999 / 9999.9 on a quantity and 999999 on a value are codes:
        masked on the recorded answer (and, for the harvest, BEFORE the
        product: never 19998 or 297).  A value of 9,999 FCFA is an amount and
        is served; a value of 99 FCFA beside 2 months is served as 99."""
        raw = pd.DataFrame({"s3bq09": [2.0, 1.0, 3.0, 2.0],
                            "s3bq10a": [9999.0, np.float32(9999.9), 99.0, 4.0],
                            "s3bq10b": ["Kilogramme"] * 4,
                            "s3bq13a": [99.0, 9999.0, np.float32(9999.9), 4.0],
                            "s3bq13b": ["Kilogramme"] * 4,
                            "s3bq14": [999999.0, 9999.0, 999999.0, 99.0]})
        got = mali.tree_crop_rows(raw, "2014-15")
        assert got["Quantity"].iloc[:3].isna().all() and got["Quantity_sold"].iloc[:3].isna().all()
        assert got["Quantity"].iloc[3] == 8.0 and got["Quantity_sold"].iloc[3] == 4.0
        assert got["Value_sold"].iloc[[0, 2]].isna().all()
        assert got["Value_sold"].iloc[1] == 9999.0 and got["Value_sold"].iloc[3] == 99.0
        # the registered harvest function itself is the bare product
        assert mali.derive_tree_twelve_month_total(
            pd.Series([33.0]), pd.Series([3], dtype="Int64")).tolist() == [99.0]

    def test_the_sale_derivation_is_the_identity(self, mali):
        qs = pd.Series([5.0, 0.0, np.nan, 99.0], index=[10, 11, 12, 13])
        v = pd.Series([60000.0, np.nan, 900.0, 99.0], index=[10, 11, 12, 13])
        got = mali.derive_tree_sale_window(qs, v)
        assert list(got.columns) == ["Quantity_sold", "Value_sold"]
        pd.testing.assert_series_equal(got["Quantity_sold"], qs, check_names=False)
        pd.testing.assert_series_equal(got["Value_sold"], v, check_names=False)

    def test_stamp_puts_the_sale_key_on_positive_sales_only(self, mali):
        """Every tree row carries the harvest key; a positive Quantity_sold OR
        Value_sold adds the sale key ('+'-joined, sorted -- what
        derivations.union_keys writes); a zero or absent sale and every field
        row do not."""
        df = pd.DataFrame({
            "perennial": pd.array([True, True, True, True, True, False], dtype="boolean"),
            "Quantity_sold": [5.0, 0.0, np.nan, 3.0, 0.0, 7.0],
            "Value_sold":    [900.0, 0.0, 500.0, np.nan, np.nan, 100.0]})
        got = mali.stamp_tree_derivations(df)
        assert str(got.dtype) == "string"
        assert got.iloc[:5].tolist() == [BOTH, KEY, BOTH, BOTH, KEY]
        assert pd.isna(got.iloc[5])
        assert BOTH == f"{SALE_KEY}+{KEY}"

    def test_finalize_masks_field_rows_but_not_a_tree_total(self, mali):
        """A tree total of 99 (33 a month x 3) is a quantity; a field 99 is the code."""
        base = dict(t="2014-15", i="h1", crop="Citronnier", u="Sac", Quantity_sold=np.nan,
                    Unit_sold=pd.NA, Value_sold=np.nan, months_harvested=pd.NA,
                    planting_month=pd.NA, harvest_month=pd.NA, intercropped=pd.NA)
        rows = pd.DataFrame([dict(base, plot_id=pd.NA, Quantity=99.0, perennial=True),
                             dict(base, plot_id="1_1", crop="Mil", Quantity=99.0, perennial=False)])
        got = mali.crop_production_finalize(rows).reset_index()
        assert got.loc[got.perennial.astype(bool), "Quantity"].tolist() == [99.0]
        assert got.loc[~got.perennial.astype(bool), "Quantity"].isna().all()


@pytest.mark.requires_s3
class TestServed:

    @pytest.fixture(scope="class")
    def built(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ll.Country(COUNTRY).crop_production()

    @pytest.fixture(scope="class")
    def trees(self, built):
        f = built.reset_index()
        return f[f["perennial"].fillna(False)]

    @pytest.fixture(scope="class")
    def raw(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ll.Country(COUNTRY).derivation_inputs(KEY).reset_index()

    @pytest.fixture(scope="class")
    def raw_sale(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ll.Country(COUNTRY).derivation_inputs(SALE_KEY).reset_index()

    def test_tree_rows_and_their_keys(self, built, trees):
        """Every tree row carries the harvest key; the sale-window key joins it
        ('+', sorted) on exactly the tree rows with a positive served sale."""
        assert trees.groupby("t").size().to_dict() == TREE_ROWS
        sold = ((trees.Quantity_sold > 0) | (trees.Value_sold > 0)).fillna(False).astype(bool)
        assert trees["Derivation"][sold].eq(BOTH).all()
        assert trees["Derivation"][~sold].eq(KEY).all()
        assert trees[sold].groupby("t").size().to_dict() == SALE_ROWS
        field = built[~built["perennial"].fillna(False)]
        assert field["Derivation"].isna().all()
        assert field["months_harvested"].isna().all()
        summ = built.attrs["derivations"][COUNTRY]
        assert summ[KEY]["rows"] == sum(TREE_ROWS.values())
        assert summ[KEY]["columns"] == ["Quantity"]
        assert summ[SALE_KEY]["rows"] == sum(SALE_ROWS.values())
        assert summ[SALE_KEY]["columns"] == ["Quantity_sold", "Value_sold"]
        assert not set(RETIRED_KEYS) & set(summ)

    def test_2014_15_harvest_is_twelve_month_and_sale_as_recorded(self, trees, raw, raw_sale):
        """Raw oracle through the registry: Quantity = s3bq10a x s3bq09 under the
        harvest edge rule; Quantity_sold = s3bq13a and Value_sold = s3bq14 as
        recorded, codes masked; months_harvested = s3bq09."""
        r = raw[raw.t == "2014-15"].copy()
        m = pd.to_numeric(r["s3bq09"], errors="coerce")
        r["m"] = m.where(m.between(0, 12))
        r["e_q"] = _expected_total(r["s3bq10a"], r["m"])
        s = raw_sale[raw_sale.t == "2014-15"].copy()
        s["e_qs"] = _recorded(s["s3bq13a"])
        s["e_v"] = _recorded(s["s3bq14"], value=True)
        # two raw rows (Mandarinier / Tangerine, both all-zero) share a served key
        g, gs = r.groupby(["t", "i", "crop"]), s.groupby(["t", "i", "crop"])
        exp = pd.DataFrame({"e_q": g["e_q"].sum(min_count=1), "e_m": g["m"].first(),
                            "e_qs": gs["e_qs"].sum(min_count=1), "e_v": gs["e_v"].sum(min_count=1)})
        j = trees[trees.t == "2014-15"].set_index(["t", "i", "crop"]).join(exp, how="inner")
        assert len(j) == TREE_ROWS["2014-15"]
        for col, e in (("Quantity", "e_q"), ("Quantity_sold", "e_qs"), ("Value_sold", "e_v"),
                       ("months_harvested", "e_m")):
            a, b = j[col].astype("float64"), j[e].astype("float64")
            assert bool(((a == b) | (a.isna() & b.isna())).all()), col
        both = j.Quantity.notna() & j.months_harvested.notna()
        assert int((both & (j.Quantity == j.months_harvested)).sum()) == STILL_EQUAL_2014

    def test_2017_18_harvest_is_twelve_month_and_sale_as_recorded(self, trees, raw, raw_sale):
        r = raw[raw.t == "2017-18"].copy()
        r["m"] = _months_2017(r["s11fq10"].astype("string"))
        t17 = (trees[trees.t == "2017-18"].merge(r, on=["t", "i", "crop"], how="inner")
               .merge(raw_sale[raw_sale.t == "2017-18"][["t", "i", "crop", "s11fq14a", "s11fq15"]],
                      on=["t", "i", "crop"], how="inner"))
        assert len(t17) == TREE_ROWS["2017-18"]
        for col, exp in (("Quantity", _expected_total(t17["s11fq11a"], t17["m"])),
                         ("Quantity_sold", _recorded(t17["s11fq14a"])),
                         ("Value_sold", _recorded(t17["s11fq15"], value=True)),
                         ("months_harvested", t17["m"])):
            a, b = t17[col].astype("float64"), exp.astype("float64")
            assert bool(((a == b) | (a.isna() & b.isna())).all()), col

    def test_a_sale_beside_a_zero_or_missing_month_count_is_served(self, trees, raw_sale):
        """The month count no longer touches the sale: the 2014-15 rows with a
        positive sale beside 0 months (3) or a missing count (3) serve their
        recorded sale (rounds 1-3 served the sold quantity there NA, round 3
        the value too), and carry the sale key."""
        s = raw_sale[raw_sale.t == "2014-15"].copy()
        m = pd.to_numeric(s["s3bq09"], errors="coerce")
        pos = (_recorded(s["s3bq13a"]) > 0) | (_recorded(s["s3bq14"], value=True) > 0)
        edge = s[pos & ((m == 0) | ~m.between(0, 12))][["t", "i", "crop"]]
        j = trees.merge(edge, on=["t", "i", "crop"], how="inner")
        assert len(j) == 6
        assert j.Quantity_sold.notna().all() and j.Value_sold.notna().all()
        assert j.Derivation.eq(BOTH).all()

    @pytest.mark.parametrize("wave, v, qs", [("2014-15", "s3bq14", "s3bq13a"),
                                             ("2017-18", "s11fq15", "s11fq14a")])
    def test_value_over_quantity_sold_is_the_recorded_ratio(self, trees, raw_sale, wave, v, qs):
        """On every priced tree row the served price is the recorded value over
        the recorded quantity sold -- the same ratio rounds 1-3 served, since
        they multiplied both by the same month count."""
        tw = trees[(trees.t == wave) & (trees.Quantity_sold > 0) & (trees.Value_sold > 0)]
        r = raw_sale[raw_sale.t == wave].drop_duplicates(["t", "i", "crop"])
        j = tw.merge(r, on=["t", "i", "crop"], how="inner")
        assert len(j) == len(tw) == PRICED_TREES[wave]
        want = pd.to_numeric(j[v]) / pd.to_numeric(j[qs])
        assert np.allclose(j.Value_sold / j.Quantity_sold, want)

    @pytest.mark.parametrize("wave, crop", sorted(KG_PRICES))
    def test_tree_price_per_kg_sold(self, trees, wave, crop):
        """The order-of-magnitude anchor of the rulings: cashew sold in Kg
        prices at 500 FCFA/kg in 2017-18 (FAOSTAT Senegal 461); the
        value-as-total reading gave about half that."""
        g = trees[(trees.t == wave) & (trees.crop == crop) & (trees.Unit_sold == "Kg")
                  & (trees.Quantity_sold > 0) & (trees.Value_sold > 0)]
        p = g.Value_sold / g.Quantity_sold
        assert (len(p), float(p.median())) == KG_PRICES[(wave, crop)]

    @pytest.mark.parametrize("wave, col", sorted(EDGE_NA))
    def test_edge_rule_na_counts(self, trees, wave, col):
        assert int(trees[trees.t == wave][col].isna().sum()) == EDGE_NA[(wave, col)]

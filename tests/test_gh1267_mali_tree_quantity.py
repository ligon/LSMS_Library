"""Mali tree-crop ``crop_production`` rows: 12-month totals -- GH #1267 + PR #1272 ruling.

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
rulings on PR #1272 (@ligon, 2026-10-23) serve the 12-MONTH TOTALS: Quantity,
Quantity_sold AND (round 3) Value_sold = per-month answer x months
harvested, one registered derivation
``Mali::crop_production::tree-twelve-month-total`` for both waves, so
``Value_sold / Quantity_sold`` is the per-month ratio, a price (cashew sold
in Kg: 275 FCFA/kg in 2014-15, 500 in 2017-18).  Round 1 had served the
value as recorded.  Edge rule: missing codes masked on the per-month answer
before the product (99 / 9999 / 9999.9 on quantities, 999999 on the value);
0 a month is 0; months NA, or months 0 beside a positive answer, is NA.  The
raw per-month answers come back from ``Country.derivation_inputs``.

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
#: Served tree rows whose total is NA, by wave and column -- the edge rule,
#: measured 2026-10-23 on a cold build.  2014-15 Quantity 7 = 3 with 0 months
#: beside a positive answer + 3 with a missing month count (one of them the
#: 9999.9 code) + 1 missing code beside one month; Quantity_sold 8 = 3 + 3 +
#: 2 missing codes beside one month (2 and 2 at e2836e5d2: the codes only).
#: 2017-18 Quantity 86 = 85 'Aucune recolte' rows with no answer + 1 beside a
#: positive harvest; Quantity_sold 85 (both 85 at e2836e5d2).
#: Value_sold (round 3): 2014-15 162 = 153 unsold or unrecorded (as at
#: e95040028) + 3 with 0 months beside a value + 3 with a missing month count
#: + 3 carrying the 999999 code; 2017-18 143 (unchanged: no such row).
EDGE_NA = {("2014-15", "Quantity"): 7, ("2014-15", "Quantity_sold"): 8,
           ("2014-15", "Value_sold"): 162,
           ("2017-18", "Quantity"): 86, ("2017-18", "Quantity_sold"): 85,
           ("2017-18", "Value_sold"): 143}
#: Served price (Value_sold / Quantity_sold) of tree rows SOLD IN Kg, median
#: FCFA/kg, (wave, crop) -> (n, median), crops with n >= 5 -- the H-monthly
#: reading the round-3 ruling rests on (cashew 275 / 500 against published
#: farm-gate prices).  At e95040028 (value as recorded): cashew 150 (n=13)
#: and 250, mango 101 (n=10) and 42.
KG_PRICES = {("2014-15", "Noix de cajou"): (12, 275.0), ("2014-15", "Mangue"): (9, 300.0),
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


def _expected_total(per_month: pd.Series, months: pd.Series, *, value: bool = False) -> pd.Series:
    """The ruling's product, written independently of mali.py.  A quantity
    loses the 99 / 9999 / 9999.9 codes first, a value the 999999 code."""
    x = pd.to_numeric(per_month, errors="coerce").astype("float64")
    if value:
        x = x.mask(x == 999999)
    else:
        x = x.mask(x.isin([99, 9999]) | np.isclose(x.to_numpy(dtype="float64"), 9999.9))
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
        recs = derivation_records(COUNTRY)
        rec = recs[KEY]
        assert list(rec.waves) == ["2014-15", "2017-18"]
        # Round 3: the value is scaled too, so Value/Quantity_sold is a price.
        assert list(rec.columns) == ["Quantity", "Quantity_sold", "Value_sold"]
        assert rec.rows == {"perennial": True}
        assert callable(resolve_callable(rec.function))
        assert callable(resolve_callable(rec.inputs))
        assert not set(RETIRED_KEYS) & set(recs)

    @pytest.mark.parametrize("wave", ["2014-15", "2017-18"])
    def test_wave_scripts_build_trees_through_the_derivation(self, wave):
        src = (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "'Quantity': pd.to_numeric(s3b['s3bq09']" not in src
        assert "tree_crop_rows(" in src and "DERIVATION_TREES" in src
        assert "per_harvest_month" not in src


class TestDerive:

    def test_2014_15_quantities_are_per_month_times_months(self, mali):
        raw = pd.DataFrame({"s3bq09": [3.0, 0.0, 99.0, 0.0, 2.0],
                            "s3bq10a": [12.0, 0.0, 4.0, 10.0, 5.0],
                            "s3bq10b": ["Sac", "Manquant", "Kilogramme", "Sac", "Sac"],
                            "s3bq13a": [5.0, 0.0, 1.0, 10.0, 0.0],
                            "s3bq13b": ["Sac", pd.NA, "Kilogramme", "Sac", pd.NA],
                            "s3bq14": [60000.0, np.nan, 900.0, 75000.0, np.nan]})
        got = mali.tree_crop_rows(raw, "2014-15")
        assert got["Quantity"].iloc[0] == 36.0 and got["Quantity_sold"].iloc[0] == 15.0
        assert got["Quantity"].iloc[1] == 0.0                   # 0 months, 0 a month
        assert got["Quantity"].iloc[[2, 3]].isna().all()       # months NA; 0 months beside 10
        assert got["Quantity_sold"].iloc[[2, 3]].isna().all()
        assert got["Quantity"].iloc[4] == 10.0 and got["Quantity_sold"].iloc[4] == 0.0
        assert got["months_harvested"].tolist()[:2] == [3, 0]
        assert pd.isna(got["months_harvested"].iloc[2])        # 99 = Manquant
        # round 3: the value is scaled like the quantities, same edge rule
        assert got["Value_sold"].iloc[0] == 180000.0            # 60,000 x 3 months
        assert got["Value_sold"].iloc[[2, 3]].isna().all()     # months NA; 0 months beside 75,000
        assert pd.isna(got["Value_sold"].iloc[1])               # no value recorded
        price = got["Value_sold"].iloc[0] / got["Quantity_sold"].iloc[0]
        assert price == 60000.0 / 5.0                           # the per-month ratio

    def test_2017_18_scales_all_three(self, mali):
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
        assert got["Quantity_sold"].tolist()[:2] == [3.0, 0.0]
        assert got["Value_sold"].iloc[0] == 1500.0              # 500 a month x 3 (round 3)

    def test_missing_codes_are_masked_before_the_product(self, mali):
        """99 / 9999 / 9999.9 on a quantity and 999999 on a value are codes:
        masked BEFORE the product (never 19998, 297 or 1,999,998).  A value of
        9,999 FCFA is an amount and is scaled."""
        raw = pd.DataFrame({"s3bq09": [2.0, 1.0, 3.0, 2.0],
                            "s3bq10a": [9999.0, np.float32(9999.9), 99.0, 4.0],
                            "s3bq10b": ["Kilogramme"] * 4,
                            "s3bq13a": [99.0, 9999.0, np.float32(9999.9), 4.0],
                            "s3bq13b": ["Kilogramme"] * 4,
                            "s3bq14": [999999.0, 9999.0, 999999.0, 99.0]})
        got = mali.tree_crop_rows(raw, "2014-15")
        assert got["Quantity"].iloc[:3].isna().all() and got["Quantity_sold"].iloc[:3].isna().all()
        assert got["Value_sold"].iloc[[0, 2]].isna().all()
        assert got["Value_sold"].iloc[1] == 9999.0 and got["Value_sold"].iloc[3] == 198.0
        # the registered function itself is the bare product
        assert mali.derive_tree_twelve_month_total(
            pd.Series([33.0]), pd.Series([3], dtype="Int64")).tolist() == [99.0]

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

    def test_tree_rows_and_their_key(self, built, trees):
        assert trees.groupby("t").size().to_dict() == TREE_ROWS
        assert trees["Derivation"].eq(KEY).all()
        field = built[~built["perennial"].fillna(False)]
        assert field["Derivation"].isna().all()
        assert field["months_harvested"].isna().all()
        summ = built.attrs["derivations"][COUNTRY]
        assert summ[KEY]["rows"] == sum(TREE_ROWS.values())
        assert not set(RETIRED_KEYS) & set(summ)

    def test_2014_15_quantities_and_value_are_twelve_month_totals(self, trees, raw):
        """Raw oracle through the registry: Quantity = s3bq10a x s3bq09,
        Quantity_sold = s3bq13a x s3bq09 and (round 3) Value_sold = s3bq14 x
        s3bq09 under the edge rule; months_harvested = s3bq09."""
        r = raw[raw.t == "2014-15"].copy()
        m = pd.to_numeric(r["s3bq09"], errors="coerce")
        r["m"] = m.where(m.between(0, 12))
        r["e_q"] = _expected_total(r["s3bq10a"], r["m"])
        r["e_qs"] = _expected_total(r["s3bq13a"], r["m"])
        r["e_v"] = _expected_total(r["s3bq14"], r["m"], value=True)
        # two raw rows (Mandarinier / Tangerine, both all-zero) share a served key
        g = r.groupby(["t", "i", "crop"])
        exp = pd.DataFrame({"e_q": g["e_q"].sum(min_count=1), "e_qs": g["e_qs"].sum(min_count=1),
                            "e_v": g["e_v"].sum(min_count=1), "e_m": g["m"].first()})
        j = trees[trees.t == "2014-15"].set_index(["t", "i", "crop"]).join(exp, how="inner")
        assert len(j) == TREE_ROWS["2014-15"]
        for col, e in (("Quantity", "e_q"), ("Quantity_sold", "e_qs"), ("Value_sold", "e_v"),
                       ("months_harvested", "e_m")):
            a, b = j[col].astype("float64"), j[e].astype("float64")
            assert bool(((a == b) | (a.isna() & b.isna())).all()), col
        both = j.Quantity.notna() & j.months_harvested.notna()
        assert int((both & (j.Quantity == j.months_harvested)).sum()) == STILL_EQUAL_2014

    def test_2017_18_quantities_and_value_are_twelve_month_totals(self, trees, raw):
        r = raw[raw.t == "2017-18"].copy()
        r["m"] = _months_2017(r["s11fq10"].astype("string"))
        t17 = trees[trees.t == "2017-18"].merge(r, on=["t", "i", "crop"], how="inner")
        assert len(t17) == TREE_ROWS["2017-18"]
        for col, exp in (("Quantity", _expected_total(t17["s11fq11a"], t17["m"])),
                         ("Quantity_sold", _expected_total(t17["s11fq14a"], t17["m"])),
                         ("Value_sold", _expected_total(t17["s11fq15"], t17["m"], value=True)),
                         ("months_harvested", t17["m"])):
            a, b = t17[col].astype("float64"), exp.astype("float64")
            assert bool(((a == b) | (a.isna() & b.isna())).all()), col

    @pytest.mark.parametrize("wave, v, qs, n", [("2014-15", "s3bq14", "s3bq13a", 190),
                                                ("2017-18", "s11fq15", "s11fq14a", 127)])
    def test_value_over_quantity_sold_is_the_per_month_ratio(self, trees, raw, wave, v, qs, n):
        """Round 3: on every priced tree row the served price is the per-month
        value over the per-month quantity sold -- both scaled by the same month
        count -- in both waves.  (At e95040028 the 2014-15 / 2017-18 price was
        that ratio divided by months_harvested.)"""
        tw = trees[(trees.t == wave) & (trees.Quantity_sold > 0) & (trees.Value_sold > 0)]
        r = raw[raw.t == wave].drop_duplicates(["t", "i", "crop"])
        j = tw.merge(r, on=["t", "i", "crop"], how="inner")
        assert len(j) == len(tw) == n
        want = pd.to_numeric(j[v]) / pd.to_numeric(j[qs])
        assert np.allclose(j.Value_sold / j.Quantity_sold, want)

    @pytest.mark.parametrize("wave, crop", sorted(KG_PRICES))
    def test_tree_price_per_kg_sold(self, trees, wave, crop):
        """The order-of-magnitude anchor of the round-3 ruling: cashew sold in Kg
        prices at 275 FCFA/kg (2014-15) and 500 (2017-18), the published farm-gate
        prices of those years; the value-as-total reading gave half that."""
        g = trees[(trees.t == wave) & (trees.crop == crop) & (trees.Unit_sold == "Kg")
                  & (trees.Quantity_sold > 0) & (trees.Value_sold > 0)]
        p = g.Value_sold / g.Quantity_sold
        assert (len(p), float(p.median())) == KG_PRICES[(wave, crop)]

    @pytest.mark.parametrize("wave, col", sorted(EDGE_NA))
    def test_edge_rule_na_counts(self, trees, wave, col):
        assert int(trees[trees.t == wave][col].isna().sum()) == EDGE_NA[(wave, col)]

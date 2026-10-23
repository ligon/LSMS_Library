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
ruling on PR #1272 (@ligon, 2026-10-23) serves the 12-MONTH TOTALS: Quantity
and Quantity_sold = per-month answer x months harvested, one registered
derivation ``Mali::crop_production::tree-twelve-month-total`` for both waves,
and Value_sold as recorded (a total, per the printed instrument), so
``Value_sold / Quantity_sold`` is a price.  Edge rule: missing codes masked
on the per-month answer before the product; 0 a month is 0; months NA, or
months 0 beside a positive answer, is NA.  The raw per-month answers come
back from ``Country.derivation_inputs``.

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
EDGE_NA = {("2014-15", "Quantity"): 7, ("2014-15", "Quantity_sold"): 8,
           ("2017-18", "Quantity"): 86, ("2017-18", "Quantity_sold"): 85}


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


def _expected_total(per_month: pd.Series, months: pd.Series) -> pd.Series:
    """The ruling's product, written independently of mali.py."""
    x = pd.to_numeric(per_month, errors="coerce").astype("float64")
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
        # Value_sold is the recorded "montant total" in both waves: NOT derived.
        assert list(rec.columns) == ["Quantity", "Quantity_sold"]
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
        # the value is the recorded "montant total", never scaled
        assert got["Value_sold"].tolist()[:1] == [60000.0] and got["Value_sold"].iloc[3] == 75000.0

    def test_2017_18_scales_the_quantities_not_the_value(self, mali):
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
        assert got["Value_sold"].iloc[0] == 500.0               # printed "montant total"

    def test_missing_codes_are_masked_before_the_product(self, mali):
        months = pd.Series([2, 1, 3], dtype="Int64")
        got = mali.derive_tree_twelve_month_total(
            pd.Series([9999.0, np.float32(9999.9), 99.0]), months)
        assert got.isna().all()                                 # never 19998 / 297

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

    def test_2014_15_quantities_are_twelve_month_totals(self, trees, raw):
        """Raw oracle through the registry: Quantity = s3bq10a x s3bq09 and
        Quantity_sold = s3bq13a x s3bq09 under the edge rule; Value_sold =
        s3bq14 as recorded; months_harvested = s3bq09."""
        r = raw[raw.t == "2014-15"].copy()
        m = pd.to_numeric(r["s3bq09"], errors="coerce")
        r["m"] = m.where(m.between(0, 12))
        r["e_q"] = _expected_total(r["s3bq10a"], r["m"])
        r["e_qs"] = _expected_total(r["s3bq13a"], r["m"])
        r["e_v"] = pd.to_numeric(r["s3bq14"], errors="coerce")
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

    def test_2017_18_quantities_are_twelve_month_totals_and_value_is_recorded(self, trees, raw):
        r = raw[raw.t == "2017-18"].copy()
        r["m"] = _months_2017(r["s11fq10"].astype("string"))
        t17 = trees[trees.t == "2017-18"].merge(r, on=["t", "i", "crop"], how="inner")
        assert len(t17) == TREE_ROWS["2017-18"]
        for col, exp in (("Quantity", _expected_total(t17["s11fq11a"], t17["m"])),
                         ("Quantity_sold", _expected_total(t17["s11fq14a"], t17["m"])),
                         ("Value_sold", pd.to_numeric(t17["s11fq15"], errors="coerce")),
                         ("months_harvested", t17["m"])):
            a, b = t17[col].astype("float64"), exp.astype("float64")
            assert bool(((a == b) | (a.isna() & b.isna())).all()), col

    def test_value_over_quantity_sold_is_a_price_on_the_12_month_window(self, trees, raw):
        """On every priced 2014-15 tree row the ratio is s3bq14 / (s3bq13a x s3bq09):
        the recorded total over the 12-month quantity, not over a month's."""
        t14 = trees[(trees.t == "2014-15") & (trees.Quantity_sold > 0) & (trees.Value_sold > 0)]
        r = raw[raw.t == "2014-15"].drop_duplicates(["t", "i", "crop"])
        j = t14.merge(r, on=["t", "i", "crop"], how="inner")
        assert len(j) == len(t14) == 192
        want = (pd.to_numeric(j.s3bq14) / (pd.to_numeric(j.s3bq13a) * pd.to_numeric(j.s3bq09)))
        assert np.allclose(j.Value_sold / j.Quantity_sold, want)

    @pytest.mark.parametrize("wave, col", sorted(EDGE_NA))
    def test_edge_rule_na_counts(self, trees, wave, col):
        assert int(trees[trees.t == wave][col].isna().sum()) == EDGE_NA[(wave, col)]

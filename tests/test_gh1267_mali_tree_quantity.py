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
revised ruling on PR #1272 serves only harvest Quantity as the 12-month
product. Quantity_sold and Value_sold are masked recorded answers, unaffected
by zero/missing harvest months, under tree-sale-window:
https://github.com/ligon/LSMS_Library/pull/1272#issuecomment-6375506358.
The earlier rounds-1-3 sale multipliers remain superseded history in CONTENTS.
Quantity codes 99/9999/9999.9 and baseline tree-value999999 retain their masks;
no999998/field-value mask is inferred. Raw harvest/sale inputs are separate.

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
#: Historical rounds1-3 tree rows whose total was NA, by wave and column -- the edge rule,
#: measured 2026-10-23 on a cold build.  2014-15 Quantity 7 = 3 with 0 months
#: beside a positive answer + 3 with a missing month count (one of them the
#: 9999.9 code) + 1 missing code beside one month; Quantity_sold 8 = 3 + 3 +
#: 2 missing codes beside one month (2 and 2 at e2836e5d2: the codes only).
#: 2017-18 Quantity 86 = 85 'Aucune recolte' rows with no answer + 1 beside a
#: positive harvest; Quantity_sold 85 (both 85 at e2836e5d2).
#: Value_sold (round 3): 2014-15 162 = 153 unsold or unrecorded (as at
#: e95040028) + 3 with 0 months beside a value + 3 with a missing month count
#: + 3 carrying the 999999 code; 2017-18 143 (unchanged: no such row).
# Revised-source metrics: independent raw oracle + owned native frames at
# 9d4b401a (cached-api824-v2/REVISED-SOURCE-METRICS.json). Harvest pins unchanged;
# six 2014 sale-month edge rows return. These are not synthetic fixture edits.
EDGE_NA = {("2014-15", "Quantity"): 7, ("2014-15", "Quantity_sold"): 2,
           ("2014-15", "Value_sold"): 156,
           ("2017-18", "Quantity"): 86, ("2017-18", "Quantity_sold"): 85,
           ("2017-18", "Value_sold"): 143}
#: Revised-source raw-pair ratios on tree rows SOLD IN Kg, (n, median).
#: Owned native outputs/raw oracle at9d4b401a preserve all common-row ratios;
#: retaining the recorded999998/unknown-month cashew edge adds one observation,
#: n12->13 and median275->300. No new sentinel/price inference is introduced.
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


def _expected_recorded(answer: pd.Series, *, value: bool = False) -> pd.Series:
    """Independent recorded-answer masks; never infer a999998 value mask."""
    per_month = answer
    x = pd.to_numeric(per_month, errors="coerce").astype("float64")
    if value:
        x = x.mask(x == 999999)
    else:
        x = x.mask(x.isin([99, 9999]) | np.isclose(x.to_numpy(dtype="float64"), 9999.9))
    return x


def _expected_total(per_month: pd.Series, months: pd.Series) -> pd.Series:
    """Harvest-only product, independent of mali.py; sale never calls this."""
    x = _expected_recorded(per_month)
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
        assert list(rec.columns) == ["Quantity"]
        sale = recs[SALE_KEY]
        assert list(sale.waves) == ["2014-15", "2017-18"]
        assert list(sale.columns) == ["Quantity_sold", "Value_sold"]
        assert callable(resolve_callable(sale.function))
        assert callable(resolve_callable(sale.inputs))
        assert rec.rows == {"perennial": True}
        assert callable(resolve_callable(rec.function))
        assert callable(resolve_callable(rec.inputs))
        assert not set(RETIRED_KEYS) & set(recs)

    @pytest.mark.parametrize("wave", ["2014-15", "2017-18"])
    def test_wave_scripts_build_trees_through_the_derivation(self, wave):
        src = (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "'Quantity': pd.to_numeric(s3b['s3bq09']" not in src
        assert "tree_crop_rows(" in src and "stamp_tree_derivations" in src
        assert "per_harvest_month" not in src


class TestDerive:

    def test_2014_15_harvest_scales_and_sales_stay_recorded(self, mali):
        raw = pd.DataFrame({"s3bq09": [3.0, 0.0, 99.0, 0.0, 2.0],
                            "s3bq10a": [12.0, 0.0, 4.0, 10.0, 5.0],
                            "s3bq10b": ["Sac", "Manquant", "Kilogramme", "Sac", "Sac"],
                            "s3bq13a": [5.0, 0.0, 1.0, 10.0, 0.0],
                            "s3bq13b": ["Sac", pd.NA, "Kilogramme", "Sac", pd.NA],
                            "s3bq14": [60000.0, np.nan, 900.0, 75000.0, np.nan]})
        got = mali.tree_crop_rows(raw, "2014-15")
        assert got["Quantity"].iloc[0] == 36.0 and got["Quantity_sold"].iloc[0] == 5.0
        assert got["Quantity"].iloc[1] == 0.0                   # 0 months, 0 a month
        assert got["Quantity"].iloc[[2, 3]].isna().all()       # months NA; 0 months beside 10
        assert got["Quantity_sold"].iloc[[2, 3]].tolist() == [1.0, 10.0]
        assert got["Quantity"].iloc[4] == 10.0 and got["Quantity_sold"].iloc[4] == 0.0
        assert got["months_harvested"].tolist()[:2] == [3, 0]
        assert pd.isna(got["months_harvested"].iloc[2])        # 99 = Manquant
        # Revised ruling: the recorded sale is independent of harvest months.
        assert got["Value_sold"].iloc[0] == 60000.0
        assert got["Value_sold"].iloc[[2, 3]].tolist() == [900.0, 75000.0]
        assert pd.isna(got["Value_sold"].iloc[1])               # no value recorded
        price = got["Value_sold"].iloc[0] / got["Quantity_sold"].iloc[0]
        assert price == 60000.0 / 5.0                           # the per-month ratio

    def test_2017_18_scales_only_harvest(self, mali):
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
        assert got["Quantity_sold"].tolist()[:2] == [1.0, 0.0]
        assert got["Value_sold"].iloc[0] == 500.0               # recorded value

    def test_missing_codes_are_masked_before_the_product(self, mali):
        """99 / 9999 / 9999.9 on a quantity and 999999 on a value are codes:
        masked BEFORE the product (never 19998, 297 or 1,999,998).  A value of
        9,999 FCFA is an amount and stays recorded."""
        raw = pd.DataFrame({"s3bq09": [2.0, 1.0, 3.0, 2.0],
                            "s3bq10a": [9999.0, np.float32(9999.9), 99.0, 4.0],
                            "s3bq10b": ["Kilogramme"] * 4,
                            "s3bq13a": [99.0, 9999.0, np.float32(9999.9), 4.0],
                            "s3bq13b": ["Kilogramme"] * 4,
                            "s3bq14": [999999.0, 9999.0, 999999.0, 99.0]})
        got = mali.tree_crop_rows(raw, "2014-15")
        assert got["Quantity"].iloc[:3].isna().all() and got["Quantity_sold"].iloc[:3].isna().all()
        assert got["Value_sold"].iloc[[0, 2]].isna().all()
        assert got["Value_sold"].iloc[1] == 9999.0 and got["Value_sold"].iloc[3] == 99.0
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
            country = ll.Country(COUNTRY)
            harvest = country.derivation_inputs(KEY)
            sale = country.derivation_inputs(SALE_KEY)
            # Positional binding preserves duplicate raw keys (no many-to-many
            # merge of Mandarinier/Tangerine after canonical mapping).
            assert harvest.index.equals(sale.index)
            shared = [name for name in harvest if name in sale]
            assert harvest[shared].equals(sale[shared])
            result = harvest.copy()
            for name in sale.columns.difference(harvest.columns, sort=False):
                result[name] = sale[name].array
            return result.reset_index()

    def test_tree_rows_and_their_key(self, built, trees):
        assert trees.groupby("t").size().to_dict() == TREE_ROWS
        positive = trees.Quantity_sold.gt(0) | trees.Value_sold.gt(0)
        expected = pd.Series(KEY, index=trees.index, dtype="string")
        expected.loc[positive] = "+".join(sorted([KEY, SALE_KEY]))
        assert trees["Derivation"].equals(expected.rename("Derivation"))
        field = built[~built["perennial"].fillna(False)]
        assert field["Derivation"].isna().all()
        assert field["months_harvested"].isna().all()
        summ = built.attrs["derivations"][COUNTRY]
        assert summ[KEY]["rows"] == sum(TREE_ROWS.values())
        assert summ[SALE_KEY]["rows"] == int(positive.sum())
        assert not set(RETIRED_KEYS) & set(summ)

    def test_2014_15_raw_harvest_total_and_recorded_sales(self, trees, raw):
        """Raw oracle through the registry: Quantity = s3bq10a x s3bq09,
        Quantity_sold and Value_sold are masked recorded answers, independent
        of harvest months; months_harvested = s3bq09."""
        r = raw[raw.t == "2014-15"].copy()
        m = pd.to_numeric(r["s3bq09"], errors="coerce")
        r["m"] = m.where(m.between(0, 12))
        r["e_q"] = _expected_total(r["s3bq10a"], r["m"])
        r["e_qs"] = _expected_recorded(r["s3bq13a"])
        r["e_v"] = _expected_recorded(r["s3bq14"], value=True)
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

    def test_2017_18_raw_harvest_total_and_recorded_sales(self, trees, raw):
        r = raw[raw.t == "2017-18"].copy()
        r["m"] = _months_2017(r["s11fq10"].astype("string"))
        t17 = trees[trees.t == "2017-18"].merge(r, on=["t", "i", "crop"], how="inner")
        assert len(t17) == TREE_ROWS["2017-18"]
        for col, exp in (("Quantity", _expected_total(t17["s11fq11a"], t17["m"])),
                         ("Quantity_sold", _expected_recorded(t17["s11fq14a"])),
                         ("Value_sold", _expected_recorded(t17["s11fq15"], value=True)),
                         ("months_harvested", t17["m"])):
            a, b = t17[col].astype("float64"), exp.astype("float64")
            assert bool(((a == b) | (a.isna() & b.isna())).all()), col

    @pytest.mark.parametrize("wave, v, qs, n", [("2014-15", "s3bq14", "s3bq13a", 196),
                                                ("2017-18", "s11fq15", "s11fq14a", 127)])
    def test_value_over_quantity_sold_is_the_per_month_ratio(self, trees, raw, wave, v, qs, n):
        """Every priced tree row retains the exact masked raw-pair ratio.
        Count196 includes six restored zero/missing-month edge rows (raw
        oracle/REVISED-SOURCE-METRICS); no sale-window multiplier remains."""
        tw = trees[(trees.t == wave) & (trees.Quantity_sold > 0) & (trees.Value_sold > 0)]
        r = raw[raw.t == wave].drop_duplicates(["t", "i", "crop"])
        j = tw.merge(r, on=["t", "i", "crop"], how="inner")
        assert len(j) == len(tw) == n
        want = _expected_recorded(j[v], value=True) / _expected_recorded(j[qs])
        np.testing.assert_array_equal(
            (j.Value_sold / j.Quantity_sold).to_numpy(dtype="float64"),
            want.to_numpy(dtype="float64"), strict=True)

    @pytest.mark.parametrize("wave, crop", sorted(KG_PRICES))
    def test_tree_price_per_kg_sold(self, trees, wave, crop):
        """Source-verified revised cohort, not a new amount inference: the
        retained999998/unknown-month edge changes cashew n12->13/275->300;
        all other pins and every common-row ratio remain unchanged."""
        g = trees[(trees.t == wave) & (trees.crop == crop) & (trees.Unit_sold == "Kg")
                  & (trees.Quantity_sold > 0) & (trees.Value_sold > 0)]
        p = g.Value_sold / g.Quantity_sold
        assert (len(p), float(p.median())) == KG_PRICES[(wave, crop)]

    @pytest.mark.parametrize("wave, col", sorted(EDGE_NA))
    def test_edge_rule_na_counts(self, trees, wave, col):
        assert int(trees[trees.t == wave][col].isna().sum()) == EDGE_NA[(wave, col)]

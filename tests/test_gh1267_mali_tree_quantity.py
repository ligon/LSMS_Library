"""Mali tree-crop ``crop_production`` rows: the harvest, not its month count -- GH #1267.

Module 3B of the 2014-15 EAC-I post-harvest questionnaire asks, per tree crop,

* 3B.09 ``s3bq09``  -- for how many months in the last 12 was it harvested;
* 3B.10 ``s3bq10a`` -- "Durant ces mois, quelle quantite a ete recoltee en
  moyenne chaque mois?" (unit ``s3bq10b``);
* 3B.13 ``s3bq13a`` -- the quantity sold, also a monthly average (unit
  ``s3bq13b``);
* 3B.14 ``s3bq14``  -- "Quel a ete le montant TOTAL de cette vente?".

Until #1267 the tree rows served ``Quantity = s3bq09`` -- the month COUNT
(357 rows, every value an integer <= 12) -- a misreading inherited from the
WB ``MLI_EACI1.do:259`` (``harvest_kg_per = s3bq09 * s3bq10b``, months times
the unit CODE).  ``Quantity`` is now ``s3bq10a``, the count is the new
optional column ``months_harvested``, and every tree row carries the
registered identity derivation ``Mali::crop_production::3b-per-harvest-month``
whose job is to state the window: ``Quantity`` / ``Quantity_sold`` are per
harvest month while ``Value_sold`` is the total (so their ratio is NOT a
price).  2017-18's 11F already served the monthly harvest ``s11fq11a`` with a
commensurate (monthly) sale pair; its rows carry
``Mali::crop_production::11f-per-harvest-month`` and the same new column.
No 12-month total is constructed.

``TestDeclared`` and ``TestDerive`` need no microdata; ``TestServed`` builds
the real table and compares it with the raw roster through
``Country.derivation_inputs`` (needs S3).
"""
from __future__ import annotations

import importlib.util
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
KEY_3B = "Mali::crop_production::3b-per-harvest-month"
KEY_11F = "Mali::crop_production::11f-per-harvest-month"
#: Served tree rows per wave (2014-15: 358 producing roster rows, two of which
#: -- Mandarinier and Tangerine, both 'Mandarine', both all-zero -- share a key).
TREE_ROWS = {"2014-15": 357, "2017-18": 270}
#: 2014-15 tree rows on which the served Quantity still EQUALS the month count
#: (both non-null): 88 are 0 months / 0 harvested and 27 coincide (16 of them
#: one month, one unit).  Before #1267 it was every one of the 351 rows with
#: a count, because Quantity WAS the count; 241 served values moved.
STILL_EQUAL_2014 = 115


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


class TestDeclared:

    def test_months_harvested_is_declared_optional(self):
        class _Loader(yaml.SafeLoader):
            pass
        _Loader.add_constructor("!make", lambda loader, node: {"__make__": True})
        with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
            entry = yaml.load(f, Loader=_Loader)["Data Scheme"][TABLE]
        assert entry["months_harvested"] == {"type": "int", "optional": True}

    @pytest.mark.parametrize("key, wave, columns", [
        (KEY_3B, "2014-15", ["Quantity", "Quantity_sold"]),
        (KEY_11F, "2017-18", ["Quantity", "Quantity_sold", "Value_sold"]),
    ])
    def test_both_windows_are_registered(self, key, wave, columns):
        rec = derivation_records(COUNTRY)[key]
        assert list(rec.waves) == [wave]
        assert list(rec.columns) == columns
        assert rec.rows == {"perennial": True}
        assert callable(resolve_callable(rec.function))
        assert callable(resolve_callable(rec.inputs))

    def test_2014_15_script_no_longer_serves_the_month_count(self):
        src = (_root() / "2014-15" / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert "'Quantity': pd.to_numeric(s3b['s3bq09']" not in src
        assert "derive_3b_per_harvest_month" in src


class TestDerive:

    def test_3b_serves_the_monthly_harvest_and_the_count_apart(self, mali):
        raw = pd.DataFrame({"s3bq09": [3.0, 0.0, 99.0], "s3bq10a": [12.0, 0.0, 4.0],
                            "s3bq10b": ["Sac", "Manquant", "Kilogramme"],
                            "s3bq13a": [5.0, 0.0, 1.0], "s3bq13b": ["Sac", pd.NA, "Kilogramme"],
                            "s3bq14": [60000.0, np.nan, 900.0]})
        got = mali.derive_3b_per_harvest_month(raw)
        assert got["Quantity"].tolist() == [12.0, 0.0, 4.0]
        assert got["months_harvested"].tolist()[:2] == [3, 0]
        assert pd.isna(got["months_harvested"].iloc[2])        # 99 = Manquant
        assert got["Value_sold"].iloc[0] == 60000.0            # the TOTAL, as recorded

    def test_11f_parses_the_labelled_month_count(self, mali):
        raw = pd.DataFrame({
            "s11fq10": ["3 mois", "12 mois",
                        "Aucune récolte (Arbre fruitier n?a pas commencé à produire)", pd.NA],
            "s11fq11a": [2.0, 1.0, np.nan, np.nan], "s11fq11b": ["Kg"] * 4,
            "s11fq14a": [1.0, 0.0, np.nan, np.nan], "s11fq14b": ["Kg"] * 4,
            "s11fq15": [500.0, np.nan, np.nan, np.nan]})
        got = mali.derive_11f_per_harvest_month(raw)
        assert got["months_harvested"].tolist()[:3] == [3, 12, 0]
        assert pd.isna(got["months_harvested"].iloc[3])
        assert got["Quantity"].tolist()[:2] == [2.0, 1.0]

    def test_the_9999_9_missing_code_is_not_a_quantity(self, mali):
        rows = pd.DataFrame([dict(
            t="2014-15", i="h1", plot_id=pd.NA, crop="Citronnier", u="Manquant",
            Quantity=np.float32(9999.9), Quantity_sold=np.float32(9999.9), Unit_sold=pd.NA,
            Value_sold=1.0, months_harvested=pd.NA, planting_month=pd.NA,
            harvest_month=pd.NA, intercropped=pd.NA, perennial=True)])
        got = mali.crop_production_finalize(rows)
        assert got["Quantity"].isna().all() and got["Quantity_sold"].isna().all()


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

    @staticmethod
    def _inputs(key):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            x = ll.Country(COUNTRY).derivation_inputs(key)
        return x

    def test_tree_rows_and_their_key(self, built, trees):
        assert trees.groupby("t").size().to_dict() == TREE_ROWS
        keys = trees.groupby("t")["Derivation"].unique().map(list).to_dict()
        assert keys == {"2014-15": [KEY_3B], "2017-18": [KEY_11F]}
        field = built[~built["perennial"].fillna(False)]
        assert field["Derivation"].isna().all()
        assert field["months_harvested"].isna().all()
        summ = built.attrs["derivations"][COUNTRY]
        assert summ[KEY_3B]["rows"] == TREE_ROWS["2014-15"]
        assert summ[KEY_11F]["rows"] == TREE_ROWS["2017-18"]

    def test_2014_15_quantity_is_the_monthly_harvest_not_the_month_count(self, trees):
        """Raw oracle through the registry: Quantity == s3bq10a (the 3B sentinels
        99 / 9999 / 9999.9 -> NA), months_harvested == s3bq09, on every row."""
        raw = self._inputs(KEY_3B).reset_index()
        for c in ("s3bq09", "s3bq10a", "s3bq13a", "s3bq14"):
            raw[c] = pd.to_numeric(raw[c], errors="coerce")
        q = raw["s3bq10a"]
        raw["s3bq10a"] = q.mask(q.isin([99, 9999]) | np.isclose(q.to_numpy(dtype="float64"), 9999.9))
        exp = raw.groupby(["t", "i", "crop"]).agg(
            q=("s3bq10a", lambda s: s.sum(min_count=1)), m=("s3bq09", "first"))
        t14 = trees[trees.t == "2014-15"].set_index(["t", "i", "crop"])
        j = t14.join(exp, how="inner")
        assert len(j) == TREE_ROWS["2014-15"]
        same_q = (j.Quantity == j.q) | (j.Quantity.isna() & j.q.isna())
        assert bool(same_q.all())
        m = j.m.where(j.m.between(0, 12))
        assert bool(((j.months_harvested == m) | (j.months_harvested.isna() & m.isna())).all())
        # ...and it is not the month count any more
        both = j.Quantity.notna() & j.months_harvested.notna()
        assert int((both & (j.Quantity == j.months_harvested)).sum()) == STILL_EQUAL_2014

    def test_2017_18_trees_are_the_recorded_monthly_answers(self, trees):
        raw = self._inputs(KEY_11F).reset_index()
        t17 = trees[trees.t == "2017-18"].merge(raw, on=["t", "i", "crop"], how="inner")
        assert len(t17) == TREE_ROWS["2017-18"]
        for col, var in (("Quantity", "s11fq11a"), ("Quantity_sold", "s11fq14a"),
                         ("Value_sold", "s11fq15")):
            r = pd.to_numeric(t17[var], errors="coerce")
            assert bool(((t17[col] == r) | (t17[col].isna() & r.isna())).all()), col

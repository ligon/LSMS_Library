"""Mali 2014-15 ``community_prices.Quantity`` is kilograms by an explicit rule.

The column head says "Poids (KG)", but both enumerator manuals tie the weight to the unit
entered beside it, with a worked example of Poids 2250, Unite 2 (Gramme).  Ruling 2026-11-04:
convert on an explicit unit-and-passage rule, as a registered derivation; rows no rule
covers stay as recorded and are counted.

``TestRule`` needs no microdata.  ``TestServed`` does (``requires_s3``): its oracle reads
``EACIS04_p1/p2`` directly and imports nothing from ``Mali/_/``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.derivations import records_for
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

KEY = "Mali::community_prices::gram-centilitre-poids"
DATA = countries_root() / "Mali" / "2014-15" / "Data"
TRIPLES = (("s04q03", "s04q04", "s04q05"), ("s04q06", "s04q07", "s04q08"),
           ("s04q09", "s04q10", "s04q11"))
FACTOR = {2: 1000.0, 4: 100.0}


def _derive():
    return records_for("Mali", "community_prices")[KEY].resolve("function")


class TestRule:
    @pytest.mark.parametrize("visit", [1, 2])
    @pytest.mark.parametrize("unit,poids,want", [
        ("Gramme", 2250.0, 2.25),         # the manuals' worked example
        ("Gramme", 60.0, 0.06),
        ("Gramme", 5.0, 0.005),           # the floor is inclusive
        ("Centilitre", 33.0, 0.33),
        ("Gramme", 0.5, 0.5),             # already kg
        ("Centilitre", 0.33, 0.33),       # already litres
        ("Gramme", 1.0, 1.0),             # not established: as recorded
        ("Gramme", 4.0, 4.0),
        ("Centilitre", 3.0, 3.0),
        ("Gramme", 0.0, 0.0),             # a recorded zero stays
        ("Kg", 25.0, 25.0),               # another unit: as recorded
        ("Litre", 90.0, 90.0),
        ("Boite", 60.0, 60.0),
        ("Sac large", 100.0, 100.0),
    ])
    def test_the_rule(self, visit, unit, poids, want):
        got = _derive()(pd.Series([poids]), pd.Series([unit]), pd.Series([visit]))
        assert np.isclose(got.iloc[0], want)

    def test_a_missing_weight_stays_missing(self):
        got = _derive()(pd.Series([np.nan]), pd.Series(["Gramme"]), pd.Series([1]))
        assert got.isna().all()

    def test_a_passage_the_rule_does_not_name_is_untouched(self):
        got = _derive()(pd.Series([60.0]), pd.Series(["Gramme"]), pd.Series([3]))
        assert got.iloc[0] == 60.0

    def test_registered_and_resolves(self):
        rec = records_for("Mali", "community_prices")[KEY]
        assert list(rec.waves) == ["2014-15"]
        assert set(rec.columns) == {"Quantity"}
        assert callable(rec.resolve("function")) and callable(rec.resolve("inputs"))


@pytest.mark.requires_s3
class TestServed:
    @pytest.fixture(scope="class")
    def oracle(self):
        """One row per priced reading of a Gramme or Centilitre unit code."""
        rows = []
        for visit in (1, 2):
            codes = get_dataframe(str(DATA / f"EACIS04_p{visit}.dta"), convert_categoricals=False)
            for obs, (unit, weight, price) in enumerate(TRIPLES, 1):
                frame = pd.DataFrame({
                    "v": codes["grappe"].astype(int).astype(str).values,
                    "variety": pd.to_numeric(codes["s04q02"]).astype(int).astype(str).values,
                    "visit": visit, "obs": obs,
                    "code": pd.to_numeric(codes[unit], errors="coerce").values,
                    "poids": pd.to_numeric(codes[weight], errors="coerce").astype("float64").values,
                    "price": pd.to_numeric(codes[price], errors="coerce").astype("float64").values})
                rows.append(frame[frame.code.isin(FACTOR) & frame.price.gt(0)])
        out = pd.concat(rows, ignore_index=True)
        return out[~out.poids.isin([999.0, 9999.0])]

    @pytest.fixture(scope="class")
    def served(self):
        df = ll.Country("Mali").community_prices().reset_index()
        return df[df.t == "2014-15"]

    def _merged(self, served, oracle):
        rows = served[served.u.astype(str).isin(["Gramme", "Centilitre"])].astype(
            {"v": str, "variety": str})
        return rows.merge(oracle, on=["v", "variety", "visit", "obs"], how="inner",
                          validate="one_to_one")

    def test_the_value_is_the_rule_applied_to_the_source(self, served, oracle):
        got = self._merged(served, oracle)
        native = got.poids.ge(5)
        factor = got.code.map(FACTOR).astype("float64")
        want = got.poids.where(~native, got.poids / factor)
        assert np.allclose(got.Quantity.astype("float64"), want, equal_nan=True)

    def test_the_key_is_on_exactly_the_converted_rows(self, served, oracle):
        got = self._merged(served, oracle)
        keyed = got.Derivation.astype("string").eq(KEY).fillna(False)
        assert (keyed.to_numpy() == got.poids.ge(5).to_numpy()).all()
        assert int(served.Derivation.astype("string").eq(KEY).fillna(False).sum()) == int(keyed.sum())

    def test_no_other_unit_is_keyed(self, served):
        rows = served[~served.u.astype(str).isin(["Gramme", "Centilitre"])]
        assert rows.Derivation.isna().all()

    def test_a_stated_size_variety_lands_on_its_size(self, served):
        """Djino, labelled 33 cl: 5 or more on a Centilitre row reads about 0.33 L."""
        rows = served[served.Description.astype(str).str.contains("33 centilitres")
                      & served.u.astype(str).eq("Centilitre")
                      & served.Derivation.notna()]
        assert len(rows)
        share = ((rows.Quantity / 0.33).between(0.5, 2.0)).mean()
        assert share > 0.85

    def test_derivation_inputs_join_the_keyed_rows(self, served):
        inputs = ll.Country("Mali").derivation_inputs(KEY, wave="2014-15").reset_index()
        rows = served[served.Derivation.notna()]
        keys = ["t", "v", "j", "u", "variety", "visit", "obs"]
        joined = rows.astype({k: str for k in keys}).merge(
            inputs.astype({k: str for k in keys}), on=keys, validate="one_to_one")
        assert len(joined) == len(rows) == len(inputs)
        assert set(inputs.columns) >= {"s04q03", "s04q04", "s04q05", "s04q09", "s04q10", "s04q11"}

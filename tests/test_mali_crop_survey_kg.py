"""GH #1436, option (A): Mali 2021-22's own harvest kilograms on ``crop_production``.

``Quantity_kg`` is ``s16cq16c`` on the 309 rows that record a quantity.  A harvest derived
from the s16d dispositions has none: s16d's ``c`` columns are the state of the product,
except ``s16dq05c``, which is the sale's.  ``TestRules`` and ``TestDeclared`` need no
microdata.  ``TestServed`` does (``requires_s3``); its oracle reads the ``.dta`` files and
imports nothing from ``Mali/_/``.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import MULTI_SEP, records_for, resolve_callable
from lsms_library.paths import countries_root
from lsms_library.transformations import harvest_kg, harvest_kg_factors
from lsms_library.yaml_utils import load_yaml

COUNTRY = "Mali"
TABLE = "crop_production"
ESTIMATE = "Mali::crop_production::survey-kg-estimate"
WINDOW = "Mali::crop_production::harvest-to-date"
TOTAL = "Mali::crop_production::harvest-from-dispositions"
ROOT = countries_root() / COUNTRY
WAVE = "2021-22"
FILE = "s16c_me_mli2021.dta"


def _stamp():
    return resolve_callable("lsms_library.countries.Mali._.mali:stamp_survey_kg_estimate")


class TestRules:
    def test_the_stamp_joins_a_window_key_in_sorted_order(self):
        frame = pd.DataFrame({
            "Quantity_kg": [10.0, np.nan, 5.0],
            "Derivation": pd.array([WINDOW, TOTAL, pd.NA], dtype="string")})
        got = _stamp()(frame)["Derivation"].tolist()
        assert got[0] == MULTI_SEP.join(sorted((ESTIMATE, WINDOW)))
        assert got[1] == TOTAL
        assert got[2] == ESTIMATE

    def test_a_zero_beside_a_positive_harvest_is_no_answer(self):
        derive = resolve_callable("lsms_library.countries.Mali._.mali:derive_survey_kg_estimate")
        got = derive(pd.Series([0.0, 0.0, 7.0, np.nan]), pd.Series([4.0, 0.0, 4.0, np.nan]))
        assert np.isnan(got.iloc[0]) and got.iloc[1] == 0.0 and got.iloc[2] == 7.0
        assert np.isnan(got.iloc[3])


class TestDeclared:
    def test_the_column_is_optional_and_not_a_level(self):
        entry = load_yaml(ROOT / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
        assert entry["Quantity_kg"] == {"type": "float", "optional": True}
        assert "Quantity_kg" not in entry["index"]

    def test_the_estimate_is_registered_for_its_wave(self):
        record = records_for(COUNTRY, TABLE)[ESTIMATE]
        assert list(record["waves"]) == [WAVE]
        assert list(record["columns"]) == ["Quantity_kg"]
        assert callable(resolve_callable(record["function"]))
        assert callable(resolve_callable(record["inputs"]))
        assert "s16cq16c" in record["raw_variables"]

    def test_the_window_key_covers_the_kilograms(self):
        assert "Quantity_kg" in records_for(COUNTRY, TABLE)[WINDOW]["columns"]


def _household(frame):
    return (frame["grappe"].astype(int).astype(str) + "0"
            + frame["menage"].astype(int).astype(str).str.zfill(2))


def _plot(frame):
    return frame["s16cq02"].astype(int).astype(str) + "_" + frame["s16cq03"].astype(int).astype(str)


def _read(name, **kw):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / WAVE / "Data" / name), **kw)


@pytest.fixture(scope="module")
def built():
    # No skip on a failed build: TestServed is `requires_s3`.
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        country = ll.Country(COUNTRY)
        df = country.crop_production()
    return country, df


@pytest.mark.requires_s3
class TestServed:
    def _wave(self, built):
        return built[1].xs(WAVE, level="t").reset_index()

    def test_only_this_wave_has_the_column(self, built):
        _, df = built
        waves = np.asarray(df.index.get_level_values("t"))
        assert df.loc[waves != WAVE, "Quantity_kg"].isna().all()

    def test_it_is_s16cq16c_row_by_row(self, built):
        raw = _read(FILE, convert_categoricals=False)
        kg = pd.to_numeric(raw["s16cq16c"], errors="coerce").astype("float64")
        key = pd.MultiIndex.from_arrays([_household(raw).values, _plot(raw).values], names=["i", "plot_id"])
        want = pd.DataFrame({"kg": kg.to_numpy(), "crop": raw["s16cq04"].to_numpy()}, index=key)
        assert int(want["kg"].notna().sum()) == 309 and int((want["kg"] == 0).sum()) == 3
        assert (pd.to_numeric(raw["s16cq16a"], errors="coerce")[kg == 0] > 0).all()
        want = want[want["kg"] > 0]
        got = self._wave(built)
        got = got[got["Quantity_kg"].notna()]
        assert len(got) == len(want) == 306
        assert np.isclose(float(got["Quantity_kg"].sum()), float(want["kg"].sum()))
        served = sorted(zip(got["i"], got["plot_id"], got["Quantity_kg"].astype(float)))
        source = sorted(zip(want.index.get_level_values("i"), want.index.get_level_values("plot_id"),
                            want["kg"]))
        assert served == source

    def test_s16d_has_no_harvest_kilograms_to_carry(self):
        """Why a derived total stays NA: the file's own labels, not an inference."""
        from lsms_library.data_access import get_data_file
        path = get_data_file(f"{COUNTRY}/{WAVE}/Data/s16d_me_mli2021.dta")
        with pd.io.stata.StataReader(str(path)) as reader:
            labels = reader.variable_labels()
        for var in ("s16dq02c", "s16dq03c", "s16dq13c"):
            assert "Etat du produit" in labels[var]
        assert "en kg" in labels["s16dq05c"]

    def test_a_derived_harvest_has_none(self, built):
        got = self._wave(built)
        derived = got["Derivation"].astype("string").str.contains(TOTAL, regex=False).fillna(False)
        assert derived.any()
        assert got.loc[derived, "Quantity_kg"].isna().all()

    def test_a_farm_level_row_has_none(self, built):
        got = self._wave(built)
        assert got.loc[got["plot_id"] == "Farm-level", "Quantity_kg"].isna().all()

    def test_the_key_is_exactly_where_a_value_is(self, built):
        got = self._wave(built)
        keyed = got["Derivation"].astype("string").str.contains(ESTIMATE, regex=False).fillna(False)
        assert (keyed == got["Quantity_kg"].notna()).all()

    def test_the_registered_inputs_join_to_the_served_rows(self, built):
        country, _ = built
        raw = country.derivation_inputs(ESTIMATE).reset_index()
        got = self._wave(built)
        got = got[got["Quantity_kg"].notna()]
        on = ["i", "plot_id", "crop"]
        assert sorted(map(tuple, raw[on].to_numpy())) == sorted(map(tuple, got[on].to_numpy()))

    def test_the_rung_serves_every_positive_total_and_only_those(self, built):
        _, df = built
        source = harvest_kg_factors(df)["KgFactorSource"].to_numpy()
        positive = df["Quantity_kg"].astype("float64").gt(0).to_numpy()
        assert (source[positive] == "survey_kg").all()
        assert not (source[~positive] == "survey_kg").any()

    def test_harvest_kg_adds_the_survey_to_the_other_layers(self, built):
        _, df = built
        got = df.xs(WAVE, level="t", drop_level=False)
        total = float(harvest_kg(got)["Harvest_kg"].sum())
        survey = got["Quantity_kg"].astype("float64")
        rest = harvest_kg_factors(got)
        rest = rest[rest["KgFactorSource"] != "survey_kg"]
        qty = pd.to_numeric(got["Quantity"], errors="coerce").reindex(rest.index)
        assert np.isclose(total, float(survey[survey > 0].sum()) + float((qty * rest["kg_per_unit"]).sum()))

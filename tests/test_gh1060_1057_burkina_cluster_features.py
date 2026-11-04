"""Burkina Faso ``cluster_features``: a registered construction.

GH #1060 (ruling @ligon 2026-11-03, option b).  EMC 2014 records ``province`` per
household and it varies within 94 of 900 ``zd``.  Each zd with a strict mode is
served that mode as ``District`` on every row, and carries
``Burkina_Faso::cluster_features::zd-modal-province``.  The 2 tied zd are left as
served, the row core's collapse selects, and carry no key; they keep the grain
report, because their rows still disagree.

The ``TestOracle*`` classes are the point.  They read the source files directly and
import nothing from ``Burkina_Faso/_/``, so a bug in the hook cannot agree with itself.
``TestRules`` exercises the registered functions on invented frames and needs no data.
"""
import importlib.util
import json
import os
import pathlib
import unicodedata
import warnings
from collections import Counter

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.country import _audit_index_collapse, grain_reports, most_complete_row
from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root


def _tests_conftest():
    """``tests/conftest.py``, loaded by path (the root conftest shadows it)."""
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
ROOT = countries_root() / COUNTRY
MODAL_KEY = "Burkina_Faso::cluster_features::zd-modal-province"
LOGEMENT = "emc2014_p1_logement_27022015.dta"


def _fold(label):
    """A province's name with case and accents out, source typos in."""
    text = unicodedata.normalize("NFKD", str(label).strip().casefold())
    text = "".join(c for c in text if not unicodedata.combining(c))
    return {"loba": "ioba", "koulpelgo": "koulpelogo",
            "komandjoari": "komondjari"}.get(text, text)


def _read(wave, name):
    return get_dataframe(str(ROOT / wave / "Data" / name))


# --------------------------------------------------------------------------
# Config: no data
# --------------------------------------------------------------------------

class TestConfig:
    def test_derivation_is_declared_optional_in_the_canonical_schema(self):
        from lsms_library.feature import _load_global_columns
        decl = _load_global_columns()["cluster_features"]["Derivation"]
        assert decl["type"] == "str" and decl.get("optional") is True
        assert not decl.get("required")

    def test_data_scheme_is_silent_on_derivation(self):
        """`_SCHEME_NON_COLUMN_KEYS`: it would read as a required column there."""
        text = (ROOT / "_" / "data_scheme.yml").read_text(encoding="utf-8")
        block = text[text.index("  cluster_features:"):text.index("  household_roster:")]
        assert "Derivation" not in block

    @pytest.mark.parametrize("key,wave,columns", [
        (MODAL_KEY, "2014", ["District"]),
    ])
    def test_registered_with_its_scope_and_resolving_callables(self, key, wave, columns):
        rec = derivation_records(COUNTRY)[key]
        assert rec.waves == [wave] and rec.columns == columns
        assert callable(rec.resolve("function")) and callable(rec.resolve("inputs"))
        assert rec.assumptions


# --------------------------------------------------------------------------
# Rules, on invented frames: no data
# --------------------------------------------------------------------------

def _households(rows):
    """(v, District) -> a household-grain frame shaped as the 2014 hook sees it."""
    frame = pd.DataFrame(rows, columns=["v", "District"]).assign(t="2014", Region="R")
    return frame.set_index(["t", "v"])[["Region", "District"]]


class TestRules:
    derive_mode = staticmethod(lambda df: resolve_callable(
        derivation_records(COUNTRY)[MODAL_KEY].function)(df))

    def test_a_strict_mode_overwrites_and_stamps_every_row(self):
        out = self.derive_mode(_households([("1", "MIN"), ("1", "MODE"), ("1", "MODE")]))
        assert out["District"].tolist() == ["MODE"] * 3
        assert out["Derivation"].tolist() == [MODAL_KEY] * 3

    def test_a_zd_already_serving_the_mode_still_carries_the_key(self):
        out = self.derive_mode(_households([("1", "MODE"), ("1", "MODE"), ("1", "MIN")]))
        assert out["Derivation"].tolist() == [MODAL_KEY] * 3

    def test_a_tie_is_not_broken_and_carries_no_key(self):
        rows = [("1", "B"), ("1", "A"), ("1", "A"), ("1", "B")]
        out = self.derive_mode(_households(rows))
        assert out["District"].tolist() == [d for _, d in rows]
        assert out["Derivation"].isna().all()

    def test_a_top_tie_is_a_tie_with_a_third_province_below_it(self):
        out = self.derive_mode(_households([("1", "A"), ("1", "B"), ("1", "A"), ("1", "B"), ("1", "C")]))
        assert out["District"].tolist() == ["A", "B", "A", "B", "C"]
        assert out["Derivation"].isna().all()

    def test_a_uniform_zd_is_untouched_and_carries_no_key(self):
        out = self.derive_mode(_households([("1", "A"), ("1", "A"), ("2", "B")]))
        assert out["District"].tolist() == ["A", "A", "B"]
        assert out["Derivation"].isna().all()

    def test_row_order_and_shape_are_preserved(self):
        rows = [("2", "X"), ("1", "B"), ("2", "X"), ("1", "A"), ("2", "Y")]
        out = self.derive_mode(_households(rows))
        assert out.index.get_level_values("v").tolist() == [v for v, _ in rows]
        assert out["District"].tolist() == ["X", "B", "X", "A", "X"]

    def test_stamping_every_row_makes_the_collapse_lossless_except_for_a_tie(self):
        """The audit compares `Derivation` like any column on the selection branch,
        so a partly stamped zd would itself be the disagreement."""
        rows = [("1", "MODE")] * 3 + [("1", "MIN")] + [("2", "A"), ("2", "B")]
        out = self.derive_mode(_households(rows))
        report = _audit_index_collapse(out, ["t", "v"])
        assert report["conflicting_groups"] == 1 and report["destroyed"] == 1
        served = most_complete_row(out, ["t", "v"], exclude={"Derivation"})
        assert served.loc[("2014", "1"), "Derivation"] == MODAL_KEY
        assert served.loc[("2014", "1"), "District"] == "MODE"
        assert served.loc[("2014", "2"), "District"] == "A"
        assert pd.isna(served.loc[("2014", "2"), "Derivation"])


# --------------------------------------------------------------------------
# Oracles: source files read directly
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _non_strict():
    """2 tied zd keep the 2014 grain report, on purpose (GH #1060)."""
    prior = os.environ.pop("LSMS_GRAIN_STRICT", None)
    yield
    if prior is not None:
        os.environ["LSMS_GRAIN_STRICT"] = prior


@pytest.fixture(scope="module")
def served(_non_strict):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame = ll.Country(COUNTRY).cluster_features()
    flat = frame.reset_index()
    flat["t"] = flat["t"].astype(str)
    flat["v"] = flat["v"].astype(str)
    return flat


@pytest.fixture(scope="module")
def census():
    """Per zd, from the raw file: Counter of province, in row order."""
    lg = _read("2014", LOGEMENT)
    out = {}
    for zd, part in lg.groupby("zd", sort=False):
        out[str(int(zd))] = list(part["province"].astype(str))
    return out, lg


def _classify(census):
    strict, ties, uniform = {}, {}, set()
    for zd, provinces in census.items():
        counts = Counter(provinces)
        if len(counts) == 1:
            uniform.add(zd)
            continue
        top = max(counts.values())
        leaders = [p for p, n in counts.items() if n == top]
        if len(leaders) == 1:
            strict[zd] = leaders[0]
        else:
            ties[zd] = provinces[0]
    return strict, ties, uniform


@requires_s3
class TestOracleModalProvince:
    def test_the_census_the_ruling_was_given(self, census):
        raw, lg = census
        strict, ties, uniform = _classify(raw)
        assert len(lg) == 10800 and len(raw) == 900
        assert len(strict) == 92 and len(ties) == 2 and len(uniform) == 806
        assert set(ties) == {"362", "727"}
        assert all(Counter(raw[z]).most_common(1)[0][1] == 6 for z in ties)
        assert sum(len(set(raw[z])) == 3 for z in strict) + sum(
            len(set(raw[z])) == 3 for z in ties) == 8
        # Household counts of the 94 conflicted zd.
        conflicted = [*strict, *ties]
        assert sum(len(raw[z]) for z in conflicted) == 1128
        assert sum(len(raw[z]) - Counter(raw[z]).most_common(1)[0][1] for z in conflicted) == 169
        assert sum(len(raw[z]) - Counter(raw[z]).most_common(1)[0][1] for z in strict) == 157

    def test_region_and_milieu_never_vary_within_a_zd(self, census):
        _, lg = census
        group = lg.groupby("zd")
        assert (group["region"].nunique() == 1).all()
        assert (group["milieu"].nunique() == 1).all()

    def test_the_mode_lies_in_the_zds_region_and_the_outliers_do_not(self, census):
        """The EHCVM-1 cover pairs each province with one region, independently of
        the 2014 file."""
        raw, lg = census
        s18 = _read("2018-19", "s00_me_bfa2018.dta")
        to_region = {_fold(p): str(r).replace("-", " ").casefold()
                     for p, r in zip(s18["s00q02"], s18["s00q01"])}
        region_of = {str(int(z)): str(r).replace("-", " ").casefold()
                     for z, r in zip(lg["zd"], lg["region"])}
        region_of = {z: {"cascade": "cascades"}.get(r, r) for z, r in region_of.items()}
        strict, ties, uniform = _classify(raw)
        assert all(to_region[_fold(p)] == region_of[z] for z, p in strict.items())
        for z in ties:
            assert all(to_region[_fold(p)] == region_of[z] for p in set(raw[z]))
        outside = Counter(z for z in raw for p in raw[z] if to_region[_fold(p)] != region_of[z])
        assert sum(outside.values()) == 90 and len(outside) == 67
        assert not set(outside) & uniform

    def test_the_served_district_is_the_mode(self, census, served):
        strict, _, _ = _classify(census[0])
        w = served[served["t"] == "2014"].set_index("v")
        assert len(w) == 900 and w.index.is_unique
        wrong = {z: (p, w.loc[z, "District"]) for z, p in strict.items()
                 if _fold(w.loc[z, "District"]) != _fold(p)}
        assert not wrong, wrong

    def test_a_tied_zd_keeps_its_first_households_province_and_no_key(self, census, served):
        _, ties, _ = _classify(census[0])
        w = served[served["t"] == "2014"].set_index("v")
        for z, first in ties.items():
            assert _fold(w.loc[z, "District"]) == _fold(first), z
            assert pd.isna(w.loc[z, "Derivation"]), z

    def test_a_uniform_zd_is_what_the_source_says(self, census, served):
        raw = census[0]
        _, _, uniform = _classify(raw)
        w = served[served["t"] == "2014"].set_index("v")
        wrong = [z for z in uniform if _fold(w.loc[z, "District"]) != _fold(raw[z][0])]
        assert not wrong, wrong

    def test_the_key_is_exactly_on_the_constructed_rows(self, census, served):
        strict, _, _ = _classify(census[0])
        w = served[served["t"] == "2014"]
        assert set(w.loc[w["Derivation"] == MODAL_KEY, "v"]) == set(strict)
        assert w["Derivation"].notna().sum() == 92
        assert set(w["Derivation"].dropna()) == {MODAL_KEY}

    def test_the_wave_frame_is_household_grain_and_every_row_of_a_zd_agrees(self, census, _non_strict):
        strict, ties, _ = _classify(census[0])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            w = ll.Country(COUNTRY)["2014"].cluster_features().reset_index()
        assert len(w) == 10800
        agree = w.groupby("v")["District"].nunique()
        assert set(agree[agree > 1].index) == set(ties)
        for z, p in strict.items():
            part = w[w["v"] == z]
            assert set(part["District"]) == {p}, z
            assert (part["Derivation"] == MODAL_KEY).all(), z

    def test_the_grain_report_names_only_the_two_ties(self, served):
        reports = [r for r in grain_reports(country=COUNTRY, table="cluster_features")
                   if r.get("wave") == "2014"]
        assert reports, "the tied zd must keep the report"
        assert all(r["conflicting_groups"] == 2 and r["destroyed"] == 22 for r in reports)
        assert not [r for r in grain_reports(country=COUNTRY, table="cluster_features")
                    if r.get("wave") in ("2018-19", "2021-22")]

    def test_the_inputs_are_the_raw_answers_of_the_keyed_rows(self, census, served):
        raw = census[0]
        inputs = ll.Country(COUNTRY).derivation_inputs(MODAL_KEY)
        assert list(inputs.index.names) == ["t", "v", "menage"]
        assert inputs.index.is_unique and len(inputs) == 1104
        keyed = set(served.loc[served["Derivation"] == MODAL_KEY, "v"])
        assert set(inputs.index.get_level_values("v")) == keyed
        assert set(inputs.index.get_level_values("t")) == {"2014"}
        # The minority answers survive, as the survey recorded them.
        got = inputs.reset_index().groupby("v")["province"].apply(list).to_dict()
        assert all(Counter(got[z]) == Counter(raw[z]) for z in got)

    def test_the_function_reproduces_the_served_value_from_its_inputs(self, census):
        strict, _, _ = _classify(census[0])
        rec = derivation_records(COUNTRY)[MODAL_KEY]
        inputs = ll.Country(COUNTRY).derivation_inputs(MODAL_KEY).rename(
            columns={"province": "District"})
        out = rec.resolve("function")(inputs).reset_index()
        assert out.groupby("v")["District"].nunique().eq(1).all()
        assert out.groupby("v")["District"].first().to_dict() == strict
        assert (out["Derivation"] == MODAL_KEY).all()


@requires_s3
class TestConsumers:
    @pytest.mark.parametrize("wave", ["2014"])
    def test_market_district_is_the_served_district(self, wave, served, _non_strict):
        c = ll.Country(COUNTRY)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            plain = c.sample(waves=[wave])
            market = ll.Country(COUNTRY).sample(waves=[wave], market="District")
        assert len(market) == len(plain)
        assert market.index.get_level_values("m").notna().all()
        by_v = served[served["t"] == wave].set_index("v")["District"].astype(str).to_dict()
        joined = plain.reset_index()[["i", "v"]].merge(
            market.reset_index()[["i", "m"]], on="i", validate="1:1")
        wrong = joined[joined["v"].astype(str).map(by_v) != joined["m"].astype(str)]
        assert wrong.empty, wrong.head()

    def test_feature_fills_na_elsewhere_and_carries_the_summary(self, _non_strict):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame = ll.Feature("cluster_features")([COUNTRY, "Togo"])
        assert frame.index.is_unique
        flat = frame.reset_index()
        other = flat[flat["country"] != COUNTRY]
        assert len(other) and other["Derivation"].isna().all()
        mine = flat[flat["country"] == COUNTRY]
        assert mine["Derivation"].value_counts().to_dict() == {MODAL_KEY: 92}
        summary = frame.attrs["derivations"][COUNTRY]
        assert summary[MODAL_KEY]["rows"] == 92
        json.dumps(summary)

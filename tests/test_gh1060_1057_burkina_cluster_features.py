"""Burkina Faso ``cluster_features``: two registered constructions.

GH #1060 (ruling @ligon 2026-11-03, option b).  EMC 2014 records ``province`` per
household and it varies within 94 of 900 ``zd``.  Each zd with a strict mode is
served that mode as ``District`` on every row, and carries
``Burkina_Faso::cluster_features::zd-modal-province``.  The 2 tied zd are left as
served, the row core's collapse selects, and carry no key; they keep the grain
report, because their rows still disagree.

GH #1057 (ruling @ligon 2026-11-03, option b, tightened).  2021-22 ships no cluster
GPS.  A panel grappe's row carries its 2018-19 fix and the key
``Burkina_Faso::cluster_features::panel-grappe-gps-from-2018-19``.  Grappes 19, 365
and 549 name a different province in the two waves and stay NA; by the ruling of
2026-11-04 so do 103, 104, 226, 229, 237 and 240, whose fix lies more than 25 km from
their own 2021-22 parcels; the cross-section (586-1185) has no antecedent and stays NA.

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

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library import local_tools
from lsms_library.country import _audit_index_collapse, grain_reports, most_complete_row
from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.local_tools import (_collect_file_paths_from_block, get_dataframe,
                                      source_fingerprint)
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
GPS_KEY = "Burkina_Faso::cluster_features::panel-grappe-gps-from-2018-19"
PROVINCE_CHANGED = {"19", "365", "549"}
FAR_FROM_PARCELS = {"103", "104", "226", "229", "237", "240"}
EXCLUDED = PROVINCE_CHANGED | FAR_FROM_PARCELS
LOGEMENT = "emc2014_p1_logement_27022015.dta"
GPS_FILE = "grappe_gps_bfa2018.dta"
LAT, LON = "coordonnes_gps__Latitude", "coordonnes_gps__Longitude"


def _spec():
    with open(ROOT / "2021-22" / "_" / "data_info.yml", encoding="utf-8") as f:
        return yaml.safe_load(f)["cluster_features"]


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
        (GPS_KEY, "2021-22", ["Latitude", "Longitude"]),
    ])
    def test_registered_with_its_scope_and_resolving_callables(self, key, wave, columns):
        rec = derivation_records(COUNTRY)[key]
        assert rec.waves == [wave] and rec.columns == columns
        assert callable(rec.resolve("function")) and callable(rec.resolve("inputs"))
        assert rec.assumptions

    def test_the_gps_assumption_cites_the_tracking_statement(self):
        rec = derivation_records(COUNTRY)[GPS_KEY]
        tracked = [a for a in rec.assumptions if "bid_bfa_ehcvm2.pdf p. 4" in a.get("evidence", "")]
        assert tracked and "Households were tracked within the EA" in tracked[0]["evidence"]
        assert "questionnaire" in rec.bases and "modelling-choice" in rec.bases

    def test_the_gps_source_is_declared_so_the_hash_covers_it(self):
        spec = _spec()
        wave_data = ROOT / "2021-22" / "Data"
        declared = {os.path.normpath(wave_data / f) for f in _collect_file_paths_from_block(spec)}
        target = str(ROOT / "2018-19" / "Data" / GPS_FILE)
        assert target in declared
        assert source_fingerprint(target).startswith("dvc:")

    def test_the_merge_cannot_serve_a_2018_19_only_grappe(self):
        """35 grappes of the GPS file are not in the panel; `outer` would serve them."""
        assert _spec()["merge_how"] == "left"
        assert _spec()["dfs"][0] == "df_main"


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
    derive_gps = staticmethod(lambda df: resolve_callable(
        derivation_records(COUNTRY)[GPS_KEY].function)(df))

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

    def _gps_frame(self):
        frame = pd.DataFrame({
            "t": "2021-22",
            "v": ["2", "2", "19", "365", "549", "83", "586", "103", "229"],
            "Region": "R",
            "Latitude": [12.5, 12.5, 13.0, 10.0, np.nan, np.nan, np.nan, 12.3, 11.2],
            "Longitude": [-1.5, -1.5, 0.5, -3.0, np.nan, np.nan, np.nan, -1.5, -1.1]})
        return frame.set_index(["t", "v"])

    def test_gps_is_kept_and_stamped_on_a_carried_grappe_only(self):
        out = self.derive_gps(self._gps_frame()).reset_index().set_index("v")
        carried = out.loc["2"]
        assert carried["Latitude"].tolist() == [12.5, 12.5]
        assert carried["Derivation"].tolist() == [GPS_KEY, GPS_KEY]
        for v in ("19", "365", "549", "83", "586", "103", "229"):
            assert pd.isna(out.loc[v, "Latitude"]) and pd.isna(out.loc[v, "Longitude"]), v
            assert pd.isna(out.loc[v, "Derivation"]), v

    def test_an_unavailable_gps_source_stamps_nothing(self):
        frame = self._gps_frame().drop(columns=["Latitude", "Longitude"])
        out = self.derive_gps(frame)
        assert "Derivation" not in out.columns and "Latitude" not in out.columns


def test_an_unfetchable_gps_file_leaves_the_wave_without_gps(monkeypatch):
    """`df_geo` is a soft sub-df (GH #515) for the failures that handler catches,
    `FileNotFoundError` among them: those cost optional columns only, so the wave
    builds with no coordinate and no key.  This does not cover an unreachable
    remote, which raises `EndpointConnectionError`, outside that handler."""
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    cover = pd.DataFrame({"grappe": [2, 2, 586], "menage": [1, 2, 1],
                          "s00q01": ["Centre", "Centre", "Est"],
                          "s00q02": ["Kadiogo", "Kadiogo", "Gourma"],
                          "s00q04": ["Urbain", "Urbain", "Rural"]})

    def read(path, **kwargs):
        name = str(path).split("/Data/")[-1]
        if name == GPS_FILE:
            raise FileNotFoundError(name)
        part = 586 if name.startswith("Cross_Section") else 2
        return cover[cover["grappe"] == part].copy()

    monkeypatch.setattr(local_tools, "get_dataframe", read)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame = ll.Country(COUNTRY)["2021-22"].cluster_features()
    assert sorted(frame.index.get_level_values("v")) == ["2", "586"]
    assert not {"Latitude", "Longitude", "Derivation"} & set(frame.columns)


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


PARCEL_COLUMNS = ["s16aq46__latitude", "s16aq46__longitude"]


def _parcel_positions(frame):
    """The parcels with a position, or a failure that says why there can be none."""
    missing = [c for c in PARCEL_COLUMNS if c not in frame.columns]
    if missing:
        pytest.fail(
            f"s16a_me_bfa2021.dta has no {missing}: this file is not the release the six "
            "withheld distances were measured on.  The current release of catalog 6224 "
            "drops the parcel coordinates (Burkina_Faso/_/CONTENTS.org, \"what the World "
            "Bank states about 6224's versions\"), so this check cannot be repeated on it, "
            "and the carry's premise needs a fresh look.",
            pytrace=False)
    return frame.dropna(subset=PARCEL_COLUMNS)


def test_the_parcel_oracle_says_why_when_the_coordinates_are_gone():
    frame = pd.DataFrame({"grappe": [1], "s16aq45": ["Oui"]})
    with pytest.raises(pytest.fail.Exception, match="not the release"):
        _parcel_positions(frame)


@pytest.fixture(scope="module")
def sources():
    gps = _read("2018-19", GPS_FILE)
    panel = _read("2021-22", "s00_me_bfa2021.dta")
    cs = _read("2021-22", "Cross_Section/s00_me_bfa2021.dta")
    return gps.assign(v=gps["grappe"].astype(int).astype(str)).set_index("v"), panel, cs


@requires_s3
class TestOracleCarriedGps:
    def test_the_census_the_ruling_was_given(self, sources):
        gps, panel, cs = sources
        panel_v = set(panel["grappe"].astype(int).astype(str))
        assert len(panel_v) == 550 and panel_v <= set(gps.index)
        assert (panel["grappe"] == panel["grappe_EHCVM1"]).all()
        assert panel["GPS__Latitude"].isna().all()
        fixed = set(gps[gps[LAT].notna()].index)
        assert len(panel_v & fixed) == 527
        assert {"19", "365"} <= fixed and "549" not in fixed
        cs_v = cs["grappe"].astype(int)
        assert cs_v.between(586, 1185).all() and cs_v.nunique() == 600
        assert gps["grappe"].max() == 585
        assert not set(cs_v.astype(str)) & panel_v

    def test_the_excluded_grappes_are_exactly_the_ones_that_name_two_provinces(self, sources):
        """The rule behind the hard-coded three: if a re-release moves it, this fails."""
        _, panel, _ = sources
        s18 = _read("2018-19", "s00_me_bfa2018.dta")
        a = s18.groupby("grappe")["s00q02"].agg(lambda s: {_fold(x) for x in s})
        b = panel.groupby("grappe")["s00q02"].agg(lambda s: {_fold(x) for x in s})
        assert (a.map(len) == 1).all() and (b.map(len) == 1).all()
        shared = a.index.intersection(b.index)
        differ = {str(int(g)) for g in shared if a[g] != b[g]}
        assert differ == PROVINCE_CHANGED

    def test_the_withheld_six_are_exactly_the_panel_fixes_over_25_km_from_their_parcels(self, sources):
        """The rule behind the hard-coded six, from the parcels themselves.  Reads the s16a we
        hold; the current release of 6224 no longer has these columns, so acquiring it fails
        here (Burkina_Faso/_/CONTENTS.org, "what the World Bank states about 6224's versions")."""
        gps, panel, _ = sources
        parcels = _parcel_positions(_read("2021-22", "s16a_me_bfa2021.dta"))
        medians = parcels.groupby(parcels["grappe"].astype(int).astype(str))[
            ["s16aq46__latitude", "s16aq46__longitude"]].median()
        shared = sorted((set(medians.index) & set(gps[gps[LAT].notna()].index)
                         & set(panel["grappe"].astype(int).astype(str))) - PROVINCE_CHANGED)
        lat1, lon1 = np.radians(gps.loc[shared, LAT].astype(float)), np.radians(gps.loc[shared, LON].astype(float))
        lat2, lon2 = (np.radians(medians.loc[shared, "s16aq46__latitude"].astype(float)),
                      np.radians(medians.loc[shared, "s16aq46__longitude"].astype(float)))
        km = 6371.0088 * 2 * np.arcsin(np.sqrt(np.sin((lat2 - lat1) / 2) ** 2
                                               + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2))
        assert set(km[km > 25].index) == FAR_FROM_PARCELS

    def test_a_carried_row_equals_the_prior_waves_fix_exactly(self, sources, served):
        gps, panel, _ = sources
        expect = (set(panel["grappe"].astype(int).astype(str))
                  & set(gps[gps[LAT].notna()].index)) - EXCLUDED
        assert len(expect) == 519   # 525 until the six were withheld (ruling of 2026-11-04)
        w = served[served["t"] == "2021-22"].set_index("v")
        carried = w[w["Latitude"].notna() | w["Longitude"].notna()]
        assert set(carried.index) == expect
        for v, row in carried.iterrows():
            assert float(row["Latitude"]) == float(gps.loc[v, LAT]), v
            assert float(row["Longitude"]) == float(gps.loc[v, LON]), v

    def test_the_2018_19_row_is_the_same_fix_and_carries_no_key(self, sources, served):
        gps, _, _ = sources
        w = served[served["t"] == "2018-19"].set_index("v")
        assert len(w) == 585 and w["Latitude"].notna().sum() == 555
        assert w["Derivation"].isna().all()
        for v, row in w[w["Latitude"].notna()].iterrows():
            assert float(row["Latitude"]) == float(gps.loc[v, LAT]), v

    def test_the_excluded_grappes_stay_na_with_no_key(self, served):
        w = served[served["t"] == "2021-22"].set_index("v")
        for v in EXCLUDED:
            assert pd.isna(w.loc[v, "Latitude"]) and pd.isna(w.loc[v, "Longitude"]), v
            assert pd.isna(w.loc[v, "Derivation"]), v

    def test_the_cross_section_stays_na_with_no_key(self, sources, served):
        _, _, cs = sources
        w = served[served["t"] == "2021-22"].set_index("v")
        assert len(w) == 1150 and w.index.is_unique
        part = w.loc[sorted(set(cs["grappe"].astype(int).astype(str)))]
        assert len(part) == 600
        assert part["Latitude"].isna().all() and part["Longitude"].isna().all()
        assert part["Derivation"].isna().all()

    def test_the_key_is_exactly_on_the_rows_carrying_a_fix(self, served):
        w = served[served["t"] == "2021-22"]
        assert set(w["Derivation"].dropna()) == {GPS_KEY}
        assert (w["Derivation"].notna() == w["Latitude"].notna()).all()
        assert w["Derivation"].notna().sum() == 519

    def test_the_inputs_are_the_fix_behind_each_keyed_row(self, sources, served):
        gps, _, _ = sources
        inputs = ll.Country(COUNTRY).derivation_inputs(GPS_KEY)
        assert list(inputs.index.names) == ["t", "v"] and inputs.index.is_unique
        keyed = served[served["Derivation"] == GPS_KEY].set_index(["t", "v"])
        assert set(inputs.index) == set(keyed.index)
        assert (inputs["grappe"] == inputs["grappe_EHCVM1"]).all()
        for (_, v), row in inputs.iterrows():
            assert float(row[LAT]) == float(gps.loc[v, LAT]), v
            assert float(row[LON]) == float(gps.loc[v, LON]), v


@requires_s3
class TestConsumers:
    @pytest.mark.parametrize("wave", ["2014", "2021-22"])
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
        assert mine["Derivation"].value_counts().to_dict() == {GPS_KEY: 519, MODAL_KEY: 92}
        summary = frame.attrs["derivations"][COUNTRY]
        assert summary[MODAL_KEY]["rows"] == 92 and summary[GPS_KEY]["rows"] == 519
        json.dumps(summary)

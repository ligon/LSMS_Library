"""Burkina Faso: fixes that are not a reading of their own grappe (GH #1057).

Ruling (@ligon, 2026-11-05): withhold the copied 2018-19 GPS fixes, 555 to 546, and stop
carrying 216, 218 and 350 into 2021-22.  Nine grappes go NA in 2018-19: the seven whose fix
is a rounded copy of another grappe's reading, and 103 and 104, which hold a point in
Kadiogo.  The rows stay and carry no key, so the registered entry is ``rows: 0``.

``TestRules`` and ``TestRegistry`` need no data.  ``TestOracle`` reads the source files
directly and imports nothing from ``Burkina_Faso/_/``, so the literal lists in the wave
modules cannot agree with themselves.
"""
import importlib.util
import json
import os
import pathlib
import unicodedata
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
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
WITHHELD_KEY = "Burkina_Faso::cluster_features::copied-grappe-gps-withheld"
CARRY_KEY = "Burkina_Faso::cluster_features::panel-grappe-gps-from-2018-19"
GPS_FILE = "grappe_gps_bfa2018.dta"
LAT, LON = "coordonnes_gps__Latitude", "coordonnes_gps__Longitude"
ACC, ALT, STAMP = "coordonnes_gps__Accuracy", "coordonnes_gps__Altitude", "coordonnes_gps__Timestamp"

COPIES = {"216": "217", "218": "219", "226": "223", "237": "232",
          "240": "232", "350": "351", "365": "369"}
KADIOGO_POINT = {"103", "104"}
WITHHELD = set(COPIES) | KADIOGO_POINT
STOPPED_CARRYING = {"216", "218", "350"}


def _module(wave):
    path = ROOT / wave / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location(f"_bf_gh1057_{wave.replace('-', '_')}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _frame(wave, grappes):
    flat = pd.DataFrame({"t": wave, "v": grappes, "Region": "R",
                         "Latitude": 12.0, "Longitude": -1.5})
    return flat.set_index(["t", "v"])


class TestRules:
    def test_the_nine_are_blanked_and_nothing_else_moves(self):
        frame = _frame("2018-19", [*sorted(WITHHELD), "2", "232", "217", "89", "136", "229"])
        out = resolve_callable(derivation_records(COUNTRY)[WITHHELD_KEY].function)(frame)
        v = out.index.get_level_values("v")
        assert v.tolist() == frame.index.get_level_values("v").tolist()
        withheld = v.isin(WITHHELD)
        assert out.loc[withheld, ["Latitude", "Longitude"]].isna().all(axis=None)
        kept = ~withheld
        pd.testing.assert_frame_equal(out[kept], frame[kept])
        assert (out["Region"] == "R").all()

    def test_a_withheld_row_carries_no_key(self):
        out = _module("2018-19").cluster_features(_frame("2018-19", ["103", "2"]))
        assert "Derivation" not in out.columns

    def test_an_unavailable_source_has_nothing_to_withhold(self):
        frame = _frame("2018-19", ["103", "2"]).drop(columns=["Latitude", "Longitude"])
        out = _module("2018-19").withhold_copied_grappe_gps(frame)
        pd.testing.assert_frame_equal(out, frame)

    def test_the_three_are_no_longer_carried_and_carry_no_key(self):
        frame = _frame("2021-22", [*sorted(STOPPED_CARRYING), "2"])
        out = resolve_callable(derivation_records(COUNTRY)[CARRY_KEY].function)(frame)
        out = out.reset_index().set_index("v")
        for v in STOPPED_CARRYING:
            assert pd.isna(out.loc[v, "Latitude"]) and pd.isna(out.loc[v, "Longitude"]), v
            assert pd.isna(out.loc[v, "Derivation"]), v
        assert out.loc["2", "Latitude"] == 12.0 and out.loc["2", "Derivation"] == CARRY_KEY

    def test_nothing_withheld_in_2018_19_is_carried_into_2021_22(self):
        """The two lists live in two modules, and each wave reads the raw file."""
        assert set(_module("2018-19")._GPS_WITHHELD_GRAPPES) <= set(
            _module("2021-22")._GPS_NOT_CARRIED_GRAPPES)

    def test_the_withheld_set_is_the_copies_and_the_kadiogo_point(self):
        mod = _module("2018-19")
        assert mod._GPS_COPIED_READINGS == COPIES
        assert set(mod._GPS_WITHHELD_GRAPPES) == WITHHELD


class TestRegistry:
    def test_the_entry_is_a_deletion_with_resolving_callables(self):
        rec = derivation_records(COUNTRY)[WITHHELD_KEY]
        assert rec.waves == ["2018-19"] and rec.columns == ["Latitude", "Longitude"]
        assert rec.rows == 0
        assert callable(rec.resolve("function")) and callable(rec.resolve("inputs"))
        assert rec.assumptions and "modelling-choice" in rec.bases

    def test_the_inputs_refuse_another_wave(self):
        with pytest.raises(ValueError, match="2018-19 only"):
            _module("2018-19").inputs_cluster_features("2021-22")


def _fold(label):
    text = unicodedata.normalize("NFKD", str(label).strip().casefold())
    return "".join(c for c in text if not unicodedata.combining(c))


def _km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    h = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0088 * 2 * np.arcsin(np.sqrt(h))


@pytest.fixture(scope="module")
def fixes():
    """The file's fixes at ``v``, with each grappe's province from the cover."""
    gps = get_dataframe(str(ROOT / "2018-19" / "Data" / GPS_FILE))
    cover = get_dataframe(str(ROOT / "2018-19" / "Data" / "s00_me_bfa2018.dta"))
    province = (cover.assign(v=cover["grappe"].astype(int).astype(str))
                .groupby("v")["s00q02"].first().map(_fold))
    out = gps.assign(v=gps["grappe"].astype(int).astype(str)).set_index("v")
    out = out[out[LAT].notna()].copy()
    for col in (LAT, LON, ACC, ALT):
        out[col] = out[col].astype(float)
    out["province"] = province.reindex(out.index)
    return out


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
    return flat, frame.attrs


def _rounds_to(source, copy):
    """True if ``source`` rounded to one or two decimals is ``copy``."""
    return any(np.isclose(round(source, d), copy, atol=1e-9) for d in (1, 2))


@requires_s3
class TestOracle:
    def test_the_low_precision_fixes_are_exactly_the_copies(self, fixes):
        low = fixes[np.isclose(fixes[LAT], fixes[LAT].round(2), atol=1e-9)
                    & np.isclose(fixes[LON], fixes[LON].round(2), atol=1e-9)]
        assert set(low.index) == set(COPIES)
        assert (low["vague"].astype(float) == 2).all()

    @pytest.mark.parametrize("copy,source", sorted(COPIES.items()))
    def test_each_copy_is_its_sources_reading_rounded(self, fixes, copy, source):
        c, s = fixes.loc[copy], fixes.loc[source]
        assert c[STAMP] == s[STAMP]
        assert c[ALT] == s[ALT]
        assert abs(c[ACC] - s[ACC]) < 0.01
        assert c["province"] == s["province"]
        assert _rounds_to(s[LAT], c[LAT]) and _rounds_to(s[LON], c[LON])

    def test_the_only_timestamps_the_file_repeats_are_the_copies_and_their_sources(self, fixes):
        repeated = fixes[fixes[STAMP].duplicated(keep=False)]
        assert set(repeated.index) == set(COPIES) | set(COPIES.values())

    def test_103_and_104_are_the_side_of_the_only_close_cross_province_pairs_far_from_home(self, fixes):
        """Fixes of different provinces within 100 m, and which of each pair has left its
        own province: the side more than 50 km from that province's median fix."""
        v, lat, lon, prov = (fixes.index.to_numpy(), fixes[LAT].to_numpy(), fixes[LON].to_numpy(),
                             fixes["province"].to_numpy())
        medians = fixes.groupby("province")[[LAT, LON]].median()
        home = _km(lat, lon, medians.loc[prov, LAT].to_numpy(), medians.loc[prov, LON].to_numpy())
        home = dict(zip(v, home))
        paired, far = set(), set()
        for n in range(len(v) - 1):
            d = _km(lat[n], lon[n], lat[n + 1:], lon[n + 1:])
            for m in np.flatnonzero((d < 0.1) & (prov[n + 1:] != prov[n])):
                pair = (v[n], v[n + 1 + m])
                paired.update(pair)
                far.update(x for x in pair if home[x] > 50)
        assert paired == KADIOGO_POINT | {"89", "136"}
        assert far == KADIOGO_POINT

    def test_the_2018_19_rows_serve_the_file_except_the_nine(self, fixes, served):
        flat, _ = served
        w = flat[flat["t"] == "2018-19"].set_index("v")
        assert len(w) == 585 and w.index.is_unique
        assert len(fixes) == 555 and w["Latitude"].notna().sum() == 546
        assert w["Longitude"].notna().sum() == 546
        assert w.loc[sorted(WITHHELD), ["Latitude", "Longitude"]].isna().all(axis=None)
        kept = fixes.drop(index=sorted(WITHHELD))
        assert set(w[w["Latitude"].notna()].index) == set(kept.index)
        np.testing.assert_array_equal(w.loc[kept.index, "Latitude"].astype(float), kept[LAT])
        np.testing.assert_array_equal(w.loc[kept.index, "Longitude"].astype(float), kept[LON])
        assert w["Derivation"].isna().all()

    def test_the_2021_22_rows_carry_none_of_the_nine(self, served):
        flat, _ = served
        w = flat[flat["t"] == "2021-22"].set_index("v")
        assert w["Latitude"].notna().sum() == 516
        assert (w["Derivation"] == CARRY_KEY).sum() == 516
        part = w.loc[sorted(WITHHELD)]
        assert part["Latitude"].isna().all() and part["Derivation"].isna().all()

    def test_the_summary_lists_the_withheld_key_at_zero(self, served):
        _, attrs = served
        summary = attrs["derivations"]
        summary = summary.get(COUNTRY, summary)
        assert summary[WITHHELD_KEY]["rows"] == 0
        json.dumps(summary)

    def test_the_inputs_are_the_files_own_rows_for_the_nine(self, fixes):
        inputs = ll.Country(COUNTRY).derivation_inputs(WITHHELD_KEY)
        assert list(inputs.index.names) == ["t", "v"] and inputs.index.is_unique
        assert set(inputs.index.get_level_values("t")) == {"2018-19"}
        assert set(inputs.index.get_level_values("v")) == WITHHELD
        for (_, v), row in inputs.iterrows():
            assert float(row[LAT]) == fixes.loc[v, LAT], v
            assert float(row[LON]) == fixes.loc[v, LON], v
            assert row[STAMP] == fixes.loc[v, STAMP], v
            if v in COPIES:
                assert row["copy_of"] == COPIES[v], v
            else:
                assert pd.isna(row["copy_of"]), v

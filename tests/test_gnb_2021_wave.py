"""GH #1697: Guinea-Bissau 2021-22 (EHCVM-2, WB 6274) is wired, under ids of its own.

The Togo / Benin / Cote d'Ivoire 2021-22 tests are the template.  ``TestDeclared``
needs no microdata.  ``TestServed`` builds real tables and carries ``requires_s3``;
its ORACLE reads the ``.dta`` files through ``get_dataframe``, imports nothing from
``Guinea-Bissau/_/`` or ``Guinea-Bissau/2021-22/_/``, spells the household id itself
and decodes values from the CODES in the file where the labels could mislead.
"""
from __future__ import annotations

import importlib.util
import warnings

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

COUNTRY = "Guinea-Bissau"
WAVE = "2021-22"
OLD = "2018-19"
ROOT = countries_root() / COUNTRY / WAVE / "Data"
PREFIX = "E2_"
STRATUM_KEY = "Guinea-Bissau::sample::region-milieu-stratum"

#: The served spelling of each 2021-22 region label, written out here: three are spelled
#: otherwise than 2018-19 and fold onto 2018-19's served labels.
REGION = {"sab": "SAB", "Bafatá": "Bafata", "Gabú": "Gabu", "Oio": "Oio", "Cacheu": "Cacheu",
          "Tombali": "Tombali", "Bolama/Bijagós": "Bolama-Bijagos", "Quinara": "Quinara",
          "Biombo": "Biombo"}


def _raw(name, labels=False, wave=WAVE):
    return get_dataframe(countries_root() / COUNTRY / wave / "Data" / name,
                         convert_categoricals=labels)


def _i(grappe, menage):
    return f"{PREFIX}{int(grappe)}0{int(menage):02d}"


def _ids(frame):
    return [_i(g, m) for g, m in zip(frame["grappe"], frame["menage"])]


def _served(table, waves=(WAVE,)):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return getattr(ll.Country(COUNTRY), table)(waves=list(waves))


def _mapping_module():
    path = countries_root() / COUNTRY / WAVE / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location("gnb_2021_mapping_for_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _info():
    return yaml.safe_load((countries_root() / COUNTRY / WAVE / "_" / "data_info.yml")
                          .read_text(encoding="utf-8"))


class TestDeclared:
    def test_this_wave_prefixes_and_2018_does_not(self):
        c = ll.Country(COUNTRY)
        new = c[WAVE].formatting_functions["i"]
        old = c[OLD].formatting_functions["i"]
        assert new(pd.Series([155325, 1])) == "E2_155325001"
        assert old(pd.Series([155325, 1])) == "155325001"

    def test_the_stratum_rule_covers_this_wave(self):
        rec = ll.Country(COUNTRY).derivations()[STRATUM_KEY]
        assert list(rec.waves) == [OLD, WAVE]
        assert _info()["sample"]["df_cover"]["myvars"]["strata"][:2] == ["s00q01", "s00q04"]

    def test_the_weight_is_the_files_one_weight_in_both_columns(self):
        w = _info()["sample"]["df_weights"]
        assert w["file"] == "ehcvm_ponderations_gnb2021.dta"
        assert w["myvars"] == {"weight": "hhweight", "panel_weight": "hhweight"}

    def test_no_panel_link_is_declared(self):
        """No panel IDs (ruling 2026-11-12): this wave adds nothing to the crosswalk."""
        root = countries_root() / COUNTRY / "_"
        assert not (root / "panel_ids.json").exists()
        assert not (root / "updated_ids.json").exists()


@pytest.mark.requires_s3
class TestServed:
    def test_sample_is_the_cover_and_the_weights(self):
        cover = _raw("s00_me_gnb2021.dta")
        labels = get_dataframe(ROOT / "s00_me_gnb2021.dta", categories_only=True)
        weights = _raw("ehcvm_ponderations_gnb2021.dta")
        s = _served("sample").droplevel("t")
        assert s.index.is_unique and len(s) == len(cover) == 5351
        assert set(s.index) == set(_ids(cover))
        region = {int(k): REGION[str(v).strip()] for k, v in labels["s00q01"].items()}
        milieu = {int(k): str(v).strip() for k, v in labels["s00q04"].items()}
        assert set(milieu.values()) == {"Urbano", "Rural"}
        by_id = dict(zip(_ids(cover), cover["s00q01"].map(region) + " " + cover["s00q04"].map(milieu)))
        assert s["strata"].astype(str).to_dict() == by_id
        assert (s["Derivation"] == STRATUM_KEY).all()
        rural = dict(zip(_ids(cover), cover["s00q04"].map({k: {"Urbano": "Urban", "Rural": "Rural"}[v]
                                                           for k, v in milieu.items()})))
        assert s["Rural"].astype(str).to_dict() == rural
        assert s["v"].astype(str).to_dict() == dict(zip(_ids(cover), cover["grappe"].astype(int).astype(str)))
        raw = pd.Series(weights["hhweight"].to_numpy() / weights["hhweight"].mean(), index=_ids(weights))
        pd.testing.assert_series_equal(s["weight"].astype(float).sort_index(),
                                       raw.sort_index(), check_names=False)
        pd.testing.assert_series_equal(s["panel_weight"].astype(float).sort_index(),
                                       raw.sort_index(), check_names=False)

    def test_both_waves_serve_one_strata_vocabulary(self):
        s = _served("sample", waves=(OLD, WAVE))
        by_wave = {t: set(g["strata"].dropna()) for t, g in s.groupby(level="t")}
        assert by_wave[OLD] == by_wave[WAVE]
        assert len(by_wave[WAVE]) == 17            # 9 regions x 2 milieux, SAB urban only
        one = s.reset_index().groupby(["t", "v"])["strata"].nunique()
        assert (one <= 1).all()

    def test_the_ids_cannot_meet_2018s(self):
        s = _served("sample", waves=(OLD, WAVE))
        new = set(s.xs(WAVE, level="t").index)
        old = set(s.xs(OLD, level="t").index)
        assert not new & old
        assert all(str(i).startswith(PREFIX) for i in new)

    def test_the_registered_inputs_rederive_the_served_strata(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = ll.Country(COUNTRY).derivation_inputs(STRATUM_KEY, wave=WAVE)
        rec = ll.Country(COUNTRY).derivations()[STRATUM_KEY]
        rebuilt = pd.Series([rec.resolve()(r, m) for r, m in zip(inputs["s00q01"], inputs["s00q04"])],
                            index=inputs.index.droplevel("t"))
        served = _served("sample").droplevel("t")["strata"]
        assert len(rebuilt) == len(served)
        # The served value is the raw composite with the region part folded.
        folded = rebuilt.map(lambda x: " ".join([REGION[x.rsplit(" ", 1)[0]], x.rsplit(" ", 1)[1]]))
        assert (folded.reindex(served.index) == served).all()

    # --- household_roster -----------------------------------------------------------

    def _members(self):
        s01 = _raw("s01_me_gnb2021.dta")
        labels = get_dataframe(ROOT / "s01_me_gnb2021.dta", categories_only=True)["s01q00aa"]
        gone = {int(k) for k, v in labels.items() if str(v).strip().lower() in ("não", "nao")}
        assert gone, labels
        return s01, s01[~s01["s01q00aa"].isin(gone)], s01[s01["s01q00aa"].isin(gone)]

    def test_the_roster_is_the_members_ehcvm_individu_lists(self):
        s01, members, left = self._members()
        assert len(left) == 341
        r = _served("household_roster").reset_index()
        assert r.duplicated(["i", "pid"]).sum() == 0
        served = set(zip(r["i"].astype(str), r["pid"].astype(str)))
        assert served == {(_i(g, m), str(int(p))) for g, m, p in
                          zip(members["grappe"], members["menage"], members["membres__id"])}
        ind = _raw("ehcvm_individu_gnb2021.dta")
        assert served == {(_i(g, m), str(int(p))) for g, m, p in
                          zip(ind["grappe"], ind["menage"], ind["numind"])}
        assert not served & {(_i(g, m), str(int(p))) for g, m, p in
                             zip(left["grappe"], left["menage"], left["membres__id"])}

    def test_roster_sex_and_relationship_are_the_files(self):
        _, members, _ = self._members()
        labels = get_dataframe(ROOT / "s01_me_gnb2021.dta", categories_only=True)
        sex = {int(k): {"Masculino": "M", "Feminino": "F"}[str(v).strip()]
               for k, v in labels["s01q01"].items()}
        r = _served("household_roster").reset_index()
        got = dict(zip(zip(r["i"].astype(str), r["pid"].astype(str)), r["Sex"].astype(str)))
        want = {(_i(g, m), str(int(p))): sex[int(s)] for g, m, p, s in
                zip(members["grappe"], members["menage"], members["membres__id"], members["s01q01"])}
        assert got == want
        # Nephews and nieces (`Sobrinho(a)`, new in this wave) decompose: kinship.yml.
        rel = {int(k): str(v).strip() for k, v in labels["s01q02"].items()}
        code = [k for k, v in rel.items() if v == "Sobrinho(a)"][0]
        nephews = {(_i(g, m), str(int(p))) for g, m, p, c in
                   zip(members["grappe"], members["menage"], members["membres__id"], members["s01q02"])
                   if c == code}
        assert len(nephews) > 6000
        x = r[[k in nephews for k in zip(r["i"].astype(str), r["pid"].astype(str))]]
        assert len(x) == len(nephews)
        assert (x["Generation"] == -1).all() and (x["Distance"] == 1).all()
        assert (x["Affinity"] == "consanguineal").all()

    def test_ages_are_plausible_and_mostly_known(self):
        r = _served("household_roster")
        age = pd.to_numeric(r["Age"], errors="coerce")
        assert age.notna().mean() > 0.95
        assert age.min() >= 0 and age.max() < 120

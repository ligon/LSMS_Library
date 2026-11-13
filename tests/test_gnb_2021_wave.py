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
        hw = weights["hhweight"].astype("float64")
        raw = pd.Series(hw.to_numpy() / hw.mean(), index=_ids(weights))
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

    def test_the_roster_serves_everyone_and_flags_the_departed(self):
        """Ruling 2026-11-13, "Serve leavers": the members are ehcvm_individu's people and
        the 341 who have left are served beside them, `Departed` True."""
        s01, members, left = self._members()
        assert len(left) == 341
        r = _served("household_roster").reset_index()
        assert r.duplicated(["i", "pid"]).sum() == 0 and len(r) == len(s01) == 41501
        key = lambda f, p: {(_i(g, m), str(int(x))) for g, m, x in zip(f["grappe"], f["menage"], f[p])}
        gone = r[r["Departed"].astype("boolean").fillna(False)]
        stay = r[~r["Departed"].astype("boolean").fillna(False)]
        assert set(zip(gone["i"].astype(str), gone["pid"].astype(str))) == key(left, "membres__id")
        assert set(zip(stay["i"].astype(str), stay["pid"].astype(str))) == key(members, "membres__id")
        assert set(zip(stay["i"].astype(str), stay["pid"].astype(str))) == key(
            _raw("ehcvm_individu_gnb2021.dta"), "numind")
        assert r["Departed"].notna().all()

    def test_roster_sex_and_relationship_are_the_files(self):
        _, members, _ = self._members()
        labels = get_dataframe(ROOT / "s01_me_gnb2021.dta", categories_only=True)
        sex = {int(k): {"Masculino": "M", "Feminino": "F"}[str(v).strip()]
               for k, v in labels["s01q01"].items()}
        r = _served("household_roster").reset_index()
        r = r[~r["Departed"].astype("boolean").fillna(False)]   # the members; leavers below
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

    # --- cluster_features -------------------------------------------------------------

    def test_the_cluster_point_is_a_cluster_attribute(self):
        cover = _raw("s00_me_gnb2021.dta")
        points = cover.groupby("grappe")[["GPS__Latitude_m", "GPS__Longitude_m"]].nunique()
        assert (points == 1).all(axis=None)
        c = _served("cluster_features")
        assert len(c) == cover["grappe"].nunique() == 450
        expected = cover.groupby("grappe")["GPS__Latitude_m"].first().astype(float)
        got = c.droplevel("t")["Latitude"].astype(float)
        got.index = got.index.astype(int)
        pd.testing.assert_series_equal(got.sort_index(), expected.sort_index(),
                                       check_names=False, check_index_type=False)

    def test_region_and_rural_meet_2018s_vocabulary(self):
        c = _served("cluster_features", waves=(OLD, WAVE))
        new, old = c.xs(WAVE, level="t"), c.xs(OLD, level="t")
        assert set(new["Region"]) == set(old["Region"]) == set(REGION.values())
        assert set(new["Rural"]) == {"Rural", "Urban"}
        # Same grappes, same region and milieu as 2018-19 on every one.
        j = new.join(old, lsuffix="_21", rsuffix="_18", how="inner")
        assert len(j) == 450
        assert (j["Region_21"] == j["Region_18"]).all()
        assert (j["Rural_21"] == j["Rural_18"]).all()

    # --- shocks -----------------------------------------------------------------------

    def test_a_shock_row_is_a_shock_with_follow_ups(self):
        s14 = _raw("s14b_me_gnb2021.dta")
        followed = s14[s14["s14bq04a"].notna()]
        sh = _served("shocks")
        assert len(sh) == len(followed) == 4417
        assert sh.index.is_unique

    def test_the_food_effects_are_purchases_and_stock_not_the_herd(self):
        s14 = _raw("s14b_me_gnb2021.dta", labels=True)
        s14 = s14[s14["s14bq04a"].notna()]
        yes = {"Diminuiu", "Aumentou"}
        sh = _served("shocks")
        assert int(sh["AffectedConsumption"].astype("boolean").sum()) == int(s14["s14bq04f"].isin(yes).sum())
        assert int(sh["AffectedFoodStock"].astype("boolean").sum()) == int(s14["s14bq04e"].isin(yes).sum())
        assert int(s14["s14bq04d"].isin(yes).sum()) != int(s14["s14bq04f"].isin(yes).sum())

    def test_the_slots_are_the_ranks(self):
        s14 = _raw("s14b_me_gnb2021.dta", labels=True)
        s14 = s14[s14["s14bq04a"].notna()]
        sh = _served("shocks")
        first = int((s14[[f"s14bq05__{n}" for n in range(1, 27) if n != 25]]
                     == "Première stratégie").any(axis=1).sum())
        assert first > 4000
        assert sh["HowCoped0"].notna().sum() >= first
        m = _mapping_module()
        assert set(sh["HowCoped0"].dropna()) <= set(m.COPING_LABELS.values())



    # --- assets -----------------------------------------------------------------------

    def test_assets_are_the_owned_lines_of_the_grid(self):
        s12 = _raw("s12_me_gnb2021.dta")
        labels = get_dataframe(ROOT / "s12_me_gnb2021.dta", categories_only=True)
        oui = [int(k) for k, v in labels["s12q02"].items() if str(v).strip() == "Oui"]
        owned = s12[s12["s12q02"].isin(oui)]
        a = _served("assets")
        assert a["Quantity"].astype(float).sum() == pytest.approx(owned["s12q03"].sum())
        assert a["Value"].astype(float).sum() == pytest.approx(owned["s12q09"].sum())
        bikes = owned[owned["s12q01"] == 30]       # `Bicicleta, bicicleta de corrida`
        got = a.xs("Bicycle", level="j")["Quantity"].astype(float).sum()
        assert got == pytest.approx(bikes["s12q03"].sum())
        assert "Bicicleta, bicicleta de corrida" not in set(a.index.get_level_values("j"))


class TestCopingHook:
    """Benin's hook after GH #1556, on synthetic rows through this wave's function."""

    def _served(self, **cope):
        m = _mapping_module()
        row = {f"Cope{n}": "Non" for n in (1, 2, 3, 25, 26)}
        row.update({k.replace("c", "Cope"): v for k, v in cope.items()})
        out = m.shocks(pd.DataFrame([row]))
        assert not [c for c in out.columns if c.startswith("Cope")]
        return [out[f"HowCoped{k}"].iloc[0] for k in range(3)], m.COPING_LABELS

    def test_ranks_order_the_slots_not_the_codes(self):
        got, lab = self._served(c1="Deuxième strategie", c2="Première stratégie")
        assert got[:2] == [lab[2], lab[1]] and pd.isna(got[2])

    def test_autre_fills_the_rank_no_other_strategy_holds(self):
        got, lab = self._served(c1="Première stratégie", c25="Oui")
        assert got[:2] == [lab[1], lab[25]] and pd.isna(got[2])

    def test_an_unknown_answer_is_refused(self):
        with pytest.raises(ValueError, match="unknown answers"):
            self._served(c1="Primeira estratégia")


@pytest.mark.requires_s3
def test_a_two_wave_roster_keeps_2018s_whole_months():
    """The served 2021-22 leavers carry MonthsSpent NA, so the column is nullable whole
    numbers (Int64), never float; 2018-19's values are unchanged, and only 2021-22 has
    Departed."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = ll.Country(COUNTRY).household_roster(waves=[OLD, WAVE])
    assert str(r["MonthsSpent"].dtype) == "Int64"
    old = r.xs(OLD, level="t")
    assert old["MonthsSpent"].notna().all() and set(old["MonthsSpent"].unique()) <= {0, 12}
    assert old["Departed"].isna().all()
    assert r.xs(WAVE, level="t")["Departed"].notna().all()


DEPARTED_MONTHS = "Guinea-Bissau::household_roster::departed-monthsspent-withheld"


@pytest.mark.requires_s3
def test_the_departed_keep_their_recorded_sex_and_nothing_is_invented():
    """The 341 leavers: 246 carry a sex in s01q01, the other 95 none, and none is given
    one."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = ll.Country(COUNTRY).household_roster(waves=[WAVE])
    gone = r[r["Departed"].astype("boolean").fillna(False)]
    assert len(gone) == 341
    assert gone["Sex"].notna().sum() == 246


def test_the_departed_months_key_is_registered():
    rec = ll.Country(COUNTRY).derivations()[DEPARTED_MONTHS]
    assert list(rec.waves) == [WAVE] and list(rec.columns) == ["MonthsSpent"]
    f = rec.resolve()
    got = f(pd.Series([12, 12, 0, None]), np.array([True, False, True, False]))
    assert got.isna().tolist() == [True, False, True, True]
    assert got.iloc[1] == 12


@pytest.mark.requires_s3
def test_a_leavers_months_are_withheld_and_keyed():
    """Ruling 2026-11-13, "Exclude from HH size": MonthsSpent is NA on every leaver, and
    the 12 rows whose s01q12 the survey recorded carry the key; their answers are the
    registry inputs, which join them at (t, i, pid)."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        r = ll.Country(COUNTRY).household_roster(waves=[WAVE])
        inp = ll.Country(COUNTRY).derivation_inputs(DEPARTED_MONTHS, wave=WAVE)
    gone = r["Departed"].astype("boolean").fillna(False)
    assert r.loc[gone, "MonthsSpent"].isna().all()
    assert r.loc[~gone, "MonthsSpent"].notna().any()
    keyed = r["Derivation"].fillna("").str.contains(DEPARTED_MONTHS, regex=False)
    assert int(keyed.sum()) == 12 and not (keyed & ~gone).any()
    assert len(inp) == 12 and (inp["s01q12"].astype(str) == "Sim").all()
    served = set(zip(r.index.get_level_values("i")[keyed.to_numpy()].astype(str),
                     r.index.get_level_values("pid")[keyed.to_numpy()].astype(str)))
    assert served == set(zip(inp.index.get_level_values("i").astype(str),
                             inp.index.get_level_values("pid").astype(str)))


ROUNDED = "Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even"
ZERO_WITHHELD = "Guinea-Bissau::food_acquired::s07bq03a-rounded-to-zero-withheld"


class TestFoodDeclared:
    def test_the_two_rounding_keys_are_registered(self):
        recs = ll.Country(COUNTRY).derivations()
        r, z = recs[ROUNDED], recs[ZERO_WITHHELD]
        assert list(r.waves) == list(z.waves) == [WAVE]
        assert r.rows == {} and z.rows == {"s": "purchased"}
        assert r.resolve()(7.0) == 7.0                      # served as released
        assert z.resolve()([0, 2]).isna().tolist() == [True, False]   # a 0 is withheld
        assert "Guinea-Bissau::food_acquired::s07b-consumed-quantity-rounded-to-zero" not in recs
        m = _mapping_module()
        assert list(m.round_half_up_to_even([0.5, 1, 1.5, 2.5, 3, 7])) == [0, 2, 2, 2, 4, 8]

    def test_the_purchase_unit_is_read_on_the_consumption_units_spelling(self):
        m = _mapping_module()
        assert m.LastPurchaseUnitCode("Lata") == "Lata/caneca"
        assert m.LastPurchaseUnitCode("Pedaço (25 F)") == "Pedaço"
        assert m.LastPurchaseUnitCode("Monte") == "Monte"


@pytest.mark.requires_s3
class TestFoodServed:
    @pytest.fixture(scope="class")
    def fa(self):
        return _served("food_acquired", waves=(OLD, WAVE))

    def test_every_2021_row_carries_one_of_the_two_keys_and_no_2018_row_does(self, fa):
        new, old = fa.xs(WAVE, level="t"), fa.xs(OLD, level="t")
        d = new["Derivation"].fillna("")
        rounded = d.str.contains(ROUNDED, regex=False)
        held = d.str.contains(ZERO_WITHHELD, regex=False)
        assert (rounded ^ held).all()
        for key in (ROUNDED, ZERO_WITHHELD):
            assert not old["Derivation"].fillna("").str.contains(key, regex=False).any()

    def test_the_zeroed_rows_are_served_one_purchased_row_na(self, fa):
        """Ruling 2026-11-13, "Serve NA, as Togo": each of the 3,945 consumed rows the
        release rounds to 0 is served one purchased row with Quantity and Expenditure NA,
        and the registry inputs, which carry s07bq06, are those rows."""
        new = fa.xs(WAVE, level="t")
        held = new[new["Derivation"].fillna("").str.contains(ZERO_WITHHELD, regex=False)]
        assert len(held) == 3945
        assert set(held.index.get_level_values("s")) == {"purchased"}
        assert held["Quantity"].isna().all() and held["Expenditure"].isna().all()
        assert new.index.is_unique
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            z = ll.Country(COUNTRY).derivation_inputs(ZERO_WITHHELD, wave=WAVE)
        assert len(z) == 3945 and "s07bq06" in z.columns
        assert (pd.to_numeric(z["s07bq03a"], errors="coerce") == 0).all()
        assert (z["s07bq02"] == "Oui").all()
        pairs = lambda f: set(zip(f.index.get_level_values("i").astype(str),
                                  f.index.get_level_values("j").astype(str)))
        assert pairs(held) == pairs(z)

    def test_the_served_quantities_are_the_releases_even_integers(self, fa):
        """Every served quantity is an integer, and even but for 46: the 23 rows where
        s07bq03a = s07bq04 + s07bq05 keep an odd s07bq04 and s07bq05, served on their
        produced and in-kind rows (they leave no purchased residual)."""
        new = fa.xs(WAVE, level="t")
        q = pd.to_numeric(new["Quantity"], errors="coerce")
        assert q.notna().sum() > 100_000
        q = q.dropna()
        assert (q == np.floor(q)).all()
        odd = q[np.mod(q, 2) != 0]
        assert len(odd) == 46
        s = pd.Series(odd.index.get_level_values("s")).value_counts().to_dict()
        assert s == {"produced": 23, "inkind": 23}

    def test_the_rounding_inputs_join_the_served_rows(self, fa):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inp = ll.Country(COUNTRY).derivation_inputs(ROUNDED, wave=WAVE)
        new = fa.xs(WAVE, level="t")
        new = new[new["Derivation"].fillna("").str.contains(ROUNDED, regex=False)]
        served = set(zip(new.index.get_level_values("i").astype(str),
                         new.index.get_level_values("j").astype(str)))
        keys = set(zip(inp.index.get_level_values("i").astype(str),
                       inp.index.get_level_values("j").astype(str)))
        assert served <= keys

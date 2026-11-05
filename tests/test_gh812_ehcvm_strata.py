"""What ``sample.strata`` holds in the EHCVM waves (GH #812, ruling option W2, 2026-11-04).

Senegal, Mali, Niger and Burkina Faso serve region x milieu as a LABEL, the documented
stratum, and register it as a derivation because no household file records a stratum.
Niger 2014-15 reads ``strate``, which a file does record.  Mali 2014-15 stays empty.
Benin, Togo, Guinea-Bissau and Cote d'Ivoire keep the region alone.

The classes marked ``requires_s3`` read the covers directly and import nothing from
``countries/``; the allocation tables are transcribed from the BIDs.  The others use
invented frames and config and need no data.
"""
import importlib.util
import pickle
import re
import unicodedata
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import data_access, local_tools
from lsms_library.build_transforms import GrainConflict
from lsms_library.derivations import derivation_records
from lsms_library.ehcvm import (
    StratumError,
    derive_region_milieu_stratum,
    stamp_region_milieu_stratum,
    strata_label,
    stratum_derivation_key,
)
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

ROOT = countries_root()
MILIEUX = ("Urbain", "Rural")
EHCVM_WAVES = ("2018-19", "2021-22")
COUNTRIES = ("Senegal", "Mali", "Niger", "Burkina_Faso")
REGION_ONLY = {"Benin": "2018-19", "Togo": "2018", "Guinea-Bissau": "2018-19",
               "CotedIvoire": "2018-19"}

#: The one addition to Region x milieu: the 2018-19 cover writes Taoudénit and the file
#: cluster_features reads writes Taoudenit.
EXTRA_ROWS = {"Mali": {"Taoudénit Urbain": "Taoudenit Urbain",
                       "Taoudénit Rural": "Taoudenit Rural"}}

#: Every cover a wave's strata is built from.
COVERS = {
    ("Senegal", "2018-19"): ["Data/s00_me_sen2018.dta"],
    ("Senegal", "2021-22"): ["Data/s00_me_sen2021.dta"],
    ("Mali", "2018-19"): ["Data/s00_me_mli2018.dta"],
    ("Mali", "2021-22"): ["Data/s00_me_mli2021.dta"],
    ("Niger", "2018-19"): ["Data/s00_me_ner2018.dta"],
    ("Niger", "2021-22"): ["Data/s00_me_ner2021.dta"],
    ("Burkina_Faso", "2018-19"): ["Data/s00_me_bfa2018.dta"],
    ("Burkina_Faso", "2021-22"): ["Data/s00_me_bfa2021.dta",
                                  "Data/Cross_Section/s00_me_bfa2021.dta"],
}
CELLS = {("Senegal", "2018-19"): 28, ("Senegal", "2021-22"): 28,
         ("Mali", "2018-19"): 20, ("Mali", "2021-22"): 17,
         ("Niger", "2018-19"): 16, ("Niger", "2021-22"): 15,
         ("Burkina_Faso", "2018-19"): 26, ("Burkina_Faso", "2021-22"): 26}

#: sen_ehcvm2_information_de_base_eng.pdf, Table 1a, p.12: clusters, both waves together.
SENEGAL_2018_TABLE_1A = {
    "Dakar": (76, 10), "Ziguinchor": (22, 18), "Diourbel": (22, 24), "Saint-Louis": (22, 20),
    "Tambacounda": (16, 20), "Kaolack": (24, 20), "Thies": (26, 22), "Louga": (20, 20),
    "Fatick": (18, 20), "Kolda": (16, 20), "Matam": (16, 18), "Kaffrine": (16, 20),
    "Kedougou": (20, 18), "Sedhiou": (16, 18),
}
#: mli_ehcvm2_information_de_base_eng.pdf, Table 1, p.10: clusters, both waves together.
MALI_2021_TABLE_1 = {
    "Kayes": (23, 42), "Koulikoro": (19, 43), "Sikasso": (26, 42), "Ségou": (20, 41),
    "Mopti": (22, 34), "Tombouctou": (18, 32), "Gao": (24, 37), "Kidal": (13, 14),
    "Bamako": (63, 0),
}
#: ner_ehcvm2_information_de_base_eng.pdf, Table 1, p.9: households, both waves together.
#: Tillaberi urban is printed 106 + 192 and is 96 + 192: the row's own total of 336 (96 +
#: 240), the column's total of 1,177 and the BID's second table, p.11, all read 96.
NIGER_2021_TABLE_1 = {
    "Agadez": (273, 356), "Diffa": (287, 492), "Dosso": (237, 581), "Maradi": (228, 743),
    "Tahoua": (238, 659), "Tillabéri": (96 + 192, 443), "Zinder": (240, 837),
    "Niamey": (720, 0),
}


def _org_tables(country):
    return all_dfs_from_orgfile(ROOT / country / "_" / "categorical_mapping.org")


def _lookup(table):
    return dict(zip(table.iloc[:, 0], table["Preferred Label"]))


def _sample_block(country, wave):
    return load_yaml(ROOT / country / wave / "_" / "data_info.yml")["sample"]


class TestLabel:
    def test_region_then_milieu(self):
        assert derive_region_milieu_stratum("Kayes", "Urbain") == "Kayes Urbain"

    def test_padding_is_stripped(self):
        assert derive_region_milieu_stratum(" Kayes ", "Rural ") == "Kayes Rural"

    @pytest.mark.parametrize("region, milieu", [
        (pd.NA, "Rural"), ("Kayes", pd.NA), (None, "Rural"), ("Kayes", float("nan")),
        ("", "Rural"), ("Kayes", "  "),
    ])
    def test_na_where_either_is_missing(self, region, milieu):
        assert derive_region_milieu_stratum(region, milieu) is pd.NA

    @pytest.mark.parametrize("region, milieu", [(1, "Rural"), ("Kayes", 2), (1.0, 2.0)])
    def test_a_code_is_refused(self, region, milieu):
        with pytest.raises(StratumError, match="never from codes"):
            derive_region_milieu_stratum(region, milieu)

    def test_the_cell_formatter_reads_region_then_milieu(self):
        row = pd.Series({"s00q01": "Gao", "s00q04": "Rural"})
        assert strata_label(row) == "Gao Rural"


class TestStamp:
    def test_only_a_row_with_a_stratum_carries_the_key(self):
        df = pd.DataFrame({"strata": ["Gao Rural", pd.NA], "v": ["1", "2"]}, index=["a", "b"])
        out = stamp_region_milieu_stratum(df, "Mali")
        assert out.loc["a", "Derivation"] == stratum_derivation_key("Mali")
        assert pd.isna(out.loc["b", "Derivation"])
        assert "Derivation" not in df.columns

    def test_the_key_is_the_registered_one(self):
        for country in COUNTRIES:
            assert stratum_derivation_key(country) in derivation_records(country)


class TestStrataTable:
    """Region x milieu, row for row, so ``strata`` and ``Region`` cannot drift apart."""

    @pytest.mark.parametrize("country", COUNTRIES)
    def test_the_table_is_the_region_table_crossed_with_milieu(self, country):
        tables = _org_tables(country)
        region = _lookup(tables["Region"])
        expected = {f"{raw} {m}": f"{preferred} {m}"
                    for raw, preferred in region.items() for m in MILIEUX}
        expected.update(EXTRA_ROWS.get(country, {}))
        assert _lookup(tables["strata"]) == expected

    @pytest.mark.parametrize("country", COUNTRIES)
    def test_no_other_schemes_label_is_a_key(self, country):
        """Mali 2017-18 and Burkina 2014 serve codes, Niger's ECVMA `Agadez urbain` and
        `Niamey`.  None may be rewritten."""
        keys = set(_lookup(_org_tables(country)["strata"]))
        assert not {"1", "45", "Niamey", "Agadez urbain", "Niamey urbain"} & keys


class TestWiring:
    @pytest.mark.parametrize("country", COUNTRIES)
    @pytest.mark.parametrize("wave", EHCVM_WAVES)
    def test_every_ehcvm_wave_builds_it_from_the_cover_labels(self, country, wave):
        myvars = _sample_block(country, wave)["df_cover"]["myvars"]
        assert myvars["strata"] == ["s00q01", "s00q04", {"mapping": "strata_label"}]

    @pytest.mark.parametrize("country, wave", REGION_ONLY.items())
    def test_the_region_only_countries_are_not_moved(self, country, wave):
        block = _sample_block(country, wave)
        assert block["df_cover"]["myvars"]["strata"] == "s00q01"
        assert stratum_derivation_key(country) not in derivation_records(country)

    def test_mali_2014_declares_no_strata(self):
        assert "strata" not in str(_sample_block("Mali", "2014-15"))

    def test_niger_2014_reads_strate(self):
        block = _sample_block("Niger", "2014-15")
        assert block["df_strate"]["file"].endswith("ECVMA2014_P1P2_Conso.dta")
        assert block["df_strate"]["myvars"] == {"strata": "strate"}

    def test_senegal_strata_is_no_longer_optional(self):
        scheme = load_yaml(ROOT / "Senegal" / "_" / "data_scheme.yml")["Data Scheme"]["sample"]
        assert scheme["strata"] == "str"

    @pytest.mark.parametrize("country", COUNTRIES)
    def test_the_registry_entry_names_the_function_and_the_cover(self, country):
        rec = derivation_records(country)[stratum_derivation_key(country)]
        assert rec.resolve() is derive_region_milieu_stratum
        assert callable(rec.resolve("inputs"))
        assert list(rec.waves) == list(EHCVM_WAVES)
        assert list(rec.columns) == ["strata"]
        assert list(rec.raw_variables) == ["s00q01", "s00q04"]


class TestNigerStrateHook:
    @staticmethod
    def _hook():
        path = ROOT / "Niger" / "2014-15" / "_" / "mapping.py"
        spec = importlib.util.spec_from_file_location("_niger_2014_gh812", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.df_strate

    @staticmethod
    def _frame(labels):
        index = pd.MultiIndex.from_arrays(
            [["2014-15"] * len(labels), ["1"] * len(labels)], names=["t", "v"])
        return pd.DataFrame({"strata": labels}, index=index)

    def test_a_constant_grappe_reduces_to_one_row(self):
        out = self._hook()(self._frame(["Niamey", "Niamey", "Niamey"]))
        assert out["strata"].tolist() == ["Niamey"]

    def test_a_grappe_with_two_strata_is_refused(self):
        with pytest.raises(StratumError, match="Niger/2014-15/sample") as caught:
            self._hook()(self._frame(["Niamey", "Dosso urbain"]))
        assert isinstance(caught.value.__cause__, GrainConflict)


class TestARefusalIsNotSwallowed:
    """`_join_v_from_sample` catches ValueError and logs it, and the derived-table paths
    catch RuntimeError, ValueError and KeyError, so a refusal must be none of them."""

    @staticmethod
    def _grab(monkeypatch, tmp_path, country, wave, frames):
        """Through Wave.grab_data, with invented frames and a private data root."""
        monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("LSMS_NO_CACHE", "1")
        monkeypatch.setattr(local_tools, "get_dataframe",
                            lambda path, **kwargs: frames[str(path).split("/Data/")[-1]].copy())
        with pytest.raises(StratumError) as caught:
            ll.Country(country)[wave].grab_data("sample")
        assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError, AttributeError,
                                             FileNotFoundError))
        return caught.value

    def test_a_grappe_with_two_strata_reaches_the_caller_of_the_wave_build(self, monkeypatch, tmp_path):
        folder = "NER_2014_ECVMA-II_v02_M_STATA8/"
        frames = {
            folder + "ECVMA2014welfare_T1c.dta": pd.DataFrame(
                {"GRAPPE": [2], "MENAGE": [1], "EXTENSION": [0], "hhweight": [9.0], "urbrur": ["Rural"]}),
            folder + "ECVMA2014_P1P2_Conso.dta": pd.DataFrame(
                {"GRAPPE": [2, 2], "strate": ["Niamey", "Dosso urbain"]}),
        }
        refusal = self._grab(monkeypatch, tmp_path, "Niger", "2014-15", frames)
        assert isinstance(refusal.__cause__, GrainConflict)

    def test_a_code_reaches_the_caller_of_the_wave_build(self, monkeypatch, tmp_path):
        frames = {
            "s00_me_mli2021.dta": pd.DataFrame(
                {"grappe": [2], "menage": [1], "s00q01": [1], "s00q04": ["Rural"]}),
            "ehcvm_ponderations_mli2021.dta": pd.DataFrame(
                {"grappe": [2], "menage": [1], "hhweight": [9.0]}),
        }
        refusal = self._grab(monkeypatch, tmp_path, "Mali", "2021-22", frames)
        assert "never from codes" in str(refusal)

    def test_it_pickles_across_parallel_wave_workers(self):
        """GH #1524: a class defined in a by-path mapping.py does not."""
        assert StratumError.__module__ == "lsms_library.ehcvm"
        back = pickle.loads(pickle.dumps(StratumError("two strata")))
        assert type(back) is StratumError and str(back) == "two strata"


# ---------------------------------------------------------------------------
# Against the source.  Nothing below imports from countries/.
# ---------------------------------------------------------------------------

def _fold(text):
    """Case, accents, punctuation away, and the covers' two misprints mended."""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z]", "", text)
    return {"gadez": "agadez", "boucledumouhoum": "boucledumouhoun"}.get(text, text)


def _read(country, wave, rel, columns):
    path = data_access.get_data_file(f"{country}/{wave}/{rel}")
    return pd.read_stata(path, columns=columns, convert_categoricals=True)


@pytest.fixture(scope="module")
def served():
    frames = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for country in COUNTRIES:
            frame = ll.Country(country).sample()
            flat = frame.reset_index()
            flat["t"] = flat["t"].astype(str)
            frames[country] = flat
    return frames


def _clusters(flat, wave):
    rows = flat[flat["t"] == wave]
    per = rows.groupby("v")["strata"].nunique()
    assert (per <= 1).all(), "a cluster holds two strata"
    return rows.drop_duplicates("v").set_index("v")["strata"]


def _cells(flat, wave):
    """{(region, milieu): (clusters, households)} of one served wave."""
    rows = flat[flat["t"] == wave].copy()
    parts = rows["strata"].str.rsplit(" ", n=1)
    rows["region"], rows["milieu"] = parts.str[0], parts.str[1]
    return {key: (g["v"].nunique(), len(g)) for key, g in rows.groupby(["region", "milieu"])}


@pytest.mark.requires_s3
class TestOracle:
    @pytest.mark.parametrize("key", COVERS, ids=lambda k: f"{k[0]}-{k[1]}")
    def test_every_cluster_serves_its_cover_labels(self, served, key):
        country, wave = key
        expected = {}
        for rel in COVERS[key]:
            cover = _read(country, wave, rel, ["grappe", "s00q01", "s00q04"])
            for grappe, region, milieu in cover.drop_duplicates("grappe").itertuples(index=False):
                expected[str(int(grappe))] = f"{_fold(region)} {milieu}"
        got = {v: f"{_fold(s.rsplit(' ', 1)[0])} {s.rsplit(' ', 1)[1]}"
               for v, s in _clusters(served[country], wave).items()}
        assert got == expected

    @pytest.mark.parametrize("key", CELLS, ids=lambda k: f"{k[0]}-{k[1]}")
    def test_cell_counts(self, served, key):
        country, wave = key
        rows = served[country][served[country]["t"] == wave]
        assert rows["strata"].nunique() == CELLS[key]
        assert rows["strata"].notna().all()

    def test_burkina_each_2021_component_has_26_cells(self):
        for rel in COVERS[("Burkina_Faso", "2021-22")]:
            cover = _read("Burkina_Faso", "2021-22", rel, ["s00q01", "s00q04"])
            assert cover.astype("string").drop_duplicates().shape[0] == 26, rel

    def test_niger_2014_serves_the_files_strate(self, served):
        conso = _read("Niger", "2014-15",
                      "Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2014_P1P2_Conso.dta",
                      ["GRAPPE", "strate"])
        conso = conso.astype({"strate": "string"})
        assert conso.groupby("GRAPPE")["strate"].nunique().max() == 1
        expected = {str(int(g)): s for g, s in conso.drop_duplicates("GRAPPE").itertuples(index=False)}
        assert _clusters(served["Niger"], "2014-15").to_dict() == expected

    def test_niger_2011_keeps_its_recorded_labels(self, served):
        got = _clusters(served["Niger"], "2011-12")
        d = _read("Niger", "2011-12", "Data/NER_2011_ECVMA_v01_M_Stata8/ecvmamen_p1_en.dta",
                  ["grappe", "strate"]).astype({"strate": "string"})
        assert got.to_dict() == {str(int(g)): s for g, s in d.drop_duplicates("grappe").itertuples(index=False)}

    def test_mali_2014_stays_empty_and_2017_keeps_its_codes(self, served):
        flat = served["Mali"]
        assert flat.loc[flat["t"] == "2014-15", "strata"].isna().all()
        codes = flat.loc[flat["t"] == "2017-18", "strata"]
        assert codes.notna().all() and codes.str.fullmatch(r"\d+").all()

    def test_only_the_constructed_rows_carry_the_key(self, served):
        for country in COUNTRIES:
            flat = served[country]
            ehcvm = flat["t"].isin(EHCVM_WAVES)
            assert (flat.loc[ehcvm, "Derivation"] == stratum_derivation_key(country)).all()
            assert flat.loc[~ehcvm, "Derivation"].isna().all()


@pytest.mark.requires_s3
class TestAllocationEqualsTheDocuments:
    """Senegal's Table 1a is an allocation: 7,176 households planned, 7,156 held.  Mali's and
    Niger's tables are titled "initial sample" but total to the delivered file, so they tabulate
    the same two cover variables: they check this reading of the cover, and do not corroborate
    that the cells are strata."""

    @staticmethod
    def _expected(table):
        return {(region, m): n for region, pair in table.items()
                for m, n in zip(MILIEUX, pair) if n}

    def test_senegal_2018_table_1a_clusters(self, served):
        got = {k: v[0] for k, v in _cells(served["Senegal"], "2018-19").items()}
        assert got == self._expected(SENEGAL_2018_TABLE_1A)

    def test_mali_2021_table_1_clusters(self, served):
        got = {k: v[0] for k, v in _cells(served["Mali"], "2021-22").items()}
        assert got == self._expected(MALI_2021_TABLE_1)

    def test_niger_2021_table_1_households(self, served):
        got = {k: v[1] for k, v in _cells(served["Niger"], "2021-22").items()}
        assert got == self._expected(NIGER_2021_TABLE_1)


@pytest.mark.requires_s3
class TestTheFrameAgrees:
    @pytest.mark.parametrize("country", COUNTRIES)
    def test_region_part_is_cluster_features_region(self, served, country):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cf = ll.Country(country).cluster_features().reset_index()
        cf["t"] = cf["t"].astype(str)
        rows = served[country]
        rows = rows[rows["t"].isin(EHCVM_WAVES)].drop_duplicates(["t", "v"])
        joined = rows.merge(cf[["t", "v", "Region"]], on=["t", "v"], how="left")
        region = joined["strata"].str.rsplit(" ", n=1).str[0]
        assert (region == joined["Region"].astype("string")).all()

    @pytest.mark.parametrize("country", COUNTRIES)
    def test_milieu_part_is_rural(self, served, country):
        rows = served[country]
        rows = rows[rows["t"].isin(EHCVM_WAVES)]
        milieu = rows["strata"].str.rsplit(" ", n=1).str[1]
        assert (milieu.map({"Urbain": "Urban", "Rural": "Rural"}) == rows["Rural"].astype("string")).all()

    @pytest.mark.parametrize("country", COUNTRIES)
    def test_the_registered_inputs_rederive_the_served_value(self, served, country):
        c = ll.Country(country)
        key = stratum_derivation_key(country)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = c.derivation_inputs(key)
        rec = c.derivations()[key]
        spell = _lookup(_org_tables(country)["strata"])
        rebuilt = pd.Series(
            [rec.resolve()(r, m) for r, m in zip(inputs["s00q01"], inputs["s00q04"])],
            index=inputs.index).map(lambda s: spell.get(s, s))
        flat = served[country]
        got = flat[flat["t"].isin(EHCVM_WAVES)].set_index(["t", "i"])["strata"]
        assert len(rebuilt) == len(got)
        assert (rebuilt.reindex(got.index) == got).all()

    def test_feature_discloses_which_country_is_constructed(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Feature("sample")(["Senegal", "Togo"])
        carried = df.attrs["derivations"]
        assert stratum_derivation_key("Senegal") in carried["Senegal"]
        assert "Togo" not in carried or stratum_derivation_key("Togo") not in carried["Togo"]
        togo = df.xs("Togo", level="country")["strata"].dropna().unique()
        assert not any(s.endswith(MILIEUX) for s in togo)

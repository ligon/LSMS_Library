"""Guinea-Bissau 2018-19 label bundle (gnb stream, 2026-11-13).

#1107 harmonize_input and the Quilograma / Grama kg routes; #1119 harmonize_seed_crop;
#1539 Djagatu / Beringela; #1118 the `Unknown` missing-unit sentinel.  The
config-only tests need no microdata; ``TestServed`` carries ``requires_s3``.
"""
from __future__ import annotations

import re
import warnings

import pytest

import lsms_library as ll
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root

COUNTRY = "Guinea-Bissau"
ORG = countries_root() / COUNTRY / "_" / "categorical_mapping.org"
SCRIPT = countries_root() / COUNTRY / "2018-19" / "_" / "plot_inputs.py"


def _table(name, key="Original Label"):
    t = all_dfs_from_orgfile(ORG)[name]
    return dict(zip(t[key].astype(str).str.strip(), t[t.columns[1]].astype(str).str.strip()))


def _fold(s):
    return " ".join(str(s).split())


class TestConfig:
    def test_harmonize_input_maps_the_three_named_inorganics_only(self):
        got = _table("harmonize_input")
        assert got == {"Adubos inorgânicos - ureia": "Urea",
                       "Adubos inorgânicos - fosfatos": "Phosphate",
                       "Adubos inorgânicos - NPK/Fórmula única": "NPK"}
        # #1107: the DAP catch-all stays unmatched until #1696 is ruled; organics are
        # refused a share by the #867 ruling whatever they are called.
        assert not any("DAP" in k or k.startswith("Adubos orgânicos") for k in got)

    def test_each_mapped_input_keys_a_nitrogen_share(self):
        from lsms_library import transformations as T
        for label in _table("harmonize_input").values():
            assert any(k in T._NITROGEN_CONTENT for k in T._normalise_input_label(label)), label

    def test_harmonize_seed_crop_covers_the_ten_seed_codes(self):
        got = {_fold(k): v for k, v in _table("harmonize_seed_crop").items()}
        # s16bq01 value labels 11-20 of s16b_me_gnb2018.dta (code 11 has a double space).
        seeds = {"Sementes de milho preto", "Sementes de milho cavalo", "Sementes de milho bacil",
                 "Sementes de arroz", "Outros Sementes de cereais", "Sementes de algodão",
                 "Sementes de sésamo", "Sementes de feijão",
                 "Plantas, cordas/caules de tuberculos", "Outras sementes"}
        assert set(got) == seeds
        assert len(set(got.values())) == len(got)          # injective

    def test_a_seed_crop_is_spelled_as_crop_production_serves_the_crop(self):
        food = _table("harmonize_food")
        seed = {_fold(k): v for k, v in _table("harmonize_seed_crop").items()}
        crops = {"Sementes de milho preto": "Milho preto", "Sementes de milho cavalo": "Milho cavalo",
                 "Sementes de milho bacil": "Milho bacil", "Sementes de arroz": "Arroz paddy",
                 "Sementes de algodão": "Algodão", "Sementes de sésamo": "Sésamo",
                 "Sementes de feijão": "Feijão"}
        for s, crop in crops.items():
            assert seed[s] == food[crop], (s, crop)

    def test_djagatu_and_beringela_are_two_crops(self):
        food = _table("harmonize_food")
        assert food["Djagatu"] == "Jaxatu"
        assert food["Beringela"] == "Aubergine"
        assert "Aubergine," not in set(food.values())

    def test_quilograma_and_grama_have_a_mass(self):
        t = all_dfs_from_orgfile(ORG)["u_kg"]
        got = dict(zip(t["Unit"].astype(str).str.strip(), t["Kg"].astype(float)))
        assert got == {"Quilograma": 1.0, "Grama": 0.001}
        factors = ll.Country(COUNTRY)._unit_kg_factors()
        assert factors["quilograma"] == 1.0 and factors["grama"] == 0.001

    def test_a_missing_unit_is_the_unknown_sentinel(self):
        text = SCRIPT.read_text(encoding="utf-8")
        assert "fillna('Unknown')" in text
        assert not re.search(r"fillna\(\s*['\"]Manquant", text)
        assert "SEED_CODES = range(11, 21)" in text
        assert "df.index.is_unique" in text


@pytest.mark.requires_s3
class TestServed:
    @pytest.fixture(scope="class")
    def pi(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ll.Country(COUNTRY).plot_inputs(waves=["2018-19"])

    def test_nitrogen_matches_the_three_inorganics(self, pi):
        from lsms_library import transformations as T
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            n = T.nitrogen_kg(pi)
        tally = n.attrs["nitrogen_input_match"]
        assert set(tally["matched_labels"]) == {"Urea", "Phosphate", "NPK"}
        assert tally["matched_rows"] == 293
        assert "DAP e outros adubos inorgânicos" in tally["unmatched_labels"]
        assert len(n) > 0 and (n["Nitrogen_kg"] > 0).any()

    def test_seed_rows_carry_their_crop_and_inputs_their_sentinel(self, pi):
        crop = pi.index.get_level_values("crop")
        inp = pi.index.get_level_values("input").astype(str)
        seed = inp.str.contains("ementes|Plantas")
        assert (crop[~seed] == "(not crop-specific)").all()
        assert not (crop[seed] == "(not crop-specific)").any()
        assert set(crop[seed]) == set(_table("harmonize_seed_crop").values())
        assert pi.index.is_unique

    def test_no_unit_is_minted(self, pi):
        u = set(pi.index.get_level_values("u"))
        assert "Manquant" not in u
        assert u <= {"Quilograma", "Grama", "Tonelada", "Litro", "Carroça", "Outras", "Unknown"}

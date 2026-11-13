"""GH #1453: Mali 2018-19 community units take the household's sized spelling.

``food_acquired.u`` serves ``Tas (Petit)`` since GH #1224; the s05 community file
served ``Tas - Petit``, so a sized price joined nothing.  The ruling (2026-11-04,
option (b)): respell the community side, and do NOT fold it.  Two source units under
one label would make the served price ambiguous, so a heap of meat stays
``Avec os en tas (Petit)`` where the household's ``u`` table serves ``Tas (Petit)``.

The first class needs no microdata.  ``TestServed`` does (``requires_s3``); its oracle
reads the ``.dta`` directly.
"""
from __future__ import annotations

import importlib.util
import sys

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.ehcvm import served_unit_labels
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

WAVE = countries_root() / "Mali" / "2018-19"
SOURCE = str(WAVE / "Data" / "s05_co_mli2018.dta")
SIZES = {1: "Petit", 2: "Moyen", 3: "Grand"}


@pytest.fixture(scope="module")
def mapping():
    path = WAVE / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location("_mali_1453_mapping", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TestRule:
    @pytest.mark.parametrize("code,label,served", [
        (1431, "Tas - Petit", "Tas (Petit)"),
        (1472, "Unité - Moyen", "Unité (Moyen)"),
        (1471, "Unité- Petit", "Unité (Petit)"),          # the file's own spacing
        (1483, "Verre -Grand", "Verre (Grand)"),
        (1263, "Morceau- Grand", "Morceau (Grand)"),
        (1413, "Sans os en tas - grand", "Sans os en tas (Grand)"),
        # One source unit each side, spelled differently: respelled.
        (1022, "Alvéoole - Moyen", "Alvéole (Moyen)"),
        (1111, "Motte(boule)- Petit", "Boule (Petit)"),
        (4603, "Moude (Moudé) - Grand", "Moude (Grand)"),
        (4661, "Seau (5 Kg ) - Petit", "Seau (5 Kg) (Petit)"),
        # A household fold, not a spelling: kept apart.
        (1041, "Avec os en tas-petit", "Avec os en tas (Petit)"),
        (1081, "Boite de tomate - Petit", "Boite de tomate (Petit)"),
        (4701, "Boite de lait concentrée - Petit", "Boite de lait concentrée (Petit)"),
        # No size digit: verbatim.
        (1000, "Kg", "Kg"),
        (1030, "Avec os au kg", "Avec os au kg"),
        (1350, "Sac (100 Kg)", "Sac (100 Kg)"),
    ])
    def test_the_spelling(self, mapping, code, label, served):
        assert mapping._s05_unit_label(code, label) == served

    @pytest.mark.parametrize("code,label", [
        (1431, "Tas - Grand"),     # the word disagrees with the digit
        (1431, "Tas"),             # a size digit and no size word
        (1434, "Tas - Quart"),     # a digit nothing here defines
    ])
    def test_a_label_that_contradicts_its_code_is_refused(self, mapping, code, label):
        with pytest.raises(ValueError):
            mapping._s05_unit_label(code, label)

    def test_two_codes_on_one_label_are_refused(self, mapping):
        codes = pd.DataFrame({"grappe": [1, 1], "vague": [1, 1], "Existe": [1, 1],
                              "s05q01": [5, 5], "s05q02": [1022, 1022 + 10],
                              "s05q03": [1.0, 2.0], "s05q04": [1.0, 2.0]})
        labels = codes.copy()
        labels["s05q01"] = ["Mil", "Mil"]
        labels["s05q02"] = ["Alvéoole - Moyen", "Alvéole - Moyen"]
        with pytest.raises(ValueError, match="one to one"):
            mapping.community_prices_s05(codes, labels)

    def test_the_read_time_table_does_not_fold_a_respelled_label(self, mapping):
        """The auto-applied ``u`` table is keyed on bare labels."""
        labels = ["Tas (Petit)", "Avec os en tas (Petit)", "Boite de tomate (Grand)",
                  "Boule (Moyen)", "Seau (5 Kg) (Petit)"]
        assert list(served_unit_labels("Mali", labels)) == labels


@pytest.mark.requires_s3
class TestServed:
    @pytest.fixture(scope="class")
    def served(self):
        df = ll.Country("Mali").community_prices().reset_index()
        return df[df.t == "2018-19"]

    @pytest.fixture(scope="class")
    def source(self):
        codes = get_dataframe(SOURCE, convert_categoricals=False)
        labels = get_dataframe(SOURCE)
        return pd.DataFrame({
            "code": pd.to_numeric(codes["s05q02"]).astype(int),
            "label": labels["s05q02"].astype("string").str.strip(),
            "v": codes["grappe"].astype(int).astype(str),
            "variety": codes["s05q01"].astype(int).astype(str),
        })

    def test_every_source_row_keeps_both_quotes(self, served, source):
        assert len(served) == 2 * len(source)

    def test_one_label_per_source_unit_code(self, served, source):
        """The point of not folding: a served label names one source unit."""
        assert served.u.astype(str).nunique() == source.code.nunique()

    @pytest.mark.parametrize("digit,word", SIZES.items())
    def test_the_size_in_a_served_label_is_the_digit_of_its_code(self, served, source, digit, word):
        expected = 2 * int((source.code % 10 == digit).sum())
        assert int(served.u.astype(str).str.endswith(f" ({word})").sum()) == expected

    @pytest.mark.parametrize("label", ["Tas (Petit)", "Sachet (Moyen)", "Unité (Grand)",
                                       "Boule (Petit)", "Paquet (Grand)", "Verre (Moyen)"])
    def test_a_sized_label_is_one_a_household_serves(self, served, label):
        household = ll.Country("Mali").food_acquired().reset_index()
        household = household[household.t == "2018-19"]
        assert (served.u.astype(str) == label).any()
        assert (household.u.astype(str) == label).any()

    @pytest.mark.parametrize("label", ["Avec os en tas (Petit)", "Boite de tomate (Grand)",
                                       "Avec os au kg"])
    def test_a_household_fold_stays_apart(self, served, label):
        household = ll.Country("Mali").food_acquired().reset_index()
        household = household[household.t == "2018-19"]
        assert (served.u.astype(str) == label).any()
        assert not (household.u.astype(str) == label).any()

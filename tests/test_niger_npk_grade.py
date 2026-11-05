"""Niger's ECVMA NPK is NPK 15-15-15; EHCVM's grade is unstated -- GH #1445.

Ruling @ligon 2026-11-04, option A, the Malawi #1106 pattern: a relabel in the
country table plus a graded key in ``transformations._NITROGEN_CONTENT``, so the
product identity lives in the label and the map needs no country axis.

ECVMA's code 5 says ``npk 15/15`` (2011-12) and ``NPK 15/15`` (2014-15).  EHCVM's
says ``NPK/Formule unique``, and no held document gives its grade, so it keeps
``NPK`` at the map's nominal 0.17.

``test_the_relabel_and_the_key_go_together`` is the guard: the map matches
exactly, and the name carries no stem the coverage warning looks for, so a relabel
without the key would remove these rows' nitrogen silently.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library import transformations as T
from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

COUNTRY = "Niger"
GRADED = "NPK 15-15-15"
BARE = "NPK"
ECVMA = {"2011-12": ("NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta", "as02cq02", "as02cq03"),
         "2014-15": ("NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS02CP1.dta", "AS02CQ02", "AS02CQ03")}
EHCVM = {"2018-19": "s16b_me_ner2018.dta", "2021-22": "s16b_me_ner2021.dta"}


def _table():
    df = df_from_orgfile(str(countries_root() / COUNTRY / "_" / "categorical_mapping.org"),
                         name="harmonize_input")
    return dict(zip(df["Original Label"], df["Preferred Label"]))


class TestConfig:
    @pytest.mark.parametrize("label", ["engrais inorganiques - npk 15/15",
                                       "Engrais inorganiques - NPK 15/15"])
    def test_the_ecvma_labels_state_the_grade(self, label):
        assert _table()[label] == GRADED

    def test_the_ehcvm_label_states_none(self):
        assert _table()["Engrais inorganiques - NPK/Formule unique"] == BARE

    def test_the_relabel_and_the_key_go_together(self):
        shares, tally = T._nitrogen_shares([GRADED], T._NITROGEN_CONTENT)
        assert tally["unmatched_rows"] == 0, (
            f"{GRADED!r} is on Niger's table and keys no nitrogen share: add "
            "'npk 15-15-15': 0.15 to transformations._NITROGEN_CONTENT (GH #1445)")
        assert shares.iloc[0] == 0.15

    def test_the_grade_does_not_reach_the_bare_label(self):
        shares, _ = T._nitrogen_shares([BARE, GRADED], T._NITROGEN_CONTENT)
        assert shares.tolist() == [0.17, 0.15]


def _raw(wave, name):
    from lsms_library.local_tools import get_dataframe
    path = str(countries_root() / COUNTRY / wave / "Data" / name)
    return (get_dataframe(path, convert_categoricals=False),
            get_dataframe(path, convert_categoricals=True))


@pytest.fixture(scope="module")
def served():
    # No skip on a failed build: TestServed is `requires_s3`, so reaching here means
    # the data is available, and a build that raises must fail this file.
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country(COUNTRY).plot_inputs()
    return df


def _rows(df, wave, label):
    x = df.xs(wave, level="t")
    return x[x.index.get_level_values("input") == label]


@pytest.mark.requires_s3
class TestServed:
    @pytest.mark.parametrize("wave", sorted(ECVMA))
    def test_every_applied_code_5_line_is_graded(self, served, wave):
        name, slot, used = ECVMA[wave]
        codes, labels = _raw(wave, name)
        applied = codes[slot].eq(5) & codes[used].eq(1)
        assert set(labels.loc[applied, slot].astype(str).str.lower()) == \
            {"engrais inorganiques - npk 15/15"}
        assert len(_rows(served, wave, GRADED)) == int(applied.sum()) > 0
        assert _rows(served, wave, BARE).empty

    @pytest.mark.parametrize("wave", sorted(EHCVM))
    def test_every_ehcvm_npk_line_stays_bare(self, served, wave):
        codes, labels = _raw(wave, EHCVM[wave])
        code5 = codes["s16bq01"].eq(5)
        assert set(labels.loc[code5, "s16bq01"].astype(str)) == \
            {"Engrais inorganiques - NPK/Formule unique"}
        assert _rows(served, wave, GRADED).empty
        assert len(_rows(served, wave, BARE)) > 0

    @pytest.mark.parametrize("wave", sorted(ECVMA))
    def test_the_nitrogen_is_fifteen_percent_of_the_product(self, served, wave):
        rows = _rows(served, wave, GRADED)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            product = T.nitrogen_kg(rows, nitrogen_content={"npk 15-15-15": 1.0})
            nitrogen = T.nitrogen_kg(rows)
        assert float(product["Nitrogen_kg"].sum()) > 0
        assert float(nitrogen["Nitrogen_kg"].sum()) == pytest.approx(
            0.15 * float(product["Nitrogen_kg"].sum()))

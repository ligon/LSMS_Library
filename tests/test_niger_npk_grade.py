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
import hashlib

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

# Independent test source roles, not imported from the producer (ledger1687 §5).
# The same household/slot in 2C and 3C is two observations, in two windows.
INPUT_SOURCES = {
    "2011-12": ((ECVMA["2011-12"][0], "as02cq"),
                ("NER_2011_ECVMA_v01_M_Stata8/ecvmaas3c_p1.dta", "as03cq")),
    "2014-15": ((ECVMA["2014-15"][0], "AS02CQ"),),
    "2018-19": ((EHCVM["2018-19"], "s16bq"),),
    "2021-22": ((EHCVM["2021-22"], "s16bq"),),
}


def _map_table(name):
    table = df_from_orgfile(str(countries_root() / COUNTRY / "_" /
                               "categorical_mapping.org"), name=name)
    return dict(zip(table["Original Label"], table["Preferred Label"]))


def _source_prefix(wave, name, prefix):
    assert (name, prefix) in INPUT_SOURCES[wave], "wrong source/field roles"
    path = countries_root() / COUNTRY / wave / "Data" / name
    digest = hashlib.sha256(path.with_name(path.name + ".dvc").read_bytes()).hexdigest()
    slot = prefix + "02"
    return f"slot:{COUNTRY}/{wave}/Data/{name}@dvc:{path.name}:{digest}:{slot}="


def _project_input_source(wave, name, prefix, codes, labels, *, input_map, unit_map):
    """Independent source-row denominator, before comparing served results.

    Explicit application answer, own unit, own purchase flag and own amounts.
    Keep zero and missing quantities; only the existing all-payload-NA finalizer
    removes a row. No producer/helper/API is an expected-value oracle.
    """
    assert (name, prefix) in INPUT_SOURCES[wave], "wrong source/field roles"
    assert codes.index.equals(labels.index), "numeric/category rows misaligned"
    ecvma = wave in ECVMA
    upper = lambda x: x.upper() if prefix == "AS02CQ" else x
    slot, gate = prefix + ("02" if ecvma else "01"), prefix + ("03" if ecvma else "02")
    qty = prefix + upper("05a" if ecvma else "03a")
    unit = prefix + upper("05b" if ecvma else "03b")
    bought = prefix + ("07" if ecvma else "05")
    qty_bought = prefix + upper("08a" if ecvma else "07a")
    selected = codes[gate].eq(1)
    n, c = codes.loc[selected], labels.loc[selected]
    raw_input = c[slot].astype("string")
    raw_unit = c[unit].astype("string")
    mapped = lambda values, mapping: values.map(
        lambda x: mapping.get(x, x) if pd.notna(x) else pd.NA).astype("string")
    quantity = pd.to_numeric(n[qty], errors="coerce").astype("Float64")
    if wave == "2021-22":
        quantity = quantity.mask(quantity.eq(9999))  # own existing GH865 rule
    purchased = (n[bought].map({1: True, 2: False}) if ecvma else
                 c[bought].astype("string").map({"Oui": True, "Non": False}))
    rows = pd.DataFrame({
        "input": mapped(raw_input, input_map),
        "u": mapped(raw_unit, unit_map).fillna("Unknown"),
        "Quantity": quantity,
        "Purchased": purchased.astype("boolean"),
        "Quantity_purchased": pd.to_numeric(n[qty_bought], errors="coerce").astype("Float64"),
    })
    if ecvma:
        slots = pd.to_numeric(n[slot], errors="raise")
        assert slots.notna().all() and ((slots > 0) & (slots % 1 == 0)).all()
        rows["input_obs"] = _source_prefix(wave, name, prefix) + slots.astype("Int64").astype("string")
    else:
        rows["input_obs"] = "unresolved_observation"
    payload = ["Quantity", "Purchased", "Quantity_purchased"]
    rows = rows.loc[rows["input"].notna() & rows[payload].notna().any(axis=1)].copy()
    rows["t"], rows["source"] = wave, name
    return rows


def _expected_plot_inputs(read=None):
    read = _raw if read is None else read
    input_map, unit_map = _map_table("harmonize_input"), _map_table("u")
    parts = []
    for wave, specs in INPUT_SOURCES.items():
        for name, prefix in specs:
            codes, labels = read(wave, name)
            parts.append(_project_input_source(wave, name, prefix, codes, labels,
                                              input_map=input_map, unit_map=unit_map))
    return pd.concat(parts, ignore_index=True)


def _assert_plot_input_counts(actual, expected, *, by_wave=False):
    """Keep total, Unknown and per-wave obligations with an independent source denominator."""
    if by_wave:
        def tally(frame):
            return (frame.assign(_s=frame["u"].eq("Unknown"))
                         .groupby("t")["_s"].sum().astype(int).to_dict())
        assert tally(actual) == tally(expected)
    else:
        assert len(actual) == len(expected)
        assert int(actual["u"].eq("Unknown").sum()) == int(expected["u"].eq("Unknown").sum())


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
        total = 0
        for source, prefix in INPUT_SOURCES[wave]:
            # Reuse the already-read original source; read dry3C separately.
            numeric, categorical = ((codes, labels) if source == name else _raw(wave, source))
            code5 = numeric[prefix + "02"].eq(5) & numeric[prefix + "03"].eq(1)
            own_labels = categorical.loc[code5, prefix + "02"].astype("string")
            assert set(own_labels.map(_table())) == {GRADED}
            expected = _project_input_source(wave, source, prefix, numeric, categorical,
                                            input_map=_table(), unit_map=_map_table("u"))
            expected = expected.loc[expected["input"] == GRADED]
            assert len(expected) == int(code5.sum()) > 0
            got = _rows(served, wave, GRADED)
            tokens = got.index.get_level_values("input_obs")
            assert tokens.notna().all()
            assert sum(bool(x) for x in tokens.str.startswith(
                _source_prefix(wave, source, prefix))) == len(expected)
            total += len(expected)
        assert len(_rows(served, wave, GRADED)) == total > 0
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

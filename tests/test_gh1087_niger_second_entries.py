"""Niger CS07: a record entered twice serves its FIRST entry -- GH #1087.

Ruling @ligon 2026-11-04, option (c).  2011-12 ``ecvmacoms07_p2`` re-ships 150 of
its 232 grappes and 2014-15 grappe 212 has two ``numquest``: duplicate entries of
one questionnaire, not second readings.  A second entry's slot that repeats a
first-entry row is a copy (GH #840).  One that repeats nothing is not served, and
every served row of its record says so in ``second_entry_disagrees``.  2018-19 s05
is a different mechanism and keeps ``obs`` 3 / 4.

Two tiers: config and synthetic (no microdata), and ``TestServed``, whose oracle
reads the raw grids through ``get_dataframe`` and imports nothing from ``Niger/_/``.
"""
from __future__ import annotations

import os
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library.derivations import records_for, resolve_callable
from lsms_library.paths import countries_root

COUNTRY = "Niger"
KEY = "Niger::community_prices::first-entry-of-duplicate"
FLAG = "second_entry_disagrees"
LEVELS = ["t", "v", "j", "u", "variety", "visit", "obs"]

#: wave -> (file, passage), (price, quantity, unit) of each slot.  Written out:
#: this is the oracle's reading of the instrument, not the build's.
GRIDS = {
    "2011-12": ([("NER_2011_ECVMA_v01_M_Stata8/ecvmacoms07_p1.dta", 1),
                 ("NER_2011_ECVMA_v01_M_Stata8/ecvmacoms07_p2.dta", 2)],
                [("cs07q03", "cs07q04", "cs07q05"),
                 ("cs07q06", "cs07q07", "cs07q08"),
                 ("cs07q09", "cs07q10", "cs07q11")]),
    "2014-15": ([("NER_2014_ECVMA-II_v02_M_STATA8/comprixcs07.dta", 1)],
                [("cs07q04", "cs07q05", "cs07q03"),
                 ("cs07q06", "cs07q07", "cs07q03"),
                 ("cs07q08", "cs07q09", "cs07q03")]),
}
#: "No answer" codes on a CS07 price and on a CS07 quantity (GH #1088).
PRICE_CODES = {9999.0, 99999.0}
QTY_CODES = PRICE_CODES | {9999.9, 9999.900390625}


@pytest.fixture(scope="module")
def niger():
    """``Niger/_/niger.py``, imported as the wave scripts do; it reads no data at
    import."""
    import sys
    sys.path.insert(0, str(countries_root() / COUNTRY / "_"))
    try:
        import niger as module
    finally:
        sys.path.pop(0)
    return module


def _slots(**over):
    """Long slots as ``_community_price_slots`` returns them."""
    base = dict(v="1", j="Mil", u="Kg", variety="7011", visit=1, obs=1, slot=1,
                rank=0, source_row=0, Description="mil", Price=100.0, Quantity=2.0)
    return {**base, **over}


class TestRule:
    def test_second_entry_is_not_served_and_flags_its_whole_record(self, niger):
        out = niger.derive_cs07_first_entry(pd.DataFrame([
            _slots(),
            _slots(obs=2, slot=2, Price=150.0),
            _slots(obs=4, slot=1, rank=1, source_row=1, u="Gramme"),
        ])).set_index("obs")
        assert sorted(out.index) == [1, 2]
        # Slot 2 has no second-entry counterpart: the flag is the record's.
        assert out[FLAG].tolist() == [True, True]
        assert not {"rank", "slot", "source_row"} & set(out.columns)

    @pytest.mark.parametrize("other", [{"v": "2"}, {"variety": "7012"}, {"visit": 2}])
    def test_the_record_is_grappe_variety_passage(self, niger, other):
        out = niger.derive_cs07_first_entry(pd.DataFrame([
            _slots(),
            _slots(**other),
            _slots(obs=4, slot=1, rank=1, source_row=1, u="Gramme"),
        ]))
        assert out[FLAG].tolist() == [True, False]

    def test_obs_is_the_slot_and_nothing_flags_without_a_second(self, niger):
        out = niger.derive_cs07_first_entry(pd.DataFrame([
            _slots(), _slots(obs=3, slot=3, Price=125.0)]))
        assert out["obs"].tolist() == [1, 3]
        assert out[FLAG].dtype == "boolean" and not out[FLAG].any()

    def test_a_record_with_no_first_entry_row_serves_nothing(self, niger):
        # The 99999 case: the first entry's slot never reaches the frame.
        out = niger.derive_cs07_first_entry(pd.DataFrame([
            _slots(obs=4, slot=1, rank=1, source_row=1)]))
        assert out.empty

    def test_the_tail_passes_the_flag_and_fills_na_where_unset(self, niger):
        frame = niger.derive_cs07_first_entry(pd.DataFrame([_slots()]))
        out = niger._finish_community_prices(frame, "2011-12")
        assert out[FLAG].tolist() == [False]
        s05 = pd.DataFrame([_slots()]).drop(columns=["slot", "rank", "source_row"])
        out = niger._finish_community_prices(s05, "2018-19")
        assert out[FLAG].dtype == "boolean" and out[FLAG].isna().all()

    def test_s05_keeps_its_second_records(self, niger):
        # A different mechanism (GH #823 D5): two readings per key, obs 3 / 4.
        codes = pd.DataFrame({
            "grappe": [1, 1], "vague": [1, 1], "Existe": [1, 1], "s05q01": [1, 1],
            "s05q02": [1000, 1000], "s05q03": [100.0, 110.0], "s05q04": [100.0, 100.0],
        })
        labels = codes.assign(s05q01="Mil")
        out = niger._s05_community_prices(codes, labels, {1: "Mil"}, {1000: "Kg"})
        assert sorted(out["obs"]) == [1, 2, 3]
        assert FLAG not in out.columns


class TestDeclared:
    def test_the_column_is_optional_on_the_country_scheme(self):
        with open(countries_root() / COUNTRY / "_" / "data_scheme.yml",
                  encoding="utf-8") as f:
            text = f.read()
        # The !make tag on panel_ids is not safe_load-able; read the one block.
        entry = yaml.safe_load(text.replace("!make", "make"))["Data Scheme"]["community_prices"]
        assert entry[FLAG] == {"type": "bool", "optional": True}
        assert FLAG not in entry["index"]

    def test_the_rule_is_a_registered_deletion(self):
        record = records_for(COUNTRY, "community_prices")[KEY]
        assert record["rows"] == 0
        assert sorted(record["waves"]) == sorted(GRIDS)
        assert callable(resolve_callable(record["function"]))
        assert callable(resolve_callable(record["inputs"]))

    def test_the_inputs_callable_reads_what_the_scripts_read(self, niger):
        assert {w: [tuple(t) for t in g[1]] for w, g in GRIDS.items()} == \
            {w: [tuple(t) for t in niger.CS07_TRIPLES[w]] for w in GRIDS}
        assert {w: list(g[0]) for w, g in GRIDS.items()} == \
            {w: list(niger._CS07_FILES[w]) for w in GRIDS}


def _raw(wave, name):
    """Codes and labels of one held CS07 file."""
    from lsms_library.local_tools import get_dataframe
    path = str(countries_root() / COUNTRY / wave / "Data" / name)
    return (get_dataframe(path, convert_categoricals=False).reset_index(drop=True),
            get_dataframe(path, convert_categoricals=True).reset_index(drop=True))


def _oracle(wave):
    """(grappe, variety, passage, slot) -> (price, quantity, unit code, label) by
    entry, read off the raw grids: entries are ranked by (numquest, file order)
    after byte-identical rows are dropped, and a slot counts only if it has a price
    and a unit that names one (the build's own two no-price rules)."""
    rows = []
    for name, passage in GRIDS[wave][0]:
        codes, labels = _raw(wave, name)
        keep = ~codes.duplicated()
        codes, labels = codes[keep], labels[keep]
        order = (codes.sort_values("numquest", kind="stable").index
                 if "numquest" in codes else codes.index)
        rank = (codes.loc[order].groupby(["grappe", "cs07q02"]).cumcount()
                .reindex(codes.index))
        for slot, (pc, qc, uc) in enumerate(GRIDS[wave][1], start=1):
            price = pd.to_numeric(codes[pc], errors="coerce")
            qty = pd.to_numeric(codes[qc], errors="coerce")
            qty = qty.where((qty > 0) & ~qty.isin(QTY_CODES))
            # 'manquant' / 'produit absent' mark no price, and a blank unit is
            # dropped by the tail.  A label test, so the unit table is not read.
            unit = labels[uc].astype("string").str.strip().str.lower()
            has_unit = unit.notna() & ~unit.isin(["manquant", "produit absent"])
            ok = price.notna() & (price > 0) & ~price.isin(PRICE_CODES) & has_unit
            rows.append(pd.DataFrame({
                "v": codes["grappe"].map(lambda g: str(int(g))),
                "variety": pd.to_numeric(codes["cs07q02"]).astype("Int64").astype(str),
                "visit": passage, "slot": slot, "rank": rank,
                "Price": price, "Quantity": qty, "unit": unit,
                "label": labels["cs07q02"].astype("string").str.strip(),
            })[ok])
    return pd.concat(rows, ignore_index=True)


def _disagreeing(oracle):
    """The second-entry slots that repeat no first-entry row in slot, price,
    quantity, unit and label."""
    key = ["v", "variety", "visit", "slot"]
    content = ["Price", "Quantity", "unit", "label"]
    first = oracle[oracle["rank"] == 0]
    second = oracle[oracle["rank"] > 0]
    m = second.merge(first, on=key, how="left", suffixes=("", "_1"), indicator=True)
    same = pd.Series(True, index=m.index)
    for c in content:
        a, b = m[c].astype("object"), m[c + "_1"].astype("object")
        same &= (a == b).fillna(False).astype(bool) | (a.isna() & b.isna())
    return m[~(same & m["_merge"].eq("both"))]


@pytest.fixture(scope="module")
def served():
    import lsms_library as ll
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            country = ll.Country(COUNTRY)
            df = country.community_prices()
    except Exception as e:  # pragma: no cover - data not available
        pytest.skip(f"{COUNTRY} community_prices not buildable here: {e}")
    return country, df


@pytest.mark.requires_s3
class TestServed:
    @pytest.mark.parametrize("wave", sorted(GRIDS))
    def test_every_served_row_is_a_first_entry_reading(self, served, wave):
        _, df = served
        got = df.xs(wave, level="t").reset_index()
        oracle = _oracle(wave)
        first = oracle[oracle["rank"] == 0].rename(columns={"slot": "obs"})
        key = ["v", "variety", "visit", "obs"]
        assert set(got["obs"]) <= {1, 2, 3}
        merged = got.merge(first, on=key, how="left", suffixes=("", "_o"),
                           validate="one_to_one")
        assert merged["Price_o"].notna().all()
        assert (merged["Price"].astype(float) == merged["Price_o"].astype(float)).all()
        # And nothing the first entry prices is missing.
        assert len(got) == len(first)

    @pytest.mark.parametrize("wave", sorted(GRIDS))
    def test_the_flag_is_on_exactly_the_records_that_disagree(self, served, wave):
        _, df = served
        got = df.xs(wave, level="t").reset_index()
        rec = ["v", "variety", "visit"]
        flagged = {tuple(r) for r in got.loc[got[FLAG].astype(bool), rec].to_numpy()}
        expected = {tuple(r) for r in _disagreeing(_oracle(wave))[rec].to_numpy()}
        with_rows = {tuple(r) for r in got[rec].to_numpy()}
        assert flagged == expected & with_rows
        assert not got[FLAG].isna().any()

    def test_the_flag_is_na_where_second_records_are_served(self, served):
        _, df = served
        s05 = df.xs("2018-19", level="t")
        assert s05[FLAG].isna().all()
        assert set(s05.index.get_level_values("obs")) == {1, 2, 3, 4}

    @pytest.mark.parametrize("wave", sorted(GRIDS))
    def test_the_registered_inputs_are_the_slots_that_left(self, served, wave):
        country, df = served
        got = country.derivation_inputs(KEY, wave=wave).reset_index()
        expected = _disagreeing(_oracle(wave))
        key = ["v", "variety", "visit", "slot"]
        assert sorted(map(tuple, got[key].astype(str).to_numpy())) == \
            sorted(map(tuple, expected[key].astype(str).to_numpy()))
        # The source row is there under its own names, and it holds the price.
        for row, (_, want) in zip(got.sort_values(key).itertuples(),
                                  expected.sort_values(key).iterrows()):
            price_col = GRIDS[wave][1][row.slot - 1][0]
            assert float(getattr(row, price_col)) == float(want["Price"])

    def test_the_deletion_is_disclosed_on_the_frame(self, served):
        _, df = served
        assert df.attrs["derivations"][COUNTRY][KEY]["rows"] == 0

"""Niger ``crop_production`` serves the canonical ``condition`` level -- GH #1436.

A harvest Quantity is a quantity of something in some physical state, and the
EHCVM instrument says which: "Etat du produit", 2018-19 ``s16cq12c`` and 2021-22
``s16cq16d``.  The ECVMA harvest modules (2011-12, 2014-15) ask no such question.

The rule pinned here is the mapping, not today's counts:

* the served level is what the SOURCE code says, read from the source file with
  ``convert_categoricals=False`` and mapped by a table written out in this test,
  so a wrong table cannot agree with itself;
* the two code lists COLLIDE (3 is Decortique in 2018-19 and ``na`` in 2021-22),
  so each wave has to use its own;
* a blank code is the ``unknown_condition`` sentinel, never a NaN key;
* every served value is in the vocabulary ``lsms_library/data_info.yml`` declares.

Ruling @ligon 2026-11-04 (option D): the fold stays on the level, and the column
``condition_native`` keeps the 2018-19 answer as recorded.  Its oracle imports
nothing from ``Niger/_/``.

Two tiers: config/synthetic (no microdata) and ``TestServed`` (builds the real
table; skipped when Niger is unavailable).
"""
from __future__ import annotations

import ast
import os
import sys
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

COUNTRY = "Niger"
TABLE = "crop_production"
SENTINEL = "unknown_condition"

#: The source codes, written out here on purpose: this is the oracle, and it
#: must not read the table it is checking.
EHCVM1 = {1: "unshelled", 2: "shelled", 3: "shelled", 4: "unshelled",
          5: "shell_not_applicable"}
EHCVM2 = {1: "unshelled", 2: "shelled", 3: "shell_not_applicable"}

#: wave -> (source, crop, unit, quantity, state, code map, table)
SOURCES = {
    "2018-19": ("../Data/s16c_me_ner2018.dta", "s16cq04", "s16cq12b",
                "s16cq12a", "s16cq12c", EHCVM1,
                "harmonize_crop_condition_ehcvm1"),
    "2021-22": ("../Data/s16c_me_ner2021.dta", "s16cq04", "s16cq16b",
                "s16cq16a", "s16cq16d", EHCVM2,
                "harmonize_crop_condition_ehcvm2"),
}
NO_STATE_QUESTION = ("2011-12", "2014-15")

#: The 2018-19 ``s16cq12c`` value labels, written out here: the oracle for
#: ``condition_native`` must not read the helper or the table it checks.
RECORDED_2018 = {1: "Epis", 2: "Grain", 3: "Décortiqué", 4: "Non-décortiqué",
                 5: "Non concerné"}
#: The ruled fold, by recorded label.
FOLD_2018 = {"Epis": "unshelled", "Non-décortiqué": "unshelled", "Grain": "shelled",
             "Décortiqué": "shelled", "Non concerné": "shell_not_applicable"}


def _root() -> Path:
    return countries_root() / COUNTRY


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the ``!make`` tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


def _state_read(source: str) -> tuple[str, str, bool]:
    """What a wave script's one ``_crop_conditions(<frame>['<column>'], '<table>')``
    call actually reads: (column, table, is <frame> raw codes?).  Parsed, so a
    docstring or comment naming the right column cannot satisfy it."""
    tree = ast.parse(source)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "id", None) == "_crop_conditions"]
    assert len(calls) == 1, f"{len(calls)} _crop_conditions calls"
    column, table = calls[0].args
    assert isinstance(column, ast.Subscript) and isinstance(column.value, ast.Name)
    raw_codes = any(
        isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
        and any(isinstance(t, ast.Name) and t.id == column.value.id for t in n.targets)
        and any(k.arg == "convert_categoricals" and getattr(k.value, "value", None) is False
                for k in n.value.keywords)
        for n in ast.walk(tree))
    return column.slice.value, table.value, raw_codes


def _label_read(source: str) -> tuple[str, bool] | None:
    """What a wave script's ``_crop_condition_labels(<frame>['<column>'])`` call
    reads: (column, is <frame> the labelled read?).  None when there is no call."""
    tree = ast.parse(source)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "id", None) == "_crop_condition_labels"
             # condition_sold_native reads the SOLD state through the same helper (GH #1462).
             and getattr(getattr(n.args[0], "slice", None), "value", None) != "s16cq16c"]
    if not calls:
        return None
    assert len(calls) == 1, f"{len(calls)} _crop_condition_labels calls"
    column = calls[0].args[0]
    labelled = any(
        isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
        and any(isinstance(t, ast.Name) and t.id == column.value.id for t in n.targets)
        and any(k.arg == "convert_categoricals" and getattr(k.value, "value", None) is True
                for k in n.value.keywords)
        for n in ast.walk(tree))
    return column.slice.value, labelled


def _canonical_scheme() -> dict:
    path = Path(__file__).resolve().parents[1] / "lsms_library" / "data_info.yml"
    with open(path, encoding="utf-8") as f:
        return yaml.load(f, Loader=_SchemeLoader)["Columns"][TABLE]


def _canonical_vocabulary() -> set[str]:
    return set(_canonical_scheme()["condition"]["spellings"])


@pytest.fixture(scope="module")
def niger_module():
    """``Niger/_/niger.py``, imported the way the wave scripts import it."""
    sys.path.insert(0, str(_root() / "_"))
    try:
        import niger
    finally:
        sys.path.pop(0)
    return niger


@pytest.fixture
def in_country_dir(monkeypatch):
    """The cwd ``get_categorical_mapping``'s default ``dirs`` resolve against."""
    monkeypatch.chdir(_root() / "_")


# ---------------------------------------------------------------------------
# config / synthetic
# ---------------------------------------------------------------------------

class TestDeclared:

    def test_data_scheme_declares_the_level(self):
        with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
            scheme = yaml.load(f, Loader=_SchemeLoader)
        index = scheme["Data Scheme"][TABLE]["index"]
        levels = [lvl.strip() for lvl in index.strip("()").split(",")]
        # Malawi's order: the state qualifies the unit, the observation axis is last.
        assert levels[-3:] == ["u", "condition", "production_obs"], index

    @pytest.mark.parametrize("wave", list(SOURCES))
    def test_table_maps_every_code_to_a_canonical_token(self, niger_module,
                                                        in_country_dir, wave):
        *_, codes, table = SOURCES[wave]
        got = niger_module._harmonized_codes(table)
        assert got == codes
        assert set(got.values()) <= _canonical_vocabulary()

    def test_sentinel_is_in_the_canonical_vocabulary(self, niger_module):
        assert niger_module.CONDITION_NA == SENTINEL
        assert SENTINEL in _canonical_vocabulary()

    def test_the_two_code_lists_collide(self, niger_module, in_country_dir):
        """Why there are two tables: the same code means different states."""
        code = pd.Series([3, 3])
        first = niger_module._crop_conditions(code, SOURCES["2018-19"][-1])
        second = niger_module._crop_conditions(code, SOURCES["2021-22"][-1])
        assert first.tolist() == ["shelled", "shelled"]
        assert second.tolist() == ["shell_not_applicable"] * 2

    @pytest.mark.parametrize("wave", list(SOURCES))
    def test_each_ehcvm_script_reads_its_own_state_column_with_its_own_table(self, wave):
        *_, state, _codes, table = SOURCES[wave]
        script = (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert _state_read(script) == (state, table, True)

    def test_the_state_read_is_not_satisfied_by_a_docstring(self):
        """2018-19's sold state is s16cq16c; naming s16cq12c in prose must not pass."""
        script = ('"""Reads s16cq12c through harmonize_crop_condition_ehcvm1."""\n'
                  'src = get_dataframe("x.dta", convert_categoricals=True)\n'
                  'c = _crop_conditions(src["s16cq16c"], "harmonize_crop_condition_ehcvm1")\n')
        assert _state_read(script) == ("s16cq16c", "harmonize_crop_condition_ehcvm1", False)

    def test_blank_and_off_scheme_codes_are_the_sentinel(self, niger_module,
                                                         in_country_dir):
        out = niger_module._crop_conditions(pd.Series([1.0, float("nan"), 77.0]),
                                            SOURCES["2021-22"][-1])
        assert out.tolist() == ["unshelled", SENTINEL, SENTINEL]

    def test_tail_fills_the_sentinel_when_a_wave_passes_no_condition(self, niger_module):
        df = pd.DataFrame({
            "i": ["1"], "plot_id": ["1_1"], "crop": ["Mil"], "u": ["Kg"],
            "Quantity": [10.0], "Quantity_sold": [None], "Value_sold": [None]})
        out = niger_module._finish_crop_production(df, "2099-00")
        assert list(out.index.names) == ["t", "i", "plot_id", "crop", "u",
                                         "condition", "production_obs"]
        assert set(out.index.get_level_values("condition")) == {SENTINEL}

    def test_tail_keeps_a_passed_condition_and_fills_a_null(self, niger_module):
        df = pd.DataFrame({
            "i": ["1", "1"], "plot_id": ["1_1", "1_2"], "crop": ["Mil", "Mil"],
            "u": ["Kg", "Kg"], "condition": ["shelled", None],
            "Quantity": [10.0, 3.0], "Quantity_sold": [None, None],
            "Value_sold": [None, None]})
        out = niger_module._finish_crop_production(df, "2099-00")
        assert out.index.get_level_values("condition").tolist() == ["shelled", SENTINEL]


class TestNativeColumnDeclared:
    """``condition_native``: declared, and only where a fold happened."""

    def test_canonical_declaration_is_optional_and_not_a_vocabulary(self):
        entry = _canonical_scheme()["condition_native"]
        assert entry["type"] == "str" and entry["optional"] is True
        # A `spellings` block is read as CLOSED; the labels are the survey's.
        assert "spellings" not in entry

    def test_niger_declares_it_optional_and_not_as_a_level(self):
        with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
            entry = yaml.load(f, Loader=_SchemeLoader)["Data Scheme"][TABLE]
        assert entry["condition_native"] == {"type": "str", "optional": True}
        assert "condition_native" not in entry["index"]

    @pytest.mark.parametrize("wave, expected", [
        ("2018-19", ("s16cq12c", True)), ("2021-22", None),
        ("2011-12", None), ("2014-15", None)])
    def test_only_2018_19_reads_a_label_and_from_the_labelled_frame(self, wave, expected):
        script = (_root() / wave / "_" / f"{TABLE}.py").read_text(encoding="utf-8")
        assert _label_read(script) == expected

    def test_label_read_is_not_satisfied_by_a_docstring(self):
        script = ('"""Reads s16cq12c."""\n'
                  'raw = get_dataframe("x.dta", convert_categoricals=False)\n'
                  'c = _crop_condition_labels(raw["s16cq07"])\n')  # any other column; s16cq16c is now the sold state's
        assert _label_read(script) == ("s16cq07", False)

    def test_helper_keeps_the_label_and_a_blank_stays_na(self, niger_module):
        out = niger_module._crop_condition_labels(
            pd.Series(["Non-décortiqué", " Grain ", "", None], dtype="object"))
        assert out.iloc[0] == "Non-décortiqué" and out.iloc[1] == "Grain"
        assert out.iloc[2:].isna().all()
        assert str(out.dtype) == "string"

    def test_helper_is_not_french_only(self, niger_module):
        """Guinea-Bissau shares the codes under Portuguese labels."""
        out = niger_module._crop_condition_labels(pd.Series(["Não descascado"]))
        assert out.tolist() == ["Não descascado"]

    def test_tail_serves_na_when_a_wave_passes_none_and_keeps_it_a_column(self, niger_module):
        df = pd.DataFrame({
            "i": ["1"], "plot_id": ["1_1"], "crop": ["Mil"], "u": ["Kg"],
            "Quantity": [10.0], "Quantity_sold": [None], "Value_sold": [None]})
        out = niger_module._finish_crop_production(df, "2099-00")
        assert "condition_native" in out.columns
        assert "condition_native" not in out.index.names
        assert out["condition_native"].isna().all()


# ---------------------------------------------------------------------------
# the built table, against the source
# ---------------------------------------------------------------------------

def _source_tuples(niger, wave):
    """Multiset of (plot, crop, unit, state, quantity) read straight from the
    source file, for lines reporting a positive quantity (never dropped)."""
    from lsms_library.local_tools import format_id, get_dataframe

    source, crop, unit, qty, state, codes, _table = SOURCES[wave]
    cwd = os.getcwd()
    os.chdir(_root() / wave / "_")
    try:
        lab = get_dataframe(source, convert_categoricals=True)
        raw = get_dataframe(source, convert_categoricals=False)
        crop_map, unit_map = niger._crop_maps()
        crops = niger._crop_labels(raw[crop], lab[crop], crop_map)
        units = niger._unit_labels(lab[unit], unit_map)
    finally:
        os.chdir(cwd)
    frame = pd.DataFrame({
        "plot_id": (raw["s16cq02"].apply(format_id).astype(str) + "_"
                    + raw["s16cq03"].apply(format_id).astype(str)).values,
        "crop": pd.Series(crops).values,
        "u": pd.Series(units).fillna(niger.U_NA).values,
        "condition": pd.to_numeric(raw[state], errors="coerce").map(codes)
                     .fillna(SENTINEL).values,
        "Quantity": pd.to_numeric(raw[qty], errors="coerce").values,
    })
    frame = frame[frame["crop"].notna() & (frame["Quantity"] > 0)]
    return Counter(map(tuple, frame.astype({"Quantity": float}).values.tolist()))


def _recorded_source_2018() -> pd.DataFrame:
    """(plot, recorded state, quantity), straight from s16c.  Imports nothing from
    ``Niger/_/``: the codes go through :data:`RECORDED_2018`, written out above."""
    from lsms_library.local_tools import format_id, get_dataframe

    cwd = os.getcwd()
    os.chdir(_root() / "2018-19" / "_")
    try:
        raw = get_dataframe(SOURCES["2018-19"][0], convert_categoricals=False)
    finally:
        os.chdir(cwd)
    return pd.DataFrame({
        "plot_id": (raw["s16cq02"].apply(format_id).astype(str) + "_"
                    + raw["s16cq03"].apply(format_id).astype(str)).values,
        "condition_native": pd.to_numeric(raw["s16cq12c"], errors="coerce")
                            .map(RECORDED_2018).values,
        "Quantity": pd.to_numeric(raw["s16cq12a"], errors="coerce").values,
    })


@pytest.mark.requires_s3
class TestServed:

    @pytest.fixture(scope="class")
    def served(self):
        import lsms_library as ll

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ll.Country(COUNTRY).crop_production()

    def test_level_is_served_and_the_index_stays_unique(self, served):
        assert "condition" in served.index.names
        assert served.index.is_unique

    def test_no_null_key_and_nothing_outside_the_vocabulary(self, served):
        values = served.index.get_level_values("condition")
        assert not pd.isna(values).any()
        assert set(values) <= _canonical_vocabulary()

    def test_declared_spellings_check_passes(self, served):
        from lsms_library.diagnostics import _check_declared_spellings

        assert _check_declared_spellings(served, TABLE).status == "pass"

    @pytest.mark.parametrize("wave", NO_STATE_QUESTION)
    def test_ecvma_waves_carry_only_the_sentinel(self, served, wave):
        got = set(served.xs(wave, level="t").index.get_level_values("condition"))
        assert got == {SENTINEL}

    @pytest.mark.parametrize("wave", list(SOURCES))
    def test_served_state_is_the_source_code(self, served, niger_module, wave):
        g = served.xs(wave, level="t").reset_index()
        g = g[g["Quantity"].fillna(0) > 0]
        got = Counter(map(tuple, g[["plot_id", "crop", "u", "condition", "Quantity"]]
                          .astype({"Quantity": float}).astype(object).values.tolist()))
        assert got == _source_tuples(niger_module, wave)

    @pytest.mark.parametrize("wave", list(SOURCES))
    def test_state_is_blank_exactly_where_the_unit_is(self, served, wave):
        """An idiosyncrasy of both EHCVM files, and why the sentinel is not a
        new loss: a line with no state is a line with no unit either."""
        g = served.xs(wave, level="t").reset_index()
        pd.testing.assert_series_equal(g["condition"].eq(SENTINEL).astype(bool),
                                       g["u"].eq("Unknown").astype(bool),
                                       check_names=False)

    def test_native_column_counts_are_the_source_codes(self, served):
        got = served.xs("2018-19", level="t")["condition_native"].value_counts()
        expected = _recorded_source_2018()["condition_native"].value_counts()
        assert got.to_dict() == expected.to_dict()

    def test_native_column_is_the_source_code_row_by_row(self, served):
        g = served.xs("2018-19", level="t").reset_index()
        g = g[g["Quantity"].fillna(0) > 0]
        src = _recorded_source_2018()
        src = src[src["Quantity"] > 0]
        def tuples(frame):
            return Counter(
                (p, None if pd.isna(c) else c, float(q))
                for p, c, q in zip(frame["plot_id"], frame["condition_native"], frame["Quantity"]))
        assert tuples(g) == tuples(src)

    def test_the_level_is_the_ruled_fold_of_the_column(self, served):
        g = served.xs("2018-19", level="t").reset_index()
        expected = g["condition_native"].astype(object).map(FOLD_2018).fillna(SENTINEL)
        assert (g["condition"].astype(object) == expected).all()

    @pytest.mark.parametrize("wave", NO_STATE_QUESTION + ("2021-22",))
    def test_native_column_is_na_where_nothing_was_folded(self, served, wave):
        assert served.xs(wave, level="t")["condition_native"].isna().all()

    def test_native_column_is_not_a_level_and_moves_no_kilogram(self, served):
        from lsms_library.transformations import harvest_kg

        assert "condition_native" not in served.index.names
        pd.testing.assert_frame_equal(harvest_kg(served),
                                      harvest_kg(served.drop(columns="condition_native")))

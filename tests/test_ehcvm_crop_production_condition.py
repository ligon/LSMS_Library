"""EHCVM ``crop_production`` serves ``condition`` and the recorded answer -- GH #1436.

A harvest Quantity is a quantity of something in some physical state, and the
EHCVM instrument says which: "Etat du produit", ``s16cq12c`` in the 2018-19 round.
Ruling @ligon 2026-11-04 (option D, set on Niger, #1471): the fold stays on the
``condition`` level and the column ``condition_native`` keeps the answer.

The rule pinned here is the mapping, not today's counts:

* the served level and column are what the SOURCE code says, read from the source
  file with ``convert_categoricals=False`` and mapped by tables written out in this
  test, so a wrong table cannot agree with itself;
* a blank code is the ``unknown_condition`` sentinel on the level and NA in the
  column, never a NaN key and never a sentinel in the column;
* every served value is in the vocabulary ``lsms_library/data_info.yml`` declares;
* the table is read strictly, because a missing one would serve every row as the
  sentinel.

The wave scripts run their build at import, so the config tier reads them with
``ast`` and executes only their helper functions.  Niger keeps its own file
(``tests/test_niger_crop_production_condition.py``).

Two tiers: config/synthetic (no microdata) and ``TestServed`` (builds the real
table; skipped when the country is unavailable).
"""
from __future__ import annotations

import ast
import os
import warnings
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pytest
import yaml

import lsms_library.local_tools as tools
from lsms_library.paths import countries_root

TABLE = "crop_production"
SENTINEL = "unknown_condition"
STATE = "s16cq12c"

#: The ruled fold, by code, written out here on purpose: this is the oracle.
FOLD = {1: "unshelled", 2: "shelled", 3: "shelled", 4: "unshelled",
        5: "shell_not_applicable"}
FRENCH = {1: "Epis", 2: "Grain", 3: "Décortiqué", 4: "Non-décortiqué",
          5: "Non concerné"}


@dataclass(frozen=True)
class Cell:
    wave: str
    source: str
    labels: dict
    fold: dict
    #: Left out of the row-level oracles, by source code and by served label.
    excluded_crop_codes: tuple = ()
    excluded_crop_labels: tuple = ()


CELLS = {
    "Benin": Cell("2018-19", "../Data/s16c_me_ben2018.dta", FRENCH, FOLD),
    "Togo": Cell("2018", "../Data1/s16c_me_tgo2018.dta", FRENCH, FOLD),
}
COUNTRIES = list(CELLS)


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the ``!make`` tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


def _wave_dir(country: str) -> Path:
    return countries_root() / country / CELLS[country].wave / "_"


def _script(country: str) -> str:
    return (_wave_dir(country) / f"{TABLE}.py").read_text(encoding="utf-8")


def _canonical_vocabulary() -> set[str]:
    path = Path(__file__).resolve().parents[1] / "lsms_library" / "data_info.yml"
    with open(path, encoding="utf-8") as f:
        scheme = yaml.load(f, Loader=_SchemeLoader)["Columns"][TABLE]
    return set(scheme["condition"]["spellings"])


def _helpers(country: str, *names: str) -> dict:
    """The named functions of a wave script, executed without running its build."""
    tree = ast.parse(_script(country))
    keep = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in keep} == set(names), f"{country}: {names}"
    namespace = {"pd": pd, "tools": tools,
                 "get_categorical_mapping": tools.get_categorical_mapping}
    exec(compile(ast.Module(keep, []), f"{country}/{TABLE}.py", "exec"), namespace)
    return namespace


def _call_read(source: str, helper: str) -> tuple[str, bool | None]:
    """What a script's one ``helper(<frame>['<column>'], ...)`` call reads: the
    column, and whether <frame> is the raw-code read (True), the labelled read
    (False) or neither (None).  Parsed, so prose naming a column cannot satisfy it."""
    tree = ast.parse(source)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "id", None) == helper]
    assert len(calls) == 1, f"{len(calls)} {helper} calls"
    column = calls[0].args[0]
    assert isinstance(column, ast.Subscript) and isinstance(column.value, ast.Name)
    raw = None
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                and any(isinstance(t, ast.Name) and t.id == column.value.id
                        for t in node.targets)):
            for keyword in node.value.keywords:
                if keyword.arg == "convert_categoricals":
                    raw = keyword.value.value is False
    return column.slice.value, raw


# ---------------------------------------------------------------------------
# config / synthetic
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("country", COUNTRIES)
class TestDeclared:

    def test_scheme_declares_the_level_and_the_optional_column(self, country):
        path = countries_root() / country / "_" / "data_scheme.yml"
        with open(path, encoding="utf-8") as f:
            entry = yaml.load(f, Loader=_SchemeLoader)["Data Scheme"][TABLE]
        levels = [lvl.strip() for lvl in entry["index"].strip("()").split(",")]
        assert "condition" in levels
        assert "condition_native" not in levels
        assert entry["condition_native"] == {"type": "str", "optional": True}

    def test_table_is_the_ruled_fold(self, country, monkeypatch):
        monkeypatch.chdir(_wave_dir(country))
        got = tools.get_categorical_mapping(
            tablename="harmonize_crop_condition_ehcvm1", idxvars="Code",
            **{"Preferred Label": "Preferred Label"})
        assert {int(k): v for k, v in got.items()} == CELLS[country].fold
        assert set(got.values()) <= _canonical_vocabulary()
        assert SENTINEL not in got.values()

    def test_source_labels_are_the_recorded_labels(self, country, monkeypatch):
        monkeypatch.chdir(_wave_dir(country))
        got = tools.get_categorical_mapping(
            tablename="harmonize_crop_condition_ehcvm1", idxvars="Code",
            **{"Source Label": "Source Label"})
        assert {int(k): v for k, v in got.items()} == CELLS[country].labels

    def test_script_reads_the_harvest_state_by_code_and_by_label(self, country):
        script = _script(country)
        assert _call_read(script, "_crop_conditions") == (STATE, True)
        assert _call_read(script, "_crop_condition_labels") == (STATE, False)

    def test_blank_and_off_scheme_codes_are_the_sentinel(self, country, monkeypatch):
        monkeypatch.chdir(_wave_dir(country))
        fn = _helpers(country, "_crop_conditions")["_crop_conditions"]
        out = fn(pd.Series([1.0, 5.0, float("nan"), 77.0]))
        assert out.tolist() == [FOLD[1], FOLD[5], SENTINEL, SENTINEL]
        assert str(out.dtype) == "string"

    def test_a_missing_table_raises(self, country, tmp_path, monkeypatch):
        """A tolerant read would serve every row as the sentinel, with no signal."""
        monkeypatch.chdir(tmp_path)
        fn = _helpers(country, "_crop_conditions")["_crop_conditions"]
        with pytest.raises((KeyError, FileNotFoundError)):
            fn(pd.Series([1.0]))


class TestScriptReadHelper:

    def test_the_state_read_is_not_satisfied_by_a_docstring(self):
        """The sold state is s16cq16c; naming s16cq12c in prose must not pass."""
        script = ('"""Reads s16cq12c."""\n'
                  'src = get_dataframe("x.dta", convert_categoricals=True)\n'
                  'c = _crop_conditions(src["s16cq16c"])\n')
        assert _call_read(script, "_crop_conditions") == ("s16cq16c", False)


# ---------------------------------------------------------------------------
# the built table, against the source
# ---------------------------------------------------------------------------

def _source(country: str) -> pd.DataFrame:
    """(plot, code, recorded state, ruled fold, quantity), straight from s16c.  Imports
    nothing from ``countries/``: the codes go through the tables written out above."""
    from lsms_library.local_tools import format_id, get_dataframe

    cell = CELLS[country]
    cwd = os.getcwd()
    os.chdir(_wave_dir(country))
    try:
        raw = get_dataframe(cell.source, convert_categoricals=False)
    finally:
        os.chdir(cwd)
    codes = pd.to_numeric(raw[STATE], errors="coerce")
    frame = pd.DataFrame({
        "plot_id": (raw["s16cq02"].apply(format_id).astype(str) + "_"
                    + raw["s16cq03"].apply(format_id).astype(str)).values,
        "crop_code": pd.to_numeric(raw["s16cq04"], errors="coerce").values,
        "code": codes.values,
        "condition_native": codes.map(cell.labels).values,
        "condition": codes.map(cell.fold).fillna(SENTINEL).values,
        "Quantity": pd.to_numeric(raw["s16cq12a"], errors="coerce").values,
    })
    return frame[~frame["crop_code"].isin(cell.excluded_crop_codes)]


def _tuples(frame: pd.DataFrame) -> Counter:
    frame = frame[frame["Quantity"].fillna(0) > 0]
    return Counter(
        (p, None if pd.isna(n) else n, c, float(q))
        for p, n, c, q in zip(frame["plot_id"], frame["condition_native"],
                              frame["condition"], frame["Quantity"]))


@pytest.fixture(scope="module")
def built():
    import lsms_library as ll

    cache = {}

    def get(country):
        if country not in cache:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cache[country] = getattr(ll.Country(country), TABLE)()
        return cache[country]

    return get


def _wave_frame(built, country: str) -> pd.DataFrame:
    return built(country).xs(CELLS[country].wave, level="t").reset_index()


@pytest.mark.requires_s3
@pytest.mark.parametrize("country", COUNTRIES)
class TestServed:

    def test_level_is_served_and_the_index_stays_unique(self, built, country):
        served = built(country)
        assert "condition" in served.index.names
        assert "condition_native" not in served.index.names
        assert served.index.is_unique

    def test_no_null_key_and_nothing_outside_the_vocabulary(self, built, country):
        values = built(country).index.get_level_values("condition")
        assert not pd.isna(values).any()
        assert set(values) <= _canonical_vocabulary()

    def test_declared_spellings_check_passes(self, built, country):
        from lsms_library.diagnostics import _check_declared_spellings

        assert _check_declared_spellings(built(country), TABLE).status == "pass"

    def test_the_level_is_the_ruled_fold_of_the_column(self, built, country):
        cell = CELLS[country]
        by_label = {cell.labels[code]: token for code, token in cell.fold.items()}
        g = _wave_frame(built, country)
        expected = g["condition_native"].astype(object).map(by_label).fillna(SENTINEL)
        assert (g["condition"].astype(object) == expected).all()

    def test_native_counts_are_the_source_codes(self, built, country):
        cell = CELLS[country]
        g = _wave_frame(built, country)
        excluded = g["crop"].isin(cell.excluded_crop_labels)
        got = g.loc[~excluded, "condition_native"].value_counts().to_dict()
        assert got == _source(country)["condition_native"].value_counts().to_dict()
        assert set(got) <= set(cell.labels.values())

    def test_served_rows_are_the_source_rows(self, built, country):
        g = _wave_frame(built, country)
        g = g[~g["crop"].isin(CELLS[country].excluded_crop_labels)]
        assert _tuples(g) == _tuples(_source(country))

    def test_native_column_moves_no_kilogram(self, built, country):
        from lsms_library.transformations import harvest_kg

        served = built(country)
        pd.testing.assert_frame_equal(harvest_kg(served),
                                      harvest_kg(served.drop(columns="condition_native")))

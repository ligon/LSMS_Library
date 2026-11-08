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
from lsms_library.ehcvm import crop_condition_codes, crop_condition_labels

TABLE = "crop_production"
SENTINEL = "unknown_condition"
STATE = "s16cq12c"

#: The ruled fold, by code, written out here on purpose: this is the oracle.
FOLD = {1: "unshelled", 2: "shelled", 3: "shelled", 4: "unshelled",
        5: "shell_not_applicable"}
FRENCH = {1: "Epis", 2: "Grain", 3: "Décortiqué", 4: "Non-décortiqué",
          5: "Non concerné"}
#: Cote d'Ivoire's vague 2 instrument adds two answers the .dta does not label
#: (CIV-EHCVM1-QNR-MEN-CAPI-VAGUE2.pdf, p. 197).  Neither is claimed to be another answer.
IVOIRE_LABELS = {**FRENCH, 6: "Fève", 7: "Autre à préciser"}
IVOIRE_FOLD = {**FOLD, 6: "other_condition", 7: "other_condition"}


@dataclass(frozen=True)
class Cell:
    wave: str
    source: str
    labels: dict
    fold: dict
    #: Left out of the row-level oracles, by source code and by served label.
    excluded_crop_codes: tuple = ()
    excluded_crop_labels: tuple = ()
    state: str = STATE
    quantity: str = "s16cq12a"


CELLS = {
    "Benin": Cell("2018-19", "../Data/s16c_me_ben2018.dta", FRENCH, FOLD),
    "Togo": Cell("2018", "../Data1/s16c_me_tgo2018.dta", FRENCH, FOLD),
    "Senegal": Cell("2018-19", "../Data/s16c_me_sen2018.dta", FRENCH, FOLD),
    "Burkina_Faso": Cell("2018-19", "../Data/s16c_me_bfa2018.dta", FRENCH, FOLD),
    "CotedIvoire": Cell("2018-19", "../Data/Menage/s16c_me_CIV2018.dta",
                        IVOIRE_LABELS, IVOIRE_FOLD),
}
COUNTRIES = list(CELLS)


class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the ``!make`` tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


#: Mali has two EHCVM waves, on one list, read by shared helpers in ``mali.py``.
MALI = {
    "2018-19": Cell("2018-19", "../Data/s16c_me_mli2018.dta", FRENCH, FOLD),
    "2021-22": Cell("2021-22", "../Data/s16c_me_mli2021.dta", FRENCH, FOLD,
                    state="s16cq16d", quantity="s16cq16a"),
}
#: Mali's EAC-I waves carry only the sentinel.  2014-15's harvest files hold no state;
#: 2017-18 records one (s7fq13b) in a list of its own, unread until ruled, when this
#: tuple moves.
MALI_SENTINEL_WAVES = ("2014-15", "2017-18")


def _wave_dir(country: str, wave: str | None = None) -> Path:
    return countries_root() / country / (wave or CELLS[country].wave) / "_"


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
                 "get_categorical_mapping": tools.get_categorical_mapping,
                 "crop_condition_codes": crop_condition_codes,
                 "crop_condition_labels": crop_condition_labels}
    exec(compile(ast.Module(keep, []), f"{country}/{TABLE}.py", "exec"), namespace)
    return namespace


def _call_read(source: str, helper: str, expected_column: str | None = None) -> tuple[str, bool | None]:
    """What a script's one ``helper(<frame>['<column>'], ...)`` call reads: the
    column, and whether <frame> is the raw-code read (True), the labelled read
    (False) or neither (None).  Parsed, so prose naming a column cannot satisfy it."""
    tree = ast.parse(source)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "id", None) == helper]
    # Harvest and sale may use the same literal-label helper. Select the
    # required source column, still refusing missing or duplicate bindings.
    if expected_column is not None:
        calls = [n for n in calls if n.args and isinstance(n.args[0], ast.Subscript)
                 and isinstance(n.args[0].slice, ast.Constant)
                 and n.args[0].slice.value == expected_column]
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
        assert _call_read(script, "_crop_conditions", STATE) == (STATE, True)
        assert _call_read(script, "_crop_condition_labels", STATE) == (STATE, False)

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

    def test_required_column_cannot_be_satisfied_by_the_other_answer(self):
        script = 'c = _crop_condition_labels(src["s16cq16c"])\n'
        with pytest.raises(AssertionError, match="0 .* calls"):
            _call_read(script, "_crop_condition_labels", STATE)

    def test_duplicate_required_bindings_are_refused(self):
        script = 'c = _crop_condition_labels(src["s16cq12c"])\n' * 2
        with pytest.raises(AssertionError, match="2 .* calls"):
            _call_read(script, "_crop_condition_labels", STATE)


# ---------------------------------------------------------------------------
# the built table, against the source
# ---------------------------------------------------------------------------

def _source(country: str, cell: Cell | None = None) -> pd.DataFrame:
    """(plot, code, recorded state, ruled fold, quantity), straight from s16c.  Imports
    nothing from ``countries/``: the codes go through the tables written out above."""
    from lsms_library.local_tools import format_id, get_dataframe

    cell = cell or CELLS[country]
    cwd = os.getcwd()
    os.chdir(_wave_dir(country, cell.wave))
    try:
        raw = get_dataframe(cell.source, convert_categoricals=False)
    finally:
        os.chdir(cwd)
    codes = pd.to_numeric(raw[cell.state], errors="coerce")
    frame = pd.DataFrame({
        "plot_id": (raw["s16cq02"].apply(format_id).astype(str) + "_"
                    + raw["s16cq03"].apply(format_id).astype(str)).values,
        "crop_code": pd.to_numeric(raw["s16cq04"], errors="coerce").values,
        "code": codes.values,
        "condition_native": codes.map(cell.labels).values,
        "condition": codes.map(cell.fold).fillna(SENTINEL).values,
        "Quantity": pd.to_numeric(raw[cell.quantity], errors="coerce").values,
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


class TestCotedIvoireUnlabelledCodes:
    """Codes 6 and 7 carry no .dta label; the column takes the instrument's."""

    @pytest.fixture
    def labels(self, monkeypatch):
        monkeypatch.chdir(_wave_dir("CotedIvoire"))
        return _helpers("CotedIvoire", "_crop_condition_labels")["_crop_condition_labels"]

    def test_the_two_codes_read_the_instrument_and_the_rest_the_file(self, labels):
        out = labels(pd.Series(["Epis", 6.0, 7.0, None], dtype="object"),
                     pd.Series([1.0, 6.0, 7.0, float("nan")]))
        assert out.iloc[:3].tolist() == ["Epis", "Fève", "Autre à préciser"]
        assert pd.isna(out.iloc[3])
        assert str(out.dtype) == "string"

    def test_a_bare_number_is_never_served(self, labels):
        out = labels(pd.Series([6.0, 7.0], dtype="object"), pd.Series([6.0, 7.0]))
        assert not out.astype(str).str.contains(r"^\d").any()

    @pytest.mark.requires_s3
    def test_the_two_codes_occur_only_in_vague_2(self):
        """What makes the instrument's vague 2 list the right reading: vague 1's has five."""
        from lsms_library.local_tools import get_dataframe

        cwd = os.getcwd()
        os.chdir(_wave_dir("CotedIvoire"))
        try:
            raw = get_dataframe(CELLS["CotedIvoire"].source, convert_categoricals=False)
        finally:
            os.chdir(cwd)
        added = pd.to_numeric(raw[STATE], errors="coerce").isin([6, 7])
        assert added.any()
        assert set(raw.loc[added, "vague"]) == {2}


def _mali():
    """``Mali/_/mali.py``, imported the way the wave scripts import it."""
    import importlib
    import sys

    path = str(countries_root() / "Mali" / "_")
    sys.path.insert(0, path)
    try:
        return importlib.import_module("mali")
    finally:
        sys.path.remove(path)


class TestMaliDeclared:
    """Mali's index has no ``u``, and its helpers are in ``mali.py``."""

    def test_scheme_appends_the_level_and_declares_the_optional_column(self):
        path = countries_root() / "Mali" / "_" / "data_scheme.yml"
        with open(path, encoding="utf-8") as f:
            entry = yaml.load(f, Loader=_SchemeLoader)["Data Scheme"][TABLE]
        assert entry["index"] == "(t, i, plot_id, crop, condition)"
        assert entry["condition_native"] == {"type": "str", "optional": True}

    @pytest.mark.parametrize("wave", list(MALI))
    def test_script_reads_its_own_state_by_code_and_by_label(self, wave):
        script = (_wave_dir("Mali", wave) / f"{TABLE}.py").read_text(encoding="utf-8")
        state = MALI[wave].state
        assert _call_read(script, "ehcvm_crop_conditions", state) == (state, True)
        assert _call_read(script, "ehcvm_crop_condition_labels", state) == (state, False)

    @pytest.mark.parametrize("wave", list(MALI))
    def test_table_is_the_ruled_fold_in_both_waves(self, wave, monkeypatch):
        monkeypatch.chdir(_wave_dir("Mali", wave))
        out = _mali().ehcvm_crop_conditions(
            pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, float("nan"), 77.0]))
        assert out.tolist() == [FOLD[c] for c in range(1, 6)] + [SENTINEL, SENTINEL]
        assert str(out.dtype) == "string"

    def test_a_missing_table_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises((KeyError, FileNotFoundError)):
            _mali().ehcvm_crop_conditions(pd.Series([1.0]))

    def test_the_label_helper_keeps_the_answer_and_a_blank_stays_na(self):
        out = _mali().ehcvm_crop_condition_labels(
            pd.Series(["Non-décortiqué", " Grain ", "", None], dtype="object"))
        assert out.iloc[:2].tolist() == ["Non-décortiqué", "Grain"]
        assert out.iloc[2:].isna().all()

    def test_the_ehcvm_tail_fills_a_row_with_no_state_and_appends_the_level(self):
        frame = pd.DataFrame({
            "i": ["1", "1"], "plot_id": ["1_1", "Farm-level"], "crop": ["Mil", "Mil"],
            "u": ["Kg", None], "Quantity": [10.0, None], "Quantity_sold": [None, 3.0],
            "Value_sold": [None, 90.0], "intercropped": [True, None],
            "condition": ["shelled", None], "condition_native": ["Grain", None]})
        out = _mali().finish_ehcvm_crop_production(frame, "2099-00")
        assert list(out.index.names) == ["t", "i", "plot_id", "crop", "condition"]
        got = out.reset_index().set_index("plot_id")
        assert got.loc["1_1", "condition"] == "shelled"
        assert got.loc["Farm-level", "condition"] == SENTINEL
        assert pd.isna(got.loc["Farm-level", "condition_native"])

    def test_the_eaci_tail_appends_the_sentinel_after_the_collapse(self, monkeypatch):
        monkeypatch.chdir(_wave_dir("Mali", "2014-15"))  # the crop and unit tables
        base = dict(t="2099-00", i="h1", crop="Mil", u="Sac", Quantity_sold=float("nan"),
                    Unit_sold=pd.NA, Value_sold=float("nan"), months_harvested=pd.NA,
                    planting_month=pd.NA, harvest_month=pd.NA, intercropped=pd.NA,
                    perennial=False, plot_id="1_1")
        rows = pd.DataFrame([dict(base, Quantity=2.0), dict(base, Quantity=3.0)])
        out = _mali().crop_production_finalize(rows)
        assert list(out.index.names) == ["t", "i", "plot_id", "crop", "condition"]
        assert out["Quantity"].tolist() == [5.0]
        assert out.index.get_level_values("condition").tolist() == [SENTINEL]
        assert out["condition_native"].isna().all()


@pytest.mark.requires_s3
class TestMaliServed:

    def test_level_is_served_and_the_index_stays_unique(self, built):
        served = built("Mali")
        assert "condition" in served.index.names
        assert served.index.is_unique
        assert set(served.index.get_level_values("condition")) <= _canonical_vocabulary()

    @pytest.mark.parametrize("wave", MALI_SENTINEL_WAVES)
    def test_the_eaci_waves_carry_only_the_sentinel(self, built, wave):
        got = built("Mali").xs(wave, level="t")
        assert set(got.index.get_level_values("condition")) == {SENTINEL}
        assert got["condition_native"].isna().all()

    @pytest.mark.parametrize("wave", list(MALI))
    def test_the_stated_rows_are_the_source_rows(self, built, wave):
        """The rows that state one, tuple by tuple.  Blank states are left out on both
        sides: a Farm-level row or a derived harvest has a Quantity and no state."""
        g = built("Mali").xs(wave, level="t").reset_index()
        src = _source("Mali", MALI[wave])
        keys = ["plot_id", "condition_native", "condition"]

        def counts(frame):
            frame = frame[frame["condition_native"].notna()]
            return Counter(map(tuple, frame[keys].astype(object).values.tolist()))

        assert counts(g) == counts(src)

    @pytest.mark.parametrize("wave", list(MALI))
    def test_the_level_is_the_ruled_fold_of_the_column(self, built, wave):
        by_label = {FRENCH[code]: token for code, token in FOLD.items()}
        g = built("Mali").xs(wave, level="t").reset_index()
        expected = g["condition_native"].astype(object).map(by_label).fillna(SENTINEL)
        assert (g["condition"].astype(object) == expected).all()

    def test_a_farm_level_row_states_no_condition(self, built):
        g = built("Mali").xs("2021-22", level="t").reset_index()
        farm = g[g["plot_id"].eq("Farm-level")]
        assert len(farm) > 0
        assert set(farm["condition"]) == {SENTINEL}

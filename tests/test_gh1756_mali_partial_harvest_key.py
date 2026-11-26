"""GH #1756: Mali 2017-18 partial harvests carry the ``harvest-to-date`` key.

Ruling (@ligon, 2026-11-25): "Add the key and correct the documentation".  The P2
manual's 7F.13 records, on a harvest that is not finished, only what has already been
harvested.  A seasonal (``s7f``) row whose line answers ``s7fq10`` ("Fin de la
récolte") ``Non`` and whose served ``Quantity`` is recorded takes the key the EHCVM
waves already carry, ``Mali::crop_production::harvest-to-date``.  An identity: no
quantity, unit, factor or sale moves, and tree rows and Farm-level sale rows never
carry it.  The wave docstring named ``s7fq06`` (7F.06, pre-harvest loss) as the
"fully harvested" answer; it is ``s7fq10``.

Everything outside ``TestOracle`` runs on invented rows and reads no survey file.
``TestOracle`` (``requires_s3``) builds the wave and derives the expected cohort from
``eaci17_s7fp2.dta`` itself, importing nothing from ``Mali/_/``; it pins no count.  It
was written with the change and had not been run on the survey file at that time.
"""
from __future__ import annotations

import ast
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library import local_tools as L
from lsms_library.countries.Mali._ import mali as M
from lsms_library.derivations import MULTI_SEP, records_for

COUNTRY_DIR = Path(M.__file__).resolve().parents[1]
WAVE = "2017-18"
FARM = "Farm-level"
WINDOW = "Mali::crop_production::harvest-to-date"
SALE = "Mali::crop_production::hh-crop-sale-single-row"
TREES = "Mali::crop_production::tree-twelve-month-total"
SCRIPT = COUNTRY_DIR / WAVE / "_" / "crop_production.py"


# --- the wave script, statically (it reads survey files at import) -----------------

class TestScript:

    def test_the_docstring_names_s7fq10_and_not_s7fq06_as_the_finished_answer(self):
        doc = ast.get_docstring(ast.parse(SCRIPT.read_text()))
        assert "harvest_finished=s7fq10" in doc
        assert "fully_harvested=s7fq06" not in doc
        assert "Perte pré-récolte" in doc      # what 7F.06 is, said where it was mis-said

    def test_s7fq06_is_still_not_read(self):
        tree = ast.parse(SCRIPT.read_text())
        doc = tree.body[0]
        literals = {n.value for stmt in tree.body if stmt is not doc for n in ast.walk(stmt)
                    if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        assert "s7fq06" not in literals and "s7fq10" in literals

    def test_the_stamp_follows_the_finalizer_and_the_answer_stays_beside_the_frame(self):
        text = SCRIPT.read_text()
        finalize = text.index("df = crop_production_finalize(df)")
        stamp = text.index("df['Derivation'] = stamp_eaci17_harvest_to_date(")
        assert finalize < stamp
        # `finished` is a column of the side table only, never of the frame finalized.
        tree = ast.parse(text)
        seasonal, = [n.value for n in ast.walk(tree) if isinstance(n, ast.Assign)
                     and getattr(n.targets[0], "id", None) == "seasonal"
                     and isinstance(n.value, ast.Call) and n.value.args
                     and isinstance(n.value.args[0], ast.Dict)]
        keys = {k.value for k in seasonal.args[0].keys}
        assert "finished" not in keys and "s7fq10" not in keys


# --- the stamp, on invented rows through the real finalizer -------------------------

@pytest.fixture
def invented(monkeypatch):
    monkeypatch.delenv("LSMS_GRAIN_STRICT", raising=False)
    monkeypatch.setattr(M, "_crop_labels", lambda x: x.astype("string").str.strip())
    monkeypatch.setattr(M, "_unit_labels", lambda x: x.astype("string"))

    def deny(*a, **kw):
        raise AssertionError("an invented-row test attempted source acquisition")
    monkeypatch.setattr(L, "_ensure_dvc_pulled", deny)
    monkeypatch.setattr(M.tools, "get_dataframe", deny)


#: A row with no s7f line behind it (a tree row, a Farm-level sale row).
NO_LINE = "<no s7f line>"

#: (i, plot_id, crop, Quantity, Quantity_sold, perennial, finished or NO_LINE)
ROWS = [
    ("h1", "1_1", "Mil", 5.0, np.nan, False, "Non"),       # unfinished, recorded
    ("h2", "1_1", "Mil", 9999.0, np.nan, False, "Non"),    # unfinished, "Manquant" code
    ("h3", "1_1", "Mil", 5.0, np.nan, False, "Oui"),       # finished
    ("h4", "1_1", "Mil", 5.0, 2.0, False, "Non"),          # unfinished, holding a sale
    ("h5", FARM, "Manguier", 7.0, np.nan, True, NO_LINE),     # a tree row
    ("h6", FARM, "Mil", np.nan, 3.0, False, NO_LINE),         # a Farm-level sale row
    ("h7", "1_1", "Mil", np.nan, np.nan, False, "Non"),    # unfinished, nothing recorded
    ("h8", "1_1", "Mil", 5.0, np.nan, False, pd.NA),       # 7F.10 unanswered
    ("h9", "1_1", "Mil", 0.0, np.nan, False, "Non"),       # unfinished, a recorded 0
    ("h10", pd.NA, "Mil", 5.0, np.nan, False, "Non"),      # unfinished, no plot id
    ("h11", "1_1", "Mil ", 5.0, np.nan, False, " Non "),   # padded label and answer
]
EXPECTED = {"h1": WINDOW, "h4": MULTI_SEP.join(sorted((SALE, WINDOW))), "h5": TREES,
            "h9": WINDOW, "h10": WINDOW, "h11": WINDOW}


def _raw(rows):
    cols = ["i", "plot_id", "crop", "Quantity", "Quantity_sold", "perennial", "finished"]
    frame = pd.DataFrame(rows, columns=cols)
    raw = frame.drop(columns="finished").assign(
        t=WAVE, u="Sac", KgFactor=100.0, Value_sold=np.nan, planting_month=pd.NA,
        harvest_month=pd.NA, intercropped=pd.NA)
    has_line = ~frame["finished"].astype("string").eq(NO_LINE).fillna(False)
    lines = frame.loc[has_line, ["i", "plot_id", "crop", "finished"]]
    return raw, lines


def _build(rows):
    raw, lines = _raw(rows)
    df = M.crop_production_finalize(raw)
    before = M.stamp_hh_crop_sale(M.stamp_tree_derivations(df), df)
    return df, lines, before


class TestStamp:

    def test_only_an_unfinished_recorded_seasonal_row_takes_the_key(self, invented):
        df, lines, before = _build(ROWS)
        got = M.stamp_eaci17_harvest_to_date(before, df, lines)
        by_household = dict(zip(df.index.get_level_values("i"), got))
        assert {k: v for k, v in by_household.items() if not pd.isna(v)} == EXPECTED
        assert len(by_household) == len(ROWS)

    def test_the_stamp_adds_to_derivation_and_touches_nothing_else(self, invented):
        df, lines, before = _build(ROWS)
        frozen, was = df.copy(deep=True), before.copy(deep=True)
        got = M.stamp_eaci17_harvest_to_date(before, df, lines)
        pd.testing.assert_frame_equal(df, frozen, check_exact=True)   # Quantity, KgFactor, sales
        pd.testing.assert_series_equal(before, was)                  # its input, too
        window = got.astype("string").str.contains(WINDOW, regex=False).fillna(False)
        pd.testing.assert_series_equal(got[~window], before[~window])
        # Non-destructive: every key the row had is still on it.
        for new, old in zip(got[window], before[window]):
            had = set() if pd.isna(old) else set(old.split(MULTI_SEP))
            assert set(new.split(MULTI_SEP)) == had | {WINDOW}
        assert got.dtype == "string" and got.index.equals(df.index)

    def test_tree_and_farm_level_rows_never_take_it(self, invented):
        df, lines, before = _build(ROWS)
        assert not {"h5", "h6"} & set(lines["i"])            # neither has an s7f line
        got = M.stamp_eaci17_harvest_to_date(before, df, lines)
        plot = pd.Series(df.index.get_level_values("plot_id"), index=df.index)
        off_grid = plot.eq(FARM).fillna(False) | df["perennial"].fillna(False).astype(bool)
        assert off_grid.sum() == 2
        assert not got[off_grid].astype("string").str.contains(WINDOW, regex=False).fillna(False).any()

    def test_two_unfinished_lines_on_one_served_key_are_one_keyed_row(self, invented):
        rows = [("h1", "1_1", "Mil", 5.0, np.nan, False, "Non"),
                ("h1", "1_1", "Mil", 4.0, np.nan, False, "Non")]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df, lines, before = _build(rows)
        got = M.stamp_eaci17_harvest_to_date(before, df, lines)
        assert len(df) == 1 and df["Quantity"].iloc[0] == 9.0 and got.iloc[0] == WINDOW

    # -- refused, never guessed ------------------------------------------------------

    def test_lines_that_disagree_on_one_served_key_are_refused(self, invented):
        rows = [("h1", "1_1", "Mil", 5.0, np.nan, False, "Non"),
                ("h1", "1_1", "Mil", 4.0, np.nan, False, "Oui")]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df, lines, before = _build(rows)
        with pytest.raises(ValueError, match="disagree on s7fq10"):
            M.stamp_eaci17_harvest_to_date(before, df, lines)

    def test_an_unfinished_line_with_no_served_row_is_refused(self, invented):
        df, lines, before = _build(ROWS)
        stray = pd.DataFrame({"i": ["nobody"], "plot_id": ["1_1"], "crop": ["Mil"],
                              "finished": ["Non"]})
        with pytest.raises(ValueError, match="land on"):
            M.stamp_eaci17_harvest_to_date(before, df, pd.concat([lines, stray]))

    def test_a_finished_line_with_no_served_row_is_not_a_refusal(self, invented):
        df, lines, before = _build(ROWS)
        stray = pd.DataFrame({"i": ["nobody"], "plot_id": ["1_1"], "crop": ["Mil"],
                              "finished": ["Oui"]})
        got = M.stamp_eaci17_harvest_to_date(before, df, pd.concat([lines, stray]))
        assert int(got.astype("string").str.contains(WINDOW, regex=False).fillna(False).sum()) == 5

    def test_an_unmapped_crop_is_dropped_as_the_finalizer_drops_it(self, invented, monkeypatch):
        monkeypatch.setattr(M, "_crop_labels", lambda x: x.astype("string").str.strip().where(
            x.astype("string").str.strip().ne("Non exploitée")))
        rows = ROWS + [("h12", "1_1", "Non exploitée", 5.0, np.nan, False, "Non")]
        df, lines, before = _build(rows)
        got = M.stamp_eaci17_harvest_to_date(before, df, lines)
        assert "h12" not in set(df.index.get_level_values("i"))
        assert int(got.astype("string").str.contains(WINDOW, regex=False).fillna(False).sum()) == 5

    def test_an_undecoded_answer_column_is_refused(self, invented):
        df, lines, before = _build(ROWS)
        coded = lines.assign(finished=[2] * len(lines))
        with pytest.raises(ValueError, match="neither Oui nor Non"):
            M.stamp_eaci17_harvest_to_date(before, df, coded)

    def test_an_unfinished_key_on_a_tree_or_farm_level_row_is_refused(self, invented):
        df, lines, before = _build(ROWS)
        stray = pd.DataFrame({"i": ["h6"], "plot_id": [FARM], "crop": ["Mil"], "finished": ["Non"]})
        with pytest.raises(ValueError, match="tree or Farm-level"):
            M.stamp_eaci17_harvest_to_date(before, df, pd.concat([lines, stray]))

    def test_the_plot_id_is_the_wave_scripts_spelling(self):
        src = pd.DataFrame({"s7fq01": [1.0, 2.0, np.nan], "s7fq02": [3.0, np.nan, 4.0]})
        assert M.eaci17_plot_ids(src).tolist()[0] == "1_3"
        assert M.eaci17_plot_ids(src).isna().tolist() == [False, True, True]


# --- the registry and the exact-key inputs ------------------------------------------

def _s7f():
    """An invented s7f: (grappe, exploitation, field, parcel, crop, 7F.10, quantity, unit)."""
    rows = [(1, 3, 1, 1, "mil", "Non", 5.0, "Sac"),         # keyed
            (1, 3, 2, 1, "mil", "Oui", 6.0, "Sac"),
            (1, 6, 1, 1, "mil", "Non", 9999.0, "Sac"),      # "Manquant"
            (1, 6, 2, 1, "mil", "Non", np.nan, pd.NA),      # nothing recorded
            (2, 9, 1, 1, "jachère", "Non", 5.0, "Sac"),     # a crop the build drops
            (2, 9, 2, 1, "sorgho", "Non", 0.0, "Kilogramme"),   # keyed: a recorded 0
            (2, 9, 3, 2, "sorgho", pd.NA, 4.0, "Sac")]
    return pd.DataFrame(rows, columns=["grappe", "exploitation", "s7fq01", "s7fq02", "s7fq03",
                                       "s7fq10", "s7fq13a", "s7fq13c"]).assign(s7fq13d=100.0)


class TestRegistry:

    def test_the_existing_key_covers_2017_18_and_names_its_variables(self):
        rec = records_for("Mali", "crop_production")[WINDOW]
        assert list(rec.waves) == ["2017-18", "2018-19", "2021-22"]
        assert {"s7fq10", "s7fq13a", "s7fq13c"} <= set(rec.raw_variables)
        assert "s7fq06" not in rec.raw_variables
        assert rec.function == "lsms_library.countries.Mali._.mali:derive_harvest_to_date"
        assert rec.inputs == "lsms_library.countries.Mali._.mali:inputs_harvest_to_date"
        assert callable(rec.resolve("function")) and callable(rec.resolve("inputs"))

    def test_no_second_window_key_was_minted(self):
        names = [k for k in records_for("Mali", "crop_production") if "harvest-to-date" in k]
        assert names == [WINDOW]

    def test_the_inputs_are_the_native_answers_of_the_keyed_rows(self, invented, monkeypatch):
        monkeypatch.setattr(M.tools, "get_dataframe", lambda path, **kw: _s7f())
        monkeypatch.setattr(M, "_ehcvm_crop_map", lambda wave: {"mil": "Mil", "sorgho": "Sorgho"})
        raw = M.inputs_harvest_to_date(WAVE)     # the callable the entry names, above
        assert list(raw.index.names) == ["t", "i", "plot_id", "crop"]
        assert list(raw.columns) == ["s7fq10", "s7fq13a", "s7fq13c"]
        assert sorted(raw.index) == [(WAVE, "EACI17_1003", "1_1", "Mil"),
                                     (WAVE, "EACI17_2009", "2_1", "Sorgho")]
        assert raw["s7fq13a"].tolist() == [5.0, 0.0] and raw["s7fq10"].eq("Non").all()

    def test_the_inputs_join_the_rows_the_build_keys(self, invented, monkeypatch):
        """Same invented file through the build's steps and through ``inputs``."""
        src = _s7f()
        labels = {"mil": "Mil", "sorgho": "Sorgho"}
        monkeypatch.setattr(M, "_crop_labels", lambda x: x.astype("string").str.strip().map(labels))
        monkeypatch.setattr(M.tools, "get_dataframe", lambda path, **kw: src.copy())
        monkeypatch.setattr(M, "_ehcvm_crop_map", lambda wave: labels)
        i = src.apply(lambda r: M.eaci17_i(pd.Series([r["grappe"], r["exploitation"]])), axis=1)
        seasonal = pd.DataFrame({
            "t": WAVE, "i": i, "plot_id": M.eaci17_plot_ids(src), "crop": src["s7fq03"],
            "u": src["s7fq13c"], "Quantity": src["s7fq13a"], "KgFactor": src["s7fq13d"],
            "Quantity_sold": np.nan, "Value_sold": np.nan, "planting_month": pd.NA,
            "harvest_month": pd.NA, "intercropped": pd.NA, "perennial": False})
        lines = seasonal[["i", "plot_id", "crop"]].assign(finished=src["s7fq10"])
        df = M.crop_production_finalize(seasonal)
        got = M.stamp_eaci17_harvest_to_date(
            M.stamp_hh_crop_sale(M.stamp_tree_derivations(df), df), df, lines)
        keyed = df[got.astype("string").str.contains(WINDOW, regex=False).fillna(False)]
        raw = M.inputs_harvest_to_date(WAVE)
        assert len(keyed) == 2
        assert sorted(keyed.index.droplevel("condition")) == sorted(raw.index)

    def test_the_ehcvm_waves_still_read_their_own_variables(self):
        assert set(M._HARVEST_WINDOW_RAW) == {"2018-19", "2021-22"}
        assert M._HARVEST_WINDOW_RAW["2018-19"][1:] == ("s16cq10", "s16cq11", "s16cq12a", "s16cq12b")
        assert M._HARVEST_WINDOW_RAW["2021-22"][1:] == ("s16cq11", "s16cq12", "s16cq16a", "s16cq16b")


# --- the oracle: the survey file, read directly (not run when written) --------------

@pytest.mark.requires_s3
class TestOracle:
    """The expected cohort comes from ``eaci17_s7fp2.dta``; no count is pinned."""

    @pytest.fixture(scope="class")
    def served(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            country = ll.Country("Mali")
            df = country.crop_production(waves=[WAVE])
        return country, df.reset_index()

    @pytest.fixture(scope="class")
    def expected(self):
        from lsms_library.local_tools import df_from_orgfile, get_dataframe
        table = df_from_orgfile(str(COUNTRY_DIR / "_" / "categorical_mapping.org"),
                                name="harmonize_food")
        crops = dict(zip(table["Code"].astype(str).str.strip(), table["Preferred Label"]))
        raw = get_dataframe(str(COUNTRY_DIR / WAVE / "Data" / "eaci17_s7fp2.dta"))
        q = pd.to_numeric(raw["s7fq13a"], errors="coerce").astype("float64")
        out = pd.DataFrame({
            "i": ("EACI17_" + raw["grappe"].astype("Int64").astype("string") + "0"
                  + raw["exploitation"].astype("Int64").astype("string").str.zfill(2)).values,
            "plot_id": (raw["s7fq01"].astype("Int64").astype("string") + "_"
                        + raw["s7fq02"].astype("Int64").astype("string")).values,
            "crop": raw["s7fq03"].astype("string").str.strip().map(crops).values,
            "answer": raw["s7fq10"].astype("string").str.strip().values,
            "q": q.mask(q.isin((99, 9999)) | np.isclose(q, 9999.9)).values})
        return out.dropna(subset=["crop"])

    @staticmethod
    def _keys(frame):
        """Sorted (i, plot_id, crop) tuples; a missing part is spelled, so the sort holds."""
        on = ["i", "plot_id", "crop"]
        return sorted(map(tuple, frame[on].astype("string").fillna("<NA>").to_numpy()))

    @staticmethod
    def _keyed(frame):
        return frame["Derivation"].astype("string").str.contains(WINDOW, regex=False).fillna(False)

    def test_the_answer_is_decoded_and_some_harvest_is_unfinished(self, expected):
        assert set(expected["answer"].dropna()) >= {"Oui", "Non"}
        assert expected["answer"].eq("Non").any()

    def test_the_keyed_rows_are_exactly_the_unfinished_recorded_lines(self, served, expected):
        _, df = served
        on = ["i", "plot_id", "crop"]
        want = expected[expected["answer"].eq("Non") & expected["q"].notna()]
        assert not want.duplicated(on).any()
        got = df[self._keyed(df)]
        assert self._keys(got) == self._keys(want)
        fill = lambda f: f.assign(**{k: f[k].astype("string").fillna("<NA>") for k in on})
        both = fill(got).merge(fill(want), on=on, validate="one_to_one")
        assert len(both) == len(got)
        assert both["Quantity"].eq(both["q"]).all()       # as recorded
        # Fewer than the lines answering Non wherever one records no quantity.
        assert len(got) <= int(expected["answer"].eq("Non").sum())

    def test_tree_and_farm_level_rows_carry_none(self, served):
        _, df = served
        off_grid = df["plot_id"].eq(FARM).fillna(False) | df["perennial"].fillna(False).astype(bool)
        assert off_grid.any() and not self._keyed(df[off_grid]).any()

    def test_the_inputs_join_the_keyed_rows(self, served):
        country, df = served
        raw = country.derivation_inputs(WINDOW, wave=WAVE).reset_index()
        got = df[self._keyed(df)]
        assert self._keys(raw) == self._keys(got)
        assert {"s7fq10", "s7fq13a", "s7fq13c"} <= set(raw.columns)

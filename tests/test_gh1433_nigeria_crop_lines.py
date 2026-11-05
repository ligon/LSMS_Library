"""Nigeria ``crop_production``: one row per harvest line (GH #1433).

``(t, i, plot_id, crop)`` is not a harvest line.  ``harmonize_food`` folds several
codes onto one label (Yam--roots is 1120-1124), and the W1-W3 rosters can repeat a
code on a plot.  A keep-first dedup used to discard every line past the first, with
its quantity, its valuation and, in W1/W2, its sale.  The #323 rule applies: name the
line with the canonical ``production_obs`` level, do not select one.

Two kinds of test:

1. **The rule, on synthetic frames** (data-free), loading ``nigeria.py`` by path.
2. **The oracle** (data-gated).  It reads the source files directly and imports
   NOTHING from ``Nigeria/_/``.  It needs a warm Nigeria ``crop_production`` parquet
   and never cold-builds one.
"""
from __future__ import annotations

import importlib.util
import json
import re
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

NIGERIA = countries_root() / "Nigeria"
TABLE = "crop_production"
FARM_LEVEL = "Farm-level"
SENTINEL = "unresolved_observation"


# --------------------------------------------------------------------------
# 1. the rule, on synthetic frames (data-free)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ng():
    spec = importlib.util.spec_from_file_location("nigeria", NIGERIA / "_" / "nigeria.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("nigeria", mod)
    spec.loader.exec_module(mod)
    return mod


LABELS = {1010: "Maize", 1121: "Yam--roots", 1123: "Yam--roots"}


def _frame(rows, **extra):
    """One plot-crop file.  ``rows`` are ``(hhid, plot, code, line, qty, value)``."""
    df = pd.DataFrame(rows, columns=["hhid", "plotid", "cropcode", "cropid", "qty", "val"])
    df["unit"] = 1
    dec = df.copy()
    dec["unit"] = "1. KILOGRAMS"
    return dict(df=df, dec=dec, hhid="hhid", plot="plotid", crop="cropcode", qty="qty",
                unit="unit", value_harvested="val", perennial=False, **extra)


class TestTokens:
    def test_a_roster_line_id_names_the_line(self, ng):
        fr = _frame([("h", "1", 1010, 2, 1.0, 1.0)])
        got = ng._crop_line_observations(fr["df"], "cropcode", line="cropid")
        assert got.tolist() == ["reported:crop_id:2"]

    def test_with_no_line_id_the_code_names_it(self, ng):
        fr = _frame([("h", "1", 1010, 2, 1.0, 1.0)])
        got = ng._crop_line_observations(fr["df"], "cropcode")
        assert got.tolist() == ["reported:crop_code:1010"]

    def test_a_recorded_odd_id_is_carried_as_recorded(self, ng):
        fr = _frame([("h", "1", 1010, -1, 1.0, 1.0), ("h", "1", 1121, 0, 1.0, 1.0)])
        got = ng._crop_line_observations(fr["df"], "cropcode", line="cropid")
        assert got.tolist() == ["reported:crop_id:-1", "reported:crop_id:0"]

    def test_a_missing_id_is_the_row_of_the_file_as_read(self, ng):
        fr = _frame([("h", "1", 1010, 1, 1.0, 1.0), ("h", "2", 1121, np.nan, 1.0, 1.0)])
        got = ng._crop_line_observations(fr["df"], "cropcode", line="cropid",
                                         source_ref="dvc:f.dta:abc")
        assert got.tolist() == ["reported:crop_id:1", "missing-id:row:dvc:f.dta:abc:1"]

    def test_a_constructed_token_needs_a_sidecar(self, ng):
        fr = _frame([("h", "2", 1121, np.nan, 1.0, 1.0)])
        with pytest.raises(ValueError, match="sidecar"):
            ng._crop_line_observations(fr["df"], "cropcode", line="cropid",
                                       source_ref="missing:f.dta")


class TestEveryLineIsARow:
    def test_two_codes_under_one_label_keep_their_own_answers(self, ng):
        out = ng.crop_production_for_wave(
            "2019Q1", [_frame([("h", "1", 1121, 1, 9.0, 90.0), ("h", "1", 1123, 2, 4.0, 40.0)])],
            LABELS)
        assert list(out.index.names) == ["t", "i", "plot_id", "crop", "production_obs"]
        assert out["Quantity"].sort_values().tolist() == [4.0, 9.0]
        assert out["Value_harvested"].sort_values().tolist() == [40.0, 90.0]

    def test_a_repeated_code_on_a_plot_is_two_rows_with_a_line_id(self, ng):
        out = ng.crop_production_for_wave(
            "2011Q1", [_frame([("h", "1", 1010, 1, 9.0, 90.0), ("h", "1", 1010, 2, 4.0, 40.0)],
                              line="cropid")], LABELS)
        assert out.index.get_level_values("production_obs").tolist() == [
            "reported:crop_id:1", "reported:crop_id:2"]

    def test_a_line_the_token_cannot_separate_is_refused_not_selected(self, ng):
        fr = _frame([("h", "1", 1010, 1, 9.0, 90.0), ("h", "1", 1010, 2, 4.0, 40.0)])
        with pytest.raises(ValueError, match="does not identify every harvest line"):
            ng.crop_production_for_wave("2019Q1", [fr], LABELS)

    def test_a_served_line_with_no_token_is_refused(self, ng):
        fr = _frame([("h", "1", 1010, np.nan, 9.0, 90.0)], line="cropid")
        with pytest.raises(ValueError, match="has no line id"):
            ng.crop_production_for_wave("2011Q1", [fr], LABELS)

    def test_an_unlabelled_line_is_still_dropped_before_the_token_is_judged(self, ng):
        fr = _frame([("h", "1", 9999, np.nan, 9.0, 90.0), ("h", "1", 1010, 1, 4.0, 40.0)],
                    line="cropid")
        out = ng.crop_production_for_wave("2011Q1", [fr], LABELS)
        assert out["Quantity"].tolist() == [4.0]


class TestAttachmentNamesTheLine:
    @staticmethod
    def _records(*pairs):
        return pd.DataFrame({
            "i": [p[0] for p in pairs], "crop": [p[1] for p in pairs],
            "Quantity_sold": 2.0, "Value_sold": 100.0, "Unit_sold": "Kg",
            "KgFactor_sold": np.nan, "_unit_key": "/1"})

    @staticmethod
    def _served(*rows):
        return pd.DataFrame(rows, columns=["i", "plot_id", "crop", "production_obs", "Quantity"])

    def test_one_stating_line_is_the_row(self, ng):
        out = ng.derive_hh_crop_sale_attachment(
            self._records(("h", "Yam--roots")),
            self._served(("h", "1", "Yam--roots", "reported:crop_code:1121", np.nan),
                         ("h", "1", "Yam--roots", "reported:crop_code:1123", 4.0)))
        row = out.loc[("h", "Yam--roots")]
        assert row["plot_id"] == "1" and row["production_obs"] == "reported:crop_code:1123"

    def test_two_stating_lines_of_one_plot_are_farm_level(self, ng):
        out = ng.derive_hh_crop_sale_attachment(
            self._records(("h", "Yam--roots")),
            self._served(("h", "1", "Yam--roots", "reported:crop_code:1121", 9.0),
                         ("h", "1", "Yam--roots", "reported:crop_code:1123", 4.0)))
        row = out.loc[("h", "Yam--roots")]
        assert row["plot_id"] == FARM_LEVEL and row["production_obs"] == SENTINEL
        assert row["cause"] == "several-state-harvest"


def test_the_script_names_each_waves_line_id(ng):
    """Read, never imported: the script runs data reads at module level."""
    source = (NIGERIA / "_" / "crop_production.py").read_text()
    sections = re.split(r"^# -+ W\d (\S+) -+$", source, flags=re.M)
    sections = dict(zip(sections[1::2], sections[2::2]))
    for wave in ("2010-11", "2012-13", "2015-16"):
        assert "line='cropid'" in sections[wave], wave
    for wave in ("2018-19", "2023-24"):
        assert "line=" not in sections[wave], wave
    assert "source_ref=source_fingerprint(f)" in sections["2010-11"]
    # derivation_inputs re-derives the served lines, so it must name the same id.
    assert ng._HH_SALE_SOURCE["2015-16"]["line"] == "cropid"
    assert "line" not in ng._HH_SALE_SOURCE["2018-19"]
    assert "line" not in ng._HH_SALE_SOURCE["2023-24"]


def test_the_scheme_declares_the_level_the_builder_serves():
    scheme = load_yaml(NIGERIA / "_" / "data_scheme.yml")["Data Scheme"][TABLE]
    assert scheme["index"] == "(t, i, plot_id, crop, production_obs)"


# --------------------------------------------------------------------------
# 2. the oracle (data-gated): reads the source, imports nothing from Nigeria/_/
# --------------------------------------------------------------------------

#: Each round's plot-crop files: file, crop code column, line id column (None where
#: the file ships none), harvest quantity, valuation, reported factor.
SOURCES = {
    "2011Q1": ("2010-11", [("Data/Post Harvest Wave 1/Agriculture/secta3_harvestw1.dta",
                            "sa3q2", "cropid", "sa3q6a", "sa3q18", None)]),
    "2013Q1": ("2012-13", [("Data/Post Harvest Wave 2/Agriculture/secta3_harvestw2.dta",
                            "cropcode", "cropid", "sa3q6a1", "sa3q18", None)]),
    "2016Q1": ("2015-16", [("Data/secta3i_harvestw3.dta",
                            "cropcode", "cropid", "sa3iq6i", "sa3iq6a", None)]),
    "2019Q1": ("2018-19", [("Data/secta3i_harvestw4.dta",
                            "cropcode", None, "sa3iq6i", "sa3iq6a", "sa3iq6_conv"),
                           ("Data/secta3iii_harvestw4.dta",
                            "cropcode", None, "sa3iiiq13a", "sa3iiiq14", "sa3iiiq13_conv")]),
    "2024Q1": ("2023-24", [("Data/Post Harvest Wave 5/Agriculture/secta3i_harvestw5.dta",
                            "cropcode", None, "sa3iq9a", "sa3iq10", "sa3iq9_conv"),
                           ("Data/Post Harvest Wave 5/Agriculture/secta3iii_harvestw5.dta",
                            "cropcode", None, "sa3iiiq23a", "sa3iiiq24", "sa3iiiq23_conv")]),
}
#: W1/W2 record the sale on the line.
LINE_SALES = {"2011Q1": "sa3q12", "2013Q1": "sa3q12"}
ROUNDS = sorted(SOURCES)


def _labels():
    from lsms_library.local_tools import df_from_orgfile
    table = df_from_orgfile(str(NIGERIA / "_" / "categorical_mapping.org"), name="harmonize_food")
    out = {}
    for code, label in zip(table["Code"], table["Preferred Label"]):
        code = pd.to_numeric(code, errors="coerce")
        if pd.notna(code) and pd.notna(label) and str(label).strip() not in ("", "---"):
            out[int(code)] = str(label).strip()
    return out


def _walk(t, ids):
    mapping = json.loads((NIGERIA / "_" / "updated_ids.json").read_text()).get(t, {})
    return ids.map(lambda x: mapping.get(x, x))


def _lines(t):
    """Every line of a round's plot-crop files with a labelled crop, from the files."""
    from lsms_library.local_tools import format_id, get_dataframe
    labels = _labels()
    wave, files = SOURCES[t]
    parts = []
    for rel, code_col, line_col, qty, value, factor in files:
        d = get_dataframe(str(NIGERIA / wave / rel), convert_categoricals=False)
        code = pd.to_numeric(d[code_col], errors="coerce").astype("Int64")
        if line_col is None:
            token = "reported:crop_code:" + code.astype("string")
        else:
            line = pd.to_numeric(d[line_col], errors="coerce").astype("Int64")
            token = "reported:crop_id:" + line.astype("string")
        conv = (pd.to_numeric(d[factor], errors="coerce") if factor else
                pd.Series(np.nan, index=d.index))
        parts.append(pd.DataFrame({
            "i": d["hhid"].map(format_id).astype("string").values,
            "plot_id": d["plotid"].map(format_id).astype("string").values,
            "crop": code.map(labels).astype("string").values,
            "production_obs": token.values,
            "q": pd.to_numeric(d[qty], errors="coerce").values,
            "val": pd.to_numeric(d[value], errors="coerce").values,
            "kg": conv.where(conv > 0).values,
            "sale": (pd.to_numeric(d[LINE_SALES[t]], errors="coerce").values
                     if t in LINE_SALES else np.nan),
        }))
    rows = pd.concat(parts, ignore_index=True).dropna(subset=["i", "crop"])
    return rows.assign(i=lambda f: _walk(t, f["i"]).values)


@pytest.fixture(scope="module")
def served():
    """The API frame, flat, from a warm parquet.  Never cold-builds."""
    from lsms_library.local_tools import data_root, read_parquet_cache_waves
    path = data_root() / "Nigeria" / "var" / f"{TABLE}.parquet"
    try:
        cached = pd.read_parquet(path)
    except FileNotFoundError:
        pytest.skip("no warm Nigeria crop_production parquet")
    if "production_obs" not in cached.index.names:
        pytest.skip("warm Nigeria crop_production parquet predates GH #1433 (re-warm needed)")
    coverage = read_parquet_cache_waves(path)
    if coverage is not None and set(ROUNDS) - set(coverage):
        pytest.skip(f"partial warm cache: lacks {sorted(set(ROUNDS) - set(coverage))}")
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country("Nigeria", assume_cache_fresh=True).crop_production()
    assert df.index.is_unique
    return df.reset_index()


def _plain(frame):
    """A round's harvest rows, as floats and plain strings."""
    return frame[frame["plot_id"] != FARM_LEVEL]


@pytest.mark.requires_s3
@pytest.mark.parametrize("t", ROUNDS)
class TestEveryLineIsServed:
    def test_one_row_per_labelled_line(self, served, t):
        assert len(_plain(served[served["t"] == t])) == len(_lines(t))

    @pytest.mark.parametrize("ours, theirs", [
        ("Quantity", "q"), ("Value_harvested", "val"), ("KgFactor", "kg")])
    def test_each_answer_is_conserved(self, served, t, ours, theirs):
        rows = _plain(served[served["t"] == t])
        got = pd.to_numeric(rows[ours], errors="coerce").astype("float64")
        want = _lines(t)[theirs]
        assert got.notna().sum() == want.notna().sum()
        assert got.sum() == pytest.approx(want.sum())

    def test_each_named_line_holds_its_own_answers(self, served, t):
        lines = _lines(t).dropna(subset=["production_obs"])
        key = ["i", "plot_id", "crop", "production_obs"]
        want = lines.astype({k: str for k in key}).set_index(key).sort_index()
        rows = _plain(served[served["t"] == t])
        got = rows.astype({k: str for k in key}).set_index(key).sort_index()
        got = got.loc[want.index]
        for ours, theirs in (("Quantity", "q"), ("Value_harvested", "val")):
            a = pd.to_numeric(got[ours], errors="coerce").astype("float64").to_numpy()
            b = want[theirs].to_numpy()
            assert (np.isnan(a) == np.isnan(b)).all(), ours
            assert np.allclose(a[~np.isnan(a)], b[~np.isnan(b)]), ours


@pytest.mark.requires_s3
@pytest.mark.parametrize("t", sorted(LINE_SALES))
def test_w1_w2_sales_recorded_on_a_line_are_all_served(served, t):
    """They used to be discarded with the line: 97 + 66 positive sales."""
    got = pd.to_numeric(served.loc[served["t"] == t, "Value_sold"], errors="coerce")
    want = _lines(t)["sale"]
    assert got.notna().sum() == want.notna().sum()
    assert got.sum() == pytest.approx(want.sum())


@pytest.mark.requires_s3
@pytest.mark.parametrize("t", ["2016Q1", "2019Q1", "2024Q1"])
class TestAttachedSalesNameALine:
    """No sale attached to nothing, and none attached twice."""

    def _sold(self, served, t):
        rows = served[served["t"] == t]
        return rows[rows["Value_sold"].notna() | rows["Quantity_sold"].notna()]

    def test_a_sale_is_on_one_row_of_its_household_and_crop(self, served, t):
        assert not self._sold(served, t).duplicated(["i", "crop"]).any()

    def test_an_attached_row_is_the_one_line_that_states_a_harvest(self, served, t):
        lines = _lines(t)
        stating = lines[lines["q"] > 0].astype({"i": str, "crop": str})
        counts = stating.groupby(["i", "crop"]).size()
        sold = self._sold(served, t).astype({"i": str, "crop": str})
        attached = sold[sold["plot_id"] != FARM_LEVEL]
        assert len(attached) > 0
        assert (attached["production_obs"] != SENTINEL).all()
        assert (pd.to_numeric(attached["Quantity"], errors="coerce") > 0).all()
        pairs = pd.MultiIndex.from_frame(attached[["i", "crop"]])
        assert (counts.reindex(pairs).to_numpy() == 1).all()
        tokens = stating.set_index(["i", "crop"])["production_obs"]
        only = tokens[~tokens.index.duplicated(keep=False)].reindex(pairs)
        assert (only.astype(str).to_numpy() == attached["production_obs"].astype(str).to_numpy()).all()

    def test_a_farm_level_row_answers_no_line(self, served, t):
        lines = _lines(t)
        stating = lines[lines["q"] > 0].astype({"i": str, "crop": str})
        counts = stating.groupby(["i", "crop"]).size()
        rows = served[(served["t"] == t) & (served["plot_id"] == FARM_LEVEL)]
        rows = rows.astype({"i": str, "crop": str})
        assert len(rows) > 0
        assert (rows["production_obs"] == SENTINEL).all()
        pairs = pd.MultiIndex.from_frame(rows[["i", "crop"]])
        assert (counts.reindex(pairs).fillna(0).to_numpy() != 1).all()

"""Ethiopia ``crop_production`` carries the native line axis -- GH #1461.

``(t, i, plot_id, j, u)`` repeated in 639 groups, 1,281 rows, and the framework
collapsed 642 of them to one observed row (29 disagreeing).  Those 642 are two
different things, so the fix is two things:

* 605 are one record listed twice: ``sect9_ph_w2`` ships 605 records identical
  on all 43 columns (adjacent pairs; the §4 roster lists each key once).  The
  builder drops the repeat on the full raw record and warns the count.
* 37 are distinct §9 lines: crop codes ``harmonize_crop`` folds onto one label
  (KALE / CABBAGE / SPINACH -> 'Leafy Greens'), and in W4/W5 one code on two
  roster lines.  The canonical ``production_obs`` level names them:
  ``reported:crop_code:<n>`` in W1-W3, ``reported:crop_id:<n>`` in W4/W5.

With two lines of one crop on one plot served, the §11 / §12 sale rule had to
move with the grain: a (holder, crop) sale attaches only where the crop has ONE
§9 line, so it never sits on two rows.

Two tiers: config and synthetic (no microdata), and ``requires_s3`` tests of the
served table against an oracle that reads §9 directly.
"""
from __future__ import annotations

import importlib.util
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

ROOT = countries_root() / "Ethiopia"
INDEX = ["t", "i", "plot_id", "j", "u", "production_obs"]

#: wave -> (§9 file, household id, crop code, reported harvest, §9 line id)
S9 = {
    "2011-12": ("sect9_ph_w1.dta", "household_id", "crop_code", "ph_s9q12_a", "crop_code"),
    "2013-14": ("sect9_ph_w2.dta", "household_id2", "crop_code", "ph_s9q04_a", "crop_code"),
    "2015-16": ("sect9_ph_w3.dta", "household_id2", "crop_code", "ph_s9q04_a", "crop_code"),
    "2018-19": ("sect9_ph_w4.dta", "household_id", "s9q00b", "s9q05a", "crop_id"),
    "2021-22": ("sect9_ph_w5.dta", "household_id", "s9q00b", "s9q05a", "crop_id"),
}
#: Whole-record repeats each §9 file ships (the file, not the builder).
REPEATS = {"2011-12": 0, "2013-14": 605, "2015-16": 0, "2018-19": 0, "2021-22": 0}
#: Served rows, cold rebuild after GH #1461.  Before: 7,744 / 26,520 / 25,999 /
#: 14,260 / 11,880 = 86,403.  Lines back +2 / +1 / +4 / +10 / +20; W5 -3 rows
#: whose no-unit line carried only a sale that no longer attaches.
ROWS = {"2011-12": 7746, "2013-14": 26521, "2015-16": 26003, "2018-19": 14270,
        "2021-22": 11897}


@pytest.fixture(scope="module")
def eth():
    """Import ``Ethiopia/_/ethiopia.py`` as a module, wherever the tree is."""
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_1461_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the ``!make`` tag used in data_scheme.yml."""


_SchemeLoader.add_constructor("!make", lambda loader, node: {"__make__": True})


def test_data_scheme_declares_the_observation_axis():
    with open(ROOT / "_" / "data_scheme.yml", encoding="utf-8") as f:
        scheme = yaml.load(f, Loader=_SchemeLoader)
    assert scheme["Data Scheme"]["crop_production"]["index"] == "(" + ", ".join(INDEX) + ")"


@pytest.mark.parametrize("wave, names_a_line_id", [
    ("2011-12", False), ("2013-14", False), ("2015-16", False),
    ("2018-19", True), ("2021-22", True)])
def test_only_w4_w5_name_a_line_id(wave, names_a_line_id):
    src = (ROOT / wave / "_" / "crop_production.py").read_text(encoding="utf-8")
    assert ("production_id='crop_id'" in src) is names_a_line_id


# ---------------------------------------------------------------------------
# synthetic: the builder's rules
# ---------------------------------------------------------------------------

W13 = dict(hhid="household_id", holder_id="holder_id", parcel_id="parcel_id",
           field_id="field_id", crop_code="crop_code", quantity="qty",
           s_crop_code="crop_code", s_sold_flag="sold", s_qty="sqty", s_value="sval")
W45 = dict(W13, production_id="crop_id")


def _harvest(rows):
    return pd.DataFrame(rows, columns=["household_id", "holder_id", "parcel_id",
                                       "field_id", "crop_code", "crop_id", "qty"])


def _sale(rows=()):
    return pd.DataFrame(list(rows), columns=["holder_id", "crop_code", "sold", "sqty", "sval"]).assign(household_id=lambda d: "H" + d["holder_id"].astype(str))  # the §11/§12 files name it (GH #1457)


def _units(n, label="1. Kilogram"):
    return pd.Series([label] * n, dtype=object)


def _build(eth, harvest, colmap=W13, sale=None, units=None, wave="2015-16"):
    return eth.crop_production_for_wave(
        wave, harvest, None, _sale() if sale is None else sale, colmap,
        unit_labels=_units(len(harvest)) if units is None else units)


def _obs(df):
    return df.index.get_level_values("production_obs").tolist()


def test_folded_codes_are_two_lines_named_by_their_codes(eth):
    """56 KALE and 69 SPINACH both read 'Leafy Greens'; both are served."""
    df = _build(eth, _harvest([("H", "1", 1, 1, 56, 1, 2.0), ("H", "1", 1, 1, 69, 2, 3.0)]))
    assert list(df.index.names) == INDEX
    assert set(df.index.get_level_values("j")) == {"Leafy Greens"}
    assert _obs(df) == ["reported:crop_code:56", "reported:crop_code:69"]
    assert df["Quantity"].tolist() == [2.0, 3.0]


def test_one_code_on_two_crop_id_lines_is_two_lines(eth):
    df = _build(eth, _harvest([("H", "1", 1, 1, 7, 1, 2.0), ("H", "1", 1, 1, 7, 2, 2.0)]),
                colmap=W45, wave="2018-19")
    assert _obs(df) == ["reported:crop_id:1", "reported:crop_id:2"]
    assert df["Quantity"].tolist() == [2.0, 2.0]    # same answers, distinct lines


def test_a_repeat_record_is_dropped_and_warned_and_units_stay_aligned(eth):
    harvest = _harvest([("H", "1", 1, 1, 7, 1, 2.0), ("H", "1", 1, 1, 7, 1, 2.0),
                        ("H", "1", 1, 2, 8, 1, 3.0)])
    units = pd.Series(["1. Kilogram", "1. Kilogram", "Quintal"], dtype=object)
    with pytest.warns(eth.Sect9RepeatRecordWarning, match="dropped 1 §9 records"):
        df = _build(eth, harvest, units=units)
    assert len(df) == 2
    assert df["Quantity"].tolist() == [2.0, 3.0]
    assert df.index.get_level_values("u").tolist() == ["Kg", "Quintal"]


def test_no_warning_where_nothing_repeats(eth):
    with warnings.catch_warnings():
        warnings.simplefilter("error", eth.Sect9RepeatRecordWarning)
        _build(eth, _harvest([("H", "1", 1, 1, 7, 1, 2.0)]))


def test_a_record_differing_in_any_column_is_not_a_repeat_and_is_refused(eth):
    """``crop_id`` is read by no W1-W3 colmap, yet it makes the records differ.
    Two such lines share a key and a token: a source defect, raised, not reduced."""
    harvest = _harvest([("H", "1", 1, 1, 7, 1, 2.0), ("H", "1", 1, 1, 7, 2, 2.0)])
    with pytest.raises(ValueError, match="does not identify every"):
        _build(eth, harvest)


def test_a_line_with_no_id_is_refused(eth):
    harvest = _harvest([("H", "1", 1, 1, 7, np.nan, 2.0)])
    with pytest.raises(ValueError, match="no line id"):
        _build(eth, harvest, colmap=W45, wave="2018-19")


def test_a_sale_attaches_only_to_a_crops_one_line(eth):
    sale = _sale([("1", 7, 1, 1.0, 10.0),       # one line: attached
                  ("2", 7, 1, 1.0, 20.0),       # two lines of one plot: nowhere
                  ("3", 7, 1, 1.0, 30.0),       # two plots: nowhere
                  ("4", 56, 1, 1.0, 40.0)])     # folded pair: its own code's line
    harvest = _harvest([
        ("A", "1", 1, 1, 7, 1, 5.0),
        ("B", "2", 1, 1, 7, 1, 5.0), ("B", "2", 1, 1, 7, 2, 6.0),
        ("C", "3", 1, 1, 7, 1, 5.0), ("C", "3", 1, 2, 7, 1, 6.0),
        ("D", "4", 1, 1, 56, 1, 5.0), ("D", "4", 1, 1, 69, 2, 6.0)])
    df = _build(eth, harvest, colmap=W45, sale=sale, wave="2018-19").reset_index()
    # Re-pinned for GH #1457: "nowhere" among the §9 rows, and one Farm-level row each.
    farm = df[df["plot_id"] == "Farm-level"]
    assert sorted(farm["Value_sold"]) == [20.0, 30.0] and farm["Quantity"].isna().all()
    df = df[df["plot_id"] != "Farm-level"]
    got = df.groupby("i")["Value_sold"].apply(lambda v: v.dropna().tolist()).to_dict()
    assert got == {"A": [10.0], "B": [], "C": [], "D": [40.0]}
    d = df[df["i"] == "D"].set_index("production_obs")["Value_sold"]
    assert d["reported:crop_id:1"] == 40.0 and pd.isna(d["reported:crop_id:2"])


def test_a_sect12_sale_attaches_only_to_a_crops_one_line(eth):
    """§12 counts lines too.  W1-W3 hold one line per (plot, crop), so held data
    cannot tell lines from plots on this path; only a synthetic frame can."""
    harvest = _harvest([("A", "1", 1, 1, 72, 1, 5.0),
                        ("B", "2", 1, 1, 72, 1, 5.0), ("B", "2", 1, 1, 72, 2, 6.0)])
    sale12 = pd.DataFrame([("1", 72, 1, 1.0, 10.0), ("2", 72, 1, 1.0, 20.0)],
                          columns=["holder_id", "crop_code", "q06", "q07", "q08"]).assign(household_id=lambda d: "H" + d["holder_id"].astype(str))  # the §11/§12 files name it (GH #1457)
    colmap = dict(W45, s12_crop_code="crop_code", s12_sold_flag="q06",
                  s12_qty="q07", s12_value="q08")
    with pytest.warns(eth.Sect12SaleWarning, match="several_s9_plots 1/1/20"):
        df = eth.crop_production_for_wave("2015-16", harvest, None, _sale(), colmap,
                                          unit_labels=_units(3), sale12=sale12)
    # Re-pinned for GH #1457: the sale on two lines has its own Farm-level row.
    flat = df.reset_index()
    assert flat.loc[flat["plot_id"] == "Farm-level", "Value_sold"].tolist() == [20.0]
    got = flat[flat["plot_id"] != "Farm-level"].groupby("i")["Value_sold"].apply(lambda v: v.dropna().tolist())
    assert got.to_dict() == {"A": [10.0], "B": []}


def test_lines_per_holder_crop_counts_lines_not_plots(eth):
    out = pd.DataFrame({"_holder": ["1", "1", "2", "2", "3"],
                        "_code": pd.array([7, 7, 7, 7, pd.NA], dtype="Int64"),
                        "plot_id": ["p", "p", "q", "r", "s"]})
    got = eth._s9_lines_per_holder_crop(out)
    assert got.set_index("_holder")["_nlines"].to_dict() == {"1": 2, "2": 2}


# ---------------------------------------------------------------------------
# the served table, against §9 read directly
# ---------------------------------------------------------------------------

def _raw(wave, fn):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / wave / "Data" / fn), convert_categoricals=False)


def _lines(wave):
    """One row per distinct §9 record: plot_id, token, holder, code, quantity.
    Reads the source and rebuilds the key independently of the builder."""
    from lsms_library.local_tools import format_id

    fn, hh, code, qty, line = S9[wave]
    raw = _raw(wave, fn).drop_duplicates()
    fid = lambda col: raw[col].apply(format_id).astype(str)
    out = pd.DataFrame({
        "i": raw[hh].apply(format_id),
        "holder": fid("holder_id"),
        "plot_id": fid("holder_id") + "_" + fid("parcel_id") + "_" + fid("field_id"),
        "code": pd.to_numeric(raw[code], errors="coerce").astype("Int64"),
        "line": pd.to_numeric(raw[line], errors="coerce").astype("Int64"),
        "qty": pd.to_numeric(raw[qty], errors="coerce"),
    })
    out = out.dropna(subset=["i", "code", "line"])
    out["production_obs"] = f"reported:{line}:" + out["line"].astype("string")
    return out


@pytest.fixture(scope="module")
def served():
    import lsms_library as ll
    from lsms_library.country import grain_reports

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country("Ethiopia").crop_production()
    return df, grain_reports("Ethiopia", "crop_production")


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(S9))
def test_whole_record_repeats_per_file(wave):
    raw = _raw(wave, S9[wave][0])
    assert int(raw.duplicated().sum()) == REPEATS[wave], wave


@pytest.mark.requires_s3
def test_w2_repeats_are_one_record_listed_twice():
    """The multiplicity test behind the drop.  Every repeat directly follows its
    original, no record appears three times, and the §4 roster -- which §9's
    rows are prompted from -- lists each key exactly once."""
    raw = _raw("2013-14", "sect9_ph_w2.dta")
    original = np.flatnonzero(raw.duplicated(keep="last").to_numpy())
    repeat = np.flatnonzero(raw.duplicated(keep="first").to_numpy())
    assert len(repeat) == 605
    assert (repeat - 1 == original).all()
    key = ["holder_id", "parcel_id", "field_id", "crop_code"]
    roster = _raw("2013-14", "sect4_pp_w2.dta")[key].value_counts()
    assert (roster == 1).all()
    repeated = raw.loc[raw.duplicated(), key].drop_duplicates()
    assert (pd.MultiIndex.from_frame(repeated).isin(roster.index)).all()


@pytest.mark.requires_s3
def test_index_is_unique_and_declared(served):
    df, _ = served
    assert df.index.is_unique
    assert sorted(df.index.names) == sorted(INDEX + ["v"])


@pytest.mark.requires_s3
def test_nothing_is_collapsed(served):
    _, reports = served
    assert [r for r in reports if r.get("destroyed") or r.get("dropped")] == []


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(S9))
def test_rows_and_token_kind_per_wave(served, wave):
    df, _ = served
    g = df.xs(wave, level="t")
    # Re-pinned for GH #1457: Farm-level sale rows name their module and holder instead.
    g = g[g.index.get_level_values("plot_id") != "Farm-level"]
    assert len(g) == ROWS[wave], f"{wave}: {len(g)} rows"
    kinds = set(g.index.get_level_values("production_obs").str.rsplit(":", n=1).str[0])
    assert kinds == {f"reported:{S9[wave][4]}"}, kinds


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(S9))
def test_every_reported_quantity_is_served_exactly_once(served, wave):
    """Raw oracle: the harvest of every distinct §9 line, by (plot, line), equals
    the served one.  Fails on a collapsed line and on a line served twice."""
    df, _ = served
    g = df.xs(wave, level="t").reset_index()
    got = g.dropna(subset=["Quantity"]).set_index(["plot_id", "production_obs"])["Quantity"]
    raw = _lines(wave).dropna(subset=["qty"]).set_index(["plot_id", "production_obs"])["qty"]
    assert got.index.is_unique and raw.index.is_unique
    pd.testing.assert_series_equal(
        got.astype(float).sort_index(), raw.astype(float).sort_index(),
        check_names=False, check_index_type=False)


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(S9))
def test_a_household_crop_sale_sits_on_one_row_of_a_one_line_crop(served, wave):
    """Raw oracle: every served sale belongs to a (holder, crop) with exactly one
    SERVED §9 line (GH #1457), and no (holder, crop) carries a sale on two rows."""
    df, _ = served
    lines = _lines(wave)
    to_pair = lines.set_index(["plot_id", "production_obs"])[["holder", "code"]]
    g = df.xs(wave, level="t").reset_index()
    g = g[g["plot_id"] != "Farm-level"]    # GH #1457: every other sale has its own row
    rows = g.join(to_pair, on=["plot_id", "production_obs"])
    nlines = rows.groupby(["holder", "code"]).size()
    sold = rows[rows["Value_sold"].notna()]
    assert sold["code"].notna().all()
    per_pair = sold.groupby(["holder", "code"]).size()
    assert per_pair.max() == 1, wave
    assert (nlines.reindex(per_pair.index) == 1).all(), wave

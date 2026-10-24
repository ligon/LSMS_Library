"""GH #1120 D1 / D2 / D3 and GH #1119 for Uganda (@ligon rulings, 2026-10-23).

* D1 -- ``sample`` takes a household the survey recorded in ANOTHER cover from
  that cover, with that cover's weight (2015-16: AGSEC1's ``parish_code`` and
  ``hwgt_W5``).
* D2 -- a household that no file locates gets a SINGLETON ``v`` of its own,
  ``unlocated-{hhid}`` -- Nigeria's one-cluster-per-household convention
  (``moved-{hhid}``), spelled for a household not known to have moved --
  never NaN and never a shared sentinel.
* D3 -- a plot-grain row whose plot id is blank in the source gets
  ``plot_id = 'unknown_plot'``.
* #1119 -- the not-crop-specific ``plot_inputs`` ``j`` marker is
  ``(not crop-specific)``, not ``n/a``.

Two families.  The synthetic tests need no data and run everywhere; every one
fails on the base branch (``fix/plot-sentinel-vocab-20261023``).  The
data-gated tests build ``Country('Uganda')`` and skip only when microdata are
unavailable.

PRIVACY: this repository is public.  No assertion here prints a household id,
a plot id or a cluster id on failure -- every message reports counts only.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture(scope="module")
def uganda_module():
    """The country module, loaded by PATH under a private name: a bare
    ``import uganda`` resolves to any directory called ``uganda`` on
    ``sys.path`` first (a checkout or worktree of that name, as a namespace
    package)."""
    path = countries_root() / "Uganda" / "_" / "uganda.py"
    spec = importlib.util.spec_from_file_location("_uganda_gh1120", path)
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(path.parent))
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# D2: the singleton
# ---------------------------------------------------------------------------

def test_singleton_v_follows_nigerias_convention(uganda_module):
    from lsms_library.local_tools import format_id
    u = uganda_module
    got = u.singleton_v("1013000101")
    assert got == "unlocated-1013000101"
    # The v-join maps v through format_id; it must leave the singleton alone.
    assert format_id(got) == got
    # A float-rendered id is canonicalised first, as Nigeria's cluster_id does.
    assert u.singleton_v(1013000101.0) == "unlocated-1013000101"
    assert u.singleton_v(None) is None
    assert u.singleton_v(np.nan) is None


def _sample_frame(rows):
    df = pd.DataFrame(rows, columns=["t", "i", "v", "weight", "Region"])
    return df.set_index(["t", "i"])


def test_sample_hook_gives_every_unlocated_household_its_own_v(uganda_module):
    u = uganda_module
    df = _sample_frame([
        ("2019-20", "A", "KAMPALA/X", 1.0, "Central"),
        ("2019-20", "B", None, 2.0, "Central"),      # blank cluster field
        ("2019-20", "C", "", 3.0, "Eastern"),        # blank string
        ("2019-20", "D", np.nan, np.nan, np.nan),    # frame extension only
    ])
    out = u.sample(df)
    assert list(out.index.names) == ["t", "i"]
    v = out["v"]
    n_na = int(v.isna().sum())
    assert n_na == 0, f"{n_na} household(s) still have no v"
    assert v.loc[("2019-20", "A")] == "KAMPALA/X"
    singles = [v.loc[("2019-20", h)] for h in ("B", "C", "D")]
    assert singles == ["unlocated-B", "unlocated-C", "unlocated-D"]
    # Never a SHARED sentinel: one cluster per household.
    n_distinct = len(set(singles))
    assert n_distinct == 3, f"{n_distinct} != 3"
    # Nothing else is touched.
    assert out.loc[("2019-20", "B"), "weight"] == 2.0
    assert pd.isna(out.loc[("2019-20", "D"), "weight"])


def test_sample_hook_collapses_frame_extension_repeats(uganda_module):
    """A plot-grain extension sub-df repeats a household; the repeats are
    identical once the marker is dropped and collapse to one row."""
    u = uganda_module
    df = pd.DataFrame({
        "t": ["2009-10"] * 5,
        "i": ["A", "A", "A", "E", "E"],
        "v": ["10110001", "10110001", "10110001", None, None],
        "weight": [5.0, 5.0, 5.0, np.nan, np.nan],
        "_frame": ["A", "A", np.nan, "E", "E"],
    }).set_index(["t", "i"])
    out = u.sample(df)
    n = len(out)
    assert n == 2, f"{n} != 2"
    assert "_frame" not in out.columns
    assert out.loc[("2009-10", "E"), "v"] == "unlocated-E"
    assert out.loc[("2009-10", "A"), "v"] == "10110001"


def test_sample_hook_refuses_repeats_that_disagree(uganda_module):
    u = uganda_module
    df = pd.DataFrame({
        "t": ["2009-10", "2009-10"], "i": ["A", "A"],
        "v": ["1", "2"], "weight": [5.0, 5.0],
    }).set_index(["t", "i"])
    with pytest.raises(ValueError, match="DIFFERENT values"):
        u.sample(df)


def test_alternate_cover_fills_only_households_the_cover_lacks(uganda_module):
    """D1: AGSEC1's columns reach a household gsec1 does not have, and ONLY
    that household -- a gsec1 household keeps its own (even missing) weight."""
    u = uganda_module
    df = pd.DataFrame({
        "t": ["2015-16"] * 3,
        "i": ["G1", "G2", "AG"],
        "v": ["12345678", "12345679", np.nan],
        "weight": [10.0, np.nan, np.nan],          # G2: gsec1 weight missing
        "panel_weight": [8.0, 0.0, np.nan],
        "Region": ["Central", "Eastern", np.nan],
        "_cover": ["G1", "G2", np.nan],
        "_ag_v": [12345678.0, 12345679.0, 23456789.0],   # floated by the merge
        "_ag_weight": [99.0, 77.0, 55.0],
        "_ag_panel_weight": [99.0, 77.0, 55.0],
        "_ag_Region": ["Central", "Eastern", "Western"],
    }).set_index(["t", "i"])
    # As the merge delivers them: a converted Stata label is Categorical.
    df["Region"] = df["Region"].astype("category")
    out = u.sample(df)
    temps = [c for c in out.columns if c.startswith("_")]
    assert temps == [], f"{len(temps)} temporary column(s) left"
    # A weight must stay numeric: an object column is written to the cached
    # parquet as STRING (measured on the first branch build).
    for col in ("weight", "panel_weight"):
        assert pd.api.types.is_numeric_dtype(out[col].dtype), out[col].dtype
    ag = out.loc[("2015-16", "AG")]
    assert ag["v"] == "23456789"          # formatted, not '23456789.0'
    assert ag["weight"] == 55.0 and ag["panel_weight"] == 55.0
    assert ag["Region"] == "Western"
    g2 = out.loc[("2015-16", "G2")]
    assert pd.isna(g2["weight"]), "a gsec1 gap was filled from AGSEC1"
    assert g2["panel_weight"] == 0.0
    assert out.loc[("2015-16", "G1"), "weight"] == 10.0


def test_alternate_cover_district_is_canonicalised_like_the_cover(uganda_module):
    """An unlabelled district code reaches the alternate cover as a number and
    the cover (through ``uganda.District`` = ``format_id``) as a string; the
    adopted value must be the cover's form, or ``reduce_to_agreed`` blanks a
    District the households agree on (measured: 2015-16 '229')."""
    u = uganda_module
    df = pd.DataFrame({
        "t": ["2015-16"] * 3, "i": ["G1", "AG", "AG2"],
        "v": ["12345678", np.nan, np.nan], "District": ["229", np.nan, np.nan],
        "Region": ["Northern", np.nan, np.nan], "_cover": ["G1", np.nan, np.nan],
        "_ag_v": [12345678.0, 12345678.0, 23456789.0],
        "_ag_District": [229, 229, 229],
        "_ag_Region": ["Northern", "Northern", "Northern"],
    }).set_index(["t", "v"])
    out = u.cluster_features(df)
    temps = [c for c in out.columns if str(c).startswith("_")]
    assert temps == [], f"{len(temps)} temporary column(s) left"
    # AG joins the gsec1 parish without blanking its District; AG2's parish
    # has no gsec1 household and becomes a cluster row from AGSEC1 alone.
    got = {v: out.loc[("2015-16", v), "District"]
           for v in out.index.get_level_values("v")}
    assert got == {"12345678": "229", "23456789": "229"}, got


def test_cluster_features_hook_singletons_cover_rows_only(uganda_module):
    """A cover household with no cluster id becomes its own cluster row; a
    geovar row with no cover attribute at all is still dropped (2005-06's 368
    split-offs from the 2009-10 geovar file)."""
    u = uganda_module
    df = pd.DataFrame({
        "t": ["2019-20"] * 4,
        "i": ["A", "B", "C", "Z"],
        "v": ["D1/P1", "D1/P1", None, None],
        "Region": ["Central", "Central", "Eastern", np.nan],
        "Rural": ["Rural", "Rural", "Urban", np.nan],
        "District": ["D1", "D1", np.nan, np.nan],
        "Latitude": [0.1, 0.1, np.nan, 1.5],
        "Longitude": [32.0, 32.0, np.nan, 33.0],
    }).set_index(["t", "i", "v"])
    with pytest.warns(UserWarning, match="dropping 1 household"):
        out = u.cluster_features(df)
    keys = set(out.index.get_level_values("v"))
    assert keys == {"D1/P1", "unlocated-C"}
    assert out.loc[("2019-20", "unlocated-C"), "Region"] == "Eastern"


# ---------------------------------------------------------------------------
# D3: unknown_plot
# ---------------------------------------------------------------------------

def test_plot_key_sentinel(uganda_module):
    u = uganda_module
    hh = pd.Series(["H", "H", "H", "H"])
    parcel = pd.Series(["1", None, "2", None], dtype=object)
    plot = pd.Series(["1", "1", None, None], dtype=object)
    got = u._plot_key(hh, parcel, plot).tolist()
    assert got == ["H-1-1", "unknown_plot", "unknown_plot", "unknown_plot"]
    assert u.UNKNOWN_PLOT == "unknown_plot"


def test_plot_inputs_keeps_plotless_rows_and_marks_not_crop_specific(
        uganda_module, monkeypatch):
    u = uganda_module
    # The builder reads its label tables cwd-relative, as the wave scripts run.
    monkeypatch.chdir(countries_root() / "Uganda" / "_")
    cm = u.INPUT_COLMAPS["2011-12"]
    a = cm["A"]["inputs"]
    org = a["organic"]
    df3a = pd.DataFrame({
        a["hhid"]: ["3013000101", "3013000101", "3013000101"],
        a["parcel"]: [1.0, np.nan, 2.0],
        a["plot"]: [1.0, 2.0, np.nan],
        org["used"]: [1.0, 1.0, 1.0],
        org["qty"]: [10.0, 4.0, 6.0],
    })
    out = u.plot_inputs_for_wave("2011-12", df3a, None, None, cm)
    flat = out.reset_index()
    n_na = int(flat["plot_id"].isna().sum())
    assert n_na == 0, f"{n_na} NaN plot_id"
    n_unknown = int((flat["plot_id"] == "unknown_plot").sum())
    # The two plot-less rows share (t, i, unknown_plot, input, j, season) and
    # are summed by the builder's de-dup step: 4 + 6.  Before D3 they were
    # DELETED by its groupby(dropna=True).
    assert n_unknown == 1, f"{n_unknown} != 1"
    q = float(flat.loc[flat["plot_id"] == "unknown_plot", "Quantity"].iloc[0])
    assert q == 10.0
    # #1119: the marker, not 'n/a'.
    js = set(flat["j"])
    assert js == {"(not crop-specific)"}, f"{len(js)} j value(s)"


def test_seed_rows_never_carry_the_not_crop_specific_marker(uganda_module,
                                                          monkeypatch):
    """GH #1119 follow-up: a seed is crop-specific.  790 is Watermelon (the
    2013-14 / 2015-16 AGSEC4A value label; now in harmonize_crop); an
    unmapped recorded code passes through as itself (Mali / Niger); code 0
    records no crop and is the epistemic 'unknown_crop'."""
    u = uganda_module
    monkeypatch.chdir(countries_root() / "Uganda" / "_")
    sc = u.INPUT_COLMAPS["2013-14"]["seed"]
    df4a = pd.DataFrame({
        sc["hhid"]: ["H00110-04-01"] * 4,
        sc["parcel"]: [1.0, 1.0, 1.0, 1.0],
        sc["plot"]: [1.0, 1.0, 1.0, 1.0],
        sc["crop"]: [130.0, 790.0, 0.0, 777.0],
        sc["qty"]: [5.0, 2.0, 40.0, 1.0],
        sc["unit"]: [1.0, 1.0, 1.0, 1.0],
        sc["seed_type"]: [1.0, 1.0, 1.0, 2.0],
    })
    out = u.plot_inputs_for_wave("2013-14", None, None, df4a,
                                 u.INPUT_COLMAPS["2013-14"])
    js = sorted(out.index.get_level_values("j"))
    assert js == ["777", "Maize", "Watermelon", "unknown_crop"], js


def test_plot_labor_plotless_rows_carry_unknown_plot(uganda_module):
    u = uganda_module
    cm = u.LABOR_COLMAPS["2013-14"]["A"]
    df3a = pd.DataFrame({
        cm["hhid"]: ["H00110-04-01", "H00110-04-01"],
        cm["parcel"]: [1.0, np.nan],
        cm["plot"]: [1.0, 1.0],
        cm["family"][0]: [3.0, 2.0],
        cm["hired_flag"]: [2.0, 2.0],
    })
    out = u.plot_labor_for_wave("2013-14", df3a, None, u.LABOR_COLMAPS["2013-14"])
    plots = set(out.index.get_level_values("plot_id"))
    n_na = sum(pd.isna(p) for p in plots)
    assert n_na == 0, f"{n_na} NaN plot_id"
    assert "unknown_plot" in plots


# ---------------------------------------------------------------------------
# Data-gated: the delivered tables
# ---------------------------------------------------------------------------

# Whole-wave i-format mismatches between a table and `sample` -- NOT
# unlocated households, and out of #1120's scope (CONTENTS.org records them).
_ID_MISMATCH_CELLS = {("housing", "2015-16"), ("nonfood_expenditures", "2009-10"),
                      ("nonfood_expenditures", "2010-11")}

_V_JOINED = ("anthropometry", "crop_production", "earnings", "enterprise_income",
             "food_acquired", "household_roster", "housing",
             "individual_education", "interview_date", "months_food_inadequate",
             "nonfood_expenditures", "nutrition", "people_last7days",
             "plot_features", "plot_inputs", "plot_labor")


@pytest.fixture(scope="module")
def uga():
    _conftest = importlib.util.module_from_spec(
        importlib.util.spec_from_file_location(
            "_shared_conftest_1120", Path(__file__).with_name("conftest.py")))
    _conftest.__loader__.exec_module(_conftest)
    if not _conftest.aws_creds_available():
        pytest.skip("Uganda microdata unavailable (no S3 credentials)")
    import lsms_library as ll
    return ll.Country("Uganda")


@pytest.fixture(scope="module")
def uga_sample(uga):
    return uga.sample()


@pytest.mark.requires_s3
def test_sample_has_no_missing_v(uga_sample):
    n = int(uga_sample["v"].isna().sum())
    assert n == 0, f"{n} sample rows with no v"


@pytest.mark.requires_s3
def test_sample_singletons_are_one_household_each(uga_sample):
    flat = uga_sample.reset_index()
    single = flat[flat["v"].astype(str).str.startswith("unlocated-")]
    per = single.groupby(["t", "v"]).size()
    n_shared = int((per > 1).sum())
    assert n_shared == 0, f"{n_shared} singleton v shared by >1 household"
    by_wave = single.groupby("t").size().to_dict()
    # 2009-10: 24 'no comm, no GPS' + 3 season-B-only + 1 GSEC12-only;
    # 2011-12: 57 AGSEC1-only; 2013-14 / 2019-20: one blank cluster field
    # each; 2018-19: 66 GSEC2/AGSEC1-only.  2015-16 has none: D1 locates
    # all 72 AGSEC1-only households.
    want = {"2009-10": 28, "2011-12": 57, "2013-14": 1, "2018-19": 66,
            "2019-20": 1}
    assert by_wave == want, f"{by_wave} != {want}"


@pytest.mark.requires_s3
def test_2015_16_agsec1_households_are_located(uga_sample):
    flat = uga_sample.reset_index()
    w = flat[flat["t"] == "2015-16"]
    n = len(w)
    assert n == 3305 + 72, f"{n} != {3305 + 72}"
    n_moved = int(w["v"].astype(str).str.startswith("unlocated-").sum())
    assert n_moved == 0, f"{n_moved} singleton(s) in 2015-16"


@pytest.mark.requires_s3
@pytest.mark.parametrize("table", _V_JOINED)
def test_every_served_household_is_in_sample(uga, uga_sample, table):
    df = getattr(uga, table)().reset_index()
    keys = set(zip(uga_sample.reset_index()["t"].astype(str),
                   uga_sample.reset_index()["i"].astype(str)))
    df = df[[(table, str(t)) not in _ID_MISMATCH_CELLS for t in df["t"]]]
    served = set(zip(df["t"].astype(str), df["i"].astype(str)))
    n_absent = len(served - keys)
    assert n_absent == 0, f"{table}: {n_absent} served household(s) absent from sample"
    n_nan_v = int(df["v"].isna().sum()) if "v" in df.columns else 0
    assert n_nan_v == 0, f"{table}: {n_nan_v} row(s) with no v"


@pytest.mark.requires_s3
@pytest.mark.parametrize("table", ("plot_labor", "plot_inputs", "crop_production"))
def test_plot_tables_have_no_null_plot_or_v(uga, table):
    df = getattr(uga, table)()
    for level in ("plot_id", "v"):
        n = int(pd.Series(df.index.get_level_values(level)).isna().sum())
        assert n == 0, f"{table}: {n} NaN {level}"


@pytest.mark.requires_s3
def test_plot_inputs_marker_is_canonical(uga):
    pi = uga.plot_inputs()
    j = pd.Series(pi.index.get_level_values("j"))
    n_old = int((j == "n/a").sum())
    assert n_old == 0, f"{n_old} rows still carry 'n/a'"
    n_new = int((j == "(not crop-specific)").sum())
    assert n_new > 0
    # A seed is crop-specific: no seed row carries the marker (GH #1119
    # follow-up; 6 did before 790 Watermelon and 'unknown_crop').
    inp = pd.Series(pi.index.get_level_values("input"))
    n_seed = int(((inp == "Seed") & (j == "(not crop-specific)")).sum())
    assert n_seed == 0, f"{n_seed} seed rows carry the marker"
    n_unknown = int((j == "unknown_crop").sum())
    assert n_unknown == 1, f"{n_unknown} != 1 unknown_crop seed rows"

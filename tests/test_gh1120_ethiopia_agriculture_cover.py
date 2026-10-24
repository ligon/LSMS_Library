"""Ethiopia ``sample()``: households recorded only in the agriculture cover (GH #1120 D1).

Ruling (@ligon, 2026-10-23): ``sample()`` takes the households the survey
recorded in another cover from that cover, with that cover's weight.  For
Ethiopia that is the post-planting agriculture cover ``sect_cover_pp_w{N}``:
25 / 27 / 124 / 40 households in W2-W5 (none in W1) are there, with an EA and a
positive weight, and in neither the household cover nor the roster.  Before
this, every plot-table row of theirs was served with ``v`` = NaN.

The panel crosswalk is a committed SOURCE (``_/panel_ids.json`` /
``_/updated_ids.json``).  It used to be built from the household covers only,
so these households were never walked; on the evidence below it now walks
them by the same two rules (W2 household_id2 -> household_id; W3 identity,
chained through W2).

Privacy: assertion messages carry counts only, never an id.

Tiers: synthetic / static / JSON tests need no data; ``requires_s3`` tests
read the raw covers or build the tables.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

ROOT = countries_root() / "Ethiopia"
WAVES = {"2011-12": 1, "2013-14": 2, "2015-16": 3, "2018-19": 4, "2021-22": 5}
HHID = {1: "household_id", 2: "household_id2", 3: "household_id2",
        4: "household_id", 5: "household_id"}
WEIGHT = {1: "pw", 2: "pw2", 3: "pw_w3", 4: "pw_w4", 5: "pw_w5"}
# households in the agriculture cover and not in the household cover
AG_ONLY = {"2011-12": 0, "2013-14": 25, "2015-16": 27, "2018-19": 124, "2021-22": 40}
HH_COVER = {"2011-12": 3969, "2013-14": 5262, "2015-16": 4954, "2018-19": 6770,
            "2021-22": 4959}


@pytest.fixture(scope="module")
def eth():
    path = ROOT / "_" / "ethiopia.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_1120_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# Synthetic: the helper's rules
# ---------------------------------------------------------------------------

RURAL_MAP = {"Rural": "Rural", "Small town (urban)": "Urban"}
COLMAP = dict(hhid="hid", ea="ea", weight="pw", rural="rural")


def _covers():
    """Household cover (codes + labels, aligned) and an agriculture cover.

    HA, HB are in the household cover; HC (two holder rows) and HD are only in
    the agriculture cover, which labels nothing -- it carries codes."""
    hh = pd.DataFrame({"hid": ["HA", "HB"], "ea": ["E1", "E2"],
                       "pw": [10.0, 20.0], "saq01": [3.0, 7.0],
                       "rural": [1.0, 2.0]})
    labels = hh.copy()
    labels["saq01"] = pd.Categorical(["Amhara", "SNNP"])
    labels["rural"] = pd.Categorical(["Rural", "Small town (urban)"])
    pp = pd.DataFrame({"hid": ["HA", "HC", "HC", "HD"],
                       "ea": ["E1", "E3", "E3", "E2"],
                       "pw": [10.0, 30.0, 30.0, 40.0],
                       "saq01": [3.0, 7.0, 7.0, 3.0],
                       "rural": [1.0, 2.0, 2.0, 1.0]})
    return hh, labels, pp


def _sample(labels):
    s = pd.DataFrame({"i": labels["hid"], "v": labels["ea"],
                      "weight": labels["pw"],
                      "Rural": labels["rural"].astype(str).map(RURAL_MAP)})
    s["strata"] = (labels["saq01"].astype(str).str.strip() + " "
                   + s["Rural"].fillna("")).str.strip()
    s["panel_weight"] = s["weight"]
    s["t"] = "2013-14"
    return s


def test_agriculture_only_households_enter_with_that_covers_weight(eth):
    hh, labels, pp = _covers()
    base = _sample(labels)
    out = eth.with_agriculture_cover_households(base, "2013-14", hh, labels, pp,
                                                COLMAP, RURAL_MAP)
    n = int(len(out))
    assert n == 4, n                                   # HC once (holder rows), HD
    pd.testing.assert_frame_equal(out.iloc[:2], base)  # existing rows untouched
    add = out.iloc[2:].set_index("i")
    assert add.loc["HC", "weight"] == 30.0 and add.loc["HD", "weight"] == 40.0
    assert (add["panel_weight"] == add["weight"]).all()
    assert add.loc["HC", "v"] == "E3" and add.loc["HD", "v"] == "E2"
    # decoded by CODE through the household cover's labels
    assert add.loc["HC", "strata"] == "SNNP Urban"
    assert add.loc["HD", "strata"] == "Amhara Rural"
    assert add.loc["HC", "Rural"] == "Urban"
    assert list(out.columns) == list(base.columns)


def test_nothing_to_add_returns_the_frame_itself(eth):
    hh, labels, pp = _covers()
    base = _sample(labels)
    out = eth.with_agriculture_cover_households(base, "2013-14", hh, labels,
                                                pp[pp["hid"] == "HA"], COLMAP, RURAL_MAP)
    assert out is base


def test_holder_rows_that_disagree_are_refused(eth):
    hh, labels, pp = _covers()
    pp.loc[2, "pw"] = 31.0
    with pytest.raises(ValueError, match="more than one"):
        eth.with_agriculture_cover_households(_sample(labels), "2013-14", hh,
                                              labels, pp, COLMAP, RURAL_MAP)


def test_a_code_the_household_cover_never_uses_is_refused(eth):
    hh, labels, pp = _covers()
    pp.loc[3, "saq01"] = 15.0
    with pytest.raises(ValueError, match="never uses"):
        eth.with_agriculture_cover_households(_sample(labels), "2013-14", hh,
                                              labels, pp, COLMAP, RURAL_MAP)


def test_the_crosswalk_refuses_an_ambiguous_or_absent_ea(eth):
    a = pd.DataFrame({"ea_id": ["e1", "e2", "e2", "e3"],
                      "ea_id2": ["E1", "E2a", "E2b", "E3"]})
    b = pd.DataFrame({"ea_id": ["e3", "e4", ""], "ea_id2": ["E3", "E4", "E5"]})
    xw = eth.ea_id2_crosswalk(a, b)
    assert xw == {"e1": "E1", "e3": "E3", "e4": "E4"}      # e2 ambiguous: left out
    hh, labels, pp = _covers()
    pp["ea"] = ["e1", "e2", "e2", "e4"]                    # HC's EA is ambiguous
    with pytest.raises(ValueError, match="crosswalk"):
        eth.with_agriculture_cover_households(_sample(labels), "2015-16", hh, labels,
                                              pp, COLMAP, RURAL_MAP, ea_crosswalk=xw)


# ---------------------------------------------------------------------------
# Synthetic: the cluster_features hook (GH #1120 D1 -- one v keyspace)
# ---------------------------------------------------------------------------

def _cf_files():
    """Household cover HA (EA E1), HB (EA E2); agriculture cover adds HC (a new
    EA E3, two holder rows), HD (existing EA E2) and HE (a new EA E4, no fix)."""
    hh = pd.DataFrame({"hid": ["HA", "HB"], "ea": ["E1", "E2"], "saq01": [3.0, 7.0],
                       "saq02": [1.0, 2.0], "rural": [1.0, 2.0]})
    pp = pd.DataFrame({"hid": ["HA", "HC", "HC", "HD", "HE"],
                       "ea": ["E1", "E3", "E3", "E2", "E4"],
                       "saq01": [3.0, 7.0, 7.0, 7.0, 3.0], "saq02": [1.0, 2.0, 2.0, 2.0, 1.0],
                       "rural": [1.0, 2.0, 2.0, 2.0, 1.0]})
    geo = pd.DataFrame({"hid": ["HA", "HB", "HC", "HD"], "lat": [10.0, 11.0, 12.0, 11.5],
                        "lon": [40.0, 41.0, 42.0, 41.5]})
    return {"hh.dta": hh, "pp.dta": pp, "geo.dta": geo}


def _cf_frame():
    """The YAML frame the hook receives: household grain, (t, v, i)."""
    idx = pd.MultiIndex.from_tuples([("T", "E1", "HA"), ("T", "E2", "HB")],
                                    names=["t", "v", "i"])
    return pd.DataFrame({"Region": ["Amhara", "SNNP"], "District": [1, 2],
                         "Rural": ["Rural", "Urban"], "Latitude": [10.0, 11.0],
                         "Longitude": [40.0, 41.0]}, index=idx)


@pytest.fixture()
def cf(eth, monkeypatch):
    files = _cf_files()
    monkeypatch.setattr(eth, "get_dataframe",
                        lambda path, **kw: files[str(path).rsplit("/", 1)[-1]].copy())
    monkeypatch.setitem(eth.CLUSTER_AGRICULTURE_SOURCES, "T",
                        dict(cover="hh.dta", agri="pp.dta", hhid="hid", ea="ea",
                             rural="rural", geo=("geo.dta", "lat", "lon")))
    return files


def test_cluster_features_gains_the_agriculture_only_eas(eth, cf):
    base = _cf_frame()
    out = eth.cluster_features(base.copy())
    pd.testing.assert_frame_equal(out.iloc[:2], base)      # household rows untouched
    add = out.iloc[2:]
    got = sorted(add.index.get_level_values("v"))
    assert got == ["E3", "E4"], got                          # new EAs only, HC once
    hc = add.xs("HC", level="i").iloc[0]
    # decoded by code through the household rows' own served values
    assert (hc["Region"], hc["District"], hc["Rural"]) == ("SNNP", 2, "Urban")
    assert hc["Latitude"] == 12.0 and hc["Longitude"] == 42.0
    he = add.xs("HE", level="i").iloc[0]
    assert pd.isna(he["Latitude"])                           # not in the geo file
    assert (he["Region"], he["Rural"]) == ("Amhara", "Rural")


def test_an_existing_ea_keeps_only_its_household_cover_rows(eth, cf):
    out = eth.cluster_features(_cf_frame())
    hd = int((out.index.get_level_values("i") == "HD").sum())
    assert hd == 0, hd


def test_a_code_no_household_row_uses_is_refused(eth, cf):
    cf["pp.dta"].loc[4, "saq01"] = 15.0
    with pytest.raises(ValueError, match="never uses"):
        eth.cluster_features(_cf_frame())


def test_an_unknown_wave_is_left_alone(eth, cf):
    f = _cf_frame().rename(index={"T": "X"}, level="t")
    assert eth.cluster_features(f) is f


@pytest.mark.parametrize("wave", sorted(WAVES))
def test_the_hook_reads_the_files_the_yaml_reads(eth, wave):
    """The hook's map must not drift from the YAML block it extends."""
    import yaml
    block = yaml.safe_load((ROOT / wave / "_" / "data_info.yml").read_text())["cluster_features"]
    spec = eth.CLUSTER_AGRICULTURE_SOURCES[wave]
    main, geo = block["df_main"], block["df_geo"]
    assert spec["cover"] == main["file"]
    assert (spec["ea"], spec["hhid"]) == (main["idxvars"]["v"], main["idxvars"]["i"])
    assert spec["rural"] == main["myvars"]["Rural"][0]
    assert spec["geo"] == (geo["file"], geo["myvars"]["Latitude"], geo["myvars"]["Longitude"])
    assert spec["agri"] == f"sect_cover_pp_w{WAVES[wave]}.dta"


# ---------------------------------------------------------------------------
# Static: every wave applies the rule; the crosswalk script reads both covers
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("wave,n", sorted(WAVES.items()))
def test_every_wave_sample_script_reads_the_agriculture_cover(wave, n):
    src = (ROOT / wave / "_" / "sample.py").read_text()
    assert f"sect_cover_pp_w{n}.dta" in src
    assert "with_agriculture_cover_households(" in src


def test_panel_ids_script_reads_both_agriculture_covers():
    src = (ROOT / "_" / "panel_ids.py").read_text()
    assert "sect_cover_pp_w2.dta" in src and "sect_cover_pp_w3.dta" in src


def test_the_committed_crosswalk_carries_the_agriculture_households():
    """Counts only.  W2 +25 linked; W3 +27 (26 linked, 1 identity) and 12 W3
    household-cover entries moved from identity to their W1 baseline."""
    u = json.loads((ROOT / "_" / "updated_ids.json").read_text())
    p = json.loads((ROOT / "_" / "panel_ids.json").read_text())
    n2, n3, np_ = len(u["2013-14"]), len(u["2015-16"]), len(p)
    assert (n2, n3, np_) == (3801, 4981, 4981), (n2, n3, np_)
    # identity (unlinked) W3 entries: 1,315 before; -12 moved to their W1
    # baseline, +1 agriculture-only household with no W2 record of any kind
    ident3 = sum(1 for k, v in u["2015-16"].items() if k == v)
    assert ident3 == 1304, ident3
    for w in ("2013-14", "2015-16"):
        vals = [v for k, v in u[w].items() if k != v]
        dup = len(vals) - len(set(vals))
        assert dup == 0, (w, dup)


# ---------------------------------------------------------------------------
# Raw sources: the evidence behind the rule and the link
# ---------------------------------------------------------------------------

def _raw(wave, fn):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(ROOT / wave / "Data" / fn), convert_categoricals=False)


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave,n", sorted(WAVES.items()))
def test_raw_agriculture_only_counts_and_the_weight_is_the_household_weight(wave, n):
    from lsms_library.local_tools import format_id
    hh = _raw(wave, f"sect_cover_hh_w{n}.dta")
    pp = _raw(wave, f"sect_cover_pp_w{n}.dta")
    a = pd.DataFrame({"i": hh[HHID[n]].apply(format_id),
                      "w": pd.to_numeric(hh[WEIGHT[n]])}).drop_duplicates("i")
    b = pd.DataFrame({"i": pp[HHID[n]].apply(format_id),
                      "w": pd.to_numeric(pp[WEIGHT[n]])}).drop_duplicates("i")
    ag = b[~b["i"].isin(set(a["i"]))]
    n_ag = int(len(ag))
    assert n_ag == AG_ONLY[wave], n_ag
    n_pos = int((ag["w"] > 0).sum())
    assert n_pos == n_ag, (n_pos, n_ag)
    both = a.merge(b, on="i", suffixes=("_hh", "_pp"))
    n_eq = int(np.isclose(both["w_hh"], both["w_pp"]).sum())
    assert n_eq == len(both), (n_eq, len(both))


@pytest.mark.requires_s3
def test_raw_evidence_for_the_panel_link():
    """Every W2/W3 agriculture-only household carries a W1 baseline id that is
    in the W1 household cover, in the same EA; the 25 W3 ones the W2 household
    cover already walked were walked to exactly that baseline."""
    from lsms_library.local_tools import format_id
    fi = lambda s: s.apply(format_id)
    h1 = _raw("2011-12", "sect_cover_hh_w1.dta")
    w1 = dict(zip(fi(h1["household_id"]), fi(h1["ea_id"])))
    u = json.loads((ROOT / "_" / "updated_ids.json").read_text())
    for wave, n in (("2013-14", 2), ("2015-16", 3)):
        hh = set(fi(_raw(wave, f"sect_cover_hh_w{n}.dta")["household_id2"]))
        pp = _raw(wave, f"sect_cover_pp_w{n}.dta")
        a = pd.DataFrame({"i": fi(pp["household_id2"]), "b": fi(pp["household_id"]),
                          "ea": fi(pp["ea_id"])}).drop_duplicates("i")
        a = a[~a["i"].isin(hh)]
        n_in_w1 = int(a["b"].isin(set(w1)).sum())
        n_ea = int((a["b"].map(w1) == a["ea"]).sum())
        assert n_in_w1 == len(a) and n_ea == len(a), (wave, n_in_w1, n_ea, len(a))
        linked = int((a["i"].map(u[wave]) == a["b"]).sum())
        want = {"2013-14": 25, "2015-16": 26}[wave]
        assert linked == want, (wave, linked)


# ---------------------------------------------------------------------------
# Delivered
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def sample():
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return Country("Ethiopia").sample()


@pytest.mark.requires_s3
def test_sample_carries_the_agriculture_households(sample):
    t = sample.index.get_level_values("t")
    got = {w: int((t == w).sum()) for w in WAVES}
    want = {w: HH_COVER[w] + AG_ONLY[w] for w in WAVES}
    assert got == want, got
    n_dup = int(sample.index.duplicated().sum())
    assert n_dup == 0, n_dup
    n_v = int(sample["v"].isna().sum())
    assert n_v == 0, n_v
    # weights are normalised to within-wave mean 1 (CLAUDE.md)
    m = sample.groupby(level="t")["weight"].mean()
    assert np.allclose(m.to_numpy(dtype=float), 1.0), m.round(6).to_dict()


@pytest.mark.requires_s3
@pytest.mark.parametrize("table", ["plot_inputs", "plot_labor", "crop_production",
                                   "plot_features"])
def test_ethiopia_plot_tables_are_clean_on_v(table):
    """Before D1: plot_labor 1,823 and plot_inputs 882 rows had no v (130
    households, every one in the agriculture cover)."""
    from lsms_library import Country
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = getattr(Country("Ethiopia"), table)()
    v = df.index.get_level_values("v") if "v" in df.index.names else df["v"]
    n_na = int(pd.isna(v).sum())
    assert n_na == 0, (table, n_na)

"""GH #1260 -- Malawi's IHPS panel EAs carry coordinates.

2016-17 and 2019-20 each hold two catalog entries in one wave directory
(GH #600): the IHS cross-section in ``Data/Cross_Sectional/`` and the IHPS
long-term panel in ``Data/Panel/``.  The cross-sectional entries (2936, 3818)
publish no geovariables for the panel half, and for years that was read as
"no coordinates exist for the panel EAs".  They do: each IHPS entry (2939,
3819) publishes a household geovariables file of its own, keyed on the panel
household id.  We just did not hold it, so 101 of 880 (2016-17) and 102 of
819 (2019-20) clusters served no GPS.

The two files are now held and stacked into ``df_geo`` under the
cross-sectional geovariables, exactly as ``df_main`` stacks the two cover
pages:

| wave    | file                                     | key       | lat / lon                       |
|---------+------------------------------------------+-----------+---------------------------------|
| 2016-17 | ``Panel/HouseholdGeovariablesIHPSY3.dta`` | ``y3_hhid`` | ``lat_modified`` / ``lon_modified`` |
| 2019-20 | ``Panel/householdgeovariables_y4.dta``    | ``y4_hhid`` | ``lat_mod`` / ``lon_mod``         |

ONE COORDINATE PER EA.  IHPS tracks movers and gives each household its
CURRENT (displaced) location, so a panel EA's households can sit hundreds of
km apart.  Households still within 1 km of their baseline EA
(``dist_to_IHS3location`` on the panel cover) share exactly one coordinate
per EA -- the EA's fix, identical in every IHPS round.  Each wave's
``mapping.py`` ``cluster_features`` hook keeps panel coordinates equal to
that fix (movers sitting on it included), blanks the rest, puts stayers first
so the collapse takes a stayer's attributes, and RAISES when a panel
household has no status (a renamed distance variable would otherwise serve
every mover's location).

WHY THE COVERAGE TEST MATTERS.  A two-file ``file:`` list makes ``get_data``
pass ``missing_ok=True``, so a mis-spelled column in either file becomes an
all-NaN column instead of a ``KeyError``.  The config tests below cannot see
that; the data-gated tests can, and they are the guard.

2013-14 (IHPS 2013, the short-term panel, catalog 2248) gets the same hook
(section 3).  Every one of its 4,000 households is a panel household and its
one cover file carries ``dist_to_IHS3location`` directly, so the status is a
plain ``df_main`` myvar there, and a misspelled distance raises ``KeyError``
at extraction rather than reaching the hook as an all-NaN column.  Its stayers'
fix is the coordinate 2010-11 serves in all 204 EAs and 2016-17 / 2019-20 serve
in the 102 that continue, so each EA keeps one coordinate in every round.

Two tiers: config-only tests (no data, always run) and ``requires_s3`` tests
that build the wave frame and the country table.
"""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

# wave -> (panel geo file, panel key, lat var, lon var, panel households,
#          panel EAs, all clusters, wave-frame rows)
PANEL_GEO = {
    "2016-17": ("Panel/HouseholdGeovariablesIHPSY3.dta", "y3_hhid",
                "lat_modified", "lon_modified", 2508, 102, 880, 14955),
    "2019-20": ("Panel/householdgeovariables_y4.dta", "y4_hhid",
                "lat_mod", "lon_mod", 3178, 102, 819, 14612),
}
COVER = {"2016-17": "Panel/hh_mod_a_filt_16.dta",
         "2019-20": "Panel/hh_mod_a_filt_19.dta"}
# The one exact pin kept: panel households whose coordinate IS their EA's fix
# (2016-17: 1,442 stayers + 418 movers + 1 unrecorded; 2019-20: 2,136 + 29).
# A property of the released files, not of the stayer threshold -- any
# threshold that finds the fix keeps the same households -- so it moves only
# if the WB re-releases the data.
ON_FIX = {"2016-17": 1861, "2019-20": 2165}
STATUS_COL = "_ihps_baseline_status"
STAYER_KM = 1.0
SHARED_EA = "30305580"   # 2016-17: sampled by both the IHS4 and IHPS halves

# 2013-14 (IHPS 2013): one cover, one geo file, every household a panel one.
W13 = "2013-14"
W13_GEO, W13_KEY, W13_LAT, W13_LON = (
    "HouseholdGeovariables_IHPS_13.dta", "y2_hhid", "LAT_DD_MOD", "LON_DD_MOD")
COVER[W13] = "HH_MOD_A_FILT_13.dta"
# households, EAs, EAs continuing into 2016-17 / 2019-20, households whose
# coordinate IS their EA's fix (2,660 stayers + 616 movers who sit on it; the
# other 724 are exactly the households the geo file gives a `distY1Y2`).  A
# property of the released files, like ON_FIX.
W13_HH, W13_EAS, W13_CONTINUING, ON_FIX_13 = 4000, 204, 102, 3276
# Every wave the hook runs in; the synthetic hook tests cover all three.
HOOK_WAVES = sorted(PANEL_GEO) + [W13]
PANEL_KEY = {w: PANEL_GEO[w][1] for w in PANEL_GEO} | {W13: W13_KEY}
# Same EA coordinate in every round: the source's float32 values agree to
# 1.9e-6 degrees (0.2 m) across 2010-11 / 2013-14 / 2016-17 / 2019-20.
ROUND_TOL = 2e-6


def _mapping_module(wave):
    """The wave's mapping.py, imported by path (it reads no data at import)."""
    import importlib.util
    path = countries_root() / "Malawi" / wave / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location(
        f"malawi_{wave.replace('-', '_')}_mapping", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _cluster_cfg(wave):
    path = countries_root() / "Malawi" / wave / "_" / "data_info.yml"
    if not path.exists():  # pragma: no cover
        pytest.skip(f"Malawi/{wave} data_info.yml missing")
    with open(path) as f:
        return yaml.safe_load(f)["cluster_features"]


def _file_entries(files):
    """``file:`` as ``{path: overrides}`` (a bare string has no overrides)."""
    out = {}
    for entry in [files] if isinstance(files, str) else files:
        if isinstance(entry, dict):
            (name, overrides), = entry.items()
            out[name] = overrides or {}
        else:
            out[entry] = {}
    return out


# ---------------------------------------------------------------------------
# 1.  Config: the panel geo file is wired, keyed like df_main's panel half.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_panel_geo_file_is_a_df_geo_source(wave):
    geo_file, key, lat, lon, *_ = PANEL_GEO[wave]
    cfg = _cluster_cfg(wave)
    geo = _file_entries(cfg["df_geo"]["file"])
    assert geo_file in geo, (
        f"Malawi/{wave}: df_geo must read {geo_file} (GH #1260); it reads "
        f"{sorted(geo)}"
    )
    # The cross-sectional geo file must still be there: the panel file is an
    # ADDITIONAL source, not a replacement.
    assert any(p.startswith("Cross_Sectional/") for p in geo), (
        f"Malawi/{wave}: df_geo lost its Cross_Sectional geovariables file"
    )
    ov = geo[geo_file]
    assert ov.get("i") == key, (
        f"Malawi/{wave}: the panel geo file must key `i` on {key}, got "
        f"{ov.get('i')!r}"
    )
    # Variable names the file actually carries: either inherited from the
    # block's myvars (2016-17: same spelling as IHS4) or overridden.
    myvars = cfg["df_geo"]["myvars"]
    assert ov.get("Latitude", myvars["Latitude"]) == lat
    assert ov.get("Longitude", myvars["Longitude"]) == lon


@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_panel_geo_key_matches_the_panel_cover_key(wave):
    """The join is on `i`; the panel half of BOTH sub-frames must build it
    from the same household id, or the merge silently matches nothing (the
    failure 2016-17's cross-sectional half was in until GH #323)."""
    geo_file, key, *_ = PANEL_GEO[wave]
    cfg = _cluster_cfg(wave)
    main = _file_entries(cfg["df_main"]["file"])
    geo = _file_entries(cfg["df_geo"]["file"])
    assert main[COVER[wave]].get("i") == geo[geo_file].get("i") == key
    assert cfg["merge_on"] == ["i"]
    assert "v" not in cfg["df_geo"]["idxvars"], (
        "df_main owns `v`; declaring it in df_geo would collide on the merge"
    )


@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_panel_geo_file_is_dvc_tracked_and_not_in_git(wave):
    """The file holds (displaced) household coordinates: only its sidecar
    may be in the repository.  The library refuses in-tree raw sources
    anyway (GH #831)."""
    geo_file = PANEL_GEO[wave][0]
    data = countries_root() / "Malawi" / wave / "Data"
    sidecar = data / (geo_file + ".dvc")
    assert sidecar.exists(), f"missing DVC sidecar {sidecar}"
    outs = yaml.safe_load(sidecar.read_text())["outs"]
    assert len(outs) == 1 and outs[0]["path"] == Path(geo_file).name


@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_panel_cover_supplies_the_baseline_status(wave):
    """The hook needs to know which panel households are at baseline; only
    the panel cover carries ``dist_to_IHS3location``, so it is a panel-file
    override (cross-sectional rows carry NaN)."""
    cfg = _cluster_cfg(wave)
    main = _file_entries(cfg["df_main"]["file"])
    ov = main[COVER[wave]].get(STATUS_COL)
    assert ov == ["dist_to_IHS3location", {"mapping": "ihps_baseline_status"}], ov
    assert STATUS_COL not in cfg["df_main"].get("myvars", {})


@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_baseline_status_labels(wave):
    """A string, not a numeric sentinel: grab_data rewrites any value above
    1e99 to NaN, so a +inf "unrecorded" read as a cross-sectional row."""
    f = _mapping_module(wave).ihps_baseline_status
    assert f(pd.Series([0.0])) == "stayer"
    assert f(pd.Series([1.0])) == "stayer"
    assert f(pd.Series([1.01])) == "moved"
    assert f(pd.Series([float("nan")])) == "unrecorded"


def _frame(wave, rows):
    """Synthetic hook input: rows of (v, i, lat, lon, status)."""
    idx = pd.MultiIndex.from_tuples([(wave, v, i) for v, i, *_ in rows],
                                    names=["t", "v", "i"])
    return pd.DataFrame({
        "Rural": ["Rural"] * len(rows),
        "Latitude": [r[2] for r in rows],
        "Longitude": [r[3] for r in rows],
        STATUS_COL: [r[4] for r in rows],
    }, index=idx)


@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_hook_keeps_coordinates_on_the_eas_fix(wave):
    """EA A is shared with the cross-section; EA B is panel-only."""
    hook = _mapping_module(wave).cluster_features
    df = _frame(wave, [
        ("A", "cs-1", -13.0, 34.0, pd.NA),           # cross-section
        ("A", "0001-001", -13.5, 34.5, "stayer"),    # panel stayer, shared EA
        ("B", "0002-001", -15.0, 33.5, "moved"),     # mover, elsewhere
        ("B", "0002-002", -14.0, 33.0, "moved"),     # mover ON the fix
        ("B", "0002-003", -14.0, 33.0, "unrecorded"),  # unrecorded ON the fix
        ("B", "0002-004", -16.0, 35.0, "unrecorded"),  # unrecorded, elsewhere
        ("B", "0002-005", -14.0, 33.0, "stayer"),    # stayer: defines the fix
    ])
    out = hook(df.copy())
    assert STATUS_COL not in out.columns
    assert len(out) == len(df)                       # rows never dropped
    lat = out["Latitude"]
    kept = {i for (_, _, i), x in lat.items() if pd.notna(x)}
    assert kept == {"cs-1", "0001-001", "0002-002", "0002-003", "0002-005"}
    assert out["Longitude"].isna().equals(lat.isna())
    # stayers first within each EA, so the collapse takes a stayer's row
    first = out.reset_index().groupby("v", sort=False)["i"].first()
    assert first["A"] == "0001-001" and first["B"] == "0002-005"


@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_hook_fails_closed_without_a_status(wave):
    """missing_ok turns a renamed / misspelled distance variable into an
    all-NaN status.  Every panel row would then look cross-sectional and
    every mover's location would be served -- so the hook must raise."""
    hook = _mapping_module(wave).cluster_features
    df = _frame(wave, [("B", "0002-001", -15.0, 33.5, pd.NA),
                       ("B", "0002-002", -14.0, 33.0, pd.NA),
                       ("A", "cs-1", -13.0, 34.0, pd.NA)])
    with pytest.raises(RuntimeError, match="NO baseline status"):
        hook(df.copy())
    with pytest.raises(RuntimeError, match="missing"):
        hook(df.drop(columns=STATUS_COL))


@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_hook_refuses_a_status_off_the_panel_id_shape(wave):
    hook = _mapping_module(wave).cluster_features
    df = _frame(wave, [("B", "0002-001", -14.0, 33.0, "stayer"),
                       ("A", "cs-1", -13.0, 34.0, "stayer")])
    with pytest.raises(RuntimeError, match="not a panel id"):
        hook(df.copy())


@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_hook_refuses_stayers_that_disagree(wave):
    hook = _mapping_module(wave).cluster_features
    df = _frame(wave, [("B", "0002-001", -14.0, 33.0, "stayer"),
                       ("B", "0002-002", -14.5, 33.0, "stayer")])
    with pytest.raises(RuntimeError, match="disagree"):
        hook(df.copy())


# ---------------------------------------------------------------------------
# 2.  Data: source invariants and the coverage they buy.
# ---------------------------------------------------------------------------

def _source(wave, rel):
    from lsms_library.local_tools import get_dataframe
    try:
        return get_dataframe(str(countries_root() / "Malawi" / wave / "Data" / rel))
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi/{wave} {rel} unavailable: {type(exc).__name__}: {exc}")


@pytest.fixture(scope="module")
def malawi():
    import lsms_library as ll
    return ll.Country("Malawi")


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_panel_geo_is_one_to_one_with_the_panel_cover(wave):
    geo_file, key, lat, lon, n, *_ = PANEL_GEO[wave]
    geo = _source(wave, geo_file)
    cover = _source(wave, COVER[wave])
    g, c = geo[key].astype(str), cover[key].astype(str)
    assert len(g) == g.nunique() == len(c) == c.nunique() == n
    # Counts, not sets: a failing set comparison would print household ids.
    no_geo, no_hh = len(set(c) - set(g)), len(set(g) - set(c))
    assert no_geo == no_hh == 0, (
        f"{no_geo} panel households have no geo row, {no_hh} geo rows have "
        f"no household"
    )
    assert {lat, lon} <= set(geo.columns)


def _wave_frame(malawi, wave):
    try:
        return malawi[wave].grab_data("cluster_features").reset_index()
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi/{wave} cluster_features: {type(exc).__name__}: {exc}")


def _is_panel(i):
    """Panel ids are y{3,4}_hhid (``NNNN-NNN``); the cross-section's are
    ``cs-17-<case_id>`` (2016-17) or the bare 12-digit case_id (2019-20)."""
    s = i.astype(str)
    return ~s.str.startswith("cs-") & s.str.contains("-")


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_wave_frame_gives_panel_households_their_coordinates(malawi, wave):
    *_, n_panel, n_ea, _, n_rows = PANEL_GEO[wave]
    wf = _wave_frame(malawi, wave)
    assert len(wf) == n_rows, (
        f"{wave}: {len(wf)} wave rows, expected {n_rows} -- the geo merge "
        f"must stay 1:1 on `i` (GH #323 site 4)"
    )
    assert not [c for c in wf.columns if str(c).endswith(("_x", "_y"))]
    assert STATUS_COL not in wf.columns, "the hook must drop its helper"
    panel = wf[_is_panel(wf["i"])]
    assert len(panel) == n_panel and panel["v"].nunique() == n_ea
    have = int(panel["Latitude"].notna().sum())
    assert have == ON_FIX[wave], (
        f"{wave}: {have} panel households carry a Latitude, expected the "
        f"{ON_FIX[wave]} that sit on their EA's fix.  Far fewer: a mis-spelled "
        f"panel coordinate column (NaN under missing_ok, not a KeyError).  "
        f"Far more: movers' locations are leaking through."
    )
    assert int(panel["Longitude"].notna().sum()) == have


def _stayer_fixes(wave):
    """Per panel EA, the single coordinate its stayers share (from source)."""
    geo_file, key, lat, lon, *_ = PANEL_GEO[wave]
    geo = _source(wave, geo_file)
    cover = _source(wave, COVER[wave])
    m = cover[[key, "ea_id", "dist_to_IHS3location"]].merge(
        geo[[key, lat, lon]], on=key)
    st = m[(m["dist_to_IHS3location"] <= STAYER_KM) & m[lat].notna()]
    v = st["ea_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    per = st.groupby(v)[[lat, lon]].agg(["nunique", "first"])
    split = int(((per[(lat, "nunique")] > 1) | (per[(lon, "nunique")] > 1)).sum())
    assert split == 0, f"{wave}: stayers of {split} EAs disagree"
    return pd.DataFrame({"lat": per[(lat, "first")].astype(float),
                         "lon": per[(lon, "first")].astype(float)})


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_every_panel_ea_serves_its_stayers_fix(malawi, wave):
    """The cluster coordinate of all 102 panel EAs -- the shared one included
    -- is the coordinate its stayers share.  Counts only on failure."""
    n_ea = PANEL_GEO[wave][5]
    fix = _stayer_fixes(wave)
    assert len(fix) == n_ea, f"{wave}: stayers cover {len(fix)} of {n_ea} EAs"
    try:
        cf = malawi.cluster_features()
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi cluster_features: {type(exc).__name__}: {exc}")
    w = cf.xs(wave, level="t")
    w.index = w.index.astype(str)
    served = w.reindex(fix.index)
    off = int((~(((served["Latitude"].astype(float) - fix["lat"]).abs() < 1e-9)
                 & ((served["Longitude"].astype(float) - fix["lon"]).abs() < 1e-9))).sum())
    assert off == 0, f"{wave}: {off} of {n_ea} panel EAs do not serve their stayers' fix"


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_every_panel_ea_has_exactly_one_coordinate(malawi, wave):
    """What licenses serving the stayers' coordinate as the EA's: they agree.
    Before the hook 101 / 102 panel EAs carried several (movers)."""
    n_ea = PANEL_GEO[wave][5]
    wf = _wave_frame(malawi, wave)
    panel = wf[_is_panel(wf["i"])]
    per_ea = panel.dropna(subset=["Latitude"]).groupby("v")[
        ["Latitude", "Longitude"]].nunique()
    assert int(((per_ea["Latitude"] > 1) | (per_ea["Longitude"] > 1)).sum()) == 0
    assert len(per_ea) == n_ea          # every panel EA keeps its fix


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_every_cluster_has_coordinates(malawi, wave):
    """101 / 102 panel EAs served no GPS before GH #1260; now none is bare."""
    *_, n_clusters, _ = PANEL_GEO[wave]
    try:
        cf = malawi.cluster_features()
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi cluster_features: {type(exc).__name__}: {exc}")
    w = cf.xs(wave, level="t")
    assert len(w) == n_clusters
    missing = int(w["Latitude"].isna().sum())
    assert missing == 0, f"{wave}: {missing} of {n_clusters} clusters have no GPS"
    assert int(w["Longitude"].isna().sum()) == 0
    # No bounding-box check here, deliberately: 9 IHS5 CROSS-SECTIONAL
    # clusters already served coordinates outside Malawi's box before GH #1260
    # (unchanged by it), and 5 of 3,178 IHPS-2019 households carry one too.
    # Neither is this issue's business.


@pytest.mark.requires_s3
def test_shared_ea_serves_the_ihps_stayer_fix(malawi):
    """EA 30305580 is sampled by both halves of 2016-17.  IHS4 and IHPS
    displace independently (its two fixes are 4.1 km apart).  The cluster
    serves the IHPS stayers' fix: it is the EA's coordinate in every IHPS
    round and in 2010-11, so a panel user sees no 4 km jump in 2016-17."""
    wf = _wave_frame(malawi, "2016-17")
    ea = wf[wf["v"].astype(str) == SHARED_EA]
    cs = ea[~_is_panel(ea["i"])]
    if cs.empty:  # pragma: no cover
        pytest.skip("EA 30305580 not in this build")
    fix = _stayer_fixes("2016-17").loc[SHARED_EA]
    cs_fix = cs[["Latitude", "Longitude"]].drop_duplicates()
    assert len(cs_fix) == 1
    assert not (abs(cs_fix["Latitude"].iloc[0] - fix["lat"]) < 1e-9)
    try:
        cf = malawi.cluster_features()
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi cluster_features: {type(exc).__name__}: {exc}")
    w = cf.xs("2016-17", level="t")
    row = w[w.index.astype(str) == SHARED_EA].iloc[0]
    same = bool(abs(float(row["Latitude"]) - fix["lat"]) < 1e-9
                and abs(float(row["Longitude"]) - fix["lon"]) < 1e-9)
    assert same, "EA 30305580 does not serve its IHPS stayers' fix"


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_misspelled_distance_variable_raises(wave):
    """End to end on the real panel cover: extract the status with the
    distance variable misspelled (as missing_ok would), then run the hook.
    It must raise rather than serve every mover's location.  (2013-14 reads
    its cover without missing_ok, so there the misspelling already raises
    KeyError at extraction -- section 3; this is the hook's own guard.)"""
    from lsms_library.local_tools import df_data_grabber, format_id
    hook = _mapping_module(wave).cluster_features
    key = PANEL_KEY[wave]
    path = countries_root() / "Malawi" / wave / "Data" / COVER[wave]
    try:
        df = df_data_grabber(
            str(path), {"i": (key, format_id), "v": ("ea_id", format_id)},
            **{STATUS_COL: (["dist_to_IHS3locationX"],
                            _mapping_module(wave).ihps_baseline_status)},
            missing_ok=True)
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi/{wave} cover unavailable: {type(exc).__name__}: {exc}")
    assert df[STATUS_COL].isna().all()
    df = (df.reset_index().assign(t=wave, Latitude=-14.0, Longitude=33.0)
            .set_index(["t", "v", "i"]))
    with pytest.raises(RuntimeError, match="NO baseline status"):
        hook(df)


# ---------------------------------------------------------------------------
# 3.  2013-14 (IHPS 2013): the same rule, extended (@ligon, 2026-10-23).
# ---------------------------------------------------------------------------

def test_hook_is_the_same_code_in_every_ihps_wave():
    """One rule, three copies (each wave's mapping.py is its own module):
    they must not drift apart."""
    import inspect
    mods = {w: _mapping_module(w) for w in HOOK_WAVES}
    for name in ("cluster_features", "ihps_baseline_status"):
        src = {w: inspect.getsource(getattr(m, name)) for w, m in mods.items()}
        assert len(set(src.values())) == 1, f"{name} differs across {sorted(src)}"
    for name in ("IHPS_STAYER_KM", "_STATUS", "_PANEL_ID"):
        vals = {w: getattr(m, name) for w, m in mods.items()}
        assert len(set(vals.values())) == 1, f"{name}: {vals}"


def test_2013_14_cover_supplies_the_baseline_status():
    """2013-14 reads ONE cover file, so the status is a plain df_main myvar
    (no per-file override to hang it on), from the same variable and through
    the same mapping as the later waves."""
    cfg = _cluster_cfg(W13)
    assert cfg["df_main"]["file"] == COVER[W13]
    assert cfg["df_main"]["idxvars"]["i"] == W13_KEY
    assert cfg["df_main"]["myvars"].get(STATUS_COL) == [
        "dist_to_IHS3location", {"mapping": "ihps_baseline_status"}]
    assert cfg["df_geo"]["file"] == W13_GEO
    assert cfg["df_geo"]["idxvars"] == {"i": W13_KEY}
    assert STATUS_COL not in cfg["df_geo"].get("myvars", {})
    assert cfg["merge_on"] == ["i"]


def _w13_source():
    """2013-14 cover x geo at household grain, with the structure the rule
    relies on asserted (counts only on failure)."""
    from lsms_library.local_tools import format_id
    cover = _source(W13, COVER[W13])
    geo = _source(W13, W13_GEO)
    assert len(cover) == cover[W13_KEY].nunique() == W13_HH
    assert len(geo) == geo[W13_KEY].nunique() == W13_HH
    m = cover[[W13_KEY, "ea_id", "dist_to_IHS3location", "baseline_rural"]].merge(
        geo[[W13_KEY, W13_LAT, W13_LON]], on=W13_KEY)
    assert len(m) == W13_HH, "geo and cover must be 1:1 on y2_hhid"
    m["i"] = m[W13_KEY].map(format_id).astype(str)
    assert int(m["i"].str.fullmatch(r"\d{4}-\d{3}").sum()) == W13_HH, (
        "every 2013-14 household must carry the panel id shape the hook keys on")
    # Every household has a distance, so the hook's strict `<= 1 km` and
    # test_gh323_malawi_cluster_key.py's `fillna(0) <= 1` select the same
    # 2,660 stayers.
    assert int(m["dist_to_IHS3location"].isna().sum()) == 0
    m["v"] = m["ea_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    m["d"] = m["dist_to_IHS3location"].astype(float)
    return m


def _w13_fixes(m):
    st = m[(m["d"] <= STAYER_KM) & m[W13_LAT].notna()]
    per = st.groupby("v")[[W13_LAT, W13_LON]].agg(["nunique", "first"])
    split = int(((per[(W13_LAT, "nunique")] > 1) | (per[(W13_LON, "nunique")] > 1)).sum())
    assert split == 0, f"2013-14: stayers of {split} EAs disagree"
    assert len(per) == m["v"].nunique() == W13_EAS, (
        f"2013-14: stayers cover {len(per)} of {m['v'].nunique()} EAs")
    return pd.DataFrame({"lat": per[(W13_LAT, "first")].astype(float),
                         "lon": per[(W13_LON, "first")].astype(float)})


def _country_cf(malawi):
    try:
        cf = malawi.cluster_features()
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi cluster_features: {type(exc).__name__}: {exc}")
    out = {}
    for w in ("2010-11", W13, "2016-17", "2019-20"):
        x = cf.xs(w, level="t")
        x.index = x.index.astype(str)
        out[w] = x
    return out


@pytest.mark.requires_s3
def test_2013_14_wave_frame_keeps_only_coordinates_on_the_eas_fix(malawi):
    """Before the hook every one of the 4,000 households kept its (current)
    coordinate and 188 of 204 EAs carried several.  Now exactly the 3,276 on
    their EA's fix keep it, and every EA has one coordinate."""
    wf = _wave_frame(malawi, W13)
    assert len(wf) == W13_HH
    assert not [c for c in wf.columns if str(c).endswith(("_x", "_y"))]
    assert STATUS_COL not in wf.columns, "the hook must drop its helper"
    have = int(wf["Latitude"].notna().sum())
    assert have == ON_FIX_13, (
        f"2013-14: {have} households carry a Latitude, expected the {ON_FIX_13} "
        f"on their EA's fix.  4,000: the hook did not run (movers leak).")
    assert int(wf["Longitude"].notna().sum()) == have
    per_ea = wf.dropna(subset=["Latitude"]).groupby("v")[
        ["Latitude", "Longitude"]].nunique()
    assert len(per_ea) == W13_EAS
    assert int(((per_ea["Latitude"] > 1) | (per_ea["Longitude"] > 1)).sum()) == 0


@pytest.mark.requires_s3
def test_2013_14_every_ea_serves_its_stayers_fix(malawi):
    """All 204 EAs, the 102 that continue into 2016-17 and the 102 short-term
    ones alike.  Before the hook 24 served a mover's location (11 of them
    continuing EAs), up to 608 km from the EA."""
    fix = _w13_fixes(_w13_source())
    w = _country_cf(malawi)[W13]
    assert len(w) == W13_EAS and int(w["Latitude"].isna().sum()) == 0
    served = w.reindex(fix.index)
    off = int((~(((served["Latitude"].astype(float) - fix["lat"]).abs() < 1e-9)
                 & ((served["Longitude"].astype(float) - fix["lon"]).abs() < 1e-9))).sum())
    assert off == 0, f"2013-14: {off} of {W13_EAS} EAs do not serve their stayers' fix"


@pytest.mark.requires_s3
def test_2013_14_ea_coordinate_is_the_same_in_every_round(malawi):
    """The SERVED cluster coordinate, country table: an EA's 2013-14
    coordinate is its 2010-11 one (all 204) and its 2016-17 and 2019-20 one
    (the 102 continuing EAs) -- one place per EA across rounds."""
    cf = _country_cf(malawi)
    v13 = list(cf[W13].index)
    cont = sorted(set(v13) & set(cf["2016-17"].index) & set(cf["2019-20"].index))
    assert len(cont) == W13_CONTINUING

    def apart(a, b, vs):
        d = np.maximum(
            (cf[a].loc[vs, "Latitude"].astype(float) - cf[b].loc[vs, "Latitude"].astype(float)).abs(),
            (cf[a].loc[vs, "Longitude"].astype(float) - cf[b].loc[vs, "Longitude"].astype(float)).abs())
        return int((~(d <= ROUND_TOL)).sum())

    assert set(v13) <= set(cf["2010-11"].index)
    assert apart(W13, "2010-11", v13) == 0, "2013-14 vs 2010-11 coordinate"
    assert apart(W13, "2016-17", cont) == 0, "2013-14 vs 2016-17 coordinate"
    assert apart(W13, "2019-20", cont) == 0, "2013-14 vs 2019-20 coordinate"


@pytest.mark.requires_s3
def test_2013_14_cluster_attributes_come_from_a_stayer(malawi):
    """Stayers first, so the collapse takes a stayer's Region / District /
    Rural.  Two answer keys: the stayers' modal value, and the cover's own
    `baseline_rural` (the IHS3 EA's urban/rural, constant within every EA) --
    independent of which household the collapse picks.  Before the hook the
    served value missed the stayers' mode in 3 / 18 / 7 EAs and
    `baseline_rural` in 7."""
    m = _w13_source()
    wf = _wave_frame(malawi, W13)
    wf["i"] = wf["i"].astype(str)
    st = wf[wf["i"].isin(set(m.loc[m["d"] <= STAYER_KM, "i"]))]
    assert len(st) == int((m["d"] <= STAYER_KM).sum())
    w = _country_cf(malawi)[W13]
    # Region: the API spells the wave frame's 'South' as 'Southern', so
    # compare the first five letters (north / centr / south).
    norm = {"Region": lambda s: s.astype(str).str.lower().str[:5],
            "District": lambda s: s.astype(str).str.lower(),
            "Rural": lambda s: s.astype(str).str.lower()}
    for col, f in norm.items():
        mode = st.assign(x=f(st[col])).groupby(st["v"].astype(str))["x"].agg(
            lambda s: s.mode().iloc[0])
        off = int((f(w.loc[mode.index, col]) != mode).sum())
        assert off == 0, f"2013-14 {col}: {off} of {len(mode)} EAs off the stayers' mode"
    base = m.groupby("v")["baseline_rural"].agg(lambda s: s.astype(str).str.lower().mode().iloc[0])
    assert int((m.groupby("v")["baseline_rural"].nunique() > 1).sum()) == 0
    off = int((w.loc[base.index, "Rural"].astype(str).str.lower() != base).sum())
    assert off == 0, f"2013-14: {off} of {len(base)} EAs serve a Rural that is not baseline_rural"


@pytest.mark.requires_s3
def test_2013_14_misspelled_distance_variable_raises_at_extraction(monkeypatch):
    """The real config, one variable misspelled, built through
    ``Wave.grab_data`` with the cache bypassed: the single cover is read
    without missing_ok, so extraction raises KeyError instead of serving
    every household's current location.

    What the COUNTRY table then does is framework behaviour, measured and not
    pinned here: ``build_wave`` catches the KeyError, warns and drops the
    wave, and the four-wave L2-country parquet is stamped with the current
    hash, so every later read serves it with no 2013-14 rows and no warning.
    Nothing wrong is served -- never a mover's location -- but a wave is
    lost.  2016-17 / 2019-20 read two cover files (missing_ok), so the same
    mistake reaches the hook and raises RuntimeError there instead."""
    import copy
    import lsms_library as ll
    from lsms_library.country import Wave
    orig = Wave.resources.fget

    def mutated(self):
        info = orig(self)
        if str(self.year) != W13:
            return info
        info = copy.deepcopy(info)
        mv = info["cluster_features"]["df_main"]["myvars"]
        assert mv[STATUS_COL][0] == "dist_to_IHS3location", mv.get(STATUS_COL)
        mv[STATUS_COL][0] = "dist_to_IHS3locationX"
        return info

    assert STATUS_COL in _cluster_cfg(W13)["df_main"]["myvars"], (
        "2013-14 df_main does not read the baseline status at all")
    monkeypatch.setattr(Wave, "resources", property(mutated))
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    _source(W13, COVER[W13])            # skips cleanly if the data is absent
    with pytest.raises(KeyError, match="dist_to_IHS3locationX"):
        ll.Country("Malawi")[W13].grab_data("cluster_features")

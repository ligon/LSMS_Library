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

ONLY THE STAYERS' COORDINATES LOCATE THE EA.  IHPS tracks movers and gives
each household its CURRENT (displaced) location, so a panel EA's households
can sit hundreds of km apart.  Households still within 1 km of their baseline
EA (``dist_to_IHS3location`` on the panel cover) share exactly one coordinate
per EA.  Each wave's ``mapping.py`` ``cluster_features`` hook blanks every
other panel coordinate -- and the panel's in an EA the cross-section also
covers, which keeps the cross-section's EA fix -- so the cluster serves the
EA's fix rather than whichever household the grain collapse selects.

WHY THE COVERAGE TEST MATTERS.  A two-file ``file:`` list makes ``get_data``
pass ``missing_ok=True``, so a mis-spelled column in either file becomes an
all-NaN column instead of a ``KeyError``.  The config tests below cannot see
that; the data-gated tests can, and they are the guard.

Two tiers: config-only tests (no data, always run) and ``requires_s3`` tests
that build the wave frame and the country table.
"""
from pathlib import Path

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
# Panel households that keep a coordinate after the hook: stayers with a
# coordinate (1,442 / 2,136), less 2016-17's 20 stayers in EA 30305580, the
# one EA the cross-section also covers (it keeps the IHS4 fix).
STAYERS_KEPT = {"2016-17": 1422, "2019-20": 2136}
STATUS_COL = "_ihps_baseline_status"


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


@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_baseline_status_labels(wave):
    """A string, not a numeric sentinel: grab_data rewrites any value above
    1e99 to NaN, so a +inf "unrecorded" read as a cross-sectional row."""
    f = _mapping_module(wave).ihps_baseline_status
    assert f(pd.Series([0.0])) == "stayer"
    assert f(pd.Series([1.0])) == "stayer"
    assert f(pd.Series([1.01])) == "moved"
    assert f(pd.Series([float("nan")])) == "unrecorded"


@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_hook_keeps_only_the_eas_own_fix(wave):
    """Synthetic frame: a mover, an unrecorded household and a stayer in a
    panel-only EA; a cross-sectional household and a panel stayer in a
    shared EA."""
    hook = _mapping_module(wave).cluster_features
    idx = pd.MultiIndex.from_tuples(
        [(wave, "A", "cs-1"), (wave, "A", "p-1"),
         (wave, "B", "p-2"), (wave, "B", "p-3"), (wave, "B", "p-4")],
        names=["t", "v", "i"])
    df = pd.DataFrame({
        "Region": ["R"] * 5,
        "Latitude": [-13.0, -13.1, -14.0, -15.0, -16.0],
        "Longitude": [34.0, 34.1, 33.0, 33.5, 35.0],
        STATUS_COL: [pd.NA, "stayer", "stayer", "moved", "unrecorded"],
    }, index=idx)
    out = hook(df.copy())
    assert STATUS_COL not in out.columns
    lat = out["Latitude"]
    assert lat.loc[(wave, "A", "cs-1")] == -13.0          # cross-section kept
    assert pd.isna(lat.loc[(wave, "A", "p-1")])           # shared EA: CS wins
    assert lat.loc[(wave, "B", "p-2")] == -14.0           # stayer kept
    assert pd.isna(lat.loc[(wave, "B", "p-3")])           # mover blanked
    assert pd.isna(lat.loc[(wave, "B", "p-4")])           # unrecorded blanked
    assert out["Longitude"].isna().equals(lat.isna())
    assert len(out) == len(df)                            # rows never dropped


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
    assert have == STAYERS_KEPT[wave], (
        f"{wave}: {have} panel households carry a Latitude, expected the "
        f"{STAYERS_KEPT[wave]} stayers.  Far fewer: a mis-spelled panel "
        f"coordinate column (NaN under missing_ok, not a KeyError) or a lost "
        f"baseline status.  Far more: movers' locations are leaking through."
    )
    assert int(panel["Longitude"].notna().sum()) == have


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
    # every panel EA but the shared one keeps a stayer coordinate
    shared = 1 if wave == "2016-17" else 0
    assert len(per_ea) == n_ea - shared


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
def test_shared_ea_keeps_the_cross_sections_fix(malawi):
    """EA 30305580 is sampled by both halves of 2016-17.  IHS4 and IHPS
    displace independently (its two fixes are 4.1 km apart), so the cluster
    keeps the cross-section's single fix -- what it served before the panel
    file was held."""
    wf = _wave_frame(malawi, "2016-17")
    ea = wf[wf["v"].astype(str) == "30305580"]
    cs = ea[~_is_panel(ea["i"])]
    if cs.empty:  # pragma: no cover
        pytest.skip("EA 30305580 not in this build")
    assert ea.loc[_is_panel(ea["i"]), "Latitude"].isna().all()
    fix = cs[["Latitude", "Longitude"]].drop_duplicates()
    assert len(fix) == 1
    try:
        cf = malawi.cluster_features()
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        pytest.skip(f"Malawi cluster_features: {type(exc).__name__}: {exc}")
    row = cf.xs("2016-17", level="t")
    row = row[row.index.astype(str) == "30305580"].iloc[0]
    same = bool(row["Latitude"] == fix["Latitude"].iloc[0]
                and row["Longitude"] == fix["Longitude"].iloc[0])
    assert same, "EA 30305580 no longer serves the cross-section's fix"

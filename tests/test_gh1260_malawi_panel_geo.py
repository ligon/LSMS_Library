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

ONE COORDINATE PER EA, ACROSS THE IHPS ROUNDS.  IHPS tracks movers and gives
each household its CURRENT (displaced) location, so a panel EA's households
can sit hundreds of km apart.  Households still within 1 km of their baseline
EA (``dist_to_IHS3location`` on the panel cover) share exactly one coordinate
per EA -- the EA's fix, identical in every IHPS round and equal to the EA's
IHS3 2010-11 coordinate.  It is the IHPS fix only: IHS4 and IHS5 displace
their own households, so an EA that an independent cross-section draws again
carries that survey's own fix (the EA both halves of 2016-17 sample, and one
short-term 2013-14 EA that IHS4 draws again -- section 3).

One implementation, ``malawi.ihps_panel_cluster_features``, serves all three
IHPS waves; each wave's ``mapping.py`` ``cluster_features`` is a thin caller
that says where that wave reads the status from.  The rule keeps panel
coordinates equal to the EA's fix (movers sitting on it included), blanks the
rest, puts stayers first so the collapse takes a stayer's attributes, and
RAISES when a panel household has no status (a renamed distance variable
would otherwise serve every mover's location).

WHY THE COVERAGE TEST MATTERS.  A two-file ``file:`` list makes ``get_data``
pass ``missing_ok=True``, so a mis-spelled column in either file becomes an
all-NaN column instead of a ``KeyError``.  The config tests below cannot see
that; the data-gated tests can, and they are the guard.

2013-14 (IHPS 2013, the short-term panel, catalog 2248) gets the same rule
(section 3).  Every one of its 4,000 households is a panel household and its
one cover file carries ``dist_to_IHS3location`` directly, so the status is a
plain ``df_main`` myvar there, and a misspelled distance raises ``KeyError``
at extraction rather than reaching the hook as an all-NaN column.  What the
country build then does with that ``KeyError`` is GH #1281, pinned below as a
strict xfail.  Its stayers' fix is the coordinate 2010-11 serves in all 204
EAs and 2016-17 / 2019-20 serve in the 102 that continue into the long-term
panel.

THE REPOSITORY IS PUBLIC, AND CI LOGS ARE TOO.  pytest's assertion rewrite
prints every sub-expression of a failing ``assert`` -- a frame, a Series, the
argument list of a helper -- even under ``--tb=short``.  So every data
assertion here compares scalar LOCALS (counts, booleans), each computed on its
own line, and every message is built from counts.  Never ``assert`` on an
expression that touches a frame, a Series or a list of ids: household ids, EA
codes and coordinates must not reach a failure report.

Data-gated tests skip only when a source is UNAVAILABLE (``FileNotFoundError``
from ``get_dataframe`` / ``Wave.grab_data``; missing credentials are handled
by ``tests/conftest.py``).  Any other exception is a failure.

Two tiers: config-only and synthetic tests (no data, always run) and
``requires_s3`` tests that build the wave frame and the country table.
"""
import ast
import inspect
import os
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
# The household cover each wave's df_main reads (the panel cover where a wave
# has two); `_require` probes these before a country build.
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
# Same EA coordinate in every IHPS round: the source's float32 values agree to
# 1.9e-6 degrees (0.2 m) across 2010-11 / 2013-14 / 2016-17 / 2019-20.
ROUND_TOL = 2e-6
# Where each wave's data_info.yml reads the status column from -- the one
# thing the thin callers pass to the shared implementation.  A phrase each
# wave's error must carry, and one it must not.
STATUS_SOURCE_SAYS = {
    "2013-14": ("myvar", "override"),
    "2016-17": ("Panel/hh_mod_a_filt_16.dta file override", "myvar"),
    "2019-20": ("Panel/hh_mod_a_filt_19.dta file override", "myvar"),
}


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
#     (No data: these messages carry file and variable names only.)
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
    the panel cover carries ``dist_to_IHS3location``, so in 2016-17 / 2019-20
    it is a panel-file override (cross-sectional rows carry NaN)."""
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


# ---------------------------------------------------------------------------
# 1b.  The rule, on synthetic frames, through every wave's hook.  (Synthetic
#      values only -- but asserted on counts anyway, so the leak sweep of a
#      failure report needs no exemptions.)
# ---------------------------------------------------------------------------

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
    helper_left = STATUS_COL in out.columns
    assert not helper_left, "the hook must drop its helper column"
    n_out, n_in = len(out), len(df)
    assert n_out == n_in, "rows are never dropped"
    kept = {i for (_, _, i), x in out["Latitude"].items() if pd.notna(x)}
    n_wrong = len(kept ^ {"cs-1", "0001-001", "0002-002", "0002-003", "0002-005"})
    assert n_wrong == 0, f"{n_wrong} synthetic households kept or blanked wrongly"
    n_split = int((out["Longitude"].isna() != out["Latitude"].isna()).sum())
    assert n_split == 0, f"{n_split} rows blank one coordinate but not the other"
    # stayers first within each EA, so the collapse takes a stayer's row
    first = out.reset_index().groupby("v", sort=False)["i"].first()
    stayers_first = bool(first["A"] == "0001-001" and first["B"] == "0002-005")
    assert stayers_first, "a stayer is not the first row of its EA"


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
# 1c.  ONE implementation, thin per-wave callers.  (Replaces a test that
#      required three byte-identical copies, which forced 2013-14 to carry
#      the later waves' "panel-file override" wording.)
# ---------------------------------------------------------------------------

def _shared_module():
    """The country module the wave hooks import, located via a wave module."""
    import sys
    mod = _mapping_module(W13)
    name = mod._ihps_panel_cluster_features.__module__
    return sys.modules[name]


def test_shared_implementation_is_the_country_modules():
    """The one implementation lives in Malawi/_/malawi.py of THIS checkout,
    and is not named after a table: malawi.py's callables are country-level
    formatting functions, so a ``cluster_features`` there would run as the
    df_edit hook in 2004-05 and 2010-11 too."""
    shared = _shared_module()
    src = Path(inspect.getsourcefile(shared)).resolve()
    want = (countries_root() / "Malawi" / "_" / "malawi.py").resolve()
    assert src == want, f"the hook implementation is imported from {src}, not {want}"
    assert not hasattr(shared, "cluster_features"), (
        "malawi.py must not define `cluster_features`: it would become every "
        "wave's df_edit hook for that table")


@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_wave_hook_is_a_thin_caller_of_the_shared_implementation(wave):
    """Each wave's ``cluster_features`` is ``return
    _ihps_panel_cluster_features(df, status_source=...)`` and nothing else,
    bound to the country module's function; the wave module defines none of
    the rule's constants or helpers itself, so the three cannot drift."""
    mod = _mapping_module(wave)
    shared = _shared_module()
    assert mod._ihps_panel_cluster_features is shared.ihps_panel_cluster_features
    assert mod.ihps_baseline_status is shared.ihps_baseline_status
    fn = ast.parse(inspect.getsource(mod.cluster_features)).body[0]
    body = fn.body[1:] if isinstance(fn.body[0], ast.Expr) else fn.body
    assert len(body) == 1 and isinstance(body[0], ast.Return), (
        f"{wave}: cluster_features must be a single `return` of the shared call")
    call = body[0].value
    assert isinstance(call, ast.Call) and getattr(call.func, "id", None) == \
        "_ihps_panel_cluster_features", f"{wave}: it must call the shared function"
    assert [k.arg for k in call.keywords] == ["status_source"], (
        f"{wave}: the only parameter is the wave's status source")
    tree = ast.parse(Path(inspect.getsourcefile(mod)).read_text())
    defined = {t.id for n in tree.body if isinstance(n, ast.Assign)
               for t in n.targets if isinstance(t, ast.Name)}
    defined |= {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    copies = sorted(defined & {"IHPS_STAYER_KM", "_STATUS", "_PANEL_ID",
                               "IHPS_STATUS", "IHPS_PANEL_ID",
                               "ihps_baseline_status",
                               "ihps_panel_cluster_features"})
    assert not copies, f"{wave}: mapping.py defines its own {copies}"


@pytest.mark.parametrize("wave", HOOK_WAVES)
def test_missing_status_error_names_the_waves_own_source(wave):
    """The parameter exists for this message: 2013-14 reads the status as a
    plain df_main myvar (one cover file), 2016-17 / 2019-20 as a file
    override on the panel cover.  Each wave's error must say its own."""
    hook = _mapping_module(wave).cluster_features
    says, never = STATUS_SOURCE_SAYS[wave]
    df = _frame(wave, [("B", "0002-001", -14.0, 33.0, "stayer")])
    with pytest.raises(RuntimeError, match="missing") as err:
        hook(df.drop(columns=STATUS_COL))
    msg = str(err.value)
    assert says in msg, f"{wave}: the error does not name {says!r}"
    assert never not in msg, f"{wave}: the error wrongly mentions {never!r}"


# ---------------------------------------------------------------------------
# 2.  Data: source invariants and the coverage they buy.
# ---------------------------------------------------------------------------

def _source(wave, rel):
    """A source file, or a skip if it is UNAVAILABLE -- nothing else."""
    from lsms_library.local_tools import get_dataframe
    try:
        return get_dataframe(str(countries_root() / "Malawi" / wave / "Data" / rel))
    except FileNotFoundError as exc:  # pragma: no cover
        pytest.skip(f"Malawi/{wave} {rel} unavailable: {exc}")


def _require(*waves):
    """Skip (unavailable only) unless each wave's household cover is held.

    The COUNTRY build does not raise on an unavailable source -- it drops
    the sub-frame or the wave -- so there is no unavailability error to
    catch around it; probe the sources first, then build uncaught."""
    for w in waves:
        _source(w, COVER.get(w, "Full_Sample/Household/hh_mod_a_filt.dta"))


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
    n_geo, u_geo, n_cov, u_cov = len(g), int(g.nunique()), len(c), int(c.nunique())
    assert n_geo == u_geo == n_cov == u_cov == n, "rows / unique keys, geo and cover"
    no_geo, no_hh = len(set(c) - set(g)), len(set(g) - set(c))
    assert no_geo == no_hh == 0, (
        f"{no_geo} panel households have no geo row, {no_hh} geo rows have "
        f"no household"
    )
    absent = sorted({lat, lon} - set(geo.columns))
    assert not absent, f"geo file lacks {absent}"


def _wave_frame(malawi, wave):
    """The wave's household-grain cluster_features frame; a skip only when a
    source is unavailable (``grab_data`` re-raises a missing primary file as
    ``FileNotFoundError``)."""
    try:
        return malawi[wave].grab_data("cluster_features").reset_index()
    except FileNotFoundError as exc:  # pragma: no cover
        pytest.skip(f"Malawi/{wave} cluster_features source unavailable: {exc}")


def _is_panel(i):
    """Panel ids are y{2,3,4}_hhid (``NNNN-NNN``); the cross-section's are
    ``cs-17-<case_id>`` (2016-17) or the bare 12-digit case_id (2019-20)."""
    s = i.astype(str)
    return ~s.str.startswith("cs-") & s.str.contains("-")


def _country_cf(malawi):
    """The country table, split by wave, index as str.  Built uncaught after
    probing the sources (see ``_require``)."""
    _require("2010-11", W13, "2016-17", "2019-20")
    cf = malawi.cluster_features()
    out = {}
    for w in ("2010-11", W13, "2016-17", "2019-20"):
        x = cf.xs(w, level="t")
        x.index = x.index.astype(str)
        out[w] = x
    return out


def _n_off(served, fix, tol=1e-9):
    """How many EAs of ``fix`` (index v; lat, lon) the served frame does not
    place on that coordinate.  A count: never a frame, never an id."""
    s = served.reindex(fix.index)
    on = (((s["Latitude"].astype(float) - fix["lat"]).abs() < tol)
          & ((s["Longitude"].astype(float) - fix["lon"]).abs() < tol))
    return int((~on).sum())


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_wave_frame_gives_panel_households_their_coordinates(malawi, wave):
    *_, n_panel, n_ea, _, n_rows = PANEL_GEO[wave]
    wf = _wave_frame(malawi, wave)
    n_wf = len(wf)
    assert n_wf == n_rows, (
        f"{wave}: {n_wf} wave rows, expected {n_rows} -- the geo merge "
        f"must stay 1:1 on `i` (GH #323 site 4)"
    )
    n_suffixed = sum(str(c).endswith(("_x", "_y")) for c in wf.columns)
    assert n_suffixed == 0, f"{wave}: {n_suffixed} merge-suffixed columns"
    helper_left = STATUS_COL in set(wf.columns)
    assert not helper_left, "the hook must drop its helper"
    panel = wf[_is_panel(wf["i"])]
    n_hh, n_v = len(panel), int(panel["v"].nunique())
    assert n_hh == n_panel and n_v == n_ea, (
        f"{wave}: {n_hh} panel households in {n_v} EAs, expected "
        f"{n_panel} in {n_ea}")
    have = int(panel["Latitude"].notna().sum())
    assert have == ON_FIX[wave], (
        f"{wave}: {have} panel households carry a Latitude, expected the "
        f"{ON_FIX[wave]} that sit on their EA's fix.  Far fewer: a mis-spelled "
        f"panel coordinate column (NaN under missing_ok, not a KeyError).  "
        f"Far more: movers' locations are leaking through."
    )
    have_lon = int(panel["Longitude"].notna().sum())
    assert have_lon == have, (
        f"{wave}: {have_lon} panel Longitudes against {have} Latitudes")


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
    -- is the coordinate its stayers share."""
    n_ea = PANEL_GEO[wave][5]
    fix = _stayer_fixes(wave)
    n_fix = len(fix)
    assert n_fix == n_ea, f"{wave}: stayers cover {n_fix} of {n_ea} EAs"
    off = _n_off(_country_cf(malawi)[wave], fix)
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
    n_multi = int(((per_ea["Latitude"] > 1) | (per_ea["Longitude"] > 1)).sum())
    assert n_multi == 0, f"{wave}: {n_multi} panel EAs carry more than one coordinate"
    n_with = len(per_ea)
    assert n_with == n_ea, f"{wave}: {n_with} of {n_ea} panel EAs keep their fix"


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", sorted(PANEL_GEO))
def test_every_cluster_has_coordinates(malawi, wave):
    """101 / 102 panel EAs served no GPS before GH #1260; now none is bare."""
    *_, n_clusters, _ = PANEL_GEO[wave]
    w = _country_cf(malawi)[wave]
    n_w = len(w)
    assert n_w == n_clusters, f"{wave}: {n_w} clusters, expected {n_clusters}"
    missing = int(w["Latitude"].isna().sum())
    assert missing == 0, f"{wave}: {missing} of {n_clusters} clusters have no GPS"
    missing_lon = int(w["Longitude"].isna().sum())
    assert missing_lon == 0, f"{wave}: {missing_lon} clusters have no Longitude"
    # No bounding-box check here, deliberately: 9 IHS5 CROSS-SECTIONAL
    # clusters already served coordinates outside Malawi's box before GH #1260
    # (unchanged by it), and 5 of 3,178 IHPS-2019 households carry one too.
    # Neither is this issue's business.


@pytest.mark.requires_s3
def test_shared_ea_serves_the_ihps_stayer_fix(malawi):
    """Exactly one EA is sampled by both halves of 2016-17 (found from the
    data, not named here).  IHS4 and IHPS displace independently (its two
    fixes are 4.1 km apart).  The cluster serves the IHPS stayers' fix: it
    is the EA's coordinate in every IHPS round and in 2010-11, so a panel
    user sees no 4 km jump in 2016-17."""
    wf = _wave_frame(malawi, "2016-17")
    v = wf["v"].astype(str)
    panel = _is_panel(wf["i"])
    shared = sorted(set(v[panel]) & set(v[~panel]))
    n_shared = len(shared)
    assert n_shared == 1, f"{n_shared} EAs are sampled by both halves of 2016-17, expected 1"
    ea = shared[0]
    cs = wf[(v == ea) & ~panel]
    cs_fix = cs[["Latitude", "Longitude"]].drop_duplicates()
    n_cs_fix = len(cs_fix)
    assert n_cs_fix == 1, f"the shared EA's IHS4 households carry {n_cs_fix} coordinates"
    fix = _stayer_fixes("2016-17").loc[ea]
    ihs4_is_ihps = bool(abs(float(cs_fix["Latitude"].iloc[0]) - fix["lat"]) < 1e-9)
    assert not ihs4_is_ihps, "the shared EA's IHS4 and IHPS fixes no longer differ"
    row = _country_cf(malawi)["2016-17"].loc[ea]
    same = bool(abs(float(row["Latitude"]) - fix["lat"]) < 1e-9
                and abs(float(row["Longitude"]) - fix["lon"]) < 1e-9)
    assert same, "the shared EA does not serve its IHPS stayers' fix"


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
    _source(wave, COVER[wave])          # skips only if the cover is unavailable
    path = countries_root() / "Malawi" / wave / "Data" / COVER[wave]
    df = df_data_grabber(
        str(path), {"i": (key, format_id), "v": ("ea_id", format_id)},
        **{STATUS_COL: (["dist_to_IHS3locationX"],
                        _mapping_module(wave).ihps_baseline_status)},
        missing_ok=True)
    n_known = int(df[STATUS_COL].notna().sum())
    assert n_known == 0, f"{n_known} statuses survived a misspelled variable"
    df = (df.reset_index().assign(t=wave, Latitude=-14.0, Longitude=33.0)
            .set_index(["t", "v", "i"]))
    with pytest.raises(RuntimeError, match="NO baseline status"):
        hook(df)


# ---------------------------------------------------------------------------
# 3.  2013-14 (IHPS 2013): the same rule, extended (@ligon, 2026-10-23).
# ---------------------------------------------------------------------------

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
    n_cov, u_cov = len(cover), int(cover[W13_KEY].nunique())
    n_geo, u_geo = len(geo), int(geo[W13_KEY].nunique())
    assert n_cov == u_cov == n_geo == u_geo == W13_HH, "rows / unique y2_hhid"
    m = cover[[W13_KEY, "ea_id", "dist_to_IHS3location", "baseline_rural"]].merge(
        geo[[W13_KEY, W13_LAT, W13_LON]], on=W13_KEY)
    n_m = len(m)
    assert n_m == W13_HH, f"geo and cover must be 1:1 on y2_hhid ({n_m} merged)"
    m["i"] = m[W13_KEY].map(format_id).astype(str)
    n_shaped = int(m["i"].str.fullmatch(r"\d{4}-\d{3}").sum())
    assert n_shaped == W13_HH, (
        f"{n_shaped} of {W13_HH} 2013-14 households carry the panel id shape "
        f"the hook keys on")
    # Every household has a distance, so the hook's strict `<= 1 km` and
    # test_gh323_malawi_cluster_key.py's `fillna(0) <= 1` select the same
    # 2,660 stayers.
    n_nodist = int(m["dist_to_IHS3location"].isna().sum())
    assert n_nodist == 0, f"{n_nodist} households have no dist_to_IHS3location"
    m["v"] = m["ea_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    m["d"] = m["dist_to_IHS3location"].astype(float)
    return m


def _w13_fixes(m):
    st = m[(m["d"] <= STAYER_KM) & m[W13_LAT].notna()]
    per = st.groupby("v")[[W13_LAT, W13_LON]].agg(["nunique", "first"])
    split = int(((per[(W13_LAT, "nunique")] > 1) | (per[(W13_LON, "nunique")] > 1)).sum())
    assert split == 0, f"2013-14: stayers of {split} EAs disagree"
    n_cov_ea, n_ea = len(per), int(m["v"].nunique())
    assert n_cov_ea == n_ea == W13_EAS, (
        f"2013-14: stayers cover {n_cov_ea} of {n_ea} EAs (expected {W13_EAS})")
    return pd.DataFrame({"lat": per[(W13_LAT, "first")].astype(float),
                         "lon": per[(W13_LON, "first")].astype(float)})


def _n_apart(cf, a, b, vs, tol=ROUND_TOL):
    """How many EAs of ``vs`` sit more than ``tol`` degrees apart between
    waves ``a`` and ``b`` of the country table.  A count, never the ids."""
    d = np.maximum(
        (cf[a].loc[vs, "Latitude"].astype(float) - cf[b].loc[vs, "Latitude"].astype(float)).abs(),
        (cf[a].loc[vs, "Longitude"].astype(float) - cf[b].loc[vs, "Longitude"].astype(float)).abs())
    return int((~(d <= tol)).sum())


def _km(cf, a, b, vs):
    """Great-circle km between waves ``a`` and ``b`` for the EAs ``vs``."""
    la1, lo1, la2, lo2 = (np.radians(cf[w].loc[vs, c].astype(float).to_numpy())
                          for w, c in ((a, "Latitude"), (a, "Longitude"),
                                       (b, "Latitude"), (b, "Longitude")))
    cos = (np.sin(la1) * np.sin(la2)
           + np.cos(la1) * np.cos(la2) * np.cos(lo2 - lo1))
    return 6371.0 * np.arccos(np.clip(cos, -1.0, 1.0))


@pytest.mark.requires_s3
def test_2013_14_wave_frame_keeps_only_coordinates_on_the_eas_fix(malawi):
    """Before the hook every one of the 4,000 households kept its (current)
    coordinate and 188 of 204 EAs carried several.  Now exactly the 3,276 on
    their EA's fix keep it, and every EA has one coordinate."""
    wf = _wave_frame(malawi, W13)
    n_wf = len(wf)
    assert n_wf == W13_HH, f"2013-14: {n_wf} wave rows, expected {W13_HH}"
    n_suffixed = sum(str(c).endswith(("_x", "_y")) for c in wf.columns)
    assert n_suffixed == 0, f"2013-14: {n_suffixed} merge-suffixed columns"
    helper_left = STATUS_COL in set(wf.columns)
    assert not helper_left, "the hook must drop its helper"
    have = int(wf["Latitude"].notna().sum())
    assert have == ON_FIX_13, (
        f"2013-14: {have} households carry a Latitude, expected the {ON_FIX_13} "
        f"on their EA's fix.  4,000: the hook did not run (movers leak).")
    have_lon = int(wf["Longitude"].notna().sum())
    assert have_lon == have, f"2013-14: {have_lon} Longitudes against {have} Latitudes"
    per_ea = wf.dropna(subset=["Latitude"]).groupby("v")[
        ["Latitude", "Longitude"]].nunique()
    n_with = len(per_ea)
    assert n_with == W13_EAS, f"2013-14: {n_with} of {W13_EAS} EAs keep a coordinate"
    n_multi = int(((per_ea["Latitude"] > 1) | (per_ea["Longitude"] > 1)).sum())
    assert n_multi == 0, f"2013-14: {n_multi} EAs carry more than one coordinate"


@pytest.mark.requires_s3
def test_2013_14_every_ea_serves_its_stayers_fix(malawi):
    """All 204 EAs, the 102 that continue into 2016-17 and the 102 short-term
    ones alike.  Before the hook 24 served a mover's location (11 of them
    continuing EAs), up to 608 km from the EA."""
    fix = _w13_fixes(_w13_source())
    w = _country_cf(malawi)[W13]
    n_w, n_bare = len(w), int(w["Latitude"].isna().sum())
    assert n_w == W13_EAS and n_bare == 0, (
        f"2013-14: {n_w} clusters ({n_bare} without GPS), expected {W13_EAS}")
    off = _n_off(w, fix)
    assert off == 0, f"2013-14: {off} of {W13_EAS} EAs do not serve their stayers' fix"


@pytest.mark.requires_s3
def test_2013_14_ea_coordinate_is_the_same_in_every_ihps_round(malawi):
    """The SERVED cluster coordinate, country table: an EA's 2013-14
    coordinate is its 2010-11 one (all 204) and its 2016-17 and 2019-20 one
    (the 102 EAs continuing into the long-term panel) -- one place per EA
    across the IHPS rounds.  An EA an independent cross-section draws again
    is a different matter (next test)."""
    cf = _country_cf(malawi)
    v13 = list(cf[W13].index)
    cont = sorted(set(v13) & set(cf["2016-17"].index) & set(cf["2019-20"].index))
    n_cont = len(cont)
    assert n_cont == W13_CONTINUING, (
        f"{n_cont} 2013-14 EAs continue into both later waves, expected "
        f"{W13_CONTINUING}")
    n_not_ihs3 = len(set(v13) - set(cf["2010-11"].index))
    assert n_not_ihs3 == 0, f"{n_not_ihs3} 2013-14 EAs are not IHS3 EAs"
    n10 = _n_apart(cf, W13, "2010-11", v13)
    n16 = _n_apart(cf, W13, "2016-17", cont)
    n19 = _n_apart(cf, W13, "2019-20", cont)
    assert n10 == 0, f"2013-14 vs 2010-11: {n10} of {len(v13)} EAs differ"
    assert n16 == 0, f"2013-14 vs 2016-17: {n16} of {n_cont} continuing EAs differ"
    assert n19 == 0, f"2013-14 vs 2019-20: {n19} of {n_cont} continuing EAs differ"


@pytest.mark.requires_s3
def test_2013_14_short_term_ea_drawn_again_by_ihs4_keeps_ihs4s_own_fix(malawi):
    """Why "one coordinate per EA in every round" would be FALSE: of the 102
    short-term-only 2013-14 EAs, one is drawn again by the IHS4
    cross-section in 2016-17 (16 households, none of them panel) and none by
    IHS5.  IHS4 displaces its own households, so that EA's 2016-17 cluster
    coordinate is IHS4's fix, 17.5 km from its IHS3 / IHPS one.  Measured
    on the branch build of 2026-10-23; the EA is found, not named."""
    cf = _country_cf(malawi)
    v13, v16, v19 = (set(cf[w].index) for w in (W13, "2016-17", "2019-20"))
    short = v13 - (v16 & v19)
    again16, again19 = sorted(short & v16), sorted(short & v19)
    counts = (len(short), len(again16), len(again19))
    assert counts == (W13_EAS - W13_CONTINUING, 1, 0), (
        f"(short-term EAs, drawn again in 2016-17, in 2019-20) = {counts}")
    wf = _wave_frame(malawi, "2016-17")
    in_ea = wf[wf["v"].astype(str).isin(again16)]
    n_hh, n_panel_hh = len(in_ea), int(_is_panel(in_ea["i"]).sum())
    assert (n_hh, n_panel_hh) == (16, 0), (
        f"the re-drawn EA holds {n_hh} 2016-17 households, {n_panel_hh} panel")
    km = float(_km(cf, W13, "2016-17", again16)[0])
    far = bool(5.0 < km < 30.0)
    assert far, f"its 2013-14 and 2016-17 coordinates are {km:.1f} km apart"
    n_same_as_ihs3 = len(again16) - _n_apart(cf, W13, "2010-11", again16)
    assert n_same_as_ihs3 == 1, "its 2013-14 coordinate is no longer its 2010-11 one"


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
    n_st, n_want = len(st), int((m["d"] <= STAYER_KM).sum())
    assert n_st == n_want, f"{n_st} stayers in the wave frame, {n_want} at source"
    w = _country_cf(malawi)[W13]
    # Region: the API spells the wave frame's 'South' as 'Southern', so
    # compare the first five letters (north / centr / south).
    norm = {"Region": lambda s: s.astype(str).str.lower().str[:5],
            "District": lambda s: s.astype(str).str.lower(),
            "Rural": lambda s: s.astype(str).str.lower()}
    for col, f in norm.items():
        mode = st.assign(x=f(st[col])).groupby(st["v"].astype(str))["x"].agg(
            lambda s: s.mode().iloc[0])
        off, n_mode = int((f(w.loc[mode.index, col]) != mode).sum()), len(mode)
        assert off == 0, f"2013-14 {col}: {off} of {n_mode} EAs off the stayers' mode"
    base = m.groupby("v")["baseline_rural"].agg(lambda s: s.astype(str).str.lower().mode().iloc[0])
    n_varying = int((m.groupby("v")["baseline_rural"].nunique() > 1).sum())
    assert n_varying == 0, f"baseline_rural varies within {n_varying} EAs"
    off, n_base = int((w.loc[base.index, "Rural"].astype(str).str.lower() != base).sum()), len(base)
    assert off == 0, f"2013-14: {off} of {n_base} EAs serve a Rural that is not baseline_rural"


def _misspell_2013_14_status(monkeypatch):
    """Route ``Wave.resources`` so 2013-14's df_main reads a misspelled
    distance variable.  ``pytest.fail`` (not ``assert``) when the config has
    no such myvar, so the #1281 xfail below cannot absorb a missing wiring."""
    import copy
    from lsms_library.country import Wave
    if STATUS_COL not in _cluster_cfg(W13)["df_main"].get("myvars", {}):
        pytest.fail("2013-14 df_main does not read the baseline status at all")
    orig = Wave.resources.fget

    def mutated(self):
        info = orig(self)
        if str(self.year) != W13:
            return info
        info = copy.deepcopy(info)
        mv = info["cluster_features"]["df_main"]["myvars"]
        mv[STATUS_COL][0] = "dist_to_IHS3locationX"
        return info

    monkeypatch.setattr(Wave, "resources", property(mutated))


@pytest.fixture
def private_data_root(tmp_path, monkeypatch):
    """A per-test, empty ``LSMS_DATA_DIR`` with the L1 blob cache shared --
    ``tests/test_canonical_shape_via_cache_miss.py``'s fixture, verbatim in
    intent.  LOAD-BEARING for the #1281 test: the build it provokes caches a
    four-wave parquet under the current hash, and in the shared root every
    later read would serve it."""
    from lsms_library import config as _config
    from lsms_library import local_tools as _lt
    from lsms_library.paths import data_root
    private = tmp_path / "data"
    private.mkdir()
    shared_l1 = Path(_lt._DVC_CACHE_DIR)
    if shared_l1.is_dir():
        (private / "dvc-cache").symlink_to(shared_l1, target_is_directory=True)
    monkeypatch.setenv("LSMS_DATA_DIR", str(private))
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    data_root.cache_clear()
    try:
        assert data_root() == private, "the private data root did not take"
        shared = [Path.home() / ".local" / "share" / "lsms_library"]
        configured = _config._load_config().get("data_dir")
        if configured:
            shared.append(Path(configured).expanduser())
        collides = any(private.resolve() == s.resolve() for s in shared)
        assert not collides, "the private data root is a shared data root"
        yield private
    finally:
        data_root.cache_clear()


@pytest.mark.requires_s3
def test_2013_14_misspelled_distance_variable_raises_at_extraction(
        monkeypatch, private_data_root):
    """The real config, one variable misspelled, built through
    ``Wave.grab_data``: the single cover is read without missing_ok, so
    extraction raises KeyError instead of serving every household's current
    location.  2016-17 / 2019-20 read two cover files (missing_ok), so the
    same mistake reaches the hook and raises RuntimeError there instead."""
    import lsms_library as ll
    _source(W13, COVER[W13])            # skips only if the cover is unavailable
    _misspell_2013_14_status(monkeypatch)
    with pytest.raises(KeyError, match="dist_to_IHS3locationX"):
        ll.Country("Malawi")[W13].grab_data("cluster_features")


def _names(exc, needle):
    """Whether ``needle`` appears in ``exc`` or anything it was raised from."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if needle in str(exc):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


@pytest.mark.requires_s3
@pytest.mark.xfail(
    strict=True, raises=AssertionError,
    reason="GH #1281: a config KeyError in a wave's PRIMARY sub-df drops the "
           "whole wave (build_wave), and the shortened country parquet is "
           "cached under the current hash.  Resolution: raise before anything "
           "is written.  Goes red (XPASS, strict) the day that lands.")
def test_2013_14_config_keyerror_is_not_cached_as_a_valid_build(
        monkeypatch, private_data_root):
    """What the COUNTRY build does with the extraction KeyError above.

    Today: ``build_wave`` catches it, warns, and drops 2013-14; the four-wave
    table is written to ``var/cluster_features.parquet`` under the current
    hash, and every later read serves it with no 2013-14 rows and no warning
    (``cluster_features(waves=['2013-14'])`` then returns 0 rows).  Nothing
    wrong is served -- never a mover's location -- but a wave is lost.

    Asserts the #1281 resolution: the call raises an error naming the
    misspelled variable, and no country parquet is written.  Any OTHER
    exception is re-raised and fails the test (``raises=AssertionError``
    keeps it from being absorbed as the expected failure)."""
    import lsms_library as ll
    _source(W13, COVER[W13])            # skips only if the cover is unavailable
    _misspell_2013_14_status(monkeypatch)
    raised = False
    n_waves = n13 = -1
    try:
        cf = ll.Country("Malawi").cluster_features()
    except Exception as exc:  # noqa: BLE001 -- re-raised unless it is #1281's
        if not _names(exc, "dist_to_IHS3locationX"):
            raise
        raised = True
    else:
        t = cf.index.get_level_values("t")
        n_waves, n13 = int(t.nunique()), int((t == W13).sum())
    var = private_data_root / "Malawi" / "var" / "cluster_features.parquet"
    written = var.exists()
    assert raised, (
        f"GH #1281: no error; the build returned {n_waves} waves with {n13} "
        f"2013-14 rows, and a country parquet was{'' if written else ' not'} "
        f"written")
    assert not written, "GH #1281: raised, but a country parquet was written first"

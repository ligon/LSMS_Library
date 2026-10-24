"""Burkina Faso 2021-22 holds two catalog entries (GH #1276).

6224 (EHCVM-P, the panel component) lives in ``2021-22/Data/Panel/``; 6277
(EHCVM-2, the cross-section) in ``2021-22/Data/Cross_Section/``.  The #600
layout of ``Malawi/2016-17``.

Config-only tests read the tree; they need no data.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from lsms_library import provenance
from lsms_library.paths import countries_root

COUNTRY = "Burkina_Faso"
WAVE = "2021-22"


def _wave_dir() -> Path:
    return countries_root() / COUNTRY / WAVE


def _data_dir() -> Path:
    return _wave_dir() / "Data"


def _declared_files(node) -> list[str]:
    """Every ``file:`` entry of a data_info.yml block, in a string or list form."""
    out: list[str] = []
    if isinstance(node, dict):
        for key, val in node.items():
            if key == "file":
                items = val if isinstance(val, list) else [val]
                for item in items:
                    if isinstance(item, dict):
                        out.extend(item.keys())
                    else:
                        out.append(item)
            else:
                out.extend(_declared_files(val))
    elif isinstance(node, list):
        for val in node:
            out.extend(_declared_files(val))
    return out


# ---------------------------------------------------------------------------
# Phase 1: the held 6224 files live in Data/Panel/
# ---------------------------------------------------------------------------

def test_no_sidecar_at_the_top_of_data():
    """A sidecar left at the top of ``Data/`` refuses every ``subdir=``
    acquisition (``data_access._check_subdir_acquisition``)."""
    top = sorted(p.name for p in _data_dir().glob("*.dvc"))
    n = len(top)
    assert n == 0, f"{n} sidecar(s) at the top of {WAVE}/Data/"


def test_the_panel_component_holds_its_53_sidecars():
    n = len(list((_data_dir() / "Panel").glob("*.dta.dvc")))
    assert n == 53, f"{n} != 53"
    weights = (_data_dir() / "Panel" / "ehcvm_panel_ponderations_bfa2021.dta.dvc").exists()
    assert weights, "the panel weights file is the 6224 component's own"


def test_source_org_maps_6224_to_panel():
    prov = provenance.read_provenance(countries_root(), COUNTRY, WAVE)
    mapping = prov.subdir_map()
    got = mapping.get("6224")
    assert got == "Panel", f"6224 -> {got!r}"
    assert prov.catalog_id == "6224"


def test_every_declared_file_has_a_sidecar():
    """Each ``file:`` in the wave's data_info.yml names a held sidecar."""
    info = yaml.safe_load((_wave_dir() / "_" / "data_info.yml").read_text())
    files = sorted(set(_declared_files(info)))
    missing = [f for f in files if not (_data_dir() / f"{f}.dvc").exists()]
    n = len(missing)
    assert files and n == 0, f"{n} declared file(s) without a sidecar: {missing}"


_LITERAL = re.compile(r"""get_dataframe\(\s*['"](\.\./[^'"]*?2021-22[^'"]*|\.\./Data/[^'"]+)['"]""")


@pytest.mark.parametrize("script", [
    "2021-22/_/plot_features.py",
    "2021-22/_/food_acquired.py",
    "_/panel_ids.py",
])
def test_script_literals_name_held_sidecars(script):
    """Every literal ``get_dataframe('../...')`` path a script reads from the
    2021-22 wave resolves to a held sidecar (relative to the script's dir)."""
    path = countries_root() / COUNTRY / script
    lits = _LITERAL.findall(path.read_text())
    assert lits, f"{script}: no literal source path found"
    bad = [lit for lit in lits
           if not (path.parent / f"{lit}.dvc").resolve().exists()]
    n = len(bad)
    assert n == 0, f"{script}: {n} literal path(s) with no sidecar: {bad}"


# ---------------------------------------------------------------------------
# Phase 2: 6277 acquired into Data/Cross_Section/ through add_wave(subdir=)
# ---------------------------------------------------------------------------

def test_source_org_records_both_entries():
    """The shipped record says what the acquisition measured (the
    ``TestShippedRecordsSayWhatWeMeasured`` precedent): both entries listed,
    each mapped to its subdirectory, 6277's held release on its own line, and
    the primary's scalars still describing 6224 (GH #1283 ruling)."""
    prov = provenance.read_provenance(countries_root(), COUNTRY, WAVE)
    ids = list(prov.catalog_ids)
    assert ids == ["6224", "6277"], ids
    mapping = prov.subdir_map()
    assert mapping == {"6224": "Panel", "6277": "Cross_Section"}, mapping
    held = prov.held_release_of("6277")
    assert held is not None and held.idno == "BFA_2021_EHCVM-2_v01_M", held
    primary = prov.held_release_of("6224")
    assert primary is not None and primary.idno == "BFA_2021_EHCVM-P_v04_M", primary
    latest = prov.latest_releases()
    n = len(latest)
    assert n == 0, f"{n} #+CATALOG_LATEST line(s)"


def test_the_cross_section_holds_its_55_sidecars():
    xs = _data_dir() / "Cross_Section"
    names = {p.name[:-4] for p in xs.glob("*.dta.dvc")}
    n = len(names)
    assert n == 55, f"{n} != 55"
    # Its own weights file, not the panel's.
    assert "ehcvm_ponderations_bfa2021.dta" in names
    assert "ehcvm_panel_ponderations_bfa2021.dta" not in names
    panel = {p.name[:-4] for p in (_data_dir() / "Panel").glob("*.dta.dvc")}
    shared = len(names & panel)
    assert shared == 52, f"{shared} != 52 file names shared with Panel/"
    raw = [p for p in xs.iterdir() if not p.name.endswith(".dvc")]
    n_raw = len(raw)
    assert n_raw == 0, f"{n_raw} raw workspace copies left in Cross_Section/"


def test_no_two_sidecars_name_the_same_blob():
    """Disjoint entries: no panel sidecar and cross-section sidecar share an
    md5, i.e. the same-named files really are different files."""
    def md5s(sub):
        out = {}
        for p in (_data_dir() / sub).glob("*.dvc"):
            out[p.name] = yaml.safe_load(p.read_text())["outs"][0]["md5"]
        return out
    panel, cross = md5s("Panel"), md5s("Cross_Section")
    common = set(panel.values()) & set(cross.values())
    n = len(common)
    # Measured 2026-10-23: 0, even for ehcvm_nsu_bfa2021 (the non-standard
    # unit table has the same 20,493 x 9 shape in both, different bytes).
    assert n == 0, f"{n} blobs shared between the components"


# ---------------------------------------------------------------------------
# Phase 3: both components are served by every 2021-22 household table
# ---------------------------------------------------------------------------
#
# Component membership is read off `v`: the panel keeps 2018-19's grappes
# (2-585), the cross-section's are 586-1185 (asserted below), so `v` <= 585 is
# the panel and >= 586 the cross-section.

PANEL_HH, CROSS_HH = 3227, 7176
PANEL_GRAPPES, CROSS_GRAPPES = 550, 600


def _component(v):
    import pandas as pd
    v = v.astype(int)
    return pd.Series(["panel" if x <= 585 else "cross" for x in v], index=v.index)


@pytest.fixture(scope="module")
def bf():
    import lsms_library as ll
    return ll.Country(COUNTRY)


def _raw(sub, name):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(str(_data_dir() / sub / name), convert_categoricals=False)


@pytest.mark.requires_s3
def test_the_two_covers_are_disjoint_and_the_grappes_split_at_585():
    p, x = _raw("Panel", "s00_me_bfa2021.dta"), _raw("Cross_Section", "s00_me_bfa2021.dta")
    n_p, n_x = len(p), len(x)
    assert (n_p, n_x) == (PANEL_HH, CROSS_HH), (n_p, n_x)
    pmax, xmin, xmax = int(p.grappe.max()), int(x.grappe.min()), int(x.grappe.max())
    assert pmax <= 585 and xmin == 586 and xmax == 1185, (pmax, xmin, xmax)
    shared = len(set(zip(p.grappe, p.menage)) & set(zip(x.grappe, x.menage)))
    assert shared == 0, f"{shared} (grappe, menage) in both components"


@pytest.mark.requires_s3
def test_sample_serves_both_components_with_their_own_weights(bf):
    s = bf.sample(waves=[WAVE]).reset_index()
    comp = _component(s["v"])
    n_panel, n_cross = int((comp == "panel").sum()), int((comp == "cross").sum())
    assert (n_panel, n_cross) == (PANEL_HH, CROSS_HH), (n_panel, n_cross)
    dup = int(s.duplicated(["t", "i"]).sum())
    assert dup == 0, f"{dup} duplicated (t, i)"
    n_w = int(s["weight"].notna().sum())
    assert n_w == PANEL_HH + CROSS_HH, n_w
    # panel_weight: the panel only (GH #1276 ruling 3).
    pw_panel = int(s.loc[comp == "panel", "panel_weight"].notna().sum())
    pw_cross = int(s.loc[comp == "cross", "panel_weight"].notna().sum())
    assert (pw_panel, pw_cross) == (PANEL_HH, 0), (pw_panel, pw_cross)


@pytest.mark.requires_s3
def test_weights_are_each_components_raw_weight_up_to_one_constant(bf):
    """Raw oracle.  The served weight is the raw weight of the household's OWN
    component divided by ONE wave-wide constant (`_normalise_sample_weights`):
    the panel's hhweight2021, the cross-section's hhweight -- never the
    other's, never the cover's rounded copy."""
    s = bf.sample(waves=[WAVE]).reset_index()
    s["g"] = s["v"].astype(int)
    pw = _raw("Panel", "ehcvm_panel_ponderations_bfa2021.dta")
    xw = _raw("Cross_Section", "ehcvm_ponderations_bfa2021.dta")
    raw = {int(g): float(w) for g, w in zip(pw.grappe, pw.hhweight2021)}
    raw.update({int(g): float(w) for g, w in zip(xw.grappe, xw.hhweight)})
    ratio = s["weight"].astype(float) / s["g"].map(raw)
    n_na = int(ratio.isna().sum())
    assert n_na == 0, n_na
    spread = float(ratio.max() / ratio.min() - 1)
    assert spread < 1e-6, spread
    rawp = {int(g): float(w) for g, w in zip(pw.grappe, pw.hhweight_panel)}
    panel = s[_component(s["v"]) == "panel"]
    rp = panel["panel_weight"].astype(float) / panel["g"].map(rawp)
    spread_p = float(rp.max() / rp.min() - 1)
    assert spread_p < 1e-6, spread_p


@pytest.mark.requires_s3
def test_cluster_features_serves_both_components(bf):
    cf = bf.cluster_features(waves=[WAVE]).reset_index()
    comp = _component(cf["v"])
    n_panel, n_cross = int((comp == "panel").sum()), int((comp == "cross").sum())
    assert (n_panel, n_cross) == (PANEL_GRAPPES, CROSS_GRAPPES), (n_panel, n_cross)
    n_region = int(cf.loc[comp == "cross", "Region"].notna().sum())
    assert n_region == CROSS_GRAPPES, n_region


HOUSEHOLD_TABLES = ["household_roster", "individual_education", "food_acquired",
                    "assets", "housing", "shocks", "food_security",
                    "subjective_well_being", "interview_date", "plot_features"]


@pytest.mark.requires_s3
@pytest.mark.parametrize("table", HOUSEHOLD_TABLES)
def test_every_household_table_carries_the_cross_section(bf, table):
    """Each 2021-22 household table serves cross-section households, and only
    households `sample()` knows (same `i`, same `v`)."""
    df = getattr(bf, table)(waves=[WAVE]).reset_index()
    s = bf.sample(waves=[WAVE]).reset_index()
    comp = _component(df["v"])
    hh_cross = df.loc[comp == "cross", "i"].nunique()
    hh_panel = df.loc[comp == "panel", "i"].nunique()
    assert hh_cross > 0 and hh_panel > 0, (hh_panel, hh_cross)
    known = set(zip(s["i"], s["v"].astype(str)))
    got = set(zip(df["i"], df["v"].astype(str)))
    orphans = len(got - known)
    assert orphans == 0, f"{orphans} (i, v) not in sample()"


@pytest.mark.requires_s3
def test_plot_features_cross_section_has_no_coordinates(bf):
    """6277's s16a ships no s16aq46: its parcels are served with Latitude /
    Longitude NA ("not shipped for this component"); the panel's are not
    touched."""
    pf = bf.plot_features(waves=[WAVE]).reset_index()
    comp = _component(pf["v"])
    n_cross = int((comp == "cross").sum())
    assert n_cross > 0, n_cross
    lat_cross = int(pf.loc[comp == "cross", "Latitude"].notna().sum())
    lon_cross = int(pf.loc[comp == "cross", "Longitude"].notna().sum())
    assert (lat_cross, lon_cross) == (0, 0), (lat_cross, lon_cross)
    lat_panel = int(pf.loc[comp == "panel", "Latitude"].notna().sum())
    assert lat_panel == 4180, lat_panel


@pytest.mark.requires_s3
def test_derivation_inputs_read_both_components(bf):
    """The registry's raw-input callable reads both s07b files and joins every
    served purchased row of 2021-22 -- including the cross-section's -- with
    no skip (ehcvm.inputs_last_purchase assumes one file)."""
    from lsms_library.ehcvm import RAW_VARIABLES, derivation_key
    raw = bf.derivation_inputs(derivation_key(COUNTRY), wave=WAVE)
    assert list(raw.columns) == RAW_VARIABLES
    served = bf.food_acquired(waves=[WAVE])
    served = served[served["Derivation"].notna()].reset_index()
    comp = _component(served["v"])
    n_cross = int((comp == "cross").sum())
    assert n_cross > 0, n_cross
    keys = set(zip(served["t"], served["i"], served["j"]))
    missing = len(keys - set(raw.index))
    assert missing == 0, f"{missing} of {len(keys)} served purchased keys have no raw row"

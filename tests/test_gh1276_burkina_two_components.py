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

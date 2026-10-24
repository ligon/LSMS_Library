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

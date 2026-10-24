"""Burkina Faso household-id namespaces for the independent samples (GH #1244).

Every wave builds ``i`` from its cluster serial and the household number, and
the serials of independent draws overlap, so unrelated households used to
share served ids: 583 between 2014 (EMC) and 2018-19 (EHCVM-1), 9 between the
2021-22 cross-section (WB 6277) and 2018-19.  @ligon's ruling: namespace the
samples that are not the EHCVM-1 panel lineage, Niger's way (``'E_'``).

* 2014 (EMC): ``'EMC_' + zd + menage.zfill(3)``  (``burkina_faso.emc_i``)
* 2021-22 cross-section: ``'CS_' + grappe + menage.zfill(3)``
  (``burkina_faso.cross_section_i``; ``cs_i`` in ``data_info.yml``)
* 2018-19 and the 2021-22 panel component: unchanged, linked by ``panel_ids``.

Data tests print counts, never ids.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root

COUNTRY = "Burkina_Faso"
ROOT = countries_root() / COUNTRY


def _module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def bf_mod():
    return _module(ROOT / "_" / "burkina_faso.py", "bf_1244_country")


# ---------------------------------------------------------------------------
# config: the formatters and where they are bound
# ---------------------------------------------------------------------------

def test_the_formatters(bf_mod):
    assert bf_mod.emc_i(1, 1) == "EMC_1001"
    assert bf_mod.emc_i(59, 12) == "EMC_59012"
    assert bf_mod.cross_section_i(590, 612) == "CS_590612"
    assert bf_mod.cs_i(pd.Series([590.0, 612.0])) == "CS_590612"
    assert bf_mod.emc_i(None, 1) is None and bf_mod.cross_section_i(1, None) is None
    # The panel lineage is untouched.
    assert bf_mod.ehcvm_i(59, 612) == "590612"
    assert bf_mod.i(pd.Series([590, 612])) == "590612"


def test_the_2014_wave_overrides_i_with_the_emc_namespace():
    m = _module(ROOT / "2014" / "_" / "mapping.py", "bf_1244_m2014")
    assert m.i(pd.Series([1, 1])) == "EMC_1001"
    # The 2018-19 and 2021-22 waves keep theirs.
    m18 = _module(ROOT / "2018-19" / "_" / "mapping.py", "bf_1244_m2018")
    assert m18.i(pd.Series([59, 12])) == "59012"
    m21 = _module(ROOT / "2021-22" / "_" / "mapping.py", "bf_1244_m2021")
    assert not hasattr(m21, "i")


def _blocks(info):
    for table, spec in info.items():
        if not isinstance(spec, dict):
            continue
        if "file" in spec:
            yield table, spec
        for sub in spec.get("dfs", []) or []:
            yield f"{table}.{sub}", spec[sub]


def test_every_cross_section_entry_that_builds_i_namespaces_it():
    """In 2021-22's data_info.yml, every ``Cross_Section/`` entry of a block
    whose ``i`` is built from ``[grappe, menage]`` overrides ``i`` with
    ``cs_i``; no ``Panel/`` entry does."""
    info = yaml.safe_load((ROOT / "2021-22" / "_" / "data_info.yml").read_text())
    checked = 0
    for name, spec in _blocks(info):
        i_spec = (spec.get("idxvars") or {}).get("i")
        if not isinstance(i_spec, list):
            continue
        for entry in spec["file"]:
            if isinstance(entry, dict):
                fname, ov = next(iter(entry.items()))
            else:
                fname, ov = entry, {}
            if fname.startswith("Cross_Section/"):
                assert ov.get("i") == ["grappe", "menage", {"mapping": "cs_i"}], (name, fname)
                checked += 1
            else:
                assert "i" not in ov, (name, fname)
    assert checked == 11, checked


@pytest.mark.parametrize("script", ["food_coping.py", "months_food_inadequate.py"])
def test_the_2014_scripts_build_i_with_emc_i(script):
    text = (ROOT / "2014" / "_" / script).read_text()
    assert "from burkina_faso import emc_i" in text
    assert "emc_i(" in text.split("from burkina_faso import emc_i", 1)[1]


# ---------------------------------------------------------------------------
# data: the served ids
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bf():
    import lsms_library as ll
    return ll.Country(COUNTRY)


@pytest.fixture(scope="module")
def sample(bf):
    return bf.sample().reset_index()


def _ids(sample, t):
    return set(sample.loc[sample["t"] == t, "i"])


@pytest.mark.requires_s3
def test_no_id_is_shared_across_an_independent_boundary(sample):
    i14, i18, i21 = (_ids(sample, t) for t in ("2014", "2018-19", "2021-22"))
    s21 = sample[sample["t"] == "2021-22"]
    cross = set(s21.loc[s21["v"].astype(int) >= 586, "i"])
    n_14_18, n_14_21, n_x_18 = len(i14 & i18), len(i14 & i21), len(cross & i18)
    assert (n_14_18, n_14_21, n_x_18) == (0, 0, 0), (n_14_18, n_14_21, n_x_18)


@pytest.mark.requires_s3
def test_each_sample_carries_its_own_namespace(sample):
    def count(t, pred):
        ids = sample.loc[sample["t"] == t, "i"].astype(str)
        return int(pred(ids).sum()), len(ids)
    emc, n14 = count("2014", lambda s: s.str.startswith("EMC_"))
    assert emc == n14 == 10411, (emc, n14)
    pref18, n18 = count("2018-19", lambda s: ~s.str.fullmatch(r"\d+"))
    assert (pref18, n18) == (0, 7010), (pref18, n18)
    s21 = sample[sample["t"] == "2021-22"]
    is_cross = s21["v"].astype(int) >= 586
    ids = s21["i"].astype(str)
    cs = int(ids[is_cross].str.fullmatch(r"CS_\d+").sum())
    plain = int(ids[~is_cross].str.fullmatch(r"\d+").sum())
    assert (cs, plain) == (7176, 3227), (cs, plain)


@pytest.mark.requires_s3
def test_every_panel_link_is_intact(sample, bf):
    """All 3,227 panel households are served under their 2018-19 id, in their
    2018-19 grappe; no cross-section household is."""
    s18 = sample[sample["t"] == "2018-19"].set_index("i")["v"].astype(str)
    s21 = sample[sample["t"] == "2021-22"]
    panel = s21[s21["v"].astype(int) <= 585].set_index("i")["v"].astype(str)
    linked = panel.index.intersection(s18.index)
    n_linked = len(linked)
    same_v = int((panel.loc[linked] == s18.loc[linked]).sum())
    assert (n_linked, same_v) == (3227, 3227), (n_linked, same_v)
    n_map = len(bf.updated_ids.get("2021-22", {}))
    assert n_map == 3227, n_map


@pytest.mark.requires_s3
@pytest.mark.parametrize("table", ["sample", "household_roster", "food_acquired",
                                   "plot_features", "interview_date", "assets"])
def test_id_walk_is_exactly_once_on_the_served_frames(bf, table):
    """GH #1282's detection: re-walking a served frame neither raises
    (``IdWalkError``) nor moves an id."""
    from lsms_library import local_tools as tools
    df = getattr(bf, table)()
    before = list(df.index.get_level_values("i"))
    again = tools.id_walk(df.copy(), bf.updated_ids)
    after = list(again.index.get_level_values("i"))
    moved = sum(a != b for a, b in zip(before, after))
    assert len(before) == len(after) and moved == 0, (len(before), len(after), moved)


@pytest.mark.requires_s3
@pytest.mark.parametrize("table", ["household_roster", "food_acquired", "shocks",
                                   "food_coping", "months_food_inadequate",
                                   "housing", "assets", "individual_education"])
def test_every_2014_table_is_namespaced(bf, table):
    df = getattr(bf, table)(waves=["2014"])
    ids = pd.Series(df.index.get_level_values("i")).astype(str)
    n = len(ids)
    bad = int((~ids.str.fullmatch(r"EMC_\d+")).sum())
    assert n > 0 and bad == 0, (n, bad)

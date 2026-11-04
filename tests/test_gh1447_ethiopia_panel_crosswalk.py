"""Ethiopia's committed panel crosswalk is the only copy of the wiring (GH #1447).

The three ``panel_ids:`` blocks removed here were never read by
``_/panel_ids.py``, but ``LSMS_BUILD_BACKEND=make`` built from them.  W2's read
the holder-grain LIVESTOCK cover: it lost the links of households without a
livestock row, and ``update_id`` minted the blank baselines of its urban
households into ``''``, ``_1``, ``_2``, ... as household ids.  W5's identity
block linked Panel II to Panel I, which the survey says do not link.

Three tiers, as in ``test_gh1264_nigeria_panel_crosswalk.py``: the wave config
as text, the committed JSON alone (including through both backends, which needs
no source file now that no wave declares the table), and (``requires_s3``) the
household covers read directly, by path, with nothing from ``Ethiopia/_/`` in
the oracle.

W3 households with a W1 baseline in W3's own ``household_id`` that are on no W2
cover are deliberately NOT pinned either way: whether to link them awaits the
maintainer (see ``Ethiopia/_/CONTENTS.org``, GH #1447).
"""
from __future__ import annotations

import json
import re

import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

ETHIOPIA = countries_root() / "Ethiopia"
PANEL_I = {"2011-12", "2013-14", "2015-16"}
PANEL_II = {"2018-19", "2021-22"}
_requires_data = pytest.mark.requires_s3


@pytest.fixture(scope="module")
def links():
    with open(ETHIOPIA / "_" / "panel_ids.json") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def updated_ids():
    with open(ETHIOPIA / "_" / "updated_ids.json") as f:
        return json.load(f)


def _flat(panel_ids):
    """``RecursiveDict`` / JSON spelling -> ``{'t,i': 't0,i0'}``."""
    data = getattr(panel_ids, "data", panel_ids)
    return {(",".join(k) if isinstance(k, tuple) else k):
            (",".join(v) if isinstance(v, tuple) else v) for k, v in data.items()}


def test_no_wave_declares_a_second_copy_of_the_wiring():
    declaring = [p.parent.parent.name for p in ETHIOPIA.glob("*/_/data_info.yml")
                 if any(line.startswith("panel_ids:") for line in p.read_text().splitlines())]
    assert declaring == []


@pytest.mark.parametrize("backend", ["dvc", "make"])
def test_both_backends_serve_the_committed_crosswalk(backend, links, updated_ids, monkeypatch, tmp_path):
    """The bug itself: the make backend served a different crosswalk."""
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LSMS_BUILD_BACKEND", backend)
    c = Country("Ethiopia", preload_panel_ids=False)
    assert _flat(c.panel_ids) == links
    assert c.updated_ids == updated_ids


def test_no_link_crosses_the_two_panels(links, updated_ids):
    """ESS4 "is not a follow-up of the previous ESS waves" (WB catalog 3823)."""
    for key, value in links.items():
        waves = {key.split(",")[0], value.split(",")[0]}
        assert waves <= PANEL_I or waves <= PANEL_II, (key, value)
    # Panel II keeps its ids, so a map for it could only be an identity or a merge.
    for wave in PANEL_II & set(updated_ids):
        assert all(k == v for k, v in updated_ids[wave].items()), wave


def test_no_household_id_is_minted_from_a_blank_baseline(updated_ids):
    for wave, mapping in updated_ids.items():
        minted = [v for v in mapping.values() if not v or re.fullmatch(r"_\d*", v)]
        assert minted == [], wave


def test_w3_links_chain_through_w2(updated_ids):
    """A W3 household on the W2 map keeps its W2 canonical id, whatever W3's ``household_id`` says.

    Keys absent from the W2 map are left out, so this does not pin the 48 either way.
    """
    w2, w3 = updated_ids["2013-14"], updated_ids["2015-16"]
    on_w2_map = {k: v for k, v in w3.items() if k in w2}
    assert on_w2_map
    assert on_w2_map == {k: w2[k] for k in on_w2_map}


def test_the_maps_are_collision_free(updated_ids):
    for wave, mapping in updated_ids.items():
        assert len(mapping) == len(set(mapping.values())), wave


# ---------------------------------------------------------------------------
# The covers, read directly: no script in the oracle
# ---------------------------------------------------------------------------

def _cover(wave, name):
    df = get_dataframe(ETHIOPIA / wave / "Data" / name, convert_categoricals=False)
    return df


@_requires_data
def test_every_w2_household_cover_link_is_committed(updated_ids):
    """The household cover's baseline, for every W2 household that has one.

    The livestock cover the block read holds 3,670 of the wave's 5,262
    households, so a holder-grain source fails this however it is reduced.
    """
    hh = _cover("2013-14", "sect_cover_hh_w2.dta")[["household_id2", "household_id"]].map(format_id)
    assert hh.household_id2.is_unique
    linked = hh[hh.household_id.notna()]
    assert len(linked) > 0
    assert linked.set_index("household_id2").household_id.to_dict().items() <= updated_ids["2013-14"].items()


@_requires_data
def test_every_w2_target_is_a_w1_household(updated_ids):
    hh = _cover("2011-12", "sect_cover_hh_w1.dta")
    w1 = set(hh.household_id.map(format_id))
    assert set(updated_ids["2013-14"].values()) <= w1


@_requires_data
def test_every_w3_household_is_on_the_map(updated_ids):
    """The W3 map covers its household cover: nothing falls through unmapped."""
    hh = _cover("2015-16", "sect_cover_hh_w3.dta")
    assert set(hh.household_id2.map(format_id)) <= set(updated_ids["2015-16"])


@_requires_data
def test_panel_ii_does_not_walk(updated_ids):
    """No W4 or W5 household id moves: both covers key on the served ``i``."""
    for wave, name in (("2018-19", "sect_cover_hh_w4.dta"), ("2021-22", "sect_cover_hh_w5.dta")):
        ids = set(_cover(wave, name).household_id.map(format_id))
        assert ids
        moving = {k for k, v in updated_ids.get(wave, {}).items() if k != v}
        assert not (ids & moving), wave

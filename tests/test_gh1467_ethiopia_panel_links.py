"""W3 households the survey counts as ESS1, and the rule that links them (GH #1467).

The ESS3 BID: "the total number of ESS1 households with complete interviews in
ESS3 is 3,699" (PDF p16), and 3,726 at the post-planting visit.  48 of those
3,699, and 1 of the 27 agriculture-only households, are on no W2 household or
post-planting cover, so the chain through W2 cannot reach them.  They are linked
by rule: a rural or small-town W3 household off W2's map has W3's own
``household_id`` as its W1 id.  The rule rests on the BID's accounting; the rosters
do not separately confirm it (``Ethiopia/_/CONTENTS.org``).

The same regeneration records the links ``panel_ids`` used to leave out: W2 -> W1,
which were only in ``updated_ids``, and W5 -> W4, identities that move no served id.
W1 and W4 are baselines, so nothing crosses from W4 to W3.

Two tiers, as in ``test_gh1447_ethiopia_panel_crosswalk.py``: the committed JSON
alone, and (``requires_s3``) the covers read directly, by path, with nothing from
``Ethiopia/_/`` in the oracle.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

import lsms_library
from lsms_library.local_tools import IdWalkError, _walk_partition, format_id, get_dataframe, id_walk
from lsms_library.paths import countries_root

ETHIOPIA = countries_root() / "Ethiopia"
_requires_data = pytest.mark.requires_s3


@pytest.fixture(scope="module")
def links():
    with open(ETHIOPIA / "_" / "panel_ids.json") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def updated_ids():
    with open(ETHIOPIA / "_" / "updated_ids.json") as f:
        return json.load(f)


def _truncated(household_id2):
    return household_id2[:6] + household_id2[10:]


def _frame(wave, ids):
    index = pd.MultiIndex.from_tuples([(wave, i) for i in ids], names=["t", "i"])
    return pd.DataFrame({"x": range(len(ids))}, index=index)


# ---------------------------------------------------------------------------
# The committed JSON
# ---------------------------------------------------------------------------

def test_every_moving_w3_id_goes_to_its_truncated_id(updated_ids):
    moving = {k: v for k, v in updated_ids["2015-16"].items() if k != v}
    assert moving
    assert all(v == _truncated(k) for k, v in moving.items())


def test_the_rule_links_w3_households_off_the_w2_map(updated_ids):
    w2, w3 = updated_ids["2013-14"], updated_ids["2015-16"]
    assert {k for k, v in w3.items() if k != v and k not in w2}


def test_no_link_makes_a_target_occupied_or_a_chain(updated_ids):
    """GH #1245: single-pass ``id_walk`` needs targets no household of the wave holds."""
    for wave, mapping in updated_ids.items():
        moving = {k: v for k, v in mapping.items() if k != v}
        assert not set(moving.values()) & set(mapping), wave
        assert not _walk_partition(mapping)[2], wave


def test_w3_is_walked_exactly_once_and_a_mixed_frame_is_refused(updated_ids):
    w2, w3 = updated_ids["2013-14"], updated_ids["2015-16"]
    linked = {k: v for k, v in w3.items() if k != v and k not in w2}
    raw = _frame("2015-16", list(w3))
    once = id_walk(raw.copy(), updated_ids)
    assert list(once.index.get_level_values("i")) == list(w3.values())
    assert once.index.is_unique

    again = once.copy()
    again.attrs = {}
    pd.testing.assert_frame_equal(id_walk(again, updated_ids), once, check_like=False)

    k, v = sorted(linked.items())[0]
    other = next(key for key in linked if key != k)
    with pytest.raises(IdWalkError):
        id_walk(_frame("2015-16", [other, v]), updated_ids)


def test_panel_ids_and_updated_ids_agree_on_w3(links, updated_ids):
    for k, v in updated_ids["2015-16"].items():
        assert links[f"2015-16,{k}"].split(",")[1] == v


def test_w1_and_w4_are_baselines_with_nothing_recorded(links, updated_ids):
    for wave in ("2011-12", "2018-19"):
        assert updated_ids[wave] == {}
        assert not any(key.startswith(f"{wave},") for key in links)


def test_each_link_points_to_the_wave_before_it_in_its_panel(links):
    before = {"2013-14": "2011-12", "2015-16": "2013-14", "2021-22": "2018-19"}
    assert {(k.split(",")[0], v.split(",")[0]) for k, v in links.items()} == set(before.items())


def test_w2_links_are_the_w2_map(links, updated_ids):
    w2 = updated_ids["2013-14"]
    assert w2
    recorded = {k.split(",")[1]: v for k, v in links.items() if k.startswith("2013-14,")}
    assert recorded == {k: f"2011-12,{v}" for k, v in w2.items()}


def test_w5_links_are_identities(links, updated_ids):
    w5 = updated_ids["2021-22"]
    assert w5
    assert all(k == v for k, v in w5.items())
    recorded = {k.split(",")[1]: v for k, v in links.items() if k.startswith("2021-22,")}
    assert recorded == {k: f"2018-19,{k}" for k in w5}


# ---------------------------------------------------------------------------
# The covers, read directly: no script in the oracle
# ---------------------------------------------------------------------------

def _cover(wave, name):
    return get_dataframe(ETHIOPIA / wave / "Data" / name, convert_categoricals=False)


@pytest.fixture(scope="module")
def w1():
    """W1 household -> locality in W3's codes.  W1 labels small towns 0; W3 labels them 2."""
    hh = _cover("2011-12", "sect_cover_hh_w1.dta")
    return dict(zip(hh.household_id.map(format_id), hh.rural.map({1: 1, 0: 2})))


@pytest.fixture(scope="module")
def w2_links():
    """The W2 covers' W1 baselines: household cover, then the post-planting-only households."""
    hh = _cover("2013-14", "sect_cover_hh_w2.dta")[["household_id2", "household_id"]].map(format_id)
    pp = _cover("2013-14", "sect_cover_pp_w2.dta")[["household_id2", "household_id"]].map(format_id)
    pp = pp[~pp.household_id2.isin(hh.household_id2)].drop_duplicates("household_id2")
    both = pd.concat([hh, pp]).dropna()
    return both.set_index("household_id2").household_id.to_dict()


@pytest.fixture(scope="module")
def w3_nodes():
    """``i``, ``rural`` and W3's own ``household_id``, one row per household."""
    cols = ["household_id2", "household_id", "rural"]
    hh = _cover("2015-16", "sect_cover_hh_w3.dta")[cols]
    pp = _cover("2015-16", "sect_cover_pp_w3.dta")[cols]
    hh, pp = (pd.DataFrame({"i": frame.household_id2.map(format_id).values,
                            "household_id": frame.household_id.map(format_id).values,
                            "rural": frame.rural.values,
                            "cover": cover})
              for frame, cover in ((hh, "hh"), (pp, "pp")))
    pp = pp.dropna(subset=["i"])
    pp = pp[~pp.i.isin(hh.i)].drop_duplicates("i")
    return pd.concat([hh, pp], ignore_index=True)


@_requires_data
def test_the_w3_map_is_the_rule(updated_ids, w2_links, w3_nodes):
    """W2's baseline where W2 holds the household; else W3's own id if rural or small town."""
    def expected(row):
        if row.i in w2_links:
            return w2_links[row.i]
        if row.rural in (1, 2):
            return row.household_id
        return row.i
    want = {row.i: expected(row) for row in w3_nodes.itertuples()}
    assert want == updated_ids["2015-16"]


@_requires_data
def test_the_links_reproduce_the_bid_accounting(updated_ids, w3_nodes):
    """ESS3 BID PDF p16: 3,726 at post-planting, 3,699 complete, 1,255 urban re-interviewed."""
    w3 = updated_ids["2015-16"]
    moved = w3_nodes[[w3[i] != i for i in w3_nodes.i]]
    assert (moved.cover == "hh").sum() == 3699
    assert len(moved) == 3726
    assert len(w3_nodes) - len(moved) == 1255


@_requires_data
def test_a_large_town_household_is_never_linked_by_its_truncation(updated_ids, w1, w3_nodes):
    """Large towns entered at ESS2; a truncated id on W1 there is a coincidence of the format."""
    w3 = updated_ids["2015-16"]
    large = w3_nodes[w3_nodes.rural == 3]
    assert len(large) > 0
    assert large.household_id.isin(w1).any()
    assert all(w3[i] == i for i in large.i)


@_requires_data
def test_w3_household_id_is_the_truncated_id(w3_nodes):
    assert w3_nodes.household_id.eq(w3_nodes.i.map(_truncated)).all()


@_requires_data
def test_every_w3_target_is_a_w1_household_of_the_same_locality(updated_ids, w1, w3_nodes):
    """W1 and W3 record locality separately; their ``ea_id`` are both the id's own prefix."""
    w2, w3 = updated_ids["2013-14"], updated_ids["2015-16"]
    moved = w3_nodes[[w3[i] != i for i in w3_nodes.i]]
    assert moved.i.map(w3).map(w1).eq(moved.rural).all()
    rule = moved[~moved.i.isin(w2)]
    assert len(rule) > 0
    assert rule.household_id.is_unique


def _panel_ii_ids(wave, hh, pp):
    ids = set(_cover(wave, hh).household_id.map(format_id))
    return ids | (set(_cover(wave, pp).household_id.map(format_id)) - {None})


@_requires_data
def test_w5_links_are_the_households_on_both_waves_covers(updated_ids):
    w4 = _panel_ii_ids("2018-19", "sect_cover_hh_w4.dta", "sect_cover_pp_w4.dta")
    w5 = _panel_ii_ids("2021-22", "sect_cover_hh_w5.dta", "sect_cover_pp_w5.dta")
    assert set(updated_ids["2021-22"]) == w4 & w5


@_requires_data
def test_w5_links_reproduce_the_bid_balanced_panel(updated_ids):
    """ESPS-5 BID: 4,906 households in the balanced panel (PDF p43), 93 in 8 new EAs (p32)."""
    w5 = _panel_ii_ids("2021-22", "sect_cover_hh_w5.dta", "sect_cover_pp_w5.dta")
    linked = set(updated_ids["2021-22"])
    assert len(linked) == 4906
    new = w5 - linked
    assert len(new) == 93
    assert len({i[:15] for i in new}) == 8


@_requires_data
def test_regeneration_reproduces_the_committed_json(links, updated_ids):
    """``panel_ids.py`` is what ``make panel-ids`` runs; the JSON is its reviewed output."""
    code = ("import json, runpy, sys; "
            "g = runpy.run_path('panel_ids.py', run_name='__regen__'); "
            "json.dump({'links': {','.join(k): ','.join(v) for k, v in g['D'].data.items()}, "
            "'updated_ids': g['updated_ids']}, sys.stdout)")
    env = dict(os.environ, PYTHONPATH=str(Path(lsms_library.__file__).parents[1]))
    done = subprocess.run([sys.executable, "-W", "ignore", "-c", code], cwd=ETHIOPIA / "_", env=env,
                          capture_output=True, text=True, timeout=600)
    assert done.returncode == 0, done.stderr[-2000:]
    out = json.loads(done.stdout)
    assert out["links"] == links
    assert out["updated_ids"] == updated_ids

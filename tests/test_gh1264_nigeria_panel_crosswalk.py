"""Nigeria's committed panel crosswalk: all rounds, interviewed households, own covers (GH #1264).

The maintainer's ruling (2026-11-03), pinned here as RULES rather than counts:

(a) W1, W4-old and W5 are linked, ``old_new`` deciding the sample;
(b) a link needs the household recorded as interviewed at BOTH ends;
(c) each round's links come from that round's own cover.

``id_walk`` stays an identity, so served ``i`` does not depend on any of this.
Whether interview status gates ROSTER admission is GH #1444 and is not touched:
320201 and 30082 are served in 2016Q1 and are not linked there.

Three tiers: the committed JSON alone, ``panel_ids.py`` on invented tables, and
(``requires_s3``) the survey's own source files through the ordinary reader.
"""
from __future__ import annotations

import importlib.util
import json
import sys

import pandas as pd
import pytest

from lsms_library.local_tools import _walk_partition, id_walk, panel_ids
from lsms_library.paths import countries_root

NIGERIA = countries_root() / "Nigeria"
_requires_data = pytest.mark.requires_s3


@pytest.fixture(scope="module")
def script():
    """``panel_ids.py``, loaded without leaving its directory on ``sys.path``."""
    path = list(sys.path)
    try:
        spec = importlib.util.spec_from_file_location("nigeria_panel_ids", NIGERIA / "_" / "panel_ids.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path[:] = path
    return mod


@pytest.fixture(scope="module")
def rounds(script):
    return script.ROUNDS


@pytest.fixture(scope="module")
def links():
    """``{(t, i): (t0, i0)}`` from the committed JSON."""
    with open(NIGERIA / "_" / "panel_ids.json") as f:
        raw = json.load(f)
    return {tuple(k.split(",")): tuple(v.split(",")) for k, v in raw.items()}


@pytest.fixture(scope="module")
def updated_ids():
    with open(NIGERIA / "_" / "updated_ids.json") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# The committed JSON alone
# ---------------------------------------------------------------------------

def test_every_link_and_every_map_is_an_identity(links, updated_ids):
    assert links
    assert [k for k, v in links.items() if k[1] != v[1]] == []
    for wave, mapping in updated_ids.items():
        assert [k for k, v in mapping.items() if k != v] == [], wave


def test_id_walk_renames_nothing(updated_ids):
    """With every map an identity no id can move, so served ``i`` is untouched."""
    for wave, mapping in updated_ids.items():
        assert _walk_partition(mapping) == (frozenset(), frozenset(), frozenset()), wave
    ids = [sorted(m)[0] for m in updated_ids.values() if m]
    waves = [w for w, m in updated_ids.items() if m]
    frame = pd.DataFrame({"x": range(len(ids))},
                         index=pd.MultiIndex.from_arrays([waves, ids], names=["t", "i"]))
    pd.testing.assert_frame_equal(id_walk(frame.copy(), updated_ids).sort_index(), frame.sort_index())


def test_the_chain_is_every_round_to_the_round_before_it(links, rounds):
    """Distinct ``t`` per round; PH to its own PP, PP to the previous wave's PH."""
    predecessor = dict(zip(rounds[1:], rounds))
    assert {(k[0], v[0]) for k, v in links.items()} == set(predecessor.items())


def test_the_first_round_is_a_baseline_with_an_empty_map(links, rounds, updated_ids):
    assert not [k for k in links if k[0] == rounds[0]]
    assert updated_ids[rounds[0]] == {}


def test_updated_ids_holds_exactly_the_linked_households(links, rounds, updated_ids):
    assert list(updated_ids) == rounds
    for wave, mapping in updated_ids.items():
        assert set(mapping) == {k[1] for k in links if k[0] == wave}, wave


@pytest.mark.parametrize("hhid", ["320201", "30082"])
def test_a_roster_without_a_recorded_interview_is_not_linked_there(links, hhid):
    """Served in 2016Q1 (GH #1120 D1, GH #1444 open) and simply not linked."""
    assert ("2016Q1", hhid) not in links
    assert ("2018Q3", hhid) not in links


def test_no_wave_declares_a_second_copy_of_the_wiring():
    """A ``panel_ids:`` block is what ``LSMS_BUILD_BACKEND=make`` would read, and
    it can express neither own-cover rounds nor an interview gate."""
    declaring = [p.parent.parent.name for p in NIGERIA.glob("*/_/data_info.yml")
                 if any(line.startswith("panel_ids:") for line in p.read_text().splitlines())]
    assert declaring == []


# ---------------------------------------------------------------------------
# panel_ids.py on invented tables
# ---------------------------------------------------------------------------

def _tables(script, overrides=None):
    """Household ``a`` of the old sample interviewed everywhere; ``n`` of the
    refresh sample interviewed from W4 on."""
    tables = {}
    for rnd in script.ROUNDS:
        rows = {"a": (True, "old")}
        if rnd >= "2018Q3":
            rows["n"] = (True, "new")
        tables[rnd] = pd.DataFrame.from_dict(rows, orient="index",
                                             columns=["interviewed", "sample"])
    for rnd, frame in (overrides or {}).items():
        tables[rnd] = frame
    return tables


def _table(**rows):
    return pd.DataFrame.from_dict(rows, orient="index", columns=["interviewed", "sample"])


def _built(script, tables):
    return panel_ids(script.build_frame(tables))


def test_the_refresh_sample_starts_a_chain_at_its_own_draw(script):
    D, updated = _built(script, _tables(script))
    assert ("2018Q3", "n") not in D.data
    assert D.data[("2019Q1", "n")] == ("2018Q3", "n")
    assert D.data[("2018Q3", "a")] == ("2016Q1", "a")
    assert "n" not in updated["2018Q3"]
    assert updated["2010Q3"] == {}


@pytest.mark.parametrize("cur, target", [(True, False), (False, True), (False, False)])
def test_a_link_needs_an_interview_at_both_ends(script, cur, target):
    tables = _tables(script, {"2013Q1": _table(a=(cur, "old")),
                              "2012Q3": _table(a=(target, "old"))})
    D, updated = _built(script, tables)
    assert ("2013Q1", "a") not in D.data
    assert "a" not in updated["2013Q1"]


def test_a_household_off_the_target_cover_is_not_linked(script):
    """W2's PH-only households: on the PH cover and interviewed, not on the PP cover."""
    tables = _tables(script, {"2010Q3": _table(a=(True, "old"), b=(True, "old")),
                              "2013Q1": _table(a=(True, "old"), b=(True, "old"))})
    D, _ = _built(script, tables)
    assert ("2013Q1", "a") in D.data
    assert ("2013Q1", "b") not in D.data


def test_a_shared_hhid_alone_is_not_a_link(script, monkeypatch):
    """``old_new`` decides even with the refusal out of the way."""
    monkeypatch.setattr(script, "_refuse_bare_hhid_matches", lambda tables: None)
    tables = _tables(script, {"2016Q1": _table(a=(True, "old"), n=(True, "old"))})
    D, _ = _built(script, tables)
    assert ("2018Q3", "a") in D.data
    assert ("2018Q3", "n") not in D.data


def test_a_gap_is_not_bridged(script):
    """One chain: no link skips the round the household missed."""
    D, _ = _built(script, _tables(script, {"2013Q1": _table(a=(False, "old"))}))
    assert ("2015Q3", "a") not in D.data
    assert D.data[("2016Q1", "a")] == ("2015Q3", "a")


@pytest.mark.parametrize("overrides", [
    # A refresh hhid already used by the original sample: a bare hhid match
    # would join two households.
    {"2013Q1": _table(a=(True, "old"), n=(True, "old"))},
    # A household that changes sample between W4 and W5.
    {"2023Q3": _table(a=(True, "new"), n=(True, "new"))},
    # A W5 household W4 never saw.
    {"2023Q3": _table(a=(True, "old"), n=(True, "new"), q=(True, "new"))},
    # An "old" household the original sample never saw.
    {"2018Q3": _table(a=(True, "old"), n=(True, "new"), q=(True, "old"))},
], ids=["refresh-reuses-an-old-hhid", "sample-flips", "w5-new-unseen", "w4-old-unseen"])
def test_an_hhid_naming_two_households_is_refused(script, overrides):
    with pytest.raises(ValueError):
        script.build_frame(_tables(script, overrides))


def test_an_unclassified_status_is_refused_not_guessed(script):
    values = pd.Series(["1. COMPLETE", "9. SOMETHING NEW"])
    with pytest.raises(ValueError, match="unclassified"):
        script._classify(values, script._INTERVIEW_RESULT, script._code, "x")
    with pytest.raises(ValueError, match="no numeric code"):
        script._code(float("nan"))
    with pytest.raises(ValueError, match="unclassified"):
        script._classify(pd.Series(["completed", None]), script._W1_RESULT, script._label, "x")


@pytest.mark.parametrize("value, code", [(1, 1), (5.0, 5), (91, 91), ("1. COMPLETE", 1),
                                         ("91. Not interviewed for unknown reason", 91)])
def test_status_codes_read_from_numbers_and_labels(script, value, code):
    assert script._code(value) == code


@pytest.mark.parametrize("table, interviewed", [
    ("_HHTRACK_STATUS", {1, 5}),
    ("_INTERVIEW_RESULT", {1, 2, 3}),
    ("_W1_RESULT", {"completed", "partially completed"}),
])
def test_the_interviewed_codes(script, table, interviewed):
    assert {k for k, v in getattr(script, table).items() if v} == interviewed


# ---------------------------------------------------------------------------
# The survey's own files
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def tables(script):
    return {rnd: script.round_table(rnd) for rnd in script.ROUNDS}


def _interviewed(tables, rnd):
    t = tables[rnd]
    return set(t.index[t["interviewed"]])


def _read(rel, **kw):
    from lsms_library.local_tools import format_id, get_dataframe
    df = get_dataframe(str(NIGERIA / rel), **kw)
    return df.assign(i=df["hhid"].apply(format_id)).set_index("i")


def _track_codes(df, column, script):
    return df[column].astype(object).map(script._code)


@_requires_data
def test_the_committed_json_is_what_the_script_builds(script, tables, links, updated_ids):
    D, updated = panel_ids(script.build_frame(tables))
    assert {(k[0], str(k[1])): (v[0], str(v[1])) for k, v in D.data.items()} == links
    assert updated == updated_ids


@_requires_data
def test_links_are_exactly_the_households_interviewed_at_both_ends(script, tables, links):
    """Both directions: soundness alone would let links be silently dropped."""
    expected = set()
    for rnd, before in zip(script.ROUNDS[1:], script.ROUNDS):
        table, prior = tables[rnd], tables[before]
        both = _interviewed(tables, rnd) & _interviewed(tables, before)
        expected |= {(rnd, i) for i in both
                     if table.loc[i, "sample"] == prior.loc[i, "sample"]}
    assert set(links) == expected


@_requires_data
def test_the_w4_refresh_is_not_linked_into_w1_to_w3(tables, links):
    w4 = tables["2018Q3"]
    refresh = set(w4.index[w4["sample"] == "new"])
    assert refresh
    assert not [i for t, i in links if t == "2018Q3" and i in refresh]
    assert not [i for (t, i), (t0, _) in links.items() if i in refresh and t0 < "2018Q3"]


@_requires_data
def test_refresh_ids_share_nothing_with_the_earlier_covers(tables):
    """The hhid RANGES overlap, the ids do not, which is why ``old_new`` decides."""
    w4 = tables["2018Q3"]
    refresh = set(w4.index[w4["sample"] == "new"])
    for rnd in ("2010Q3", "2011Q1", "2012Q3", "2013Q1", "2015Q3", "2016Q1"):
        assert not refresh & set(tables[rnd].index), rnd


@_requires_data
def test_w5_nests_in_w4(tables, links):
    w4 = tables["2018Q3"]["sample"]
    for rnd in ("2023Q3", "2024Q1"):
        w5 = tables[rnd]["sample"]
        assert (w4.reindex(w5.index) == w5).all(), rnd
    assert [k for k in links if k[0] == "2023Q3"]


@_requires_data
def test_hhtrack_files_agree_and_w3_labels_code_91(script):
    w2 = _read("2012-13/Data/HHTrack.csv")
    w3 = _read("2015-16/Data/HHTrack.dta")
    for column in ("hhstatus_w1v1", "hhstatus_w1v2", "hhstatus_w2v1", "hhstatus_w2v2"):
        assert (_track_codes(w2, column, script).sort_index()
                == _track_codes(w3, column, script).sort_index()).all(), column
    labels = set(w3["hhstatus_w2v1"].astype(str))
    assert "91. Not interviewed for unknown reason" in labels
    assert "5. Tracked (and interviewed)" in labels


@_requires_data
@pytest.mark.parametrize("rnd, column", [("2010Q3", "hhstatus_w1v1"), ("2011Q1", "hhstatus_w1v2")])
def test_w1_own_cover_names_the_households_hhtrack_names(script, tables, rnd, column):
    """HHTrack does record W1; the own-cover reading loses nothing against it."""
    codes = _track_codes(_read("2015-16/Data/HHTrack.dta"), column, script)
    assert set(codes.index[codes.isin([1, 5])]) == _interviewed(tables, rnd)


@_requires_data
@pytest.mark.parametrize("rnd, column", [("2012Q3", "hhstatus_w2v1"), ("2013Q1", "hhstatus_w2v2"),
                                         ("2015Q3", "hhstatus_w3v1"), ("2016Q1", "hhstatus_w3v2")])
def test_own_cover_loses_no_interviewed_household(script, tables, rnd, column):
    """(c) reads the round's cover, so a household HHTrack calls interviewed and
    the cover omits would vanish; none does."""
    codes = _track_codes(_read("2015-16/Data/HHTrack.dta"), column, script)
    assert set(codes.index[codes.isin([1, 5])]) == _interviewed(tables, rnd)


@_requires_data
@pytest.mark.parametrize("cover, result, column", [
    ("2010-11/Data/Post Planting Wave 1/Household/secta_plantingw1.dta", "saq22a", "hhstatus_w1v1"),
    ("2010-11/Data/Post Harvest Wave 1/Household/secta_harvestw1.dta", "sq1", "hhstatus_w1v2"),
    ("2012-13/Data/Post Planting Wave 2/Household/secta_plantingw2.dta", "saq22a", "hhstatus_w2v1"),
    ("2012-13/Data/Post Harvest Wave 2/Household/secta_harvestw2.dta", "saq1", "hhstatus_w2v2"),
    ("2015-16/Data/secta_plantingw3.dta", "saq22a", "hhstatus_w3v1"),
    ("2015-16/Data/secta_harvestw3.dta", "saq22a", "hhstatus_w3v2"),
])
def test_a_partial_interview_is_an_interview_to_hhtrack(script, cover, result, column):
    """The premise for counting ``interview_result`` 2 and 3 as interviewed."""
    src = _read(cover)
    partial = src.index[src[result].astype(str).str.contains("partially", case=False)]
    assert len(partial)
    codes = _track_codes(_read("2015-16/Data/HHTrack.dta"), column, script)
    assert codes.reindex(partial).isin([1, 5]).all()

"""Tests for the declarative no-v-join policy (GH #436, code/config separation).

Item 2 of the #436 loop moved the hardcoded ``_no_v_join`` set out of
``country._finalize_result`` and into ``lsms_library/data_info.yml`` (the
canonical-schema YAML).  ``_finalize_result`` now consults
``_no_v_join_tables()``, which derives the set from:

  * ``Index Info > index_info`` -- any table whose canonical index omits ``v``;
  * ``Join v from sample > skip_extra`` -- framework-special / not-yet-declared
    tables.

These tests pin (a) that the shipped YAML reproduces the *exact* historical
set (default-preserving), (b) the parsing rules, and (c) the version-skew
fallback so v-join can never silently switch on for a table that never
carried v.
"""
import lsms_library.country as C
from lsms_library.country import (
    _NO_V_JOIN_FALLBACK,
    _compute_no_v_join,
    _no_v_join_tables,
)


# --------------------------------------------------------------------------
# Default-preserving: the shipped data_info.yml must reproduce the historical
# hardcoded set exactly.  This is the load-bearing regression guard.
# --------------------------------------------------------------------------
def test_shipped_yaml_reproduces_historical_set():
    assert set(_no_v_join_tables()) == set(_NO_V_JOIN_FALLBACK)


def test_historical_members_present():
    # The eight tables the framework excluded before #436.
    for name in (
        "sample", "cluster_features", "panel_ids", "updated_ids",
        "shocks", "assets", "livestock", "income",
    ):
        assert name in _no_v_join_tables(), name


def test_v_bearing_tables_not_skipped():
    # Tables whose canonical index includes v must still get the join.
    skip = _no_v_join_tables()
    for name in (
        "household_roster", "food_acquired", "plot_features",
        "interview_date", "individual_education", "household_characteristics",
    ):
        assert name not in skip, name


# --------------------------------------------------------------------------
# Parsing rules, exercised on synthetic dicts via the pure helper.
# --------------------------------------------------------------------------
def test_index_info_without_v_is_skipped():
    data = {
        "Index Info": {"index_info": {
            "with_v": "(t, v, i, j)",
            "no_v": "(t, i, Foo)",
        }},
        "Join v from sample": {"skip_extra": []},
    }
    skip = _compute_no_v_join(data)
    assert "no_v" in skip
    assert "with_v" not in skip


def test_skip_extra_is_unioned_in():
    data = {
        "Index Info": {"index_info": {"with_v": "(t, v, i)"}},
        "Join v from sample": {"skip_extra": ["mytable", "other"]},
    }
    skip = _compute_no_v_join(data)
    assert {"mytable", "other"} <= skip
    assert "with_v" not in skip


def test_malformed_index_specs_are_ignored():
    # Non-string specs must not crash the parser.
    data = {
        "Index Info": {"index_info": {"bad": ["t", "i"], "no_v": "(t, i)"}},
        "Join v from sample": {"skip_extra": []},
    }
    skip = _compute_no_v_join(data)
    assert "no_v" in skip
    assert "bad" not in skip  # malformed -> not parsed, not skipped


# --------------------------------------------------------------------------
# Version-skew fallback: an older/foreign data_info.yml that lacks the
# declarative section must NOT re-enable the join on historically-excluded
# tables.
# --------------------------------------------------------------------------
def test_missing_section_falls_back_to_historical_set():
    assert _compute_no_v_join({"Index Info": {"index_info": {}}}) == _NO_V_JOIN_FALLBACK


def test_empty_or_nonsense_input_falls_back():
    assert _compute_no_v_join({}) == _NO_V_JOIN_FALLBACK
    assert _compute_no_v_join({"Join v from sample": "not-a-dict"}) == _NO_V_JOIN_FALLBACK


def test_empty_skip_extra_still_uses_index_info():
    # Section present but skip_extra empty: index_info still drives exclusion,
    # so this is NOT the fallback path.
    data = {
        "Index Info": {"index_info": {"no_v": "(t, i)"}},
        "Join v from sample": {"skip_extra": []},
    }
    skip = _compute_no_v_join(data)
    assert skip == frozenset({"no_v"})


# --------------------------------------------------------------------------
# Behavioral: a country with sample + a v-free table must not gain a v level.
# Uses synthetic frames + the real _finalize_result path (no I/O), mirroring
# tests/test_join_v_silent_skip_warn.py.
# --------------------------------------------------------------------------
def test_finalize_skips_join_for_no_v_table(monkeypatch):
    import pandas as pd
    import lsms_library as ll

    c = ll.Country("Uganda")
    # Synthetic sample (i, t) -> v, pre-populated so no real I/O occurs.
    sample = pd.DataFrame(
        {"v": ["c1", "c2"]},
        index=pd.MultiIndex.from_tuples([("h1", "2019"), ("h2", "2019")],
                                        names=["i", "t"]),
    )
    c._sample_v_cache = sample

    called = {"n": 0}
    real_join = c._join_v_from_sample

    def spy(df):
        called["n"] += 1
        return real_join(df)

    monkeypatch.setattr(c, "_join_v_from_sample", spy)

    # A frame for an excluded table ('shocks'): (i, t) but canonical has no v.
    shocks = pd.DataFrame(
        {"AffectedIncome": [True, False]},
        index=pd.MultiIndex.from_tuples([("h1", "2019"), ("h2", "2019")],
                                        names=["i", "t"]),
    )
    out = c._finalize_result(shocks.copy(), {}, "shocks")
    assert called["n"] == 0, "v-join must be skipped for 'shocks'"
    assert "v" not in (out.index.names if hasattr(out, "index") else [])

"""Tests for CONTENTS.org discovery through the API."""
from __future__ import annotations

import pytest

from lsms_library import notes as N

DOC = """#+TITLE: Example
#+TODO: TODO WAITING | DONE RETIRED

* Sampling Design
intro
** Weights
weight detail
** Strata
strata detail
* WAITING Missing quantities (#112)
still open
* WRONG WAVE: =*lvs5.tab= is 1989 data
not a todo state
* DONE food_acquired.py
finished
"""


def test_headlines_parse_with_levels_and_states():
    h = N.parse(DOC)
    assert [(x.level, x.keyword, x.text) for x in h] == [
        (1, None, "Sampling Design"),
        (2, None, "Weights"),
        (2, None, "Strata"),
        (1, "WAITING", "Missing quantities (#112)"),
        (1, None, "WRONG WAVE: =*lvs5.tab= is 1989 data"),
        (1, "DONE", "food_acquired.py"),
    ]


def test_a_capitalised_word_is_not_a_state_unless_declared():
    """`WRONG WAVE:` must not parse as a TODO keyword.

    Measured corpus-wide, the leading all-caps word of a headline is a real
    state in 124 cases and is NOT one in ~30 others -- GH, WARNING, TLSS,
    ENV, GPS, BID, WHERE, SOURCE, PSU.  A permissive rule invents a state for
    every one of those, so keywords are an allowlist.
    """
    wrong = [h for h in N.parse(DOC) if h.text.startswith("WRONG WAVE")]
    assert len(wrong) == 1 and wrong[0].keyword is None


def test_file_declared_keywords_win_over_the_default():
    assert N.todo_keywords(DOC) == {"TODO", "WAITING", "DONE", "RETIRED"}
    assert N.todo_keywords("* TODO x") == N.DEFAULT_STATES
    assert "RETIRED" not in N.DEFAULT_STATES


def test_topic_returns_the_whole_subtree():
    out = N.extract(DOC, topic="sampling")
    assert "** Weights" in out and "strata detail" in out
    assert "Missing quantities" not in out, "subtree ran past its level-1 sibling"


def test_topic_matches_headlines_not_body():
    assert N.extract(DOC, topic="strata detail") == ""
    assert N.extract(DOC, topic="Strata") != ""


def test_parent_and_child_both_matching_returns_the_parent_once():
    doc = "* Sampling\na\n** Sampling detail\nb\n* Other\nc\n"
    out = N.extract(doc, topic="sampling")
    assert out.count("** Sampling detail") == 1
    assert "* Other" not in out


def test_state_filter():
    out = N.extract(DOC, state="WAITING")
    assert "Missing quantities" in out and "still open" in out
    assert "Sampling Design" not in out
    assert N.extract(DOC, state="CANCELLED") == ""


def test_no_filter_returns_the_document_unchanged():
    assert N.extract(DOC) == DOC


@pytest.mark.parametrize("name", ["GhanaLSS", "South Africa"])
def test_country_api_including_a_space_in_the_name(name):
    """Three countries have spaces in their directory names."""
    ll = pytest.importorskip("lsms_library")
    c = ll.Country(name)
    if not c.notes_path.exists():
        pytest.skip(f"{name} has no CONTENTS.org")
    topics = c.note_topics
    assert topics and all(len(t) == 3 for t in topics)
    assert c.notes() == c.notes_path.read_text()


def test_missing_notes_warns_and_returns_empty(monkeypatch, tmp_path):
    ll = pytest.importorskip("lsms_library")
    c = ll.Country("GhanaLSS")
    monkeypatch.setattr(type(c), "notes_path",
                        property(lambda self: tmp_path / "nope.org"))
    with pytest.warns(UserWarning, match="No CONTENTS.org"):
        assert c.notes() == ""
    with pytest.warns(UserWarning, match="No CONTENTS.org"):
        assert c.note_topics == []


def test_features_is_a_synonym_for_data_scheme():
    """The library says "feature" nearly everywhere; the attribute said
    "data_scheme" after the file it reads.  Both must answer."""
    ll = pytest.importorskip("lsms_library")
    for name in ("Uganda", "South Africa"):
        c = ll.Country(name)
        assert c.features == c.data_scheme
        assert isinstance(c.features, list)


def test_wave_features_is_a_synonym_too():
    """Waves declare their own tables, and answer to the same name."""
    ll = pytest.importorskip("lsms_library")
    c = ll.Country("Uganda")
    w = c[c.waves[0]]
    assert w.features == w.data_scheme
    assert isinstance(w.features, list)


def test_country_and_wave_feature_lists_may_legitimately_differ():
    """The comparison the synonym is meant to make easy.

    A country surfaces runtime-derived tables (food_expenditures,
    household_characteristics, ...) that no wave declares, and a table wired
    for some waves and not others is normal.  Measured on Uganda 2013-14:
    country 28, wave 23.
    """
    ll = pytest.importorskip("lsms_library")
    c = ll.Country("Uganda")
    w = c["2013-14"]
    only_country = set(c.features) - set(w.features)
    assert only_country, "expected derived tables to be country-level only"
    assert "food_expenditures" in only_country


def test_features_is_a_synonym_not_a_replacement():
    """`data_scheme` stays: country scripts and published notebooks use it."""
    ll = pytest.importorskip("lsms_library")
    assert isinstance(getattr(type(ll.Country("Uganda")), "data_scheme"), property)
    assert isinstance(getattr(type(ll.Country("Uganda")), "features"), property)

"""Nigeria ``sampling.yml``: the "N of M linked households" a note states are the
committed crosswalk's (GH #1448).

The notes used to quote hhid overlaps of covers, which stopped being the panel when
GH #1264 rebuilt ``_/panel_ids.json`` to link interviewed households from each round's
own cover.  Each boundary is recomputed here from that file, with ``v`` read off
``sample()`` at the link's two ends, so the next crosswalk change fails this test
rather than leaving the notes behind.
"""
from __future__ import annotations

import json
import re
import warnings

import pytest

from lsms_library.paths import countries_root
from lsms_library.sampling import _first_sentence, sampling_records

NIGERIA = countries_root() / "Nigeria" / "_"
LINKED = re.compile(r"([\d,]+) of\s+([\d,]+) linked households")


def _links():
    """``{round: [(i, previous round, previous i), ...]}`` from the committed JSON."""
    links: dict[str, list[tuple[str, str, str]]] = {}
    for key, target in json.loads((NIGERIA / "panel_ids.json").read_text()).items():
        t, i = key.split(",")
        previous, previous_i = target.split(",")
        links.setdefault(t, []).append((i, previous, previous_i))
    return links


def _note(record):
    return " ".join(record["panel"]["cluster_id_note"].split())


def _stated(note):
    found = LINKED.findall(note)
    assert len(found) == 1, note
    return tuple(int(x.replace(",", "")) for x in found[0])


def test_every_boundary_counts_the_crosswalk_links():
    """Data-free: the denominator is the number of links the JSON holds."""
    links, records = _links(), sampling_records("Nigeria")
    assert set(records) == set(links)
    for t, record in records.items():
        kept, linked = _stated(_note(record))
        assert linked == len(links[t]), t
        assert kept <= linked, t
        assert "GH #1264 crosswalk" in _note(record), t


@pytest.mark.parametrize("t, renumbered", [("2018Q3", "ONE long-panel EA"), ("2023Q3", "12 EAs")])
def test_site_c_quotes_the_renumbering_not_a_panel_count(t, renumbered):
    """Site C prints the first sentence of a boundary it warns about, so that sentence
    says which EAs were renumbered, and carries no count that depends on the links."""
    record = sampling_records("Nigeria")[t]
    assert record.cluster_id_continuity == "wave-scoped"
    sentence = _first_sentence(record["panel"]["cluster_id_note"])
    assert sentence.startswith(f"Partial: {renumbered}")
    assert sentence.endswith("every other EA keeps its id.")
    assert not LINKED.search(sentence)


@pytest.mark.requires_s3
def test_the_notes_state_what_sample_and_the_crosswalk_say():
    """Every link's two ends against ``sample()``: kept, and for each change whether
    it is a mover's singleton or a real EA, which the two boundaries that declare
    ``wave-scoped`` are the only ones allowed to hold."""
    import lsms_library as ll

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        v = ll.Country("Nigeria").sample()["v"].astype("string")
    v = {(i, t): x for (i, t), x in v.items()}
    records = sampling_records("Nigeria")
    for t, links in _links().items():
        kept = moved = real = 0
        for i, previous, previous_i in links:
            now, then = v[(i, t)], v[(previous_i, previous)]
            if now == then:
                kept += 1
            elif now.startswith("moved-") or then.startswith("moved-"):
                moved += 1
            else:
                real += 1
        note = _note(records[t])
        assert _stated(note) == (kept, len(links)), t
        assert kept + moved + real == len(links)
        if records[t].cluster_id_continuity == "stable":
            assert real == 0, t
        else:
            assert f"{moved} are movers" in note, t
            assert f"{real} are" in note, t

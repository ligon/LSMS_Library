"""GH #1283: the held release of a SECONDARY catalog entry, per entry.

A wave holding two WB entries (Burkina Faso 2021-22 after 6277 lands; Malawi
2016-17 / 2019-20) used to record the held release of its primary entry only
(``#+CATALOG_IDNO:`` / ``#+CATALOG_DOI:`` are scalars), so ``discover_waves``
could never call a secondary entry stale.  Ruling (@ligon, 2026-10-23),
option (b), secondary entries only -- the primary keeps its scalars::

    #+CATALOG_HELD:   <catalog_id> <idno> [<doi>]
    #+CATALOG_LATEST: <catalog_id> <idno> <first-seen> [<doi>]

#1114 / #1128 applied per entry: only a demonstrated complete acquisition
moves ``HELD`` (the backfill never writes it); the backfill writes and clears
``LATEST``.  The parser refuses an id ``#+CATALOG_ID:`` does not list, a
``HELD`` / ``LATEST`` line naming the primary, and duplicates.

Synthetic records, zips and catalog rows only; no network.
"""

from __future__ import annotations

import datetime
import importlib.util
import os
from pathlib import Path

import pytest

from lsms_library import data_access as da
from lsms_library import provenance as pv
from tests.test_gh1276_acquisition_subdir import (  # noqa: F401 (fixture)
    COUNTRY, CROSS_FILES, CROSS_ID, CROSS_IDNO, PANEL_ID, PANEL_IDNO, URL,
    WAVE, _add_wave, _age_source, _data, _held, _record, _snapshot, _source,
    _zip, wb)

V02 = "BFA_2021_EHCVM-2_v02_M"
DOI1 = "https://doi.org/10.48529/synthetic-cross-v01"
DOI2 = "https://doi.org/10.48529/synthetic-cross-v02"
PANEL_DOI = "https://doi.org/10.48529/synthetic-panel"
SUBDIRS = [f"{PANEL_ID} Panel", f"{CROSS_ID} Cross_Section"]


def _text(held=(), latest=(), ids=(PANEL_ID, CROSS_ID), subdirs=SUBDIRS):
    """A two-entry record with *held* / *latest* lines, in render order."""
    text = _record(list(ids), subdirs, list(held))
    if latest:
        anchor = "#+CATALOG_IDNO:"
        text = text.replace(anchor, "".join(f"#+CATALOG_LATEST: {line}\n"
                                            for line in latest) + anchor, 1)
    return text


def _parse(text):
    return pv.parse_source_org(text, COUNTRY, WAVE)


# ---------------------------------------------------------------------------
# Parse, render, validate
# ---------------------------------------------------------------------------

class TestRecord:

    def test_parse_render_is_idempotent(self):
        text = _text([f"{CROSS_ID} {CROSS_IDNO} {DOI1}"],
                     [f"{CROSS_ID} {V02} 2026-10-01 {DOI2}"])
        prov = _parse(text)
        assert prov.catalog_held == [f"{CROSS_ID} {CROSS_IDNO} {DOI1}"]
        assert prov.catalog_latest == [f"{CROSS_ID} {V02} 2026-10-01 {DOI2}"]
        assert pv.render_source_org(prov) == text
        assert _parse(pv.render_source_org(prov)) == prov
        assert not {"CATALOG_HELD", "CATALOG_LATEST"} & set(prov.extra)

    def test_a_bad_line_survives_parse_and_render_and_is_refused_on_use(self):
        text = _text(["9999 X_v01_M", f"{PANEL_ID} {PANEL_IDNO}"])
        prov = _parse(text)
        assert pv.render_source_org(prov) == text
        with pytest.raises(pv.CatalogEntryError):
            prov.validate_entry_lines()

    @pytest.mark.parametrize("held,latest,subdirs,message", [
        (["9999 X_v01_M"], [], SUBDIRS, "does not list"),
        ([], ["9999 X_v02_M 2026-10-01"], SUBDIRS, "does not list"),
        ([], [], [*SUBDIRS, "9999 Other"], "does not list"),
        ([f"{PANEL_ID} {PANEL_IDNO}"], [], SUBDIRS, "names the primary"),
        ([], [f"{PANEL_ID} X_v05_M 2026-10-01"], SUBDIRS, "names the primary"),
        ([f"{CROSS_ID} {CROSS_IDNO}", f"{CROSS_ID} {CROSS_IDNO}"], [], SUBDIRS,
         "names 6277 twice"),
        ([], [f"{CROSS_ID} {V02} 2026-10-01"] * 2, SUBDIRS, "names 6277 twice"),
        ([], [], [*SUBDIRS, f"{CROSS_ID} Cross_Section"], "names 6277 twice"),
        ([f"{CROSS_ID}"], [], SUBDIRS, "is not"),
        ([f"{CROSS_ID} {CROSS_IDNO} {DOI1} extra"], [], SUBDIRS, "is not"),
        (["abc X_v01_M"], [], SUBDIRS, "is not"),
        ([], [f"{CROSS_ID} {V02}"], SUBDIRS, "is not"),
        ([], [f"{CROSS_ID} {V02} 23/10/2026"], SUBDIRS, "YYYY-MM-DD"),
        # Red team r2:
        ([f"{CROSS_ID} {CROSS_IDNO}"], [f"{CROSS_ID} {V02} 2026-13-45"],
         SUBDIRS, "YYYY-MM-DD"),                      # not a real date
        # ISO 8601 forms date.fromisoformat accepts (Python 3.11) that are
        # not the YYYY-MM-DD the format names:
        ([f"{CROSS_ID} {CROSS_IDNO}"], [f"{CROSS_ID} {V02} 20261001"],
         SUBDIRS, "YYYY-MM-DD"),
        ([f"{CROSS_ID} {CROSS_IDNO}"], [f"{CROSS_ID} {V02} 2026-W40-1"],
         SUBDIRS, "YYYY-MM-DD"),
        ([], [f"{CROSS_ID} {V02} 2026-10-01"], SUBDIRS,
         "nothing is held"),                          # nothing held to be ahead of
        ([f"{CROSS_ID} {CROSS_IDNO}"], [f"{CROSS_ID} {CROSS_IDNO} 2026-10-01"],
         SUBDIRS, "already holds"),                   # the catalog is not ahead
    ])
    def test_validation_refuses(self, held, latest, subdirs, message):
        prov = _parse(_text(held, latest, subdirs=subdirs))
        with pytest.raises(pv.CatalogEntryError, match=message):
            prov.validate_entry_lines()
        with pytest.raises(pv.CatalogEntryError):
            prov.held_release_of(CROSS_ID)

    def test_held_release_of_answers_for_any_listed_entry(self):
        prov = _parse(_text([f"{CROSS_ID} {CROSS_IDNO} {DOI1}"],
                            [f"{CROSS_ID} {V02} 2026-10-01"]))
        assert prov.held_release_of(PANEL_ID) == pv.HeldRelease(
            PANEL_ID, PANEL_IDNO, PANEL_DOI)                # the scalars
        assert prov.held_release_of(CROSS_ID) == pv.HeldRelease(
            CROSS_ID, CROSS_IDNO, DOI1)                     # the line
        assert prov.latest_release_of(CROSS_ID) == pv.LatestRelease(
            CROSS_ID, V02, "2026-10-01", None)
        assert prov.latest_release_of(PANEL_ID) is None
        with pytest.raises(pv.CatalogEntryError, match="does not list"):
            prov.held_release_of("9999")
        unheld = _parse(_text())
        assert unheld.held_release_of(CROSS_ID) is None

    def test_the_primary_keeps_its_scalars(self):
        prov = _parse(_text([f"{CROSS_ID} {CROSS_IDNO}"]))
        assert (prov.idno, prov.doi) == (PANEL_IDNO, PANEL_DOI)
        assert prov.held_releases() == {
            CROSS_ID: pv.HeldRelease(CROSS_ID, CROSS_IDNO, None)}

    def test_replacing_the_primary_drops_every_per_entry_line(self):
        prov = _parse(_text([f"{CROSS_ID} {CROSS_IDNO}"],
                            [f"{CROSS_ID} {V02} 2026-10-01"]))
        other = pv.acquired_release(prov, country=COUNTRY, wave=WAVE,
                                    catalog_id="9999", idno="X_v01_M")
        assert (other.catalog_subdirs, other.catalog_held,
                other.catalog_latest) == ([], [], [])
        same = pv.acquired_release(prov, country=COUNTRY, wave=WAVE,
                                   catalog_id=PANEL_ID, idno=PANEL_IDNO)
        assert same.catalog_held == prov.catalog_held
        assert same.catalog_latest == prov.catalog_latest


class TestHeldSecondaryEntry:

    def test_writes_the_held_line_for_a_new_secondary(self):
        prov = _parse(_record([PANEL_ID], [f"{PANEL_ID} Panel"]))
        new = pv.held_secondary_entry(prov, CROSS_ID, CROSS_IDNO, DOI1)
        assert new.catalog_ids == [PANEL_ID, CROSS_ID]
        assert new.catalog_held == [f"{CROSS_ID} {CROSS_IDNO} {DOI1}"]
        assert (new.idno, new.doi) == (prov.idno, prov.doi)

    def test_updates_in_place_and_clears_only_its_own_latest(self):
        other = "7777"
        prov = _parse(_text(
            [f"{other} O_v01_M", f"{CROSS_ID} {CROSS_IDNO} {DOI1}"],
            [f"{other} O_v02_M 2026-09-01", f"{CROSS_ID} {V02} 2026-10-01 {DOI2}"],
            ids=(PANEL_ID, other, CROSS_ID), subdirs=SUBDIRS))
        new = pv.held_secondary_entry(prov, CROSS_ID, V02)
        assert new.catalog_held == [f"{other} O_v01_M", f"{CROSS_ID} {V02} {DOI2}"]
        assert new.catalog_latest == [f"{other} O_v02_M 2026-09-01"]

    @pytest.mark.parametrize("idno,doi,expected", [
        (V02, None, DOI2),              # the queued latest's DOI describes V02
        (CROSS_IDNO, None, DOI1),       # the held line's DOI describes v01
        ("BFA_2021_EHCVM-2_v03_M", None, None),     # nothing describes v03
        (V02, "https://doi.org/x", "https://doi.org/x")])
    def test_the_doi_describes_the_release_or_is_left_blank(self, idno, doi,
                                                            expected):
        prov = _parse(_text([f"{CROSS_ID} {CROSS_IDNO} {DOI1}"],
                            [f"{CROSS_ID} {V02} 2026-10-01 {DOI2}"]))
        held = pv.held_secondary_entry(prov, CROSS_ID, idno, doi)
        assert held.held_release_of(CROSS_ID) == pv.HeldRelease(
            CROSS_ID, idno, expected)
        assert held.catalog_latest == []

    def test_the_same_release_again_is_no_change(self):
        prov = _parse(_text([f"{CROSS_ID} {CROSS_IDNO} {DOI1}"]))
        assert pv.held_secondary_entry(prov, CROSS_ID, CROSS_IDNO) is prov


# ---------------------------------------------------------------------------
# The acquisition write, and every failure path writing nothing
# ---------------------------------------------------------------------------

class TestAcquisition:

    def test_a_complete_secondary_acquisition_records_its_release(self, wb):
        root = wb["root"]
        _held(root, _record([PANEL_ID], [f"{PANEL_ID} Panel"]))
        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.held_release_of(CROSS_ID) == pv.HeldRelease(
            CROSS_ID, CROSS_IDNO, None)
        assert (prov.idno, prov.doi) == (PANEL_IDNO, PANEL_DOI)
        assert len(wb["writes"]) == 1

    def test_the_catalog_rows_doi_is_recorded_when_it_is_this_releases(
            self, wb, monkeypatch):
        root = wb["root"]
        _held(root, _record([PANEL_ID], [f"{PANEL_ID} Panel"]))
        monkeypatch.setattr(da, "_wb_catalog_search", lambda *a, **k: [
            {"id": CROSS_ID, "idno": CROSS_IDNO, "doi": DOI1}])
        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert pv.read_provenance(root, COUNTRY, WAVE).held_release_of(
            CROSS_ID).doi == DOI1

    def test_a_newer_release_moves_held_and_clears_latest(self, wb):
        root = wb["root"]
        _held(root, _text([f"{CROSS_ID} BFA_2021_EHCVM-2_v00_M"],
                          [f"{CROSS_ID} {CROSS_IDNO} 2026-10-01 {DOI1}"]),
              cross=True)
        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_held == [f"{CROSS_ID} {CROSS_IDNO} {DOI1}"]
        assert prov.catalog_latest == []

    def test_populate_and_push_records_it_too(self, wb):
        root = wb["root"]
        _held(root, _text([f"{CROSS_ID} BFA_2021_EHCVM-2_v00_M"]), cross=True)
        da.populate_and_push(COUNTRY, WAVE, subdir="Cross_Section")
        assert pv.read_provenance(root, COUNTRY, WAVE).held_release_of(
            CROSS_ID).idno == CROSS_IDNO

    def _failing(self, wb, monkeypatch, failure):
        if failure == "partial-push":
            wb["push"] = lambda paths: list(paths)[:1]
        elif failure == "no-push":
            wb["push"] = lambda paths: []
        elif failure == "download-error":
            import urllib.error
            import urllib.request

            def boom(*a, **k):
                raise urllib.error.URLError("synthetic outage")
            monkeypatch.setattr(urllib.request, "urlopen", boom)
        elif failure == "unverified-file":
            target = _data(wb["root"]) / "Cross_Section"
            target.mkdir(parents=True, exist_ok=True)
            (target / "s00_me_bfa2021.dta").write_bytes(b"untracked, differs")

    @pytest.mark.parametrize("failure", ["partial-push", "no-push",
                                         "download-error", "unverified-file"])
    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_a_failed_or_partial_acquisition_writes_nothing(
            self, wb, monkeypatch, failure, entrypoint):
        root = wb["root"]
        record = _text([f"{CROSS_ID} BFA_2021_EHCVM-2_v00_M"],
                       [f"{CROSS_ID} {CROSS_IDNO} 2026-10-01"])
        _held(root, record)
        mtime = _age_source(root)
        self._failing(wb, monkeypatch, failure)
        if entrypoint == "add_wave":
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        else:
            da.populate_and_push(COUNTRY, WAVE, subdir="Cross_Section")
        assert wb["writes"] == []
        assert _source(root).stat().st_mtime_ns == mtime
        assert _source(root).read_text(encoding="utf-8") == record

    def test_no_note_is_asked_for(self, wb, caplog):
        root = wb["root"]
        _held(root, _record([PANEL_ID], [f"{PANEL_ID} Panel"]))
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert "PROVENANCE_NOTE" not in caplog.text

    def test_an_untrustworthy_record_is_refused_before_any_download(self, wb):
        root = wb["root"]
        record = _text([f"{PANEL_ID} {PANEL_IDNO}"])     # HELD on the primary
        _held(root, record, cross=True)
        with pytest.raises(ValueError, match="names the primary"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert wb["downloads"] == [] and wb["writes"] == []


# ---------------------------------------------------------------------------
# discover_waves judges stale-release per entry
# ---------------------------------------------------------------------------

def _rows(cross_idno):
    """Catalog rows as the WB search returns them: 6224 at the release the
    record holds, 6277 at *cross_idno*."""
    return [{"id": PANEL_ID, "idno": PANEL_IDNO, "year_start": 2021,
             "year_end": 2022, "title": "EHCVM-P", "doi": PANEL_DOI,
             "repository": "lsms", "url": URL.format(PANEL_ID)},
            {"id": CROSS_ID, "idno": cross_idno, "year_start": 2021,
             "year_end": 2022, "title": "EHCVM-2", "doi": DOI2,
             "repository": "lsms", "url": URL.format(CROSS_ID)}]


@pytest.mark.parametrize("held,catalog,status,held_idnos", [
    ([f"{CROSS_ID} {CROSS_IDNO}"], V02, "stale-release", [CROSS_IDNO]),
    ([f"{CROSS_ID} {CROSS_IDNO}"], CROSS_IDNO, "yes", [CROSS_IDNO]),
    ([], V02, "yes", []),                      # nothing to judge it by
])
def test_discover_waves_judges_a_secondary_on_its_held_line(
        wb, monkeypatch, held, catalog, status, held_idnos):
    root = wb["root"]
    _held(root, _text(held), cross=True)
    monkeypatch.setattr(da, "_wb_catalog_search_many",
                        lambda code, cols: [dict(r) for r in _rows(catalog)])
    rows = {e["id"]: e for e in da.discover_waves(COUNTRY)}
    assert (rows[CROSS_ID]["local_status"], rows[CROSS_ID]["held_idnos"]) \
        == (status, held_idnos)
    assert rows[CROSS_ID]["local"] is True
    assert (rows[PANEL_ID]["local_status"], rows[PANEL_ID]["held_idnos"]) \
        == ("yes", [PANEL_IDNO])
    assert da.local_held_idnos(COUNTRY) == {
        PANEL_ID: {PANEL_IDNO}, **({CROSS_ID: set(held_idnos)}
                                   if held_idnos else {})}


def test_local_held_idnos_refuses_an_untrustworthy_record(wb):
    _held(wb["root"], _text([f"{CROSS_ID} {CROSS_IDNO}"] * 2))
    with pytest.raises(ValueError, match="names 6277 twice"):
        da.local_held_idnos(COUNTRY)


# ---------------------------------------------------------------------------
# The backfill: LATEST per secondary, never HELD
# ---------------------------------------------------------------------------

@pytest.fixture
def bf(wb, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "backfill_1283", Path(__file__).resolve().parents[1]
        / "scripts" / "backfill_wave_provenance.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    root = wb["root"]
    monkeypatch.setattr(mod, "countries_root", lambda: root)
    monkeypatch.setattr(mod, "_declared_waves", lambda c: {WAVE})
    state = {"mod": mod, "rows": _rows(V02)}
    monkeypatch.setattr(mod, "_wb_catalog_search",
                        lambda code, collection=None: [dict(r) for r in
                                                       state["rows"]])
    return state


def _table(review, name):
    import pandas as pd
    from lsms_library.local_tools import df_from_orgfile
    df = df_from_orgfile(review, name=name, to_numeric=False)
    return [{k: (None if pd.isna(v) else v) for k, v in r.items()}
            for r in df.to_dict("records")]


def _run(bf, tmp_path, *extra):
    review = tmp_path / "review.org"
    assert bf["mod"].main(["--country", COUNTRY, "--review-file",
                           str(review), *extra]) == 0
    return review


# The record as the backfill itself would stamp it, so that only the release
# lines are in question (the catalog rows carry these exact titles).
def _stamped(held=(), latest=()):
    return _text(held, latest).replace(
        "Enquête Harmonisée sur le Conditions de Vie des Ménages, Panel "
        "Survey 2021-2022", "EHCVM-P")


class TestBackfill:

    def test_writes_latest_beside_held_and_never_moves_held(self, bf, wb,
                                                           tmp_path):
        root = wb["root"]
        old = _stamped([f"{CROSS_ID} {CROSS_IDNO} {DOI1}"])
        _held(root, old, cross=True)
        review = _run(bf, tmp_path)
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        today = datetime.date.today().isoformat()
        assert prov.catalog_held == [f"{CROSS_ID} {CROSS_IDNO} {DOI1}"]
        assert prov.catalog_latest == [f"{CROSS_ID} {V02} {today} {DOI2}"]
        assert prov.catalog_subdirs == SUBDIRS
        assert (prov.idno, prov.doi, prov.latest_idno) \
            == (PANEL_IDNO, PANEL_DOI, None)
        stale = [r for r in _table(review, "release_changed")
                 if r["Catalog ID"] == CROSS_ID]
        assert len(stale) == 1
        assert (stale[0]["Held IDNO"], stale[0]["Latest IDNO"]) \
            == (CROSS_IDNO, V02)
        assert _table(review, "unresolved") == []

    def test_first_seen_is_kept_and_a_caught_up_catalog_clears_latest(
            self, bf, wb, tmp_path):
        root = wb["root"]
        _held(root, _stamped([f"{CROSS_ID} {CROSS_IDNO}"],
                             [f"{CROSS_ID} {V02} 2026-01-02 {DOI2}"]),
              cross=True)
        _run(bf, tmp_path)
        assert pv.read_provenance(root, COUNTRY, WAVE).catalog_latest \
            == [f"{CROSS_ID} {V02} 2026-01-02 {DOI2}"]
        bf["rows"] = _rows(CROSS_IDNO)              # the catalog is back
        _run(bf, tmp_path)
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_latest == []
        assert prov.catalog_held == [f"{CROSS_ID} {CROSS_IDNO}"]

    def test_a_secondary_without_held_is_listed_and_given_none(
            self, bf, wb, tmp_path):
        root = wb["root"]
        _held(root, _stamped(), cross=True)
        review = _run(bf, tmp_path)
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_held == [] and prov.catalog_latest == []
        unknown = [r for r in _table(review, "release_unknown")
                   if r["Catalog ID"] == CROSS_ID]
        assert len(unknown) == 1 and unknown[0]["Catalog IDNO"] == V02

    def test_dry_run_writes_nothing(self, bf, wb, tmp_path):
        root = wb["root"]
        old = _stamped([f"{CROSS_ID} {CROSS_IDNO}"])
        _held(root, old, cross=True)
        _run(bf, tmp_path, "--dry-run")
        assert _source(root).read_text(encoding="utf-8") == old

    def test_a_write_that_would_move_held_is_refused(self, bf, wb, tmp_path,
                                                     monkeypatch):
        """The guard, exercised: a (mutated) path that rewrote HELD from the
        catalog would be left intact, not written."""
        root = wb["root"]
        old = _stamped([f"{CROSS_ID} {CROSS_IDNO}"])
        _held(root, old, cross=True)
        mod = bf["mod"]
        real = mod.secondary_releases

        def rewrites_held(new, by_id, today):
            new, stale, unheld = real(new, by_id, today)
            from dataclasses import replace
            return replace(new, catalog_held=[f"{CROSS_ID} {V02}"]), stale, unheld

        monkeypatch.setattr(mod, "secondary_releases", rewrites_held)
        review = _run(bf, tmp_path)
        assert _source(root).read_text(encoding="utf-8") == old
        kinds = {r["Kind"] for r in _table(review, "unresolved")}
        assert kinds == {"held-release-written"}

    def test_untrustworthy_lines_leave_the_record_intact(self, bf, wb,
                                                         tmp_path):
        root = wb["root"]
        old = _stamped([f"{PANEL_ID} {PANEL_IDNO}"])     # HELD on the primary
        _held(root, old, cross=True)
        review = _run(bf, tmp_path)
        assert _source(root).read_text(encoding="utf-8") == old
        assert {r["Kind"] for r in _table(review, "unresolved")} \
            == {"entry-lines-invalid"}

"""GH #1276: ``add_wave`` / ``populate_and_push`` ``subdir=`` -- a second WB
catalog entry acquired into its own subdirectory of an existing wave.

Burkina Faso 2021-22 is to hold two disjoint entries, the #600 pattern of
``Malawi/2016-17``: 6224 (EHCVM-P, the panel component) in ``Data/Panel/``
and 6277 (EHCVM-2, the new-frame cross-section) in ``Data/Cross_Section/``.
52 of 6277's 55 member names equal files 6224 holds, and the acquisition
matches a member to an existing sidecar BY NAME, anywhere under the directory
it is given.  So without confinement 6277 overwrites the panel files, and once
both sets coexist a re-acquisition of either is "Ambiguous sidecars".

Confinement alone is not enough: naming the WRONG subdirectory confined the
acquisition onto the other entry's files (red team, 2026-10-23: 52 of 53
overwritten, exit 0).  So ``SOURCE.org`` records which subdirectory holds which
entry (``#+CATALOG_SUBDIR: <id> <subdir>``), and an acquisition that contradicts
it, or that would act on sidecars no line accounts for, is refused before any
download.

Synthetic zips and sidecars only, in ``tmp_path``; the WB download, the
catalog and the DVC push are mocked as in ``test_staged_reacquisition.py``.
No network, no credentials, no ``dvc``.
"""

from __future__ import annotations

import hashlib
import importlib.util
import io
import os
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from lsms_library import data_access as da
from lsms_library import local_tools as lt
from lsms_library import provenance as pv
from lsms_library.paths import countries_root

COUNTRY, WAVE = "Burkina_Faso", "2021-22"
PANEL_ID, PANEL_IDNO = "6224", "BFA_2021_EHCVM-P_v04_M"
CROSS_ID, CROSS_IDNO = "6277", "BFA_2021_EHCVM-2_v01_M"
IDNOS = {PANEL_ID: PANEL_IDNO, CROSS_ID: CROSS_IDNO}

# Real member names, synthetic payloads.  The first two names are shipped by
# both entries; each entry also ships one name the other does not.
PANEL_FILES = {"s00_me_bfa2021.dta": b"panel:s00",
               "s01_me_bfa2021.dta": b"panel:s01",
               "ehcvm_panel_ponderations_bfa2021.dta": b"panel:weights"}
CROSS_FILES = {"s00_me_bfa2021.dta": b"cross:s00",
               "s01_me_bfa2021.dta": b"cross:s01",
               "ehcvm_ponderations_bfa2021.dta": b"cross:weights"}

URL = "https://microdata.worldbank.org/index.php/catalog/{}"


def _record(ids, subdirs=(), held=()):
    """A SOURCE.org in render_source_org's key order, so it round-trips byte
    for byte; the per-entry lines sit right after the ids."""
    lines = [f"#+CATALOG_ID: {', '.join(ids)}"]
    lines += [f"#+CATALOG_SUBDIR: {line}" for line in subdirs]
    lines += [f"#+CATALOG_HELD: {line}" for line in held]
    return f"""SOURCE

{URL.format(PANEL_ID)}

""" + "\n".join(lines) + f"""
#+CATALOG_IDNO: {PANEL_IDNO}
#+CATALOG_TITLE: Enquête Harmonisée sur le Conditions de Vie des Ménages, Panel Survey 2021-2022
#+CATALOG_YEARS: 2021-2022
#+CATALOG_REPOSITORY: lsms
#+CATALOG_DOI: https://doi.org/10.48529/synthetic-panel
#+CATALOG_URL: {URL.format(PANEL_ID)}
#+PROVENANCE_SOURCE: worldbank
#+PROVENANCE_METHOD: source-url
#+PROVENANCE_VALIDATION: content-validated
#+PROVENANCE_RECORDED: 2026-07-12
"""


# 6224 moved into Data/Panel/, and the record says so.
PANEL_ONLY = _record([PANEL_ID], [f"{PANEL_ID} Panel"])
# ... and after 6277 has been acquired into Data/Cross_Section/: its id, its
# subdirectory and its held release (GH #1283) are recorded.
BOTH = _record([PANEL_ID, CROSS_ID],
               [f"{PANEL_ID} Panel", f"{CROSS_ID} Cross_Section"],
               [f"{CROSS_ID} {CROSS_IDNO}"])
# The same holdings with no mapping lines: what the refusals are about.
PANEL_UNMAPPED = _record([PANEL_ID])
BOTH_UNMAPPED = _record([PANEL_ID, CROSS_ID])


def _zip(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)
        archive.writestr("readme.txt", b"not a data file")
    return buffer.getvalue()


def _hold(directory: Path, files: dict) -> None:
    """A held entry: each raw file beside its DVC sidecar (``hash: md5``)."""
    directory.mkdir(parents=True, exist_ok=True)
    for name, payload in files.items():
        (directory / name).write_bytes(payload)
        (directory / f"{name}.dvc").write_text(
            f"outs:\n- md5: {hashlib.md5(payload).hexdigest()}\n"
            f"  size: {len(payload)}\n  hash: md5\n  path: {name}\n")


def _snapshot(directory: Path) -> dict:
    """Every file under *directory*, by relative path, with its bytes and inode."""
    return {str(p.relative_to(directory)): (p.read_bytes(), p.stat().st_ino)
            for p in sorted(directory.rglob("*")) if p.is_file()}


@pytest.fixture
def wb(tmp_path, monkeypatch):
    """A tmp countries tree and a hermetic WB: each catalog id serves its own
    zip; DVC is unreadable, so the real ``get_data_file`` reaches the WB
    branch and the real ``_extract_all_from_zip`` places the files.  Every
    ``write_provenance`` is recorded, so "SOURCE.org was not rewritten" is
    asserted on the call, not on bytes that a rewrite could reproduce."""
    root = tmp_path / "countries"
    state = {"root": root, "zips": {PANEL_IDNO: _zip(PANEL_FILES),
                                    CROSS_IDNO: _zip(CROSS_FILES)},
             "downloads": [], "pushes": [], "push": None, "writes": []}

    def urlopen(req, timeout=None):
        url = getattr(req, "full_url", req)
        idno = url.rsplit("/", 1)[1].removesuffix(".zip")
        state["downloads"].append(idno)
        return io.BytesIO(state["zips"][idno])

    def push(paths, remote=None, dvc_add=True):
        state["pushes"].append(sorted(root / p for p in paths))
        return state["push"](paths) if state["push"] else list(paths)

    real_write = da.write_provenance

    def write(countries_dir, prov):
        state["writes"].append(prov)
        return real_write(countries_dir, prov)

    monkeypatch.setattr(da, "_COUNTRIES_DIR", root)
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(da, "permissions", lambda path=None: {"wb_api": "read"})
    monkeypatch.setattr(da.config, "microdata_api_key", lambda: "synthetic")
    monkeypatch.setattr(da, "_get_catalog_idno",
                        lambda url: IDNOS[da._extract_catalog_id(url)])
    monkeypatch.setattr(
        da, "_find_stata_zip_url",
        lambda base, idno, key: (f"https://synthetic.invalid/{idno}.zip",
                                 f"{idno}.zip"))
    monkeypatch.setattr(da, "_wb_catalog_search", lambda *a, **k: [])
    monkeypatch.setattr(da, "push_to_cache_batch", push)
    monkeypatch.setattr(da, "write_provenance", write)
    monkeypatch.setattr(da, "_ACQUIRED_RELEASES", {})
    return state


def _wave(root: Path) -> Path:
    return root / COUNTRY / WAVE


def _data(root: Path) -> Path:
    return _wave(root) / "Data"


def _source(root: Path) -> Path:
    return pv.source_org_path(root, COUNTRY, WAVE)


def _held(root: Path, record: str, *, panel: bool = True,
          cross: bool = False, top: bool = False) -> None:
    """Write *record*; hold 6224 in ``Data/Panel/`` (or at the top of
    ``Data/`` with *top*), and with *cross* 6277 in ``Data/Cross_Section/``."""
    _source(root).parent.mkdir(parents=True, exist_ok=True)
    _source(root).write_text(record, encoding="utf-8")
    if top:
        _hold(_data(root), PANEL_FILES)
    elif panel:
        _hold(_data(root) / "Panel", PANEL_FILES)
    if cross:
        _hold(_data(root) / "Cross_Section", CROSS_FILES)


def _add_wave(catalog_id, **kwargs):
    return da.add_wave(COUNTRY, catalog_id, wave=WAVE, confirm=False,
                       verbose=False, **kwargs)


def _age_source(root: Path) -> int:
    """Back-date SOURCE.org so any rewrite shows in its mtime."""
    os.utime(_source(root), ns=(1_000_000_000, 1_000_000_000))
    return _source(root).stat().st_mtime_ns


# ---------------------------------------------------------------------------
# The second entry, acquired beside the first
# ---------------------------------------------------------------------------

class TestSecondEntryIntoItsSubdirectory:

    def test_lands_only_in_its_subdirectory_and_pushes_only_its_files(
            self, wb, caplog):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        data = _data(root)
        panel_before = _snapshot(data / "Panel")

        with caplog.at_level("INFO", logger="lsms_library.data_access"):
            result = _add_wave(CROSS_ID, push=True, subdir="Cross_Section")

        cross = data / "Cross_Section"
        expected = sorted(cross / name for name in CROSS_FILES)
        # push=True returns the push receipt: countries-relative paths.
        assert sorted(root / p for p in result) == expected
        assert {p.name: p.read_bytes() for p in expected} == CROSS_FILES
        # The panel files and sidecars are byte-identical, same inodes, and
        # nothing new appeared there or at the top of Data/.
        assert _snapshot(data / "Panel") == panel_before
        assert sorted(p.name for p in data.iterdir()) == ["Cross_Section",
                                                          "Panel"]
        # One push, of exactly the cross-section's files.
        assert wb["pushes"] == [expected]
        assert wb["downloads"] == [CROSS_IDNO]
        # Another entry's files are not this release's leftovers.
        assert "also holds" not in caplog.text

    def test_records_the_entry_and_its_subdirectory_and_keeps_the_primary(
            self, wb, caplog):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")

        # The id joins the list, its subdirectory and its held release are
        # recorded (#1283); every primary key still describes 6224.
        assert _source(root).read_text(encoding="utf-8") == BOTH
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_ids == [PANEL_ID, CROSS_ID]
        assert prov.subdir_map() == {PANEL_ID: "Panel",
                                     CROSS_ID: "Cross_Section"}
        assert prov.held_release_of(CROSS_ID) == pv.HeldRelease(
            CROSS_ID, CROSS_IDNO, None)
        assert prov.held_release_of(PANEL_ID) == pv.HeldRelease(
            PANEL_ID, PANEL_IDNO, "https://doi.org/10.48529/synthetic-panel")
        assert (prov.catalog_id, prov.idno) == (PANEL_ID, PANEL_IDNO)
        assert prov.doi == "https://doi.org/10.48529/synthetic-panel"
        assert prov.url == URL.format(PANEL_ID)
        assert len(wb["writes"]) == 1
        # The release fact is in the record now: no hand-written note asked.
        assert "PROVENANCE_NOTE" not in caplog.text

    def test_push_false_records_a_complete_local_download(self, wb):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        result = _add_wave(CROSS_ID, push=False, subdir="Cross_Section")
        assert len(result) == len(CROSS_FILES) and wb["pushes"] == []
        assert _source(root).read_text(encoding="utf-8") == BOTH

    @pytest.mark.parametrize("receipt", ["partial", "none"])
    def test_an_incomplete_push_records_nothing(self, wb, receipt):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        mtime = _age_source(root)
        wb["push"] = ((lambda paths: list(paths)[:1]) if receipt == "partial"
                      else (lambda paths: []))
        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert wb["writes"] == []
        assert _source(root).stat().st_mtime_ns == mtime
        assert _source(root).read_text(encoding="utf-8") == PANEL_ONLY
        assert da._ACQUIRED_RELEASES == {}

    def test_into_a_new_wave_the_entry_is_the_primary_and_is_mapped(self, wb):
        root = wb["root"]
        _add_wave(CROSS_ID, push=False, subdir="Cross_Section")
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_ids == [CROSS_ID] and prov.idno == CROSS_IDNO
        assert prov.subdir_map() == {CROSS_ID: "Cross_Section"}
        data = _data(root)
        assert sorted(p.relative_to(data).as_posix()
                      for p in data.rglob("*.dta")) \
            == sorted(f"Cross_Section/{n}" for n in CROSS_FILES)


# ---------------------------------------------------------------------------
# Without subdir=, nothing changes -- including the ambiguity error
# ---------------------------------------------------------------------------

class TestWithoutSubdir:
    """Without ``subdir=``: on a record with no ``#+CATALOG_SUBDIR:`` line the
    pre-#1276 behaviour is unchanged (pinned below, hazards included); on a
    record that maps its entries to subdirectories it is REFUSED (red team
    r2): unconfined, it matched sidecars across every subdirectory, put new
    members at the top of Data/ and, for a secondary entry, replaced the
    record."""

    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_unmapped_both_entries_held_is_still_ambiguous(
            self, wb, entrypoint):
        root = wb["root"]
        _held(root, BOTH_UNMAPPED, cross=True)
        data_before = _snapshot(_data(root))
        with pytest.raises(ValueError, match="Ambiguous sidecars") as caught:
            if entrypoint == "add_wave":
                _add_wave(PANEL_ID, push=True)
            else:
                da.populate_and_push(COUNTRY, WAVE)
        assert "Panel/s00_me_bfa2021.dta.dvc" in str(caught.value)
        assert "Cross_Section/s00_me_bfa2021.dta.dvc" in str(caught.value)
        assert _snapshot(_data(root)) == data_before
        assert _source(root).read_text(encoding="utf-8") == BOTH_UNMAPPED
        assert wb["pushes"] == []

    def test_unmapped_second_entry_still_matches_held_sidecars_by_name(
            self, wb):
        """The pre-#1276 hazard, pinned as unchanged on a record with no
        mapping: the second entry's same-named members land on the panel's
        sidecars (GH #1149 "revised"), and the record's primary is replaced."""
        root = wb["root"]
        _held(root, PANEL_UNMAPPED)
        _add_wave(CROSS_ID, push=True)
        panel = _data(root) / "Panel"
        assert (panel / "s00_me_bfa2021.dta").read_bytes() == b"cross:s00"
        assert pv.read_provenance(root, COUNTRY, WAVE).catalog_ids == [CROSS_ID]

    @pytest.mark.parametrize("call,hint", [
        (lambda: _add_wave(CROSS_ID, push=True), "subdir='Cross_Section'"),
        (lambda: _add_wave(PANEL_ID, push=True), "subdir='Panel'"),
        (lambda: da.populate_and_push(COUNTRY, WAVE), "subdir='Panel'"),
        (lambda: _add_wave("7777", push=True), "7777 is not mapped")])
    def test_a_mapped_record_refuses_an_unconfined_acquisition(
            self, wb, monkeypatch, call, hint):
        root = wb["root"]
        monkeypatch.setitem(IDNOS, "7777", "BFA_X_v01_M")
        _held(root, BOTH, cross=True)
        data_before = _snapshot(_data(root))
        with pytest.raises(ValueError, match="without subdir=") as caught:
            call()
        assert hint in str(caught.value)
        assert wb["downloads"] == [] and wb["pushes"] == [] and wb["writes"] == []
        assert _snapshot(_data(root)) == data_before
        assert _source(root).read_text(encoding="utf-8") == BOTH

    def test_add_members_needs_a_subdir(self, wb):
        _held(wb["root"], PANEL_UNMAPPED)
        with pytest.raises(ValueError, match="add_members= applies only"):
            _add_wave(PANEL_ID, push=True, add_members=True)
        with pytest.raises(ValueError, match="add_members= applies only"):
            da.populate_and_push(COUNTRY, WAVE, add_members=True)
        assert wb["downloads"] == []


# ---------------------------------------------------------------------------
# Re-acquisition of either entry, each with its own subdir=
# ---------------------------------------------------------------------------

class TestReacquisition:

    def test_the_secondary_is_reacquired_without_rewriting_the_record(
            self, wb):
        root = wb["root"]
        _held(root, BOTH, cross=True)
        mtime = _age_source(root)
        wb["zips"][CROSS_IDNO] = _zip({**CROSS_FILES,
                                       "s00_me_bfa2021.dta": b"cross:revised"})
        panel_before = _snapshot(_data(root) / "Panel")

        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")

        cross = _data(root) / "Cross_Section"
        assert (cross / "s00_me_bfa2021.dta").read_bytes() == b"cross:revised"
        assert _snapshot(_data(root) / "Panel") == panel_before
        assert wb["pushes"] == [sorted(cross / n for n in CROSS_FILES)]
        # Nothing new to record: no write at all, not merely equal bytes.
        assert wb["writes"] == []
        assert _source(root).stat().st_mtime_ns == mtime
        assert _source(root).read_text(encoding="utf-8") == BOTH

    def test_the_primary_is_reacquired_without_touching_the_secondary(
            self, wb):
        root = wb["root"]
        _held(root, BOTH, cross=True)
        cross_before = _snapshot(_data(root) / "Cross_Section")

        _add_wave(PANEL_ID, push=True, subdir="Panel")

        panel = _data(root) / "Panel"
        assert _snapshot(_data(root) / "Cross_Section") == cross_before
        assert wb["pushes"] == [sorted(panel / n for n in PANEL_FILES)]
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_ids == [PANEL_ID, CROSS_ID]
        assert prov.catalog_subdirs == [f"{PANEL_ID} Panel",
                                        f"{CROSS_ID} Cross_Section"]
        assert (prov.idno, prov.doi) == (PANEL_IDNO,
                                         "https://doi.org/10.48529/synthetic-panel")

    @pytest.mark.parametrize("subdir,entry,idno,files", [
        ("Cross_Section", CROSS_ID, CROSS_IDNO, CROSS_FILES),
        ("Panel", PANEL_ID, PANEL_IDNO, PANEL_FILES)])
    def test_populate_and_push_fetches_the_entry_the_record_maps(
            self, wb, subdir, entry, idno, files):
        root = wb["root"]
        _held(root, BOTH, cross=True)
        other = "Panel" if subdir == "Cross_Section" else "Cross_Section"
        other_before = _snapshot(_data(root) / other)

        pushed = da.populate_and_push(COUNTRY, WAVE, subdir=subdir)

        assert wb["downloads"] == [idno]
        target = _data(root) / subdir
        assert sorted(root / p for p in pushed) \
            == sorted(target / n for n in files)
        assert _snapshot(_data(root) / other) == other_before
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_ids == [PANEL_ID, CROSS_ID]
        assert prov.idno == PANEL_IDNO and len(prov.catalog_subdirs) == 2

    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_a_single_entry_reacquired_into_its_own_subdir_writes_the_line(
            self, wb, entrypoint):
        """The one case an unmapped, occupied subdirectory is accepted: the
        record lists exactly one entry and that entry is being acquired.
        This is how Burkina's move into Data/Panel/ gets its line without a
        hand edit."""
        root = wb["root"]
        _held(root, PANEL_UNMAPPED)
        before = _snapshot(_data(root) / "Panel")
        if entrypoint == "add_wave":
            _add_wave(PANEL_ID, push=True, subdir="Panel")
        else:
            da.populate_and_push(COUNTRY, WAVE, subdir="Panel")
        assert _snapshot(_data(root) / "Panel") == before   # all identical
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.subdir_map() == {PANEL_ID: "Panel"}
        assert prov.idno == PANEL_IDNO

    def test_a_hand_written_note_is_prose_and_is_kept(self, wb, caplog):
        """The note is no longer where the release lives (#1283), so nothing
        asks for it; a human's note is carried through untouched."""
        root = wb["root"]
        note = (f"#+PROVENANCE_NOTE: Data/Cross_Section/ is {CROSS_ID}, "
                "the EHCVM-2 cross-section.\n")
        _held(root, BOTH + note, cross=True)
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert caplog.text == "" and wb["writes"] == []
        assert _source(root).read_text(encoding="utf-8") == BOTH + note


# ---------------------------------------------------------------------------
# Refresh-only: a subdirectory that already holds sidecars (red team r2)
# ---------------------------------------------------------------------------

# Malawi 2016-17 as #1286 records it: Panel/ holds ONE round (2016) of 3819,
# whose zip re-ships the 2010, 2013 and 2019 rounds beside it (99 of 396 data
# members in the real release; 3 of 9 here).  Synthetic payloads.
M_COUNTRY, M_WAVE = "Malawi", "2016-17"
IHS4, IHS4_IDNO = "2936", "MWI_2016_IHS-IV_v04_M"
IHPS, IHPS_V06 = "3819", "MWI_2010-2019_IHPS_v06_M"
IHPS_V07 = "MWI_2010-2019_IHPS_v07_M"
IHPS_DOI = "https://doi.org/10.48529/q5q1-2a34"
ROUND16 = {f"{m}_16.dta": f"2016:{m}".encode()
           for m in ("hh_mod_a_filt", "hh_mod_b", "ag_mod_e3")}
OTHER_ROUNDS = {f"{m}_{y}.dta": f"20{y}:{m}".encode()
                for m in ("hh_mod_a_filt", "hh_mod_b") for y in ("10", "13", "19")}
IHS4_FILES = {"hh_mod_a_filt.dta": b"ihs4:a", "hh_mod_b.dta": b"ihs4:b"}
MALAWI = f"""SOURCE

{URL.format(IHS4)}

#+CATALOG_ID: {IHS4}, {IHPS}
#+CATALOG_COVERS: 2939
#+CATALOG_SUBDIR: {IHS4} Cross_Sectional
#+CATALOG_SUBDIR: {IHPS} Panel
#+CATALOG_HELD: {IHPS} {IHPS_V06} {IHPS_DOI}
#+CATALOG_IDNO: {IHS4_IDNO}
#+CATALOG_URL: {URL.format(IHS4)}
#+PROVENANCE_SOURCE: worldbank
#+PROVENANCE_RECORDED: 2026-10-23
"""


@pytest.fixture
def malawi(wb, monkeypatch):
    root = wb["root"]
    monkeypatch.setitem(IDNOS, IHS4, IHS4_IDNO)
    monkeypatch.setitem(IDNOS, IHPS, IHPS_V06)
    wb["zips"][IHPS_V06] = _zip({**ROUND16, **OTHER_ROUNDS})
    source = pv.source_org_path(root, M_COUNTRY, M_WAVE)
    source.parent.mkdir(parents=True)
    source.write_text(MALAWI, encoding="utf-8")
    data = root / M_COUNTRY / M_WAVE / "Data"
    _hold(data / "Panel", ROUND16)
    _hold(data / "Cross_Sectional", IHS4_FILES)
    return dict(wb, source=source, data=data, panel=data / "Panel")


def _m_acquire(entrypoint, **kwargs):
    if entrypoint == "add_wave":
        return da.add_wave(M_COUNTRY, IHPS, wave=M_WAVE, subdir="Panel",
                           confirm=False, verbose=False, push=True, **kwargs)
    return da.populate_and_push(M_COUNTRY, M_WAVE, subdir="Panel", **kwargs)


def _names(directory):
    return sorted(p.name for p in directory.glob("*.dta"))


class TestRefreshOnly:

    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_malawi_refresh_places_only_the_held_round(self, malawi, caplog,
                                                       entrypoint):
        """The red team's r2 repro: the call the skill prescribes used to
        grow Panel/ from 99 to 395 files and push all 396."""
        cross_before = _snapshot(malawi["data"] / "Cross_Sectional")
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            _m_acquire(entrypoint)
        n_placed = len(_names(malawi["panel"]))
        assert n_placed == len(ROUND16), f"{n_placed} != {len(ROUND16)}"
        assert _names(malawi["panel"]) == sorted(ROUND16)
        assert malawi["downloads"] == [IHPS_V06]
        assert malawi["pushes"] == [sorted(malawi["panel"] / n for n in ROUND16)]
        assert f"{len(OTHER_ROUNDS)} member(s) of {IHPS_V06} not placed" \
            in caplog.text and "add_members=True" in caplog.text
        assert _snapshot(malawi["data"] / "Cross_Sectional") == cross_before
        # The same release, every held member verified: nothing to record.
        assert malawi["writes"] == []
        assert malawi["source"].read_text(encoding="utf-8") == MALAWI

    def test_add_members_places_the_rest_on_request(self, malawi):
        _m_acquire("add_wave", add_members=True)
        n_placed = len(_names(malawi["panel"]))
        want = len(ROUND16) + len(OTHER_ROUNDS)
        assert n_placed == want, f"{n_placed} != {want}"
        assert len(malawi["pushes"][0]) == want

    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_a_newer_release_moves_held_when_every_held_member_is_refreshed(
            self, malawi, monkeypatch, entrypoint):
        monkeypatch.setitem(IDNOS, IHPS, IHPS_V07)
        malawi["zips"][IHPS_V07] = _zip(
            {**{n: b + b":v07" for n, b in ROUND16.items()}, **OTHER_ROUNDS})
        _m_acquire(entrypoint)
        prov = pv.read_provenance(malawi["root"], M_COUNTRY, M_WAVE)
        assert prov.held_release_of(IHPS) == pv.HeldRelease(IHPS, IHPS_V07, None)
        assert (prov.idno, prov.catalog_subdirs) == (
            IHS4_IDNO, [f"{IHS4} Cross_Sectional", f"{IHPS} Panel"])
        assert _names(malawi["panel"]) == sorted(ROUND16)
        assert len(malawi["writes"]) == 1

    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_a_held_member_the_release_does_not_ship_keeps_held(
            self, malawi, monkeypatch, caplog, entrypoint):
        """#1114 per entry: the refresh did not demonstrate that the held
        subset is this release's -- one held file is some other release's."""
        monkeypatch.setitem(IDNOS, IHPS, IHPS_V07)
        shipped = dict(list(ROUND16.items())[:2])
        malawi["zips"][IHPS_V07] = _zip(
            {**{n: b + b":v07" for n, b in shipped.items()}, **OTHER_ROUNDS})
        mtime = os.stat(malawi["source"]).st_mtime_ns
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            _m_acquire(entrypoint)
        assert malawi["writes"] == []
        assert os.stat(malawi["source"]).st_mtime_ns == mtime
        assert malawi["source"].read_text(encoding="utf-8") == MALAWI
        assert "does not ship" in caplog.text
        assert malawi["pushes"] == [sorted(malawi["panel"] / n for n in shipped)]

    def test_a_primary_is_refreshed_under_the_same_gate(self, wb, monkeypatch):
        """Ledger §6: the primary's #+CATALOG_IDNO: moves by the same rule."""
        root = wb["root"]
        v05 = "BFA_2021_EHCVM-P_v05_M"
        monkeypatch.setitem(IDNOS, PANEL_ID, v05)
        _held(root, PANEL_ONLY)
        partial = dict(list(PANEL_FILES.items())[:2])
        wb["zips"][v05] = _zip(partial)
        _add_wave(PANEL_ID, push=True, subdir="Panel")
        assert pv.read_provenance(root, COUNTRY, WAVE).idno == PANEL_IDNO
        assert wb["writes"] == []
        wb["zips"][v05] = _zip({**PANEL_FILES, "s99_me_bfa2021.dta": b"new"})
        _add_wave(PANEL_ID, push=True, subdir="Panel")
        assert pv.read_provenance(root, COUNTRY, WAVE).idno == v05
        assert not (_data(root) / "Panel" / "s99_me_bfa2021.dta").exists()

    def test_the_first_acquisition_into_an_empty_subdir_is_full(self, wb):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        wb["zips"][CROSS_IDNO] = _zip({**CROSS_FILES, "extra_round.dta": b"x"})
        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        names = _names(_data(root) / "Cross_Section")
        assert names == sorted([*CROSS_FILES, "extra_round.dta"])


# ---------------------------------------------------------------------------
# Refusals, all before any download, all leaving Data/ and SOURCE.org alone
# ---------------------------------------------------------------------------

REFUSALS = {
    # The red team's R2 and its mirror: the WRONG subdirectory, both mapped.
    "6277 into 6224's subdir": (
        BOTH, dict(cross=True),
        lambda: _add_wave(CROSS_ID, push=True, subdir="Panel"),
        "maps 6277 to Data/Cross_Section/"),
    "6224 into 6277's subdir": (
        BOTH, dict(cross=True),
        lambda: _add_wave(PANEL_ID, push=True, subdir="Cross_Section"),
        "maps 6224 to Data/Panel/"),
    "a new entry into a mapped subdir": (
        PANEL_ONLY, {},
        lambda: _add_wave(CROSS_ID, push=True, subdir="Panel"),
        "maps Data/Panel/ to 6224"),
    # Unmapped and occupied: whose files are they?
    "a new entry into the one entry's unmapped subdir": (
        PANEL_UNMAPPED, {},
        lambda: _add_wave(CROSS_ID, push=True, subdir="Panel"),
        "no #+CATALOG_SUBDIR: line says which entry"),
    "two entries, unmapped target": (
        BOTH_UNMAPPED, dict(cross=True),
        lambda: _add_wave(PANEL_ID, push=True, subdir="Panel"),
        "add '#+CATALOG_SUBDIR: 6224 Panel'"),
    "an unmapped sibling holds sidecars": (
        PANEL_UNMAPPED, {},
        lambda: _add_wave(CROSS_ID, push=True, subdir="Cross_Section"),
        "'#+CATALOG_SUBDIR: <catalog id> Panel'"),
    "populate_and_push, two entries, unmapped target": (
        BOTH_UNMAPPED, dict(cross=True),
        lambda: da.populate_and_push(COUNTRY, WAVE, subdir="Cross_Section"),
        "no #+CATALOG_SUBDIR: line says which one"),
    # Sidecars at the top of Data/ (the move was skipped): P5 / P5b.
    "secondary beside unmoved top-level files": (
        PANEL_UNMAPPED, dict(top=True),
        lambda: _add_wave(CROSS_ID, push=True, subdir="Cross_Section"),
        "outside any subdirectory"),
    "primary duplicated into a subdir": (
        PANEL_UNMAPPED, dict(top=True),
        lambda: da.populate_and_push(COUNTRY, WAVE, subdir="Panel"),
        "outside any subdirectory"),
    # Case: one directory or two, depending on the filesystem.
    "a case variant of an existing directory": (
        PANEL_UNMAPPED, {},
        lambda: _add_wave(PANEL_ID, push=True, subdir="panel"),
        "only in case"),
    "a case variant of a mapped subdir": (
        PANEL_ONLY, {},
        lambda: _add_wave(PANEL_ID, push=True, subdir="panel"),
        "maps 6224 to Data/Panel/"),
    # A record that cannot be trusted.
    "a mapping naming an unlisted id": (
        _record([PANEL_ID], ["3819 Panel"]), {},
        lambda: _add_wave(PANEL_ID, push=True, subdir="Panel"),
        "does not list"),
    "two ids on one subdir, ignoring case": (
        _record([PANEL_ID, CROSS_ID], [f"{PANEL_ID} Panel",
                                       f"{CROSS_ID} panel"]), {},
        lambda: _add_wave(PANEL_ID, push=True, subdir="Panel"),
        "maps both 6224 and 6277"),
    "a malformed mapping line": (
        _record([PANEL_ID], ["Panel"]), {},
        lambda: _add_wave(PANEL_ID, push=True, subdir="Panel"),
        "is not '<catalog id> <subdirectory>'"),
}


@pytest.mark.parametrize("case", sorted(REFUSALS))
def test_refused_before_any_download(wb, case):
    record, layout, call, message = REFUSALS[case]
    root = wb["root"]
    _held(root, record, **layout)
    data_before = _snapshot(_data(root))
    with pytest.raises(ValueError) as caught:
        call()
    assert message in str(caught.value)
    assert wb["downloads"] == [] and wb["pushes"] == [] and wb["writes"] == []
    assert _snapshot(_data(root)) == data_before
    assert _source(root).read_text(encoding="utf-8") == record


class TestSymlinks:

    def test_a_subdir_linked_outside_the_tree_is_refused(self, wb, tmp_path):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        outside = tmp_path / "OUTSIDE"
        outside.mkdir()
        os.symlink(outside, _data(root) / "Cross_Section")
        # The pre-check's own refusal (its message), not only the second
        # guard in _acquisition_data_dir, which get_data_file's test pins.
        with pytest.raises(ValueError, match="symbolic link; refusing"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert list(outside.iterdir()) == [] and wb["downloads"] == []

    def test_a_subdir_linked_to_a_sibling_is_refused(self, wb):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        os.symlink("Panel", _data(root) / "Cross_Section")
        before = _snapshot(_data(root) / "Panel")
        with pytest.raises(ValueError, match="symbolic link; refusing"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert _snapshot(_data(root) / "Panel") == before

    def test_a_subdir_that_is_a_file_is_refused_before_any_download(self, wb):
        """The red team's P3: without the pre-check this reached the download
        and failed part-way through placement."""
        root = wb["root"]
        _held(root, PANEL_ONLY)
        (_data(root) / "Cross_Section").write_bytes(b"a file")
        with pytest.raises(ValueError, match="not a directory"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert wb["downloads"] == [] and wb["writes"] == []
        assert (_data(root) / "Cross_Section").read_bytes() == b"a file"

    def test_a_link_inside_the_subdir_cannot_reach_a_sibling(self, wb):
        """The red team's P2c: the extraction's own escape guard, which
        confines to the resolved subdirectory."""
        root = wb["root"]
        _held(root, PANEL_ONLY)
        (_data(root) / "Cross_Section").mkdir()
        os.symlink("../Panel", _data(root) / "Cross_Section" / "inner")
        wb["zips"][CROSS_IDNO] = _zip({"inner/s00_me_bfa2021.dta": b"x",
                                       **CROSS_FILES})
        before = _snapshot(_data(root) / "Panel")
        with pytest.raises(ValueError, match="escapes Data"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert _snapshot(_data(root) / "Panel") == before
        assert wb["writes"] == [] and wb["pushes"] == []

    def test_get_data_file_refuses_a_linked_subdir_directly(self, wb):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        os.symlink("Panel", _data(root) / "Cross_Section")
        with lt.allow_intree_sources():
            with pytest.raises(ValueError, match="symbolic link"):
                da.get_data_file(f"{COUNTRY}/{WAVE}/Data/_probe_.dta",
                                 populate_cache=True,
                                 _source_url=URL.format(CROSS_ID),
                                 _subdir="Cross_Section")
        assert wb["downloads"] == []


class TestSubdirNames:

    @pytest.mark.parametrize("subdir", [
        "", " ", "Cross_Section ", " Panel", "Panel/Cross", "Panel\\Cross",
        "/abs", "../Panel", ".", "..", "...", "Panel.", "C:x", "~", "~Panel",
        "Cross\x00Section", "Cross\tSection", "CON", "nul.txt", "com1",
        "COM0", "lpt0.txt",
        PureWindowsPath("Panel/Cross"), PurePosixPath("Panel/Cross")])
    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_an_unsafe_subdir_is_refused_before_any_download(
            self, wb, monkeypatch, entrypoint, subdir):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        monkeypatch.setattr(da, "_get_catalog_idno",
                            lambda url: pytest.fail("resolved a catalog id"))
        with pytest.raises(ValueError, match="subdir"):
            if entrypoint == "add_wave":
                _add_wave(CROSS_ID, push=True, subdir=subdir)
            else:
                da.populate_and_push(COUNTRY, WAVE, subdir=subdir)
        assert wb["downloads"] == []

    @pytest.mark.parametrize("subdir,expected", [
        ("Cross_Section", "Cross_Section"), ("Post Harvest", "Post Harvest"),
        ("Panel.v2", "Panel.v2"), (Path("Panel"), "Panel"),
        (PureWindowsPath("Panel"), "Panel"), (None, None)])
    def test_one_directory_name_is_accepted(self, subdir, expected):
        assert da._normalise_subdir(subdir) == expected

    @pytest.mark.parametrize("populate_cache", [False, True])
    def test_get_data_file_confinement_requires_acquisition_mode(
            self, wb, populate_cache):
        assert not lt._intree_sources_allowed()
        with pytest.raises(ValueError, match="acquisition mode"):
            da.get_data_file(f"{COUNTRY}/{WAVE}/Data/_probe_.dta",
                             populate_cache=populate_cache, _subdir="Panel")
        assert wb["downloads"] == []


# ---------------------------------------------------------------------------
# Receipts and the no-receipt fallback
# ---------------------------------------------------------------------------

class TestReceipts:

    def test_each_confined_acquisition_has_its_own_receipt(self, wb):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        other, plain = object(), object()
        da._ACQUIRED_RELEASES[(COUNTRY, WAVE, "Panel")] = other
        da._ACQUIRED_RELEASES[(COUNTRY, WAVE)] = plain
        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert da._ACQUIRED_RELEASES == {(COUNTRY, WAVE, "Panel"): other,
                                         (COUNTRY, WAVE): plain}
        assert da._receipt_key(COUNTRY, WAVE) == (COUNTRY, WAVE)
        assert da._receipt_key(COUNTRY, WAVE, "Panel") \
            == (COUNTRY, WAVE, "Panel")

    def test_with_no_download_only_the_subdir_is_pushed(self, wb, monkeypatch):
        """No WB permission: get_data_file returns before any download, there
        is no receipt, and populate_and_push pushes what the fallback scan
        finds -- in the subdirectory only."""
        root = wb["root"]
        _held(root, BOTH, cross=True)
        monkeypatch.setattr(da, "permissions", lambda path=None: {})
        pushed = da.populate_and_push(COUNTRY, WAVE, subdir="Panel")
        panel = _data(root) / "Panel"
        assert wb["downloads"] == []
        assert wb["pushes"] == [sorted(panel / n for n in PANEL_FILES)]
        assert sorted(root / p for p in pushed) == wb["pushes"][0]
        assert wb["writes"] == []
        assert da._acquired_data_files(COUNTRY, WAVE, "Panel") \
            == sorted(panel / n for n in PANEL_FILES)


# ---------------------------------------------------------------------------
# The single decision point, and the provenance module
# ---------------------------------------------------------------------------

def test_the_destination_is_decided_by_fetch_destination(wb, monkeypatch):
    seen = []
    real = da._fetch_destination

    def spy(path, populate_cache=False):
        seen.append((Path(path), populate_cache))
        return real(path, populate_cache)

    monkeypatch.setattr(da, "_fetch_destination", spy)
    root = wb["root"]
    _held(root, PANEL_ONLY)
    _add_wave(CROSS_ID, push=False, subdir="Cross_Section")
    assert (Path(COUNTRY, WAVE, "Data", "Cross_Section"), True) in seen
    assert all(populate_cache for path, populate_cache in seen
               if path.parts[:3] == (COUNTRY, WAVE, "Data"))


class TestCatalogSubdirRecord:

    def test_parse_render_round_trip(self):
        prov = pv.parse_source_org(BOTH, COUNTRY, WAVE)
        assert prov.catalog_subdirs == [f"{PANEL_ID} Panel",
                                        f"{CROSS_ID} Cross_Section"]
        assert prov.subdir_map() == {PANEL_ID: "Panel",
                                     CROSS_ID: "Cross_Section"}
        assert pv.render_source_org(prov) == BOTH
        assert "CATALOG_SUBDIR" not in prov.extra

    def test_a_malformed_line_survives_parse_and_render(self):
        text = _record([PANEL_ID], ["Panel", f"{PANEL_ID} a/b"])
        prov = pv.parse_source_org(text, COUNTRY, WAVE)
        assert pv.render_source_org(prov) == text
        with pytest.raises(pv.CatalogSubdirError):
            prov.subdir_map()

    @pytest.mark.parametrize("lines,message", [
        (["Panel"], "is not"), (["abc Panel"], "is not"),
        ([f"{PANEL_ID} a/b"], "ONE directory name"),
        ([f"{PANEL_ID} Panel", f"{PANEL_ID} Other"], "names 6224 twice"),
        ([f"{PANEL_ID} Panel", f"{CROSS_ID} PANEL"], "maps both")])
    def test_subdir_map_refuses_what_cannot_be_trusted(self, lines, message):
        prov = pv.parse_source_org(_record([PANEL_ID, CROSS_ID], lines),
                                   COUNTRY, WAVE)
        with pytest.raises(pv.CatalogSubdirError, match=message):
            prov.subdir_map()

    def test_even_an_identical_repeated_line_is_refused(self):
        """GH #1283 ruling: duplicates are refused, identical or not."""
        prov = pv.parse_source_org(
            _record([PANEL_ID], [f"{PANEL_ID} Panel"] * 2), COUNTRY, WAVE)
        with pytest.raises(pv.CatalogSubdirError, match="names 6224 twice"):
            prov.subdir_map()

    def test_with_catalog_subdir_adds_once_and_never_rewrites(self):
        prov = pv.parse_source_org(PANEL_ONLY, COUNTRY, WAVE)
        assert pv.with_catalog_subdir(prov, PANEL_ID, "Panel") is prov
        both = pv.with_catalog_subdir(prov, CROSS_ID, "Cross_Section")
        assert both.catalog_subdirs[-1] == f"{CROSS_ID} Cross_Section"
        for cid, sub in ((PANEL_ID, "Other"), (CROSS_ID, "Panel"),
                         (CROSS_ID, "panel")):
            with pytest.raises(pv.CatalogSubdirError):
                pv.with_catalog_subdir(prov, cid, sub)

    def test_replacing_the_primary_drops_the_mapping_keeping_it_keeps_it(
            self):
        both = pv.parse_source_org(BOTH, COUNTRY, WAVE)
        other = pv.acquired_release(both, country=COUNTRY, wave=WAVE,
                                    catalog_id="9999", idno="X_v01_M")
        assert other.catalog_subdirs == []
        same = pv.acquired_release(both, country=COUNTRY, wave=WAVE,
                                   catalog_id=PANEL_ID, idno=PANEL_IDNO)
        assert same.catalog_subdirs == both.catalog_subdirs

    def test_render_refuses_a_mapping_without_a_worldbank_entry(self):
        prov = pv.WaveProvenance(country=COUNTRY, wave=WAVE,
                                 catalog_subdirs=[f"{PANEL_ID} Panel"])
        with pytest.raises(ValueError, match="explicit provenance curation"):
            pv.render_source_org(prov)

    def test_the_backfill_carries_the_mapping_through(self, tmp_path):
        spec = importlib.util.spec_from_file_location(
            "backfill_1276", Path(__file__).resolve().parents[1]
            / "scripts" / "backfill_wave_provenance.py")
        bf = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bf)
        root = tmp_path / "countries"
        source = pv.source_org_path(root, COUNTRY, WAVE)
        source.parent.mkdir(parents=True)
        source.write_text(BOTH, encoding="utf-8")
        rows = {PANEL_ID: {"id": PANEL_ID, "idno": PANEL_IDNO,
                           "title": "Panel", "year_start": 2021,
                           "year_end": 2022, "repository": "lsms",
                           "doi": "https://doi.org/10.48529/synthetic-panel",
                           "url": URL.format(PANEL_ID)}}
        rec, outcome = bf.resolve(COUNTRY, WAVE, root, rows, {}, "2026-10-23")
        assert outcome == "source-url"
        assert rec.catalog_subdirs == [f"{PANEL_ID} Panel",
                                       f"{CROSS_ID} Cross_Section"]
        assert bf.dropped_by_write(BOTH, rec) == []

    def test_held_secondary_entry_appends_the_id_and_nothing_else(self):
        prov = pv.parse_source_org(PANEL_ONLY, COUNTRY, WAVE)
        new = pv.held_secondary_entry(prov, CROSS_ID)
        assert new.catalog_ids == [PANEL_ID, CROSS_ID]
        assert (new.idno, new.doi, new.catalog_subdirs) \
            == (prov.idno, prov.doi, prov.catalog_subdirs)
        both = pv.parse_source_org(BOTH, COUNTRY, WAVE)
        assert pv.held_secondary_entry(both, CROSS_ID) is both
        with pytest.raises(ValueError, match="primary entry"):
            pv.held_secondary_entry(prov, PANEL_ID)
        with pytest.raises(ValueError, match="no World Bank primary"):
            pv.held_secondary_entry(pv.WaveProvenance(country=COUNTRY,
                                                      wave=WAVE), CROSS_ID)


def test_every_shipped_mapping_line_is_usable():
    """Every shipped ``#+CATALOG_SUBDIR:`` line parses, names an entry its
    record lists, and names a subdirectory of the wave's Data/ that holds
    sidecars.  Vacuous until a record carries the key; it is what checks the
    lines another branch adds for Malawi 2016-17 and 2019-20."""
    root = Path(countries_root())
    bad = {}
    for source in sorted(root.glob("*/*/Documentation/SOURCE.org")):
        country, wave = source.parts[-4], source.parts[-3]
        prov = pv.read_provenance(root, country, wave)
        if not prov.catalog_subdirs:
            continue
        try:
            mapping = prov.subdir_map()
        except pv.CatalogSubdirError as exc:
            bad[f"{country}/{wave}"] = str(exc)
            continue
        for cid, sub in mapping.items():
            data = root / country / wave / "Data" / sub
            if cid not in prov.catalog_ids:
                bad[f"{country}/{wave}/{cid}"] = "id not in #+CATALOG_ID:"
            elif not data.is_dir() or not any(data.rglob("*.dvc")):
                bad[f"{country}/{wave}/{cid}"] = f"Data/{sub}/ holds no sidecar"
    assert bad == {}

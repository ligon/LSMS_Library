"""GH #1276: ``add_wave`` / ``populate_and_push`` ``subdir=`` -- a second WB
catalog entry acquired into its own subdirectory of an existing wave.

Burkina Faso 2021-22 is to hold two disjoint entries, the #600 pattern of
``Malawi/2016-17``: 6224 (EHCVM-P, the panel component) in ``Data/Panel/``
and 6277 (EHCVM-2, the new-frame cross-section) in ``Data/Cross_Section/``.
52 of 6277's 55 member names equal files 6224 holds, and the acquisition
matches a member to an existing sidecar BY NAME, anywhere under the directory
it is given.  So without confinement 6277 overwrites the panel files, and once
both sets coexist a re-acquisition of either is "Ambiguous sidecars".

Synthetic zips and sidecars only, in ``tmp_path``; the WB download, the
catalog and the DVC push are mocked as in ``test_staged_reacquisition.py``.
No network, no credentials, no ``dvc``.
"""

from __future__ import annotations

import hashlib
import io
import urllib.request
import zipfile
from pathlib import Path

import pytest

from lsms_library import data_access as da
from lsms_library import local_tools as lt
from lsms_library import provenance as pv

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

# In render_source_org's key order, so it round-trips byte for byte.
PANEL_ONLY = f"""SOURCE

{URL.format(PANEL_ID)}

#+CATALOG_ID: {PANEL_ID}
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
BOTH = PANEL_ONLY.replace(f"#+CATALOG_ID: {PANEL_ID}\n",
                          f"#+CATALOG_ID: {PANEL_ID}, {CROSS_ID}\n")


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
    branch and the real ``_extract_all_from_zip`` places the files."""
    root = tmp_path / "countries"
    state = {"root": root, "zips": {PANEL_IDNO: _zip(PANEL_FILES),
                                    CROSS_IDNO: _zip(CROSS_FILES)},
             "downloads": [], "pushes": [], "push": None}

    def urlopen(req, timeout=None):
        url = getattr(req, "full_url", req)
        idno = url.rsplit("/", 1)[1].removesuffix(".zip")
        state["downloads"].append(idno)
        return io.BytesIO(state["zips"][idno])

    def push(paths, remote=None, dvc_add=True):
        state["pushes"].append(sorted(root / p for p in paths))
        return state["push"](paths) if state["push"] else list(paths)

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
    return state


def _wave(root: Path) -> Path:
    return root / COUNTRY / WAVE


def _source(root: Path) -> Path:
    return pv.source_org_path(root, COUNTRY, WAVE)


def _held(root: Path, record: str, *, cross: bool = False) -> None:
    """6224 moved to ``Data/Panel/``; with *cross*, 6277 already held too."""
    _source(root).parent.mkdir(parents=True, exist_ok=True)
    _source(root).write_text(record, encoding="utf-8")
    _hold(_wave(root) / "Data" / "Panel", PANEL_FILES)
    if cross:
        _hold(_wave(root) / "Data" / "Cross_Section", CROSS_FILES)


def _add_wave(catalog_id, **kwargs):
    return da.add_wave(COUNTRY, catalog_id, wave=WAVE, confirm=False,
                       verbose=False, **kwargs)


# ---------------------------------------------------------------------------
# The second entry, acquired beside the first
# ---------------------------------------------------------------------------

class TestSecondEntryIntoItsSubdirectory:

    def test_lands_only_in_its_subdirectory_and_pushes_only_its_files(
            self, wb, caplog):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        data = _wave(root) / "Data"
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

    def test_records_the_entry_as_a_list_and_keeps_the_primary_release(
            self, wb, caplog):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")

        # The ONLY change is the id list (GH #600): every held-release key
        # still describes 6224, the primary entry.
        assert _source(root).read_text(encoding="utf-8") == BOTH
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_ids == [PANEL_ID, CROSS_ID]
        assert prov.catalog_id == PANEL_ID
        assert prov.idno == PANEL_IDNO
        assert prov.doi == "https://doi.org/10.48529/synthetic-panel"
        assert prov.url == URL.format(PANEL_ID)
        # The secondary's release has no field; the acquisition says where
        # it has to be written by hand.
        assert CROSS_IDNO in caplog.text
        assert "Data/Cross_Section/" in caplog.text
        assert "#+PROVENANCE_NOTE:" in caplog.text
        assert "does not name it yet" in caplog.text

    def test_push_false_records_a_complete_local_download(self, wb):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        result = _add_wave(CROSS_ID, push=False, subdir="Cross_Section")
        assert len(result) == len(CROSS_FILES) and wb["pushes"] == []
        assert _source(root).read_text(encoding="utf-8") == BOTH

    @pytest.mark.parametrize("receipt", ["partial", "none"])
    def test_an_incomplete_push_does_not_list_the_entry(self, wb, receipt):
        root = wb["root"]
        _held(root, PANEL_ONLY)
        wb["push"] = ((lambda paths: list(paths)[:1]) if receipt == "partial"
                      else (lambda paths: []))
        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert _source(root).read_text(encoding="utf-8") == PANEL_ONLY
        assert (COUNTRY, WAVE) not in da._ACQUIRED_RELEASES

    def test_into_a_new_wave_the_entry_is_simply_the_primary(self, wb):
        root = wb["root"]
        _add_wave(CROSS_ID, push=False, subdir="Cross_Section")
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_ids == [CROSS_ID] and prov.idno == CROSS_IDNO
        data = _wave(root) / "Data"
        assert sorted(p.relative_to(data).as_posix()
                      for p in data.rglob("*.dta")) \
            == sorted(f"Cross_Section/{n}" for n in CROSS_FILES)


# ---------------------------------------------------------------------------
# Without subdir=, nothing changes -- including the ambiguity error
# ---------------------------------------------------------------------------

class TestWithoutSubdirBehaviourIsUnchanged:

    @pytest.mark.parametrize("entrypoint", ["add_wave", "populate_and_push"])
    def test_both_entries_held_and_no_subdir_is_still_ambiguous(
            self, wb, entrypoint):
        root = wb["root"]
        _held(root, BOTH, cross=True)
        data_before = _snapshot(_wave(root) / "Data")
        with pytest.raises(ValueError, match="Ambiguous sidecars") as caught:
            if entrypoint == "add_wave":
                _add_wave(PANEL_ID, push=True)
            else:
                da.populate_and_push(COUNTRY, WAVE)
        assert "Panel/s00_me_bfa2021.dta.dvc" in str(caught.value)
        assert "Cross_Section/s00_me_bfa2021.dta.dvc" in str(caught.value)
        assert _snapshot(_wave(root) / "Data") == data_before
        assert _source(root).read_text(encoding="utf-8") == BOTH
        assert wb["pushes"] == []

    def test_no_subdir_second_entry_still_matches_held_sidecars_by_name(
            self, wb):
        """The pre-#1276 hazard, pinned as unchanged: without ``subdir=`` the
        second entry's same-named members land on the panel's sidecars (GH
        #1149 "revised"), and the record's primary entry is replaced."""
        root = wb["root"]
        _held(root, PANEL_ONLY)
        _add_wave(CROSS_ID, push=True)
        panel = _wave(root) / "Data" / "Panel"
        assert (panel / "s00_me_bfa2021.dta").read_bytes() == b"cross:s00"
        assert pv.read_provenance(root, COUNTRY, WAVE).catalog_ids == [CROSS_ID]


# ---------------------------------------------------------------------------
# Re-acquisition of either entry, each with its own subdir=
# ---------------------------------------------------------------------------

class TestReacquisitionIsUnambiguous:

    def test_reacquiring_the_secondary_leaves_the_record_byte_identical(
            self, wb):
        root = wb["root"]
        _held(root, BOTH, cross=True)
        wb["zips"][CROSS_IDNO] = _zip({**CROSS_FILES,
                                       "s00_me_bfa2021.dta": b"cross:revised"})
        panel_before = _snapshot(_wave(root) / "Data" / "Panel")

        _add_wave(CROSS_ID, push=True, subdir="Cross_Section")

        cross = _wave(root) / "Data" / "Cross_Section"
        assert (cross / "s00_me_bfa2021.dta").read_bytes() == b"cross:revised"
        assert _snapshot(_wave(root) / "Data" / "Panel") == panel_before
        assert wb["pushes"] == [sorted(cross / n for n in CROSS_FILES)]
        assert _source(root).read_text(encoding="utf-8") == BOTH

    def test_reacquiring_the_primary_leaves_the_secondary_untouched(self, wb):
        root = wb["root"]
        _held(root, BOTH, cross=True)
        cross_before = _snapshot(_wave(root) / "Data" / "Cross_Section")

        _add_wave(PANEL_ID, push=True, subdir="Panel")

        panel = _wave(root) / "Data" / "Panel"
        assert _snapshot(_wave(root) / "Data" / "Cross_Section") == cross_before
        assert wb["pushes"] == [sorted(panel / n for n in PANEL_FILES)]
        prov = pv.read_provenance(root, COUNTRY, WAVE)
        assert prov.catalog_ids == [PANEL_ID, CROSS_ID]
        assert (prov.idno, prov.doi) == (PANEL_IDNO,
                                         "https://doi.org/10.48529/synthetic-panel")

    def test_populate_and_push_reacquires_a_single_entry_wave_into_its_subdir(
            self, wb):
        """A record listing one entry has no ambiguity about which one is
        fetched, so populate_and_push takes subdir= -- and is confined by it
        even with same-named sidecars elsewhere in Data/."""
        root = wb["root"]
        _held(root, PANEL_ONLY, cross=True)
        wb["zips"][PANEL_IDNO] = _zip({**PANEL_FILES,
                                       "s01_me_bfa2021.dta": b"panel:revised"})
        cross_before = _snapshot(_wave(root) / "Data" / "Cross_Section")

        pushed = da.populate_and_push(COUNTRY, WAVE, subdir="Panel")

        panel = _wave(root) / "Data" / "Panel"
        assert sorted(root / p for p in pushed) \
            == sorted(panel / n for n in PANEL_FILES)
        assert (panel / "s01_me_bfa2021.dta").read_bytes() == b"panel:revised"
        assert _snapshot(_wave(root) / "Data" / "Cross_Section") == cross_before
        assert pv.read_provenance(root, COUNTRY, WAVE).idno == PANEL_IDNO

    def test_a_note_naming_the_release_quiets_the_warning(self, wb, caplog):
        root = wb["root"]
        note = (f"#+PROVENANCE_NOTE: Data/Cross_Section/ is {CROSS_ID} "
                f"({CROSS_IDNO}).\n")
        _held(root, BOTH + note, cross=True)
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            _add_wave(CROSS_ID, push=True, subdir="Cross_Section")
        assert "secondary catalog entry" not in caplog.text
        assert _source(root).read_text(encoding="utf-8") == BOTH + note


# ---------------------------------------------------------------------------
# Refusals, all before any download
# ---------------------------------------------------------------------------

class TestRefusals:

    def test_populate_and_push_cannot_tell_which_entry_a_subdir_holds(
            self, wb):
        """It fetches the PRIMARY entry; on a two-entry wave that would put
        6224's files over 6277's same-named ones in Cross_Section/."""
        root = wb["root"]
        _held(root, BOTH, cross=True)
        data_before = _snapshot(_wave(root) / "Data")
        for subdir in ("Cross_Section", "Panel"):
            with pytest.raises(ValueError, match="add_wave"):
                da.populate_and_push(COUNTRY, WAVE, subdir=subdir)
        assert wb["downloads"] == [] and wb["pushes"] == []
        assert _snapshot(_wave(root) / "Data") == data_before

    @pytest.mark.parametrize("subdir", [
        "", "/abs", "../Panel", "Panel/../..", "C:/x", "Panel\\x", "Panel/",
        ".", "a//b", " "])
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

    def test_nested_subdir_is_accepted(self):
        assert da._normalise_subdir("Post Harvest/Wave 5") \
            == "Post Harvest/Wave 5"
        assert da._normalise_subdir(Path("Cross_Section")) == "Cross_Section"
        assert da._normalise_subdir(None) is None

    @pytest.mark.parametrize("populate_cache", [False, True])
    def test_get_data_file_confinement_requires_acquisition_mode(
            self, wb, populate_cache):
        assert not lt._intree_sources_allowed()
        with pytest.raises(ValueError, match="acquisition mode"):
            da.get_data_file(f"{COUNTRY}/{WAVE}/Data/_probe_.dta",
                             populate_cache=populate_cache, _subdir="Panel")
        assert wb["downloads"] == []


# ---------------------------------------------------------------------------
# The single decision point, and the provenance helper
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


class TestHeldSecondaryEntry:

    def _panel(self):
        return pv.parse_source_org(PANEL_ONLY, COUNTRY, WAVE)

    def test_appends_the_id_and_changes_nothing_else(self):
        prov = pv.held_secondary_entry(self._panel(), CROSS_ID)
        assert pv.render_source_org(prov) == BOTH

    def test_an_id_already_listed_returns_the_record_itself(self):
        both = pv.parse_source_org(BOTH, COUNTRY, WAVE)
        assert pv.held_secondary_entry(both, CROSS_ID) is both

    def test_the_primary_is_not_a_secondary(self):
        with pytest.raises(ValueError, match="primary entry"):
            pv.held_secondary_entry(self._panel(), PANEL_ID)

    def test_needs_a_worldbank_primary(self):
        unknown = pv.WaveProvenance(country=COUNTRY, wave=WAVE)
        with pytest.raises(ValueError, match="no World Bank primary"):
            pv.held_secondary_entry(unknown, CROSS_ID)

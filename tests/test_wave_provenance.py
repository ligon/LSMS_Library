"""Wave provenance: recording which WB catalog entry a wave dir actually holds.

The regression under test is that ``discover_waves`` used to decide whether we
held a catalog entry by rebuilding a wave *label* from the entry's year range
and string-matching it against directory names.  Labels collide across
distinct surveys, so this was wrong in both directions:

* *False positive* -- ``Nigeria/2018-19/`` holds GHS-Panel Wave 4 (id 3557).
  The Living Standards Survey (id 3827) also spans 2018-2019, so it rendered
  as the same label and was reported as already held.  It is not.
* *False negative* -- WB id 1001 (``UGA_2005-2009_UNPS``) renders as
  ``"2005-10"``, matching no directory, so it looked missing even though we
  hold it, split across ``2005-06/`` and ``2009-10/``.

Matching is now on the catalog id recorded in each wave's SOURCE.org.

All tests here are hermetic: the WB catalog is mocked, no network is touched.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lsms_library import provenance as pv


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_source(root, country, wave, text):
    p = root / country / wave / "Documentation"
    p.mkdir(parents=True, exist_ok=True)
    (p / "SOURCE.org").write_text(text)


def _mkwave(root, country, wave):
    """Create a wave dir with no SOURCE.org."""
    (root / country / wave / "Data").mkdir(parents=True, exist_ok=True)


# Catalog rows mirroring the real WB entries these tests are about.
NGA_ROWS = [
    {"id": "1002", "idno": "NGA_2010_GHSP-W1_v03_M", "title": "GHS Panel W1",
     "year_start": 2010, "year_end": 2011, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/1002"},
    {"id": "3557", "idno": "NGA_2018_GHSP-W4_v03_M", "title": "GHS Panel W4",
     "year_start": 2018, "year_end": 2019, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/3557"},
    {"id": "3827", "idno": "NGA_2018_LSS_v01_M", "title": "Living Standards Survey",
     "year_start": 2018, "year_end": 2019, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/3827"},
]

UGA_ROWS = [
    {"id": "1001", "idno": "UGA_2005-2009_UNPS_v03_M", "title": "UNPS 2005-2009",
     "year_start": 2005, "year_end": 2010, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/1001"},
]

GHA_ROWS = [
    {"id": "2313", "idno": "GHA_1987_GLSS_v02_M", "title": "GLSS I",
     "year_start": 1987, "year_end": 1988, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2313"},
    {"id": "2534", "idno": "GHA_2009_GSPS_v01_M", "title": "GSPS 2009-2010",
     "year_start": 2009, "year_end": 2010, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2534"},
]


@pytest.fixture()
def countries(tmp_path, monkeypatch):
    """A temp countries tree wired into data_access."""
    from lsms_library import data_access as da
    root = tmp_path / "countries"
    root.mkdir()
    monkeypatch.setattr(da, "_COUNTRIES_DIR", root)
    return root


def _mock_catalog(monkeypatch, rows):
    from lsms_library import data_access as da
    monkeypatch.setattr(da, "_wb_catalog_search",
                        lambda code, collection="lsms": list(rows))


# ---------------------------------------------------------------------------
# SOURCE.org parsing / rendering
# ---------------------------------------------------------------------------

class TestSourceOrgFormat:
    def test_legacy_bare_url_yields_catalog_id(self):
        """A pre-existing SOURCE.org (bare /catalog/{id} URL) still parses."""
        prov = pv.parse_source_org(
            "SOURCE\n\nhttps://microdata.worldbank.org/index.php/catalog/3557",
            "Nigeria", "2018-19")
        assert prov.source == pv.SOURCE_WORLDBANK
        assert prov.catalog_id == "3557"
        assert prov.is_worldbank

    def test_legacy_wb_doi_is_worldbank_but_id_unknown(self):
        """A WB DOI identifies a WB study but does not encode the numeric id."""
        prov = pv.parse_source_org(
            "SOURCE\n\nhttps://doi.org/10.48529/q9zx-4b28]]", "Albania", "2002")
        assert prov.source == pv.SOURCE_WORLDBANK
        assert prov.catalog_id is None
        assert not prov.is_worldbank  # no id -> cannot match on identity

    def test_non_wb_host_is_external(self):
        prov = pv.parse_source_org(
            "SOURCE\n\nhttps://doi.org/10.7910/DVN/T8G8IV", "EthiopiaRHS", "1989")
        assert prov.source == pv.SOURCE_EXTERNAL
        assert prov.catalog_id is None

    def test_missing_file_is_unknown_never_a_guess(self, tmp_path):
        prov = pv.read_provenance(tmp_path, "Bulgaria", "1995")
        assert prov.source == pv.SOURCE_UNKNOWN
        assert prov.catalog_id is None
        assert not prov.is_resolved

    def test_round_trip(self):
        orig = pv.WaveProvenance(
            country="Nigeria", wave="2018-19", source=pv.SOURCE_WORLDBANK,
            catalog_id="3557", idno="NGA_2018_GHSP-W4_v03_M",
            title="General Household Survey, Panel 2018-2019, Wave 4",
            years="2018-2019", repository="lsms",
            url="https://microdata.worldbank.org/index.php/catalog/3557",
            method="manual-override", recorded="2026-07-12")
        back = pv.parse_source_org(pv.render_source_org(orig),
                                   "Nigeria", "2018-19")
        for f in ("source", "catalog_id", "idno", "title", "years",
                  "repository", "url", "method", "recorded"):
            assert getattr(back, f) == getattr(orig, f), f

    def test_unknown_is_recorded_explicitly(self):
        """Silence must not masquerade as knowledge: 'unknown' is written out."""
        text = pv.render_source_org(pv.WaveProvenance(
            country="Bulgaria", wave="1995", source=pv.SOURCE_UNKNOWN))
        assert "#+CATALOG_ID: unknown" in text
        assert "#+PROVENANCE_SOURCE: unknown" in text

    def test_external_records_none_not_unknown(self):
        """'no WB id exists' is distinct from 'we do not know the WB id'."""
        text = pv.render_source_org(pv.WaveProvenance(
            country="EthiopiaRHS", wave="1989", source=pv.SOURCE_EXTERNAL,
            url="https://doi.org/10.7910/DVN/T8G8IV"))
        assert "#+CATALOG_ID: none" in text

    def test_human_prose_is_preserved_not_overwritten(self):
        """Legacy SOURCE.org files carry hand-written notes (the EthiopiaRHS
        round map, for one).  Stamping provenance must not destroy them."""
        original = (
            "SOURCE\n\nhttps://doi.org/10.7910/DVN/T8G8IV\n\n"
            "Ethiopian Rural Household Survey (ERHS) -- 1989 wave.\n"
            "All ERHS rounds share one Dataverse deposit; see "
            "../../_/CONTENTS.org\nfor the round->file map.\n")

        prose = pv.preserved_prose(original)
        assert "round->file map" in prose
        assert "SOURCE" not in prose            # boilerplate dropped
        assert "doi.org" not in prose           # the bare URL is not prose

        rendered = pv.render_source_org(pv.WaveProvenance(
            country="EthiopiaRHS", wave="1989", source=pv.SOURCE_EXTERNAL,
            url="https://doi.org/10.7910/DVN/T8G8IV", notes=prose))
        assert "round->file map" in rendered
        assert "All ERHS rounds share one Dataverse deposit" in rendered

    def test_rendering_is_idempotent(self):
        """Re-stamping an already-stamped file must not nest or duplicate the
        preserved-notes block."""
        prov = pv.WaveProvenance(
            country="EthiopiaRHS", wave="1989", source=pv.SOURCE_EXTERNAL,
            url="https://doi.org/10.7910/DVN/T8G8IV",
            notes="All ERHS rounds share one Dataverse deposit.")
        once = pv.render_source_org(prov)
        twice = pv.render_source_org(
            pv.parse_source_org(once, "EthiopiaRHS", "1989"))
        assert once == twice
        assert twice.count(pv.NOTES_HEADING) == 1

    def test_superseded_url_never_self_references(self):
        """Guard for a bug this work introduced and then fixed: on a re-run the
        'URL we replaced' must not be overwritten with the current URL, which
        would silently erase the record of the correction."""
        text = pv.render_source_org(pv.WaveProvenance(
            country="Nigeria", wave="2018-19", source=pv.SOURCE_WORLDBANK,
            catalog_id="3557",
            url="https://microdata.worldbank.org/index.php/catalog/3557",
            superseded_url="https://microdata.worldbank.org/index.php/catalog/3557"))
        assert "PROVENANCE_SUPERSEDED_URL" not in text

        # A genuinely different prior URL IS recorded.
        text = pv.render_source_org(pv.WaveProvenance(
            country="Nigeria", wave="2018-19", source=pv.SOURCE_WORLDBANK,
            catalog_id="3557",
            url="https://microdata.worldbank.org/index.php/catalog/3557",
            superseded_url="https://microdata.worldbank.org/index.php/catalog/3827"))
        assert "PROVENANCE_SUPERSEDED_URL: " \
               "https://microdata.worldbank.org/index.php/catalog/3827" in text

    def test_url_stays_first_url_for_legacy_reader(self):
        """data_access._read_source_url greps the first http(s):// in the file.

        The structured record must not displace it, or the WB download path
        breaks for every backfilled wave.
        """
        from lsms_library.data_access import _read_source_url
        from lsms_library import data_access as da

        prov = pv.WaveProvenance(
            country="Nigeria", wave="2018-19", source=pv.SOURCE_WORLDBANK,
            catalog_id="3557", idno="NGA_2018_GHSP-W4_v03_M",
            url="https://microdata.worldbank.org/index.php/catalog/3557")
        text = pv.render_source_org(prov)

        import re
        first = re.search(r"https?://[^\s\]\)]+", text).group(0)
        assert first == "https://microdata.worldbank.org/index.php/catalog/3557"


# ---------------------------------------------------------------------------
# discover_waves: identity matching
# ---------------------------------------------------------------------------

class TestDiscoverWavesMatchesOnCatalogId:

    def test_nigeria_lss_reported_missing_not_held(self, countries, monkeypatch):
        """THE regression: Nigeria/2018-19 holds GHS-Panel W4 (3557), not the
        LSS (3827).  Both span 2018-2019.  The LSS must report as MISSING."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, NGA_ROWS)

        _write_source(countries, "Nigeria", "2018-19", pv.render_source_org(
            pv.WaveProvenance(country="Nigeria", wave="2018-19",
                              source=pv.SOURCE_WORLDBANK, catalog_id="3557",
                              idno="NGA_2018_GHSP-W4_v03_M",
                              url="https://microdata.worldbank.org"
                                  "/index.php/catalog/3557")))
        _write_source(countries, "Nigeria", "2010-11", pv.render_source_org(
            pv.WaveProvenance(country="Nigeria", wave="2010-11",
                              source=pv.SOURCE_WORLDBANK, catalog_id="1002",
                              url="https://microdata.worldbank.org"
                                  "/index.php/catalog/1002")))

        by_id = {e["id"]: e for e in discover_waves("Nigeria")}

        # The survey we actually hold.
        assert by_id["3557"]["local"] is True
        assert by_id["3557"]["local_status"] == "yes"
        assert by_id["3557"]["local_waves"] == ["2018-19"]

        # The survey we do NOT hold, whose label collides with the one we do.
        assert by_id["3827"]["local"] is False
        assert by_id["3827"]["local_status"] == "no"
        assert by_id["3827"]["local_waves"] == []

        # Both still render to the same colliding label -- which is precisely
        # why label matching could not tell them apart.
        assert by_id["3557"]["wave"] == by_id["3827"]["wave"] == "2018-19"

    def test_one_catalog_id_can_back_several_wave_dirs(self, countries,
                                                       monkeypatch):
        """WB 1001 (UNPS 2005-2009) backs BOTH Uganda/2005-06 and /2009-10.

        Its year range renders as '2005-10', which matches no directory, so
        label matching called it missing.  Identity matching finds it."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, UGA_ROWS)

        for wave in ("2005-06", "2009-10"):
            _write_source(countries, "Uganda", wave, pv.render_source_org(
                pv.WaveProvenance(country="Uganda", wave=wave,
                                  source=pv.SOURCE_WORLDBANK, catalog_id="1001",
                                  url="https://microdata.worldbank.org"
                                      "/index.php/catalog/1001")))

        entry = discover_waves("Uganda")[0]
        assert entry["wave"] == "2005-10"          # matches no directory
        assert entry["local"] is True              # ... yet we hold it
        assert entry["local_status"] == "yes"
        assert entry["local_waves"] == ["2005-06", "2009-10"]

    def test_unprovenanced_wave_is_unknown_not_a_confident_claim(
            self, countries, monkeypatch):
        """A dir with no SOURCE.org falls back to the label heuristic, but the
        result is marked 'unknown' -- never a confident True."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, NGA_ROWS)

        _mkwave(countries, "Nigeria", "2018-19")   # no SOURCE.org at all

        by_id = {e["id"]: e for e in discover_waves("Nigeria")}
        for cid in ("3557", "3827"):
            assert by_id[cid]["local_status"] == "unknown", cid
            # An unverified claim must not read as "held".
            assert by_id[cid]["local"] is False, cid

        # A label matching no directory is still a confident "no".
        assert by_id["1002"]["local_status"] == "no"

    def test_local_remains_a_bool_for_existing_callers(self, countries,
                                                       monkeypatch):
        """Backwards compatibility: `local` keeps its type and truthiness."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, NGA_ROWS)
        _mkwave(countries, "Nigeria", "2018-19")

        for e in discover_waves("Nigeria"):
            assert isinstance(e["local"], bool)
            assert {"wave", "local", "id", "idno", "title"} <= set(e)


class TestSharedIsoCodeDisambiguation:
    """GHA and TZA each back two distinct survey series in this repo."""

    def test_ghana_lss_does_not_see_the_panel_surveys_waves(self, countries,
                                                            monkeypatch):
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, GHA_ROWS)
        _mkwave(countries, "GhanaLSS", "1987-88")

        idnos = {e["idno"] for e in discover_waves("GhanaLSS")}
        assert idnos == {"GHA_1987_GLSS_v02_M"}
        assert "GHA_2009_GSPS_v01_M" not in idnos

    def test_ghana_sps_does_not_see_the_living_standards_waves(self, countries,
                                                               monkeypatch):
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, GHA_ROWS)
        _mkwave(countries, "GhanaSPS", "2009-10")

        idnos = {e["idno"] for e in discover_waves("GhanaSPS")}
        assert idnos == {"GHA_2009_GSPS_v01_M"}

    def test_tanzania_and_kagera_are_separate_series(self):
        from lsms_library.data_access import _COUNTRY_CATALOG
        nps = _COUNTRY_CATALOG["Tanzania"]
        khds = _COUNTRY_CATALOG["Tanzania_Kegera"]
        assert nps.code == khds.code == "TZA"

        npsrow = {"idno": "TZA_2020_NPS-R5_v02_M"}
        khdsrow = {"idno": "TZA_2010_KHDS_v01_M"}
        assert nps.matches(npsrow) and not nps.matches(khdsrow)
        assert khds.matches(khdsrow) and not khds.matches(npsrow)


class TestSourceOrgIsLoadBearing:
    """SOURCE.org's *presence* declares a directory to be a wave.

    Country.waves (country.py) falls back to scanning for subdirectories
    containing Documentation/SOURCE.org when the country declares no explicit
    wave list.  So creating a SOURCE.org in a directory that is not a wave
    silently promotes it into one.

    This bit us for real: Benin/2018-2019/ is a stray duplicate of
    Benin/2018-19/ (identical .dta files, no SOURCE.org).  Stamping it made it
    a wave, and sample() did not cover it -- breaking
    test_sample.py::test_covers_all_waves[Benin].

    The backfill must therefore only ever UPDATE an existing SOURCE.org, or
    CREATE one in a directory the library already declares a wave.
    """

    def test_backfill_refuses_to_create_source_org_in_an_undeclared_dir(
            self, tmp_path):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "backfill", Path(__file__).resolve().parents[1]
            / "scripts" / "backfill_wave_provenance.py")
        backfill = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(backfill)

        root = tmp_path / "countries"
        # A declared wave that already has a SOURCE.org.
        _write_source(root, "Benin", "2018-19", "SOURCE\n\nhttps://x/catalog/4291")
        # A stray duplicate directory: data, but no SOURCE.org, not declared.
        (root / "Benin" / "2018-2019" / "Data").mkdir(parents=True)

        declared = {"2018-19"}
        assert backfill._may_write("Benin", "2018-19", root, declared) is True
        assert backfill._may_write("Benin", "2018-2019", root, declared) is False

        # An undeclared dir stays undeclared: no Documentation/ is created.
        assert not (root / "Benin" / "2018-2019" / "Documentation").exists()

    def test_may_write_is_conservative_when_waves_cannot_be_read(self, tmp_path):
        """If we cannot determine the declared waves, never create a new
        SOURCE.org -- only update ones that already exist."""
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "backfill", Path(__file__).resolve().parents[1]
            / "scripts" / "backfill_wave_provenance.py")
        backfill = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(backfill)

        root = tmp_path / "countries"
        _write_source(root, "Benin", "2018-19", "SOURCE\n\nhttps://x/catalog/4291")
        (root / "Benin" / "2018-2019" / "Data").mkdir(parents=True)

        assert backfill._may_write("Benin", "2018-19", root, None) is True
        assert backfill._may_write("Benin", "2018-2019", root, None) is False


class TestCountryCatalogRegistry:
    def test_serbia_and_montenegro_has_a_code(self):
        """SCG returns catalog ids 80 and 81, matching our 2002/ and 2003/."""
        from lsms_library.data_access import _COUNTRY_CATALOG, _COUNTRY_CODES
        assert _COUNTRY_CATALOG["Serbia and Montenegro"].code == "SCG"
        assert _COUNTRY_CODES["Serbia and Montenegro"] == "SCG"

    def test_ethiopia_rhs_is_explicitly_not_discoverable(self, countries):
        """EthiopiaRHS is an IFPRI/Dataverse study, not a WB one.  It must be
        marked not-discoverable, not merely left out of the registry."""
        from lsms_library.data_access import _COUNTRY_CATALOG, discover_waves
        spec = _COUNTRY_CATALOG["EthiopiaRHS"]
        assert spec.discoverable is False
        assert spec.reason and "Dataverse" in spec.reason
        # Returns empty without erroring, and without hitting the network.
        assert discover_waves("EthiopiaRHS") == []

    def test_unregistered_country_returns_empty(self, countries):
        from lsms_library.data_access import discover_waves
        assert discover_waves("Atlantis") == []

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

import datetime
from dataclasses import replace
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


class TestEveryDeclaredWaveIsResolved:
    """The shipped tree records a terminal answer for every declared wave.

    'unknown' is a legitimate *state* -- but it must not be a resting place.
    Each of the five waves that started out unknown was resolved from evidence:
    three to a catalog id (Niger/2011-12 from a Data/ directory literally named
    NER_2011_ECVMA_v01_M_Stata8; Senegal/2021-22 from 58 ehcvm_*_sen2021 files;
    Nepal/2003-04 on catalog metadata alone, flagged catalog-only because its
    Data/ is empty), and two to 'none' (GhanaSPS 2013-14 / 2017-18 are
    EGC-ISSER, and the entire WB catalog holds exactly one GSPS study).
    """

    def test_no_shipped_source_org_is_left_unknown(self):
        """Every SOURCE.org we ship records a terminal answer.

        Iterates the files themselves rather than ``Country.waves`` -- provenance
        is recorded per *directory*, and ``Country.waves`` is not always a list
        of directory names (Nigeria's are rounds like ``2010Q3``, Tanzania's are
        logical waves; both map to folders via ``wave_folder_map``).
        """
        from lsms_library.paths import countries_root

        root = countries_root()
        unknown = []
        for src in sorted(root.glob("*/*/Documentation/SOURCE.org")):
            country, wave = src.parts[-4], src.parts[-3]
            if not pv.read_provenance(root, country, wave).is_resolved:
                unknown.append(f"{country}/{wave}")
        assert not unknown, (
            "SOURCE.org files with unresolved provenance: " + ", ".join(unknown))

    @pytest.mark.parametrize("country,wave,catalog_id,validation", [
        ("Nigeria", "2018-19", "3557", pv.VALIDATION_CONTENT),
        ("Niger", "2011-12", "2050", pv.VALIDATION_CONTENT),
        ("Senegal", "2021-22", "6278", pv.VALIDATION_CONTENT),
        # Provenance known, data absent -- two different facts, kept distinct.
        ("Nepal", "2003-04", "74", pv.VALIDATION_CATALOG_ONLY),
    ])
    def test_evidence_resolved_waves(self, country, wave, catalog_id, validation):
        from lsms_library.paths import countries_root
        prov = pv.read_provenance(countries_root(), country, wave)
        assert prov.catalog_id == catalog_id
        assert prov.is_worldbank
        assert prov.validation == validation

    @pytest.mark.parametrize("wave", ["2013-14", "2017-18"])
    def test_ghana_sps_egc_isser_waves_are_none_not_unknown(self, wave):
        """A terminal 'none' -- otherwise these get re-searched forever."""
        from lsms_library.paths import countries_root
        prov = pv.read_provenance(countries_root(), "GhanaSPS", wave)
        assert prov.source == pv.SOURCE_EXTERNAL
        assert prov.catalog_id is None
        assert prov.is_resolved            # resolved, though not a WB study
        text = pv.source_org_path(countries_root(), "GhanaSPS",
                                  wave).read_text()
        assert "#+CATALOG_ID: none" in text
        assert "#+CATALOG_ID: unknown" not in text


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


# ---------------------------------------------------------------------------
# GH #600 -- catalog_id is not a 1:1 key, in EITHER direction.
#
# The failure this section pins down is not a gap but a CONFIDENT FALSE CLAIM:
# `local_status='no'` on studies we demonstrably hold.  Every relation asserted
# below was checked against the WB datafile API
# (`/api/catalog/{id}/data_files`) -- see `.coder/ledger/600-provenance-1to1.md`
# for the file counts and the instrument's positive/negative controls.
# ---------------------------------------------------------------------------

# Malawi: ONE directory, TWO catalog entries.  Malawi/2016-17/Data/
# Cross_Sectional/ is IHS4 (2936); Data/Panel/ is the 2016 wave of the IHPS
# long-term panel (2939) -- 97 of its 98 files are in 2939's datafile list and
# NONE in 2936's.
MWI_ROWS = [
    {"id": "1003", "idno": "MWI_2010_IHS-III_v01_M", "title": "IHS3",
     "year_start": 2010, "year_end": 2011, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/1003"},
    {"id": "2936", "idno": "MWI_2016_IHS-IV_v04_M", "title": "IHS4",
     "year_start": 2016, "year_end": 2017, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2936"},
    {"id": "2939", "idno": "MWI_2010-2016_IHPS_v03_M", "title": "IHPS long panel",
     "year_start": 2010, "year_end": 2016, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2939"},
]

# Tanzania: ONE directory (2008-15/), whose held release (the Uniform Panel
# Dataset, 3814) SUBSUMES four entries whose files we do not hold.
TZA_ROWS = [
    {"id": "76", "idno": "TZA_2008_NPS-R1_v03_M", "title": "NPS Round 1",
     "year_start": 2008, "year_end": 2009, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/76"},
    {"id": "1050", "idno": "TZA_2010_NPS-R2_v03_M", "title": "NPS Round 2",
     "year_start": 2010, "year_end": 2011, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/1050"},
    {"id": "2252", "idno": "TZA_2012_NPS-R3_v01_M", "title": "NPS Round 3",
     "year_start": 2012, "year_end": 2013, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2252"},
    {"id": "2862", "idno": "TZA_2014_NPS-R4_v03_M", "title": "NPS Round 4",
     "year_start": 2014, "year_end": 2015, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2862"},
    {"id": "3814", "idno": "TZA_2008-2014_NPS-UPD_v01_M", "title": "Uniform Panel Dataset",
     "year_start": 2008, "year_end": 2015, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/3814"},
    # An alternative version of Round 4 whose master (2862) we do NOT hold, and
    # a different survey entirely.  Neither is covered by the UPD.
    {"id": "3455", "idno": "TZA_2014_NPS-R4_v03_M_v03_A_EXT", "title": "NPS R4 extended",
     "year_start": 2013, "year_end": 2016, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/3455"},
    {"id": "2863", "idno": "TZA_2016_NPS-FTFISS_v01_M", "title": "Feed the Future ISS",
     "year_start": 2016, "year_end": 2016, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2863"},
]

# Nigeria: the WB re-releases the four GHS-Panel waves we hold as one
# harmonized "Uniform Panel Dataset" (5835).  We hold every constituent.
NGA_NUPD_ROWS = NGA_ROWS + [
    {"id": "2734", "idno": "NGA_2015_GHSP-W3_v02_M", "title": "GHS Panel W3",
     "year_start": 2015, "year_end": 2016, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/2734"},
    {"id": "1952", "idno": "NGA_2012_GHSP-W2_v02_M", "title": "GHS Panel W2",
     "year_start": 2012, "year_end": 2013, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/1952"},
    {"id": "5835", "idno": "NGA_2010-2019_NUPD_v01_M", "title": "GHS Panel Uniform Panel Data",
     "year_start": 2010, "year_end": 2019, "doi": "", "repository": "lsms",
     "url": "https://microdata.worldbank.org/index.php/catalog/5835"},
]


def _wb(country, wave, catalog_id=None, catalog_ids=None, covers=None):
    """A rendered worldbank SOURCE.org for a wave."""
    return pv.render_source_org(pv.WaveProvenance(
        country=country, wave=wave, source=pv.SOURCE_WORLDBANK,
        catalog_id=catalog_id, catalog_ids=list(catalog_ids or []),
        covers=list(covers or []),
        url=("https://microdata.worldbank.org/index.php/catalog/"
             f"{(catalog_ids or [catalog_id])[0]}")))


class TestMultipleIdsPerWaveDir:
    """One directory can hold the files of SEVERAL catalog entries."""

    def test_preexisting_positional_constructor_order_is_unchanged(self):
        prov = pv.WaveProvenance("Nigeria", "2018-19", pv.SOURCE_WORLDBANK,
                                 "3557", "NGA_2018_GHSP-W4_v03_M", "GHS Panel W4")
        assert prov.catalog_id == "3557"
        assert prov.catalog_ids == ["3557"]
        assert prov.idno == "NGA_2018_GHSP-W4_v03_M"
        assert prov.title == "GHS Panel W4"
        assert prov.covers == []

    def test_repeated_catalog_id_lines_accumulate(self):
        """They used to silently LAST-WIN: a dict comprehension over findall.

        You could not record a second id even by hand, and nothing said so."""
        prov = pv.parse_source_org(
            "SOURCE\n\nhttps://x/catalog/2936\n\n"
            "#+CATALOG_ID: 2936\n#+CATALOG_ID: 2939\n"
            "#+PROVENANCE_SOURCE: worldbank\n", "Malawi", "2016-17")
        assert prov.catalog_ids == ["2936", "2939"]

    def test_comma_separated_catalog_id_list(self):
        prov = pv.parse_source_org(
            "SOURCE\n\nhttps://x/catalog/2936\n\n"
            "#+CATALOG_ID: 2936, 2939\n#+PROVENANCE_SOURCE: worldbank\n",
            "Malawi", "2016-17")
        assert prov.catalog_ids == ["2936", "2939"]
        # The scalar stays the PRIMARY entry -- every pre-#600 reader still works.
        assert prov.catalog_id == "2936"
        assert prov.is_worldbank

    def test_single_id_still_parses_as_a_one_element_list(self):
        """The 121 other shipped SOURCE.org files must be unaffected."""
        prov = pv.parse_source_org(
            "SOURCE\n\nhttps://x/catalog/3557\n\n#+CATALOG_ID: 3557\n"
            "#+PROVENANCE_SOURCE: worldbank\n", "Nigeria", "2018-19")
        assert prov.catalog_ids == ["3557"]
        assert prov.catalog_id == "3557"

    def test_render_then_parse_round_trips_a_multi_id_record(self):
        text = _wb("Malawi", "2016-17", catalog_ids=["2936", "2939"])
        assert "#+CATALOG_ID: 2936, 2939" in text
        back = pv.parse_source_org(text, "Malawi", "2016-17")
        assert back.catalog_ids == ["2936", "2939"]
        # Idempotent: rendering the parsed record reproduces the file.
        assert pv.render_source_org(back) == text

    def test_local_catalog_ids_reports_every_id_a_dir_holds(self, countries):
        from lsms_library.data_access import local_catalog_ids
        _write_source(countries, "Malawi", "2016-17",
                      _wb("Malawi", "2016-17", catalog_ids=["2936", "2939"]))
        held = local_catalog_ids("Malawi")
        assert held == {"2936": ["2016-17"], "2939": ["2016-17"]}

    def test_ihps_panel_we_hold_is_not_reported_missing(self, countries,
                                                       monkeypatch):
        """THE regression (GH #600).  Malawi/2016-17/Data/Panel/ IS the 2016
        wave of the IHPS long-term panel (2939): 97 of its 98 files appear in
        2939's WB datafile list and none in IHS4's.  Recording only 2936 made
        discovery report a study we hold as MISSING."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, MWI_ROWS)
        _write_source(countries, "Malawi", "2010-11",
                      _wb("Malawi", "2010-11", catalog_id="1003"))
        _write_source(countries, "Malawi", "2016-17",
                      _wb("Malawi", "2016-17", catalog_ids=["2936", "2939"]))

        by_id = {e["id"]: e for e in discover_waves("Malawi")}
        for cid in ("2936", "2939"):
            assert by_id[cid]["local_status"] == "yes", cid
            assert by_id[cid]["local"] is True, cid
            assert by_id[cid]["local_waves"] == ["2016-17"], cid


class TestCoveredIsNotHeldAndIsNotMissing:
    """Tanzania/2008-15/ holds ONE entry (the UPD) that SUBSUMES four others."""

    def test_covers_round_trips(self):
        text = _wb("Tanzania", "2008-15", catalog_id="3814",
                   covers=["76", "1050", "2252", "2862"])
        assert "#+CATALOG_COVERS: 76, 1050, 2252, 2862" in text
        back = pv.parse_source_org(text, "Tanzania", "2008-15")
        assert back.catalog_ids == ["3814"]
        assert back.covers == ["76", "1050", "2252", "2862"]
        assert pv.render_source_org(back) == text

    def test_the_four_nps_rounds_are_covered_not_missing(self, countries,
                                                         monkeypatch):
        """THE regression (GH #600).  We hold the Uniform Panel Dataset, whose
        `round` column carries rounds 1-4 -- which is why Country('Tanzania')
        exposes four waves out of one directory.  Discovery reported all four
        individual NPS entries as `no`: the largest cluster of false 'missing
        wave' rows in the census."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, TZA_ROWS)
        _write_source(countries, "Tanzania", "2008-15",
                      _wb("Tanzania", "2008-15", catalog_id="3814",
                          covers=["76", "1050", "2252", "2862"]))

        by_id = {e["id"]: e for e in discover_waves("Tanzania")}
        assert by_id["3814"]["local_status"] == "yes"
        for cid in ("76", "1050", "2252", "2862"):
            assert by_id[cid]["local_status"] == "covered", cid
            # Covered is not held: we do NOT have these entries' files.  The
            # bool contract is unchanged for every existing caller.
            assert by_id[cid]["local"] is False, cid
            assert by_id[cid]["local_waves"] == ["2008-15"], cid

    def test_covered_does_not_sweep_up_neighbouring_entries(self, countries,
                                                            monkeypatch):
        """`covered` must be earned per-entry, not inferred from the folder.

        3455 is an alternative version of Round 4 whose master we do not hold;
        2863 is a different survey (Feed the Future).  Both stay `no`."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, TZA_ROWS)
        _write_source(countries, "Tanzania", "2008-15",
                      _wb("Tanzania", "2008-15", catalog_id="3814",
                          covers=["76", "1050", "2252", "2862"]))

        by_id = {e["id"]: e for e in discover_waves("Tanzania")}
        assert by_id["3455"]["local_status"] == "no"
        assert by_id["2863"]["local_status"] == "no"


class TestPartialRecordCannotMasqueradeAsKnowledge:
    """A multi-round folder whose record does not account for a logical wave
    must yield `unknown` -- never a confident `no`."""

    def test_unrecorded_logical_wave_is_unknown_not_no(self, countries,
                                                       monkeypatch):
        """The structural backstop: with NO `covers` line at all, the four NPS
        entries are still not called missing, because Tanzania exposes waves
        with exactly those labels out of its multi-round folder.  Silence must
        not masquerade as knowledge -- and neither must a partial record."""
        from lsms_library import data_access as da
        _mock_catalog(monkeypatch, TZA_ROWS)
        # The folder records the UPD and nothing else -- the pre-#600 state.
        _write_source(countries, "Tanzania", "2008-15",
                      _wb("Tanzania", "2008-15", catalog_id="3814"))
        # Tanzania's wave_folder_map backs four logical waves out of 2008-15/.
        monkeypatch.setattr(da, "_logical_wave_labels",
                            lambda c: {"2008-09", "2010-11", "2012-13",
                                       "2014-15"})

        by_id = {e["id"]: e for e in da.discover_waves("Tanzania")}
        for cid in ("76", "1050", "2252", "2862"):
            assert by_id[cid]["local_status"] == "unknown", cid
            assert by_id[cid]["local"] is False, cid
        # A different survey that shares no logical-wave label is still a
        # confident `no`: the escape hatch must not become a blanket amnesty.
        assert by_id["2863"]["local_status"] == "no"

    def test_logical_wave_labels_finds_tanzanias_multiround_waves(self):
        """Validate the instrument itself: the backstop reads the country's own
        wave_folder_map, so it must actually SEE Tanzania's four folder-backed
        waves.  A check that cannot find anything proves nothing."""
        from lsms_library.data_access import _logical_wave_labels
        assert _logical_wave_labels("Tanzania") == {
            "2008-09", "2010-11", "2012-13", "2014-15"}
        # A country with a dir per wave has no folder-backed logical waves.
        assert _logical_wave_labels("Malawi") == set()


class TestCatalogRelations:
    """Facts about the CATALOG, which no wave dir of ours can state alone."""

    def test_a_uniform_panel_built_from_waves_we_hold_is_not_a_gap(
            self, countries, monkeypatch):
        """Nigeria 5835 (`NGA_2010-2019_NUPD`) is the four GHS-Panel waves we
        hold, harmonized -- not new fieldwork, not an acquisition target."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, NGA_NUPD_ROWS)
        for wave, cid in (("2010-11", "1002"), ("2012-13", "1952"),
                          ("2015-16", "2734"), ("2018-19", "3557")):
            _write_source(countries, "Nigeria", wave,
                          _wb("Nigeria", wave, catalog_id=cid))

        by_id = {e["id"]: e for e in discover_waves("Nigeria")}
        assert by_id["5835"]["local_status"] == "derived"
        assert by_id["5835"]["local"] is False          # we hold no NUPD file
        assert by_id["5835"]["local_waves"] == ["2010-11", "2012-13",
                                                "2015-16", "2018-19"]
        # The survey we genuinely do not hold is still a confident `no`.
        assert by_id["3827"]["local_status"] == "no"

    def test_derived_needs_EVERY_constituent_and_downgrades_on_its_own(
            self, countries, monkeypatch):
        """The completeness rule is what keeps `derived_from` honest.  Drop one
        constituent wave and 5835 goes back to being reported missing, with
        nobody having to remember to edit catalog_relations.yml."""
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, NGA_NUPD_ROWS)
        for wave, cid in (("2010-11", "1002"), ("2012-13", "1952"),
                          ("2018-19", "3557")):       # 2734 (W3) NOT held
            _write_source(countries, "Nigeria", wave,
                          _wb("Nigeria", wave, catalog_id=cid))

        by_id = {e["id"]: e for e in discover_waves("Nigeria")}
        assert by_id["5835"]["local_status"] == "no"

    @pytest.mark.parametrize("held_id", ["297", "902"])
    def test_same_study_under_two_repository_ids_reads_as_held(
            self, countries, monkeypatch, held_id):
        """South Africa 1993 is `lsms` 297 AND `datafirst` 902 -- identical
        71-file lists, one survey.  A census that surfaces 902 must not call it
        a missing wave.

        Today two things keep 902 out of the census: the search is `lsms`-only,
        and (with GH #597/PR #599) South Africa's `idno_pattern` pins the
        IHS/GHS series.  Both are *incidental* guards -- widen either and the
        duplicate reappears.  So the test admits the entry deliberately (a
        pattern-free spec, i.e. a widening) and checks the guard that is
        actually about identity: the same-study alias."""
        from lsms_library.data_access import (CountryCatalog, _COUNTRY_CATALOG,
                                              discover_waves)
        monkeypatch.setitem(_COUNTRY_CATALOG, "South Africa",
                            CountryCatalog("ZAF"))
        _mock_catalog(monkeypatch, [
            {"id": "297", "idno": "ZAF_1993_IHS_v01_M", "title": "IHS 1993",
             "year_start": 1993, "year_end": 1993, "doi": "",
             "repository": "lsms",
             "url": "https://microdata.worldbank.org/index.php/catalog/297"},
            {"id": "902", "idno": "ZAF_1993_PSLSD_v01_M", "title": "PSLSD 1993",
             "year_start": 1993, "year_end": 1993, "doi": "",
             "repository": "datafirst",
             "url": "https://microdata.worldbank.org/index.php/catalog/902"},
        ])
        _write_source(countries, "South Africa", "1993",
                      _wb("South Africa", "1993", catalog_id=held_id))

        by_id = {e["id"]: e for e in discover_waves("South Africa")}
        assert by_id["297"]["local_status"] == "yes"
        assert by_id["902"]["local_status"] == "yes"
        assert by_id["902"]["local_waves"] == ["1993"]

    def test_a_derived_subset_is_not_an_alias(self):
        """Malawi `central` 3016 is a 4-file ML subset of IHS3 (1003), NOT a
        co-equal re-catalogue.  Flattening a subset into an equivalence would
        repeat the original sin: a real relation crushed onto a key that cannot
        express it."""
        from lsms_library.catalog_relations import (derived_from,
                                                    same_study_aliases)
        assert derived_from()["3016"] == ["1003"]
        assert "3016" not in same_study_aliases()

    def test_every_relation_carries_its_evidence(self):
        """An unevidenced negative is unfalsifiable, and therefore permanent."""
        from lsms_library import catalog_relations as cr
        for cid in list(cr.derived_from()) + list(cr.same_study_aliases()):
            assert cr.evidence_for(cid), f"{cid} has no recorded evidence"


class TestBackfillDoesNotClobberHandRecordedFacts:

    def test_restamp_preserves_extra_ids_covers_note_and_validation(self):
        """`backfill_wave_provenance.py` rebuilds each record from the catalog.
        The catalog cannot tell it that a dir holds a SECOND entry, or that a
        release covers others -- so a naive re-stamp would silently delete
        exactly the facts this issue exists to record."""
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        import backfill_wave_provenance as bf

        entry = {"id": "3814", "idno": "TZA_2008-2014_NPS-UPD_v01_M",
                 "title": "UPD", "year_start": 2008, "year_end": 2015,
                 "doi": "", "repository": "lsms",
                 "url": "https://microdata.worldbank.org/index.php/catalog/3814"}
        prov = bf._from_entry(
            "Tanzania", "2008-15", entry, "source-url", "2026-07-12",
            note="evidence here", validation=pv.VALIDATION_CONTENT,
            also_ids=["9999"], covers=["76", "1050", "2252", "2862"])
        assert prov.catalog_ids == ["3814", "9999"]
        assert prov.covers == ["76", "1050", "2252", "2862"]
        assert prov.note == "evidence here"
        assert prov.validation == pv.VALIDATION_CONTENT


class TestShippedRecordsSayWhatWeMeasured:
    """The three directories whose provenance was structurally incomplete."""

    def test_tanzania_2008_15_covers_the_four_nps_rounds(self):
        from lsms_library.data_access import _COUNTRIES_DIR
        prov = pv.read_provenance(_COUNTRIES_DIR, "Tanzania", "2008-15")
        assert prov.catalog_ids == ["3814"]        # we hold the UPD, only
        assert prov.covers == ["76", "1050", "2252", "2862"]

    @pytest.mark.parametrize("wave,ids", [("2016-17", ["2936", "2939"]),
                                          ("2019-20", ["3818", "3819"])])
    def test_malawi_wave_dirs_hold_two_entries_each(self, wave, ids):
        from lsms_library.data_access import _COUNTRIES_DIR
        prov = pv.read_provenance(_COUNTRIES_DIR, "Malawi", wave)
        assert prov.catalog_ids == ids
        assert prov.covers == []                   # held, not covered

    # The two queued releases (Nigeria/2023-24, Ethiopia/2021-22) are NOT
    # pinned here: the queue is cleared by the acquisitions (#978, #1127) and
    # a pin on the shipped tree's queue state breaks the day they merge (red
    # team r1).  ``test_every_queued_release_differs_from_the_one_held`` pins
    # the invariant whatever the queue holds; the frozen literal records in
    # ``TestReleaseChangeIsNotAReStamp`` carry the held v01/v02 fixtures.

    def test_every_queued_release_differs_from_the_one_held(self):
        """A latest-seen key is present only beside an older held release
        of a WB entry: an equal release leaves none behind."""
        bad = []
        for path in _shipped_source_orgs():
            c, w = path.parts[-4], path.parts[-3]
            prov = pv.parse_source_org(path.read_text(), c, w)
            if any((prov.latest_idno, prov.latest_doi, prov.latest_seen)) \
                    and not (prov.is_worldbank and prov.is_stale_release
                             and prov.latest_seen):
                bad.append(f"{c}/{w}")
        assert bad == []


# ---------------------------------------------------------------------------
# GH #600: a failed resolution leaves the record intact
# ---------------------------------------------------------------------------
#
# Ruling (@ligon, 2026-10-18): on a failed resolution the backfill leaves the
# existing record intact and writes a separate review artifact listing what it
# could not resolve.  It never downgrades a recorded id to UNKNOWN.

def _load_backfill():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "backfill_600", Path(__file__).resolve().parents[1]
        / "scripts" / "backfill_wave_provenance.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def bf(countries, monkeypatch):
    """The backfill script, pointed at the temp tree, every dir a declared wave."""
    mod = _load_backfill()
    monkeypatch.setattr(mod, "countries_root", lambda: countries)
    monkeypatch.setattr(
        mod, "_declared_waves",
        lambda c: {d.name for d in (countries / c).iterdir() if d.is_dir()})
    return mod


def _catalog(monkeypatch, bf, rows):
    monkeypatch.setattr(bf, "_wb_catalog_search",
                        lambda code, collection="lsms": list(rows))


def _unresolved(review):
    from lsms_library.local_tools import df_from_orgfile
    df = df_from_orgfile(review, name="unresolved", to_numeric=False)
    return {(r["Country"], r["Wave"]): r for r in df.to_dict("records")}


def _snapshot(root):
    return {p: p.read_bytes() for p in root.rglob("SOURCE.org")}


# Catalog 617 as the live catalog reported it on 2026-10-19: no DOI, although
# the shipped Peru/1994 record carries one.
PER_617_NO_DOI = {
    "id": "617", "idno": "PER_1994_ENNIV_v01_M",
    "title": ("Encuesta Nacional de Hogares sobre Medición de Niveles de "
              "Vida 1994"),
    "year_start": 1994, "year_end": 1994, "doi": "", "repository": "lsms",
    "url": "https://microdata.worldbank.org/index.php/catalog/617"}

TZA_UPD = (
    "SOURCE\n\nhttps://microdata.worldbank.org/index.php/catalog/3814\n\n"
    "#+CATALOG_ID: 3814\n"
    "#+CATALOG_COVERS: 76, 1050, 2252, 2862\n"
    "#+CATALOG_IDNO: TZA_2008-2014_NPS-UPD_v01_M\n"
    "#+CATALOG_URL: https://microdata.worldbank.org/index.php/catalog/3814\n"
    "#+PROVENANCE_SOURCE: worldbank\n"
    "#+PROVENANCE_METHOD: source-url\n"
    "#+PROVENANCE_VALIDATION: content-validated\n"
    "#+PROVENANCE_RECORDED: 2026-07-12\n"
    "#+PROVENANCE_NOTE: hand-recorded evidence for the covers line.\n")


class TestFailedResolutionLeavesRecordIntact:

    def test_catalog_outage_leaves_every_shipped_source_org_byte_identical(
            self, countries, monkeypatch, tmp_path):
        """THE regression, on a copy of every SOURCE.org we ship.

        The catalog is made unreachable the way it really fails: a key is
        configured and ``urlopen`` raises, so ``_wb_catalog_search`` logs and
        returns ``[]``.  The base script rewrote all 130 files here and
        downgraded 104 recorded WB ids to ``unknown``.  Not a dry run."""
        import shutil
        import urllib.error
        import urllib.request
        from lsms_library import config
        from lsms_library.data_access import _COUNTRY_CATALOG
        from lsms_library.paths import countries_root as real_root

        shipped = real_root()
        n = 0
        for c in sorted(shipped.iterdir()):
            if not c.is_dir() or c.name.startswith((".", "_")):
                continue
            for w in sorted(d.name for d in c.iterdir() if d.is_dir()):
                if not w[:4].isdigit():
                    continue
                (countries / c.name / w).mkdir(parents=True)
                src = pv.source_org_path(shipped, c.name, w)
                if src.exists():
                    dst = pv.source_org_path(countries, c.name, w)
                    dst.parent.mkdir(parents=True)
                    shutil.copy2(src, dst)
                    n += 1
        assert n > 100, "expected to copy the shipped SOURCE.org tree"

        bf = _load_backfill()                   # real _declared_waves
        monkeypatch.setattr(bf, "countries_root", lambda: countries)
        monkeypatch.setattr(config, "microdata_api_key", lambda: "dummy")

        def unreachable(*a, **k):
            raise urllib.error.URLError("simulated catalog outage")
        monkeypatch.setattr(urllib.request, "urlopen", unreachable)

        before = _snapshot(countries)
        review = tmp_path / "review" / "unresolved.org"
        # Non-zero: every country queried returned nothing, which is an
        # outage, not a clean run (it used to exit 0 here).
        assert bf.main(["--review-file", str(review)]) == 1

        assert _snapshot(countries) == before   # same files, same bytes

        # Every stamped wave of a WB-catalogued country is listed, and no other.
        expected = {
            (c, w) for (c, w) in ((p.parts[-4], p.parts[-3]) for p in before)
            if (spec := _COUNTRY_CATALOG.get(c)) is not None
            and spec.discoverable and spec.code}
        listed = _unresolved(review)
        assert set(listed) == expected
        assert len(expected) > 100
        assert {r["Kind"] for r in listed.values()} \
            == {"catalog-returned-nothing"}
        downgradable = [k for k, r in listed.items()
                        if r["Recorded source"] == "worldbank"]
        assert len(downgradable) > 90           # recorded ids, still recorded

        from lsms_library.local_tools import df_from_orgfile
        q = df_from_orgfile(review, name="catalog_queries", to_numeric=False)
        assert set(q["Rows returned"]) == {"0"}

    def test_recorded_id_the_catalog_did_not_return_is_left_intact(
            self, bf, countries, monkeypatch, tmp_path):
        """A filtered-out id (the catalog answered, just not with this id).
        The covers line, validation and note are hand-recorded and survive."""
        _write_source(countries, "Tanzania", "2008-15", TZA_UPD)
        _catalog(monkeypatch, bf, NGA_ROWS)     # non-empty, lacks 3814
        before = _snapshot(countries)

        review = tmp_path / "review.org"
        bf.main(["--country", "Tanzania", "--review-file", str(review)])

        assert _snapshot(countries) == before
        row = _unresolved(review)[("Tanzania", "2008-15")]
        assert row["Kind"] == "id-not-in-catalog"
        assert "3814" in row["Reason"]

    def test_unmatched_wb_doi_is_left_intact(self, bf, countries, monkeypatch,
                                             tmp_path):
        legacy = "SOURCE\n\nhttps://doi.org/10.48529/q9zx-4b28\n"
        _write_source(countries, "Albania", "2002", legacy)
        _catalog(monkeypatch, bf, NGA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "Albania", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Albania", "2002").read_text() \
            == legacy
        assert _unresolved(review)[("Albania", "2002")]["Kind"] \
            == "doi-not-in-catalog"

    def test_absent_override_id_does_not_fall_through_to_recorded_id(
            self, bf, countries, monkeypatch, tmp_path):
        """Nigeria/2018-19's override exists because its recorded id (3827,
        the LSS) is the WRONG survey.  If 3557 cannot be found, re-stamping
        the recorded 3827 -- what the fall-through did -- re-asserts the error."""
        legacy = ("SOURCE\n\n"
                  "https://microdata.worldbank.org/index.php/catalog/3827\n")
        _write_source(countries, "Nigeria", "2018-19", legacy)
        _catalog(monkeypatch, bf, [r for r in NGA_ROWS if r["id"] != "3557"])
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Nigeria", "2018-19").read_text() \
            == legacy
        assert _unresolved(review)[("Nigeria", "2018-19")]["Kind"] \
            == "override-id-not-in-catalog"

    def test_no_evidence_writes_nothing_and_creates_nothing(
            self, bf, countries, monkeypatch, tmp_path):
        """An explicit 'unknown' carrying a human's reason stays as written,
        and a declared wave with no SOURCE.org does not get one."""
        explicit = pv.render_source_org(pv.WaveProvenance(
            country="Nigeria", wave="2015-16", source=pv.SOURCE_UNKNOWN,
            method="unresolved", recorded="2026-07-12",
            note="Asked the NBS on 2026-07-01; awaiting reply."))
        _write_source(countries, "Nigeria", "2015-16", explicit)
        _mkwave(countries, "Nigeria", "2010-11")
        _catalog(monkeypatch, bf, NGA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Nigeria", "2015-16").read_text() \
            == explicit
        assert not pv.source_org_path(countries, "Nigeria", "2010-11").exists()
        rows = _unresolved(review)
        assert rows[("Nigeria", "2015-16")]["Kind"] == "no-resolvable-evidence"
        assert rows[("Nigeria", "2010-11")]["Kind"] == "no-resolvable-evidence"
        assert rows[("Nigeria", "2010-11")]["SOURCE.org"] == "absent"

    def test_country_without_a_catalog_entry_is_left_intact(
            self, bf, countries, monkeypatch, tmp_path):
        from lsms_library.data_access import _COUNTRY_CATALOG
        monkeypatch.delitem(_COUNTRY_CATALOG, "Nigeria")
        text = _wb("Nigeria", "2010-11", catalog_id="1002")
        _write_source(countries, "Nigeria", "2010-11", text)
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Nigeria", "2010-11").read_text() \
            == text
        assert _unresolved(review)[("Nigeria", "2010-11")]["Kind"] \
            == "no-catalog-mapping"

    def test_restamp_that_would_delete_a_line_is_refused(
            self, bf, countries, monkeypatch, tmp_path):
        """Peru/1994 records a CATALOG_DOI the catalog no longer returns for
        entry 617 (measured 2026-10-19).  The re-stamp 'succeeds', and the
        write would delete the recorded DOI.

        This test used GhanaSPS/2017-18's five #+SECONDARY_SOURCE* lines as
        its fixture, because render_source_org could not emit them.  Since GH
        #1115 it can (``TestRoundTripIsLossless``), so that file is no longer
        an instance; a catalog-side regression still is."""
        text = _shipped("Peru", "1994")
        assert "#+CATALOG_DOI:" in text
        _write_source(countries, "Peru", "1994", text)
        _catalog(monkeypatch, bf, [PER_617_NO_DOI])
        review = tmp_path / "review.org"
        bf.main(["--country", "Peru", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Peru", "1994").read_text() \
            == text
        row = _unresolved(review)[("Peru", "1994")]
        assert row["Kind"] == "restamp-would-delete"
        assert "#+CATALOG_DOI" in row["Reason"]

    def test_dropped_by_write_names_what_a_catalog_regression_deletes(self):
        """Peru/1994 records CATALOG_DOI; on 2026-10-19 the catalog returned no
        DOI for entry 617.  A re-stamp must not delete the recorded one."""
        bf = _load_backfill()
        old = pv.render_source_org(pv.WaveProvenance(
            country="Peru", wave="1994", source=pv.SOURCE_WORLDBANK,
            catalog_id="617", doi="https://doi.org/10.48529/ntxg-ed63",
            url="https://microdata.worldbank.org/index.php/catalog/617",
            method="doi-lookup", recorded="2026-07-12"))
        entry = {"id": "617", "idno": "PER_1994_ENNIV_v01_M", "title": "t",
                 "year_start": 1994, "year_end": 1994, "doi": "",
                 "repository": "lsms",
                 "url": "https://microdata.worldbank.org/index.php/catalog/617"}
        new = bf._from_entry("Peru", "1994", entry, "doi-lookup", "2026-10-19")
        assert bf.dropped_by_write(old, new) == ["#+CATALOG_DOI"]
        # An update is not a deletion: the same record with a new title is fine.
        entry["doi"] = "https://doi.org/10.48529/ntxg-ed63"
        new = bf._from_entry("Peru", "1994", entry, "doi-lookup", "2026-10-19")
        assert bf.dropped_by_write(old, new) == []

    @pytest.mark.parametrize("country,wave", [("KenyaLPS", "2007-2009"),
                                              ("GhanaAHIES", "2022")])
    def test_non_wb_country_keeps_its_hand_recorded_fields(
            self, bf, countries, tmp_path, country, wave):
        """The not-a-WB-dataset branch used to rebuild each record from
        _COUNTRY_CATALOG's reason alone, deleting six lines from each of these
        (Dataverse/NADA title, years, DOI or idno, validation, the note) with
        no network involved.  Already-stamped: nothing to write."""
        from lsms_library.paths import countries_root as real_root
        text = pv.source_org_path(real_root(), country, wave).read_text()
        _write_source(countries, country, wave, text)
        review = tmp_path / "review.org"
        bf.main(["--country", country, "--review-file", str(review)])

        assert pv.source_org_path(countries, country, wave).read_text() == text
        assert _unresolved(review) == {}

    def test_non_wb_country_with_a_recorded_wb_id_is_a_conflict(
            self, bf, countries, tmp_path):
        text = _wb("KenyaLPS", "2015", catalog_id="1234")
        _write_source(countries, "KenyaLPS", "2015", text)
        review = tmp_path / "review.org"
        bf.main(["--country", "KenyaLPS", "--review-file", str(review)])

        assert pv.source_org_path(countries, "KenyaLPS", "2015").read_text() \
            == text
        assert _unresolved(review)[("KenyaLPS", "2015")]["Kind"] \
            == "recorded-id-not-discoverable"

    def test_successful_restamp_still_writes_and_identical_is_not_rewritten(
            self, bf, countries, monkeypatch, tmp_path):
        """Positive control: the guard must not freeze the tree.  A catalog
        update is written; a record the catalog confirms is left byte-for-byte
        alone (PROVENANCE_RECORDED is not bumped)."""
        _write_source(countries, "Nigeria", "2010-11",
                      "SOURCE\n\n"
                      "https://microdata.worldbank.org/index.php/catalog/1002\n")
        _catalog(monkeypatch, bf, NGA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        path = pv.source_org_path(countries, "Nigeria", "2010-11")
        stamped = path.read_text()
        prov = pv.parse_source_org(stamped, "Nigeria", "2010-11")
        # A legacy record names no release, so the stamp supplies none
        # (#1128 ruling, @ligon 2026-10-20); it used to take the catalog's.
        assert prov.idno is None and prov.title == "GHS Panel W1"
        assert prov.method == "source-url"

        path.write_text(stamped.replace(
            f"#+PROVENANCE_RECORDED: {prov.recorded}",
            "#+PROVENANCE_RECORDED: 2026-07-12"))
        frozen = path.read_bytes()
        bf.main(["--country", "Nigeria", "--review-file", str(review)])
        assert path.read_bytes() == frozen
        assert _unresolved(review) == {}

    def test_dry_run_writes_no_source_org_but_does_write_the_review(
            self, bf, countries, monkeypatch, tmp_path):
        _write_source(countries, "Nigeria", "2010-11",
                      "SOURCE\n\n"
                      "https://microdata.worldbank.org/index.php/catalog/1002\n")
        _catalog(monkeypatch, bf, NGA_ROWS)
        before = _snapshot(countries)
        review = tmp_path / "review.org"
        bf.main(["--dry-run", "--country", "Nigeria",
                 "--review-file", str(review)])
        assert _snapshot(countries) == before
        assert review.exists()

    def test_review_artifact_is_refused_inside_countries_root(
            self, bf, countries):
        with pytest.raises(SystemExit, match="countries_root"):
            bf.main(["--dry-run", "--review-file",
                     str(countries / "Nigeria" / "review.org")])
        assert not (countries / "Nigeria").exists()

    def test_default_review_artifact_lives_outside_countries_root(self):
        bf = _load_backfill()
        from lsms_library.paths import countries_root as real_root
        path = bf.review_path(None, real_root(), "2026-10-19T000000")
        assert not path.resolve().is_relative_to(real_root().resolve())
        assert path.name == "backfill_unresolved_2026-10-19T000000.org"


# ---------------------------------------------------------------------------
# GH #600 follow-up (red team r1): a SUCCESSFUL re-stamp may not overwrite a
# recorded fact either -- a release we do not hold, a hand-edited note
# ---------------------------------------------------------------------------

def _shipped(country, wave):
    from lsms_library.paths import countries_root as real_root
    return pv.source_org_path(real_root(), country, wave).read_text()


def _changed(review):
    """The ``changed`` table as ``(country, wave, key, old, new)``; an empty
    cell (``---``) is ``None``."""
    import pandas as pd
    from lsms_library.local_tools import df_from_orgfile
    df = df_from_orgfile(review, name="changed", to_numeric=False)
    return [tuple(None if pd.isna(r[c]) else r[c]
                  for c in ("Country", "Wave", "Key", "Old", "New"))
            for r in df.to_dict("records")]


# Catalog 6410 as the shipped Nigeria/2023-24 record describes it (v01_M) ...
NGA_W5 = {"id": "6410", "idno": "NGA_2023_GHSP-W5_v01_M",
          "title": "General Household Survey, Panel 2023-2024",
          "year_start": 2023, "year_end": 2024, "repository": "lsms",
          "doi": "https://doi.org/10.48529/zd5s-tj25",
          "url": "https://microdata.worldbank.org/index.php/catalog/6410"}
# ... and as the live catalog reports it since 2026-09-16 (v02_M, new DOI).
NGA_W5_V02 = dict(NGA_W5, idno="NGA_2023_GHSP-W5_v02_M",
                  doi="https://doi.org/10.48529/90ec-pv69")

# Catalog 6161 as the shipped Ethiopia/2021-22 record describes it (no DOI).
ETH_W5 = {"id": "6161", "idno": "ETH_2021_ESPS-W5_v02_M",
          "title": "Socio-Economic Panel Survey 2021-2022",
          "year_start": 2021, "year_end": 2022, "repository": "lsms",
          "doi": "",
          "url": "https://microdata.worldbank.org/index.php/catalog/6161"}


def _release_changed(review):
    """The ``release_changed`` table, keyed by ``(country, wave)``."""
    import pandas as pd
    from lsms_library.local_tools import df_from_orgfile
    df = df_from_orgfile(review, name="release_changed", to_numeric=False)
    return {(r["Country"], r["Wave"]):
            {k: (None if pd.isna(v) else v) for k, v in r.items()}
            for r in df.to_dict("records")}


def _lines(text, prefix):
    return [ln for ln in text.splitlines() if ln.startswith(prefix)]


class TestReleaseChangeIsNotAReStamp:
    """``CATALOG_IDNO`` / ``CATALOG_DOI`` name the release a directory HOLDS.

    The catalog reports its latest.  Nigeria/2023-24 holds v01_M -- 106
    files, no household geovariables (``Nigeria/_/CONTENTS.org``, "Wave 5
    geovariables await acquisition of WB v02") -- and the catalog moved to
    v02_M on 2026-09-16.  A re-stamp must never record v02_M over v01_M files
    (#1114 ruling).  Until GH #1128 the wave was refused (``release-changed``)
    and the file left untouched, so the newer release was recorded nowhere
    durable.  Now the held keys are kept byte-for-byte and the newer release
    is written beside them as ``#+CATALOG_LATEST_*`` -- the acquisition
    queue -- and listed in the review's ``release_changed`` table."""

    def test_nigeria_w5_v02_is_recorded_beside_the_held_v01(
            self, bf, countries, monkeypatch, tmp_path):
        text = _record("Nigeria", "2023-24", **NGA_W5_HELD_V01)
        assert "NGA_2023_GHSP-W5_v01_M" in text
        _write_source(countries, "Nigeria", "2023-24", text)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        review = tmp_path / "review.org"
        assert bf.main(["--country", "Nigeria",
                        "--review-file", str(review)]) == 0   # write mode

        after = pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text()
        # The held release is untouched, line for line ...
        for key in ("#+CATALOG_IDNO:", "#+CATALOG_DOI:", "#+CATALOG_ID:"):
            assert _lines(after, key) == _lines(text, key), key
        prov = pv.parse_source_org(after, "Nigeria", "2023-24")
        assert prov.idno == "NGA_2023_GHSP-W5_v01_M"
        assert prov.doi == "https://doi.org/10.48529/zd5s-tj25"
        # ... and the catalog's latest is recorded beside it.
        assert prov.latest_idno == "NGA_2023_GHSP-W5_v02_M"
        assert prov.latest_doi == "https://doi.org/10.48529/90ec-pv69"
        today = datetime.date.today().isoformat()
        assert prov.latest_seen == today
        assert prov.is_stale_release
        assert _unresolved(review) == {}
        assert _changed(review) == [
            ("Nigeria", "2023-24", "CATALOG_LATEST_IDNO", None,
             "NGA_2023_GHSP-W5_v02_M"),
            ("Nigeria", "2023-24", "CATALOG_LATEST_DOI", None,
             "https://doi.org/10.48529/90ec-pv69"),
            ("Nigeria", "2023-24", "CATALOG_LATEST_SEEN", None, today)]
        row = _release_changed(review)[("Nigeria", "2023-24")]
        assert row["Held IDNO"] == "NGA_2023_GHSP-W5_v01_M"
        assert row["Latest IDNO"] == "NGA_2023_GHSP-W5_v02_M"
        assert row["SOURCE.org"] == "recorded"

    def test_a_recorded_latest_release_is_not_rewritten_but_still_listed(
            self, bf, countries, monkeypatch, tmp_path):
        """``CATALOG_LATEST_SEEN`` is the date the latest was FIRST seen, so a
        second run against the same catalog changes nothing -- and the wave
        stays in the queue table."""
        text = pv.render_source_org(replace(
            pv.parse_source_org(_record("Nigeria", "2023-24", **NGA_W5_HELD_V01),
                                "Nigeria", "2023-24"),
            latest_idno="NGA_2023_GHSP-W5_v02_M",
            latest_doi="https://doi.org/10.48529/90ec-pv69",
            latest_seen="2026-10-19"))
        _write_source(countries, "Nigeria", "2023-24", text)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == text
        assert _changed(review) == []
        row = _release_changed(review)[("Nigeria", "2023-24")]
        assert row["SOURCE.org"] == "already recorded"
        assert row["Latest seen"] == "2026-10-19"

    def test_a_newer_latest_replaces_the_queued_one(
            self, bf, countries, monkeypatch, tmp_path):
        text = pv.render_source_org(replace(
            pv.parse_source_org(_record("Nigeria", "2023-24", **NGA_W5_HELD_V01),
                                "Nigeria", "2023-24"),
            latest_idno="NGA_2023_GHSP-W5_v02_M",
            latest_doi="https://doi.org/10.48529/90ec-pv69",
            latest_seen="2026-10-19"))
        _write_source(countries, "Nigeria", "2023-24", text)
        v03 = dict(NGA_W5_V02, idno="NGA_2023_GHSP-W5_v03_M", doi="")
        _catalog(monkeypatch, bf, NGA_ROWS + [v03])
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        prov = pv.read_provenance(countries, "Nigeria", "2023-24")
        assert prov.idno == "NGA_2023_GHSP-W5_v01_M"          # still held
        assert prov.latest_idno == "NGA_2023_GHSP-W5_v03_M"
        assert prov.latest_doi is None        # v03 has none; v02's is not v03's
        assert prov.latest_seen == datetime.date.today().isoformat()

    def test_the_latest_keys_go_when_the_catalog_matches_the_held_release(
            self, bf, countries, monkeypatch, tmp_path):
        """Equal releases: the latest-seen keys are absent.  Removing them is
        not a deletion the backfill refuses."""
        text = pv.render_source_org(replace(
            pv.parse_source_org(_record("Nigeria", "2023-24", **NGA_W5_HELD_V01),
                                "Nigeria", "2023-24"),
            latest_idno="NGA_2023_GHSP-W5_v02_M",
            latest_seen="2026-10-19"))
        _write_source(countries, "Nigeria", "2023-24", text)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5])           # v01 again
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        after = pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text()
        assert "CATALOG_LATEST" not in after
        assert pv.parse_source_org(after, "Nigeria", "2023-24").idno \
            == "NGA_2023_GHSP-W5_v01_M"
        assert _unresolved(review) == {}
        assert _release_changed(review) == {}
        assert {k for _, _, k, _, _ in _changed(review)} \
            == {"CATALOG_LATEST_IDNO", "CATALOG_LATEST_SEEN"}

    def test_the_release_on_record_is_still_confirmed_unchanged(
            self, bf, countries, monkeypatch, tmp_path):
        """Control: the same row at the release we hold -> nothing to do."""
        text = _record("Nigeria", "2023-24", **NGA_W5_HELD_V01)
        _write_source(countries, "Nigeria", "2023-24", text)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5])
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == text
        assert _unresolved(review) == {}
        assert _changed(review) == []
        assert _release_changed(review) == {}

    def test_ethiopia_w5_v03_is_recorded_beside_the_held_v02(
            self, bf, countries, monkeypatch, tmp_path):
        """The held tree is <= v02 (``Data/eth_plotgeovariables_y5.dta``; v03
        renamed it ``Pub_ETH_PlotGeovariables_Y5.dta``).  The catalog reports
        no DOI for v03, so none is recorded -- and the held record keeps
        having none: a DOI the catalog reports for v03 is v03's."""
        text = _record("Ethiopia", "2021-22", **ETH_W5_HELD_V02)
        _write_source(countries, "Ethiopia", "2021-22", text)
        _catalog(monkeypatch, bf, [dict(ETH_W5,
                                        idno="ETH_2021_ESPS-W5_v03_M")])
        review = tmp_path / "review.org"
        bf.main(["--country", "Ethiopia", "--review-file", str(review)])

        after = pv.source_org_path(countries, "Ethiopia", "2021-22") \
            .read_text()
        assert _lines(after, "#+CATALOG_IDNO:") \
            == ["#+CATALOG_IDNO: ETH_2021_ESPS-W5_v02_M"]
        prov = pv.parse_source_org(after, "Ethiopia", "2021-22")
        assert prov.latest_idno == "ETH_2021_ESPS-W5_v03_M"
        assert prov.latest_doi is None and prov.doi is None
        assert _unresolved(review) == {}

    def test_a_doi_the_catalog_reports_for_a_newer_release_is_not_held(
            self, bf, countries, monkeypatch, tmp_path):
        """The counterpart of the newly-added-DOI case below, at a DIFFERENT
        idno: the DOI belongs to the latest release, not the held one."""
        _write_source(countries, "Ethiopia", "2021-22",
                      _record("Ethiopia", "2021-22", **ETH_W5_HELD_V02))
        doi = "https://doi.org/10.48529/xxxx-yyyy"
        _catalog(monkeypatch, bf, [dict(ETH_W5, idno="ETH_2021_ESPS-W5_v03_M",
                                        doi=doi)])
        review = tmp_path / "review.org"
        bf.main(["--country", "Ethiopia", "--review-file", str(review)])

        prov = pv.read_provenance(countries, "Ethiopia", "2021-22")
        assert prov.doi is None
        assert prov.latest_doi == doi

    def test_a_newly_added_doi_is_an_addition_not_a_release_change(
            self, bf, countries, monkeypatch, tmp_path):
        """Only a CHANGE is held back.  Ethiopia/2021-22 records no DOI; the
        catalog supplying one at the SAME idno is written, and listed."""
        _write_source(countries, "Ethiopia", "2021-22",
                      _record("Ethiopia", "2021-22", **ETH_W5_HELD_V02))
        doi = "https://doi.org/10.48529/xxxx-yyyy"
        _catalog(monkeypatch, bf, [dict(ETH_W5, doi=doi)])
        review = tmp_path / "review.org"
        bf.main(["--country", "Ethiopia", "--review-file", str(review)])

        prov = pv.read_provenance(countries, "Ethiopia", "2021-22")
        assert prov.doi == doi
        assert prov.idno == "ETH_2021_ESPS-W5_v02_M"
        assert _unresolved(review) == {}
        assert _changed(review) == [
            ("Ethiopia", "2021-22", "CATALOG_DOI", None, doi)]

    def test_a_doi_changed_at_the_same_idno_is_refused(
            self, bf, countries, monkeypatch, tmp_path):
        """No newer idno, so nothing to queue; the idno says this IS the held
        release, and which DOI is right is a human's call."""
        text = _record("Nigeria", "2023-24", **NGA_W5_HELD_V01)
        _write_source(countries, "Nigeria", "2023-24", text)
        _catalog(monkeypatch, bf, NGA_ROWS + [
            dict(NGA_W5, doi="https://doi.org/10.48529/aaaa-bbbb")])
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == text
        row = _unresolved(review)[("Nigeria", "2023-24")]
        assert row["Kind"] == "doi-changed"
        assert "zd5s-tj25" in row["Reason"] and "aaaa-bbbb" in row["Reason"]

    def test_a_doi_spelled_differently_is_not_a_change(self):
        bf = _load_backfill()
        old = pv.render_source_org(pv.WaveProvenance(
            country="Nigeria", wave="2023-24", source=pv.SOURCE_WORLDBANK,
            catalog_id="6410", doi="https://doi.org/10.48529/zd5s-tj25",
            url=NGA_W5["url"]))
        new = bf._from_entry("Nigeria", "2023-24",
                             dict(NGA_W5, doi="10.48529/zd5s-tj25/"),
                             "source-url", "2026-10-19")
        assert bf.release_changed(old, new) == []

    def test_an_override_rerun_at_the_same_id_is_held_too(
            self, bf, countries, monkeypatch, tmp_path):
        """Nigeria/2018-19's override already resolved to 3557 (v03_M).  A
        re-run whose catalog row is a newer release is a release change like
        any other -- the override exemption is for CORRECTING an id."""
        text = _record("Nigeria", "2018-19", **NGA_W4_HELD_V03)
        assert "NGA_2018_GHSP-W4_v03_M" in text
        _write_source(countries, "Nigeria", "2018-19", text)
        rows = [dict(r, idno="NGA_2018_GHSP-W4_v04_M",
                     doi="https://doi.org/10.48529/1hgw-dq47")
                if r["id"] == "3557" else r for r in NGA_ROWS]
        _catalog(monkeypatch, bf, rows)
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        after = pv.source_org_path(countries, "Nigeria", "2018-19") \
            .read_text()
        for key in ("#+CATALOG_IDNO:", "#+CATALOG_DOI:"):
            assert _lines(after, key) == _lines(text, key), key
        prov = pv.parse_source_org(after, "Nigeria", "2018-19")
        assert prov.latest_idno == "NGA_2018_GHSP-W4_v04_M"
        assert prov.method == "manual-override"
        assert _unresolved(review) == {}

    def test_dry_run_lists_the_queue_and_writes_nothing(
            self, bf, countries, monkeypatch, tmp_path):
        text = _record("Nigeria", "2023-24", **NGA_W5_HELD_V01)
        _write_source(countries, "Nigeria", "2023-24", text)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        review = tmp_path / "review.org"
        bf.main(["--dry-run", "--country", "Nigeria",
                 "--review-file", str(review)])
        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == text
        assert _release_changed(review)[("Nigeria", "2023-24")]["SOURCE.org"] \
            == "would record"


class TestOverrideCorrectionThroughMain:
    """The positive branch of the ``manual-override`` exemption, end to end.

    Without it a mutant that drops the exemption in ``dropped_by_write``
    passes every other test, and leaves the known-wrong 3827 forever
    (red team r1, R11)."""

    LEGACY = ("SOURCE\n\n"
              "https://microdata.worldbank.org/index.php/catalog/3827\n")

    def test_legacy_3827_is_corrected_to_3557(self, bf, countries,
                                              monkeypatch, tmp_path):
        _write_source(countries, "Nigeria", "2018-19", self.LEGACY)
        _catalog(monkeypatch, bf, NGA_ROWS)
        review = tmp_path / "review.org"
        assert bf.main(["--country", "Nigeria",
                        "--review-file", str(review)]) == 0

        text = pv.source_org_path(countries, "Nigeria", "2018-19").read_text()
        assert "#+CATALOG_ID: 3557\n" in text
        assert ("#+PROVENANCE_SUPERSEDED_URL: "
                "https://microdata.worldbank.org/index.php/catalog/3827\n") \
            in text
        prov = pv.parse_source_org(text, "Nigeria", "2018-19")
        assert prov.catalog_ids == ["3557"]
        assert prov.method == "manual-override"
        # The legacy 3827 record held no release, so the corrected entry
        # gets none from the catalog either (#1128 ruling); it used to be
        # stamped NGA_2018_GHSP-W4_v03_M.
        assert prov.idno is None
        assert _unresolved(review) == {}
        assert ("Nigeria", "2018-19", "CATALOG_ID", "3827", "3557") \
            in _changed(review)

    def test_a_stamped_primary_correction_requires_acquisition(
            self, bf, countries, monkeypatch, tmp_path):
        """A catalog override cannot establish another entry's held release.
        Preserve the complete stamped record rather than invent an IDNO."""
        lss = next(r for r in NGA_ROWS if r["id"] == "3827")
        wrong = pv.render_source_org(bf._from_entry(
            "Nigeria", "2018-19", lss, "source-url", "2026-07-12"))
        assert "NGA_2018_LSS_v01_M" in wrong
        _write_source(countries, "Nigeria", "2018-19", wrong)
        _catalog(monkeypatch, bf, NGA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert pv.source_org_path(countries, "Nigeria", "2018-19").read_text() == wrong
        assert _unresolved(review)[("Nigeria", "2018-19")]["Kind"] \
            == "held-release-correction"


class TestRecordedValuesSurviveASuccessfulReStamp:
    """``dropped_by_write`` checks that a KEY survives, not its value; the
    two template-built paths could overwrite a hand edit (red team r1 H1/H2)."""

    GHA_NOTE = ("#+PROVENANCE_NOTE: Obtained from www2.statsghana.gov.gh, "
                "not the World Bank Microdata Library.")
    H1_EDITS = {
        # r1 H1: the template note was WRITTEN over a hand edit.
        "note": (GHA_NOTE, "#+PROVENANCE_NOTE: GLSS4, GSS NADA catalog 14; "
                           "file list checked against the NADA page by hand."),
        # ... and so was a hand-set method.
        "method": ("#+PROVENANCE_METHOD: external-source",
                   "#+PROVENANCE_METHOD: content-inspection"),
        # A validation line was not carried at all (refused as a deletion).
        "validation": ("#+PROVENANCE_RECORDED: 2026-07-12",
                       "#+PROVENANCE_RECORDED: 2026-07-12\n"
                       "#+PROVENANCE_VALIDATION: content-validated"),
    }

    @pytest.mark.parametrize("edit", sorted(H1_EDITS))
    def test_h1_hand_edited_external_record_stays_byte_identical(
            self, bf, countries, monkeypatch, tmp_path, edit):
        text = _shipped("GhanaLSS", "1998-99")
        old, new = self.H1_EDITS[edit]
        assert text.count(old) == 1
        edited = text.replace(old, new)
        _write_source(countries, "GhanaLSS", "1998-99", edited)
        _catalog(monkeypatch, bf, GHA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "GhanaLSS", "--review-file", str(review)])

        assert pv.source_org_path(countries, "GhanaLSS", "1998-99") \
            .read_text() == edited
        assert _unresolved(review) == {}
        assert _changed(review) == []

    def test_shipped_external_record_is_unchanged(
            self, bf, countries, monkeypatch, tmp_path):
        """Control: the shipped (template-note) record is confirmed as is."""
        text = _shipped("GhanaLSS", "1998-99")
        _write_source(countries, "GhanaLSS", "1998-99", text)
        _catalog(monkeypatch, bf, GHA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "GhanaLSS", "--review-file", str(review)])
        assert pv.source_org_path(countries, "GhanaLSS", "1998-99") \
            .read_text() == text

    def test_legacy_external_url_still_gets_the_template_note(
            self, bf, countries, monkeypatch, tmp_path):
        """Control: a bare legacy URL has no note to keep; it is stamped."""
        _write_source(countries, "GhanaLSS", "1998-99",
                      "SOURCE\n\nhttps://www2.statsghana.gov.gh/nada/"
                      "index.php/catalog/14/get_microdata\n")
        _catalog(monkeypatch, bf, GHA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "GhanaLSS", "--review-file", str(review)])
        prov = pv.read_provenance(countries, "GhanaLSS", "1998-99")
        assert prov.method == "external-source"
        assert prov.note == ("Obtained from www2.statsghana.gov.gh, not the "
                             "World Bank Microdata Library.")

    def test_h2_explicit_unknown_with_a_note_in_a_non_wb_country(
            self, bf, countries, tmp_path):
        explicit = pv.render_source_org(pv.WaveProvenance(
            country="KenyaLPS", wave="2015", source=pv.SOURCE_UNKNOWN,
            method="unresolved", recorded="2026-07-12",
            note="Round 4 files arrived by email from IPA; origin unconfirmed."))
        _write_source(countries, "KenyaLPS", "2015", explicit)
        review = tmp_path / "review.org"
        bf.main(["--country", "KenyaLPS", "--review-file", str(review)])

        assert pv.source_org_path(countries, "KenyaLPS", "2015").read_text() \
            == explicit
        row = _unresolved(review)[("KenyaLPS", "2015")]
        assert row["Kind"] == "explicit-unknown"
        assert "origin unconfirmed" in row["Reason"]

    def test_bare_unknown_in_a_non_wb_country_is_still_resolved(
            self, bf, countries, tmp_path):
        """Control: an 'unknown' with no reason given is the config's to
        settle, and is stamped external with the config's reason."""
        from lsms_library.data_access import _COUNTRY_CATALOG
        bare = pv.render_source_org(pv.WaveProvenance(
            country="KenyaLPS", wave="2015", source=pv.SOURCE_UNKNOWN,
            method="unresolved", recorded="2026-07-12"))
        _write_source(countries, "KenyaLPS", "2015", bare)
        review = tmp_path / "review.org"
        bf.main(["--country", "KenyaLPS", "--review-file", str(review)])

        prov = pv.read_provenance(countries, "KenyaLPS", "2015")
        assert prov.source == pv.SOURCE_EXTERNAL
        assert prov.note == _COUNTRY_CATALOG["KenyaLPS"].reason
        assert ("KenyaLPS", "2015", "PROVENANCE_SOURCE", "unknown",
                "external") in _changed(review)


class TestReviewArtifactIsWrittenWhateverHappens:

    def test_a_run_that_dies_part_way_still_lists_what_it_wrote(
            self, bf, countries, monkeypatch, tmp_path):
        for w, cid in (("2010-11", "1002"), ("2018-19", "3557")):
            _write_source(countries, "Nigeria", w,
                          "SOURCE\n\nhttps://microdata.worldbank.org/"
                          f"index.php/catalog/{cid}\n")
        _catalog(monkeypatch, bf, NGA_ROWS)
        real = bf.resolve

        def dies_on_the_second(country, wave, *a, **k):
            if wave == "2018-19":
                raise RuntimeError("simulated crash")
            return real(country, wave, *a, **k)
        monkeypatch.setattr(bf, "resolve", dies_on_the_second)

        review = tmp_path / "review.org"
        with pytest.raises(RuntimeError, match="simulated crash"):
            bf.main(["--country", "Nigeria", "--review-file", str(review)])

        text = review.read_text()
        assert "DID NOT FINISH" in text and "simulated crash" in text
        written = {(c, w) for c, w, *_ in _changed(review)}
        assert written == {("Nigeria", "2010-11")}
        assert pv.read_provenance(countries, "Nigeria", "2010-11").title \
            == "GHS Panel W1"              # no release: #1128 ruling           # it really was written

    def test_exit_status_is_zero_when_any_queried_country_answered(
            self, bf, countries, monkeypatch, tmp_path):
        _write_source(countries, "Nigeria", "2010-11",
                      _wb("Nigeria", "2010-11", catalog_id="1002"))
        _write_source(countries, "Uganda", "2005-06",
                      _wb("Uganda", "2005-06", catalog_id="1001"))
        monkeypatch.setattr(
            bf, "_wb_catalog_search",
            lambda code, collection="lsms": list(NGA_ROWS) if code == "NGA"
            else [])
        review = tmp_path / "review.org"
        assert bf.main(["--country", "Nigeria", "--country", "Uganda",
                        "--review-file", str(review)]) == 0
        assert _unresolved(review)[("Uganda", "2005-06")]["Kind"] \
            == "catalog-returned-nothing"


# ---------------------------------------------------------------------------
# GH #1115: parse -> render is lossless
# ---------------------------------------------------------------------------

def _shipped_source_orgs():
    from lsms_library.paths import countries_root as real_root
    return sorted(real_root().glob("*/*/Documentation/SOURCE.org"))


# Lines that carry no information, so a render may drop them: the placeholder
# body (boilerplate since GH #1115) and the notes heading of a notes block that
# held nothing else.
_INFORMATION_FREE = {pv.URL_PLACEHOLDER, pv.NOTES_HEADING}


class TestRoundTripIsLossless:
    """``parse_source_org`` -> ``render_source_org`` loses nothing (GH #1115).

    Four defects made it lossy: ``render_source_org`` dropped ``#+KEY:`` lines
    it has no field for; the URL was taken from the first ``http(s)://``
    anywhere in the file; ``#+`` lines inside the notes block were dropped
    from the prose; and the ``(source URL not recorded)`` placeholder was
    re-ingested as prose.  On the shipped tree the first render lost five
    lines of GhanaSPS/2017-18 (its #+SECONDARY_SOURCE* record of a second,
    Dataverse vintage of ten files), and six files carried the re-ingested
    placeholder in their notes."""

    def test_every_shipped_source_org_round_trips_losslessly(self):
        paths = _shipped_source_orgs()
        assert len(paths) >= 130
        lost, unstable, drifted = {}, [], []
        for p in paths:
            c, w = p.parts[-4], p.parts[-3]
            text = p.read_text(encoding="utf-8")
            rec = pv.parse_source_org(text, c, w)
            r1 = pv.render_source_org(rec)
            back = pv.parse_source_org(r1, c, w)
            r2 = pv.render_source_org(back)
            if r1 != r2:
                unstable.append(f"{c}/{w}")
            if back != rec:
                drifted.append(f"{c}/{w}")
            have = {ln.rstrip() for ln in r1.splitlines()}
            gone = [ln for ln in (x.rstrip() for x in text.splitlines())
                    if ln.strip() and ln not in have
                    and ln.strip() not in _INFORMATION_FREE]
            if gone:
                lost[f"{c}/{w}"] = gone
        assert lost == {}, f"the first render loses lines: {lost}"
        assert unstable == [], f"render(parse(render)) moves: {unstable}"
        assert drifted == [], f"the parsed record moves: {drifted}"

    def test_unknown_keys_in_the_record_are_rendered(self):
        text = pv.render_source_org(pv.WaveProvenance(
            country="X", wave="2020", source=pv.SOURCE_EXTERNAL,
            url="https://example.org/x", recorded="2026-10-19",
            extra={"SECONDARY_SOURCE": ["https://doi.org/10.7910/DVN/E5QP0F"],
                   "LICENSE_SPDX": "CC0-1.0"}))      # a str renders too
        assert "#+SECONDARY_SOURCE: https://doi.org/10.7910/DVN/E5QP0F\n" \
            in text
        assert "#+LICENSE_SPDX: CC0-1.0\n" in text
        back = pv.parse_source_org(text, "X", "2020")
        assert back.extra == {"SECONDARY_SOURCE":
                              ["https://doi.org/10.7910/DVN/E5QP0F"],
                              "LICENSE_SPDX": ["CC0-1.0"]}
        assert pv.render_source_org(back) == text

    def test_a_repeated_unknown_key_keeps_every_line(self):
        """``extra`` used to keep the LAST value of a repeated key, so the
        first ``#+SECONDARY_SOURCE:`` line vanished on render (red team r1)."""
        text = ("SOURCE\n\nhttps://example.org/x\n\n"
                "#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n"
                "#+PROVENANCE_RECORDED: 2026-10-19\n"
                "#+SECONDARY_SOURCE: https://a.example/one\n"
                "#+SECONDARY_SOURCE: https://b.example/two\n")
        prov = pv.parse_source_org(text, "X", "2020")
        assert prov.extra == {"SECONDARY_SOURCE": ["https://a.example/one",
                                                   "https://b.example/two"]}
        out = pv.render_source_org(prov)
        assert out.count("#+SECONDARY_SOURCE:") == 2
        assert "#+SECONDARY_SOURCE: https://a.example/one\n" in out
        assert pv.render_source_org(pv.parse_source_org(out, "X", "2020")) \
            == out

    def test_an_empty_valued_key_does_not_swallow_the_next_line(self):
        """``\\s*`` after the colon spanned the newline under MULTILINE, so
        ``#+EMPTY_KEY:`` took the next line as its value and BOTH lines
        vanished from the render (red team r1).  The value ends at its line."""
        text = ("SOURCE\n\nhttps://example.org/x\n\n"
                "#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n"
                "#+PROVENANCE_RECORDED: 2026-10-19\n"
                "#+EMPTY_KEY:\n#+SPACED_KEY: value\n")
        prov = pv.parse_source_org(text, "X", "2020")
        assert prov.extra == {"EMPTY_KEY": [""], "SPACED_KEY": ["value"]}
        out = pv.render_source_org(prov)
        assert "#+EMPTY_KEY:\n" in out and "#+SPACED_KEY: value\n" in out

    def test_dropped_by_write_refuses_a_lost_duplicate_or_empty_key(self):
        """The refusal counts OCCURRENCES: a set of key names saw neither a
        repeated known scalar collapsing to one line nor an empty-valued key
        the render omits (red team r1)."""
        bf = _load_backfill()
        base = pv.WaveProvenance(
            country="Nigeria", wave="2010-11", source=pv.SOURCE_WORLDBANK,
            catalog_id="1002", note="second", recorded="2026-10-19",
            url="https://microdata.worldbank.org/index.php/catalog/1002")
        old = pv.render_source_org(base).replace(
            "#+PROVENANCE_NOTE: second", "#+PROVENANCE_NOTE: first\n"
                                         "#+PROVENANCE_NOTE: second")
        assert bf._keys(old)["PROVENANCE_NOTE"] == 2
        from dataclasses import replace
        parsed = pv.parse_source_org(old, "Nigeria", "2010-11")
        assert pv.render_source_org(parsed).count("#+PROVENANCE_NOTE:") == 2
        # A catalog-built reconstruction that did not carry the occurrences
        # must still be refused instead of silently losing the first fact.
        lost = bf.dropped_by_write(old, replace(parsed, scalar_keywords={}))
        assert lost == ["#+PROVENANCE_NOTE x1"]
        # Two CATALOG_ID lines render as one: judged by id, not by count.
        two_ids = pv.render_source_org(base).replace(
            "#+CATALOG_ID: 1002", "#+CATALOG_ID: 1002\n#+CATALOG_ID: 1952")
        assert bf.dropped_by_write(
            two_ids, pv.parse_source_org(two_ids, "Nigeria", "2010-11")) == []

    def test_the_url_comes_from_the_body_only(self):
        """No URL in the body and none recorded: the record has no URL, even
        with one in an unknown key or in the notes."""
        text = ("SOURCE\n\n(source URL not recorded)\n\n"
                "#+CATALOG_ID: none\n"
                "#+SECONDARY_SOURCE: https://doi.org/10.7910/DVN/E5QP0F\n"
                "#+PROVENANCE_SOURCE: external\n\n"
                f"{pv.NOTES_HEADING}\nSee https://example.org/elsewhere\n")
        prov = pv.parse_source_org(text, "GhanaSPS", "2017-18")
        assert prov.url is None
        # ... while a body URL is still found where there is no CATALOG_URL.
        legacy = ("SOURCE\n\nhttps://microdata.worldbank.org/index.php/"
                  "catalog/3557\n\n#+PROVENANCE_NOTE: see "
                  "https://example.org/x\n")
        assert pv.parse_source_org(legacy, "Nigeria", "2018-19").url \
            == "https://microdata.worldbank.org/index.php/catalog/3557"

    def test_keyword_lines_in_the_notes_stay_where_they_were_written(self):
        notes = ("* Mixed vintage\n\nTen files came from Dataverse:\n\n"
                 "#+SECONDARY_SOURCE: https://doi.org/10.7910/DVN/E5QP0F\n"
                 "#+SECONDARY_SOURCE_LICENSE: CC0 1.0\n\n"
                 "They are additions, not replacements.")
        text = pv.render_source_org(pv.WaveProvenance(
            country="GhanaSPS", wave="2017-18", source=pv.SOURCE_EXTERNAL,
            recorded="2026-07-12", note="EGC-ISSER.", notes=notes))
        prov = pv.parse_source_org(text, "GhanaSPS", "2017-18")
        assert prov.notes == notes               # verbatim, blank lines kept
        assert prov.extra == {}                  # prose, not record keys
        assert prov.url is None
        assert text.count("#+SECONDARY_SOURCE:") == 1
        assert pv.render_source_org(prov) == text

    def test_the_placeholder_is_not_reingested(self):
        """It used to come back as the first line of the notes on the next
        render (GhanaSPS/2017-18, five other shipped files)."""
        first = pv.render_source_org(pv.WaveProvenance(
            country="GhanaSPS", wave="2013-14", source=pv.SOURCE_EXTERNAL,
            recorded="2026-07-12"))
        assert pv.URL_PLACEHOLDER in first
        prov = pv.parse_source_org(first, "GhanaSPS", "2013-14")
        assert prov.notes is None
        assert pv.render_source_org(prov) == first
        # A placeholder already carried into a notes block is dropped there.
        carried = first + f"\n{pv.NOTES_HEADING}\n{pv.URL_PLACEHOLDER}\n"
        assert pv.parse_source_org(carried, "GhanaSPS", "2013-14").notes \
            is None

    def test_ghanasps_2017_18_is_no_longer_refused(
            self, bf, countries, monkeypatch, tmp_path):
        """Positive control for the re-fixtured refusal test: the backfill
        refused this file on every run (``restamp-would-delete``) because a
        re-stamp would delete its #+SECONDARY_SOURCE* lines.  Now nothing is
        lost, and the five lines are still in the file afterwards."""
        text = _shipped("GhanaSPS", "2017-18")
        assert text.count("#+SECONDARY_SOURCE") == 5
        _write_source(countries, "GhanaSPS", "2017-18", text)
        _catalog(monkeypatch, bf, GHA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "GhanaSPS", "--review-file", str(review)])

        assert _unresolved(review) == {}
        after = pv.source_org_path(countries, "GhanaSPS", "2017-18") \
            .read_text()
        assert after.count("#+SECONDARY_SOURCE") == 5
        assert pv.parse_source_org(after, "GhanaSPS", "2017-18").url is None

    def test_a_wb_restamp_carries_unknown_record_keys(
            self, bf, countries, monkeypatch, tmp_path):
        """Once render emits ``extra``, a WB re-stamp built from the catalog
        must carry it, or the key reads as deleted and the wave is refused."""
        text = _wb("Nigeria", "2010-11", catalog_id="1002").replace(
            "#+PROVENANCE_SOURCE:", "#+FIELDWORK_NOTE: two visits\n"
                                    "#+PROVENANCE_SOURCE:")
        _write_source(countries, "Nigeria", "2010-11", text)
        _catalog(monkeypatch, bf, NGA_ROWS)
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        assert _unresolved(review) == {}
        prov = pv.read_provenance(countries, "Nigeria", "2010-11")
        assert prov.extra == {"FIELDWORK_NOTE": ["two visits"]}
        # A legacy record names no release, so the stamp supplies none
        # (#1128 ruling, @ligon 2026-10-20); it used to take the catalog's.
        assert prov.idno is None and prov.title == "GHS Panel W1"


# ---------------------------------------------------------------------------
# GH #1128: the queue, as discover_waves sees it, and as acquisition clears it
# ---------------------------------------------------------------------------

def _record(country, wave, **kw):
    """A rendered worldbank record for a wave (held release in ``kw``)."""
    return pv.render_source_org(pv.WaveProvenance(
        country=country, wave=wave, source=pv.SOURCE_WORLDBANK,
        recorded="2026-07-12", **kw))


NGA_W5_HELD_V01 = dict(
    catalog_id="6410", idno="NGA_2023_GHSP-W5_v01_M",
    title="General Household Survey, Panel 2023-2024", years="2023-2024",
    repository="lsms", doi="https://doi.org/10.48529/zd5s-tj25",
    url="https://microdata.worldbank.org/index.php/catalog/6410",
    method="source-url")
# Frozen literals of the shipped records as held BEFORE the #978 / #1127
# acquisitions (any ``#+CATALOG_LATEST_*`` line removed), so these tests read
# nothing from the shipped tree and pass whatever its queue holds today.
ETH_W5_HELD_V02 = dict(
    catalog_id="6161", idno="ETH_2021_ESPS-W5_v02_M",
    title="Socio-Economic Panel Survey 2021-2022", years="2021-2022",
    repository="lsms",
    url="https://microdata.worldbank.org/index.php/catalog/6161",
    method="source-url")
NGA_W4_HELD_V03 = dict(
    catalog_id="3557", idno="NGA_2018_GHSP-W4_v03_M",
    title="General Household Survey, Panel 2018-2019, Wave 4",
    years="2018-2019", repository="lsms",
    doi="https://doi.org/10.48529/1hgw-dq47",
    url="https://microdata.worldbank.org/index.php/catalog/3557",
    method="manual-override", validation=pv.VALIDATION_CONTENT,
    superseded_url="https://microdata.worldbank.org/index.php/catalog/3827",
    note="Corrected from catalog 3827 (NGA_2018_LSS).")
NGA_W5_QUEUED = dict(
    NGA_W5_HELD_V01, latest_idno="NGA_2023_GHSP-W5_v02_M",
    latest_doi="https://doi.org/10.48529/90ec-pv69", latest_seen="2026-10-19",
    validation=pv.VALIDATION_CONTENT,
    notes="106 files; no household geovariables (GH #978).")


class TestStaleReleaseIsASixthState:
    """``discover_waves`` reports a held entry whose recorded release is older
    than the catalog's latest as ``stale-release``: held (``local=True``) and
    an acquisition target."""

    def test_nigeria_w5_held_at_v01_with_the_catalog_at_v02(
            self, countries, monkeypatch):
        from lsms_library.data_access import discover_waves
        _write_source(countries, "Nigeria", "2023-24",
                      _record("Nigeria", "2023-24", **NGA_W5_HELD_V01))
        _mock_catalog(monkeypatch, NGA_ROWS + [NGA_W5_V02])
        e = {r["id"]: r for r in discover_waves("Nigeria")}["6410"]
        assert e["local"] is True
        assert e["local_status"] == "stale-release"
        assert e["held_idnos"] == ["NGA_2023_GHSP-W5_v01_M"]
        assert e["idno"] == "NGA_2023_GHSP-W5_v02_M"        # the latest
        assert e["local_waves"] == ["2023-24"]

    def test_the_release_held_is_the_latest_is_yes(self, countries,
                                                   monkeypatch):
        from lsms_library.data_access import discover_waves
        _write_source(countries, "Nigeria", "2023-24",
                      _record("Nigeria", "2023-24", **NGA_W5_HELD_V01))
        _mock_catalog(monkeypatch, NGA_ROWS + [NGA_W5])
        e = {r["id"]: r for r in discover_waves("Nigeria")}["6410"]
        assert e["local_status"] == "yes"
        assert e["held_idnos"] == ["NGA_2023_GHSP-W5_v01_M"]

    def test_no_recorded_release_cannot_be_judged_stale(self, countries,
                                                        monkeypatch):
        """A record with no idno, and a dir's secondary id (whose release is
        never recorded), stay ``yes``: no evidence, no claim."""
        from lsms_library.data_access import discover_waves
        _write_source(countries, "Nigeria", "2023-24",
                      _wb("Nigeria", "2023-24", catalog_id="6410"))
        _mock_catalog(monkeypatch, NGA_ROWS + [NGA_W5_V02])
        e = {r["id"]: r for r in discover_waves("Nigeria")}["6410"]
        assert e["local_status"] == "yes" and e["held_idnos"] == []

        _write_source(countries, "Malawi", "2016-17", _record(
            "Malawi", "2016-17", catalog_ids=["2936", "2939"],
            idno="MWI_2016_IHS-IV_v04_M",
            url="https://microdata.worldbank.org/index.php/catalog/2936"))
        _mock_catalog(monkeypatch, [
            {"id": "2939", "idno": "MWI_2010-2016_IHPS_v99_M", "title": "IHPS",
             "year_start": 2010, "year_end": 2016, "doi": "",
             "repository": "lsms",
             "url": "https://microdata.worldbank.org/index.php/catalog/2939"}])
        e = {r["id"]: r for r in discover_waves("Malawi")}["2939"]
        assert e["local_status"] == "yes" and e["held_idnos"] == []

    def test_every_row_carries_held_idnos(self, countries, monkeypatch):
        from lsms_library.data_access import discover_waves
        _mock_catalog(monkeypatch, NGA_ROWS)
        assert all(r["held_idnos"] == [] for r in discover_waves("Nigeria"))


def _zip(*names):
    import io
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for n in names:
            zf.writestr(f"NGA_2023_GHSP-W5_v02_M_Stata/{n}", b"release-2")
    return buf.getvalue()


@pytest.fixture()
def wb(countries, monkeypatch):
    """The WB download path, hermetic: release v02 of 6410 is a zip of two
    files; DVC is unreadable, so the real get_data_file reaches the WB
    branch and the real _extract_all_from_zip unpacks it."""
    import io
    import urllib.error
    import urllib.request
    from lsms_library import data_access as da
    state = {"zip": _zip("a.dta", "b.dta"), "fail": False}

    def urlopen(req, timeout=None):
        if state["fail"]:
            raise urllib.error.URLError("simulated outage")
        return io.BytesIO(state["zip"])
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(da, "permissions", lambda path=None: {"wb_api": "read"})
    monkeypatch.setattr(da.config, "microdata_api_key", lambda: "key")
    monkeypatch.setattr(da, "_get_catalog_idno",
                        lambda url: "NGA_2023_GHSP-W5_v02_M")
    monkeypatch.setattr(da, "_find_stata_zip_url",
                        lambda base, idno, key: ("https://x/z.zip", "z.zip"))
    monkeypatch.setattr(da, "push_to_cache_batch",
                        lambda paths, remote=None, dvc_add=True: list(paths))
    _mock_catalog(monkeypatch, NGA_ROWS + [NGA_W5_V02])
    return state


class TestAcquisitionWritesTheHeldRelease:
    """``add_wave`` / ``populate_and_push`` are the only writers of the held
    keys, and they clear the queued latest release (GH #1128) -- once the
    whole release has landed, and not otherwise."""

    def test_populate_and_push_acquires_the_queued_release(
            self, countries, wb):
        from lsms_library import data_access as da
        _write_source(countries, "Nigeria", "2023-24",
                      _record("Nigeria", "2023-24", **NGA_W5_QUEUED))
        pushed = da.populate_and_push("Nigeria", "2023-24")

        assert sorted(p.name for p in pushed) == ["a.dta", "b.dta"]
        prov = pv.read_provenance(countries, "Nigeria", "2023-24")
        assert prov.idno == "NGA_2023_GHSP-W5_v02_M"
        assert prov.doi == "https://doi.org/10.48529/90ec-pv69"
        assert (prov.latest_idno, prov.latest_doi, prov.latest_seen) \
            == (None, None, None)
        assert not prov.is_stale_release
        # Everything else the record held for this entry is kept.
        assert prov.method == "source-url"
        assert prov.validation == pv.VALIDATION_CONTENT
        assert prov.notes == NGA_W5_QUEUED["notes"]
        assert prov.catalog_ids == ["6410"]
        text = pv.source_org_path(countries, "Nigeria", "2023-24").read_text()
        assert "CATALOG_LATEST" not in text

    def test_a_file_already_in_data_is_not_recorded_as_the_new_release(
            self, countries, wb, caplog):
        """``_extract_all_from_zip`` never overwrites: a.dta stays v01's.  The
        directory is then a mix, so the record must not claim v02."""
        from lsms_library import data_access as da
        before = _record("Nigeria", "2023-24", **NGA_W5_QUEUED)
        _write_source(countries, "Nigeria", "2023-24", before)
        (countries / "Nigeria" / "2023-24" / "Data").mkdir(parents=True)
        (countries / "Nigeria" / "2023-24" / "Data" / "a.dta") \
            .write_bytes(b"release-1")
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            da.populate_and_push("Nigeria", "2023-24")

        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == before
        assert "not overwritten" in caplog.text and "a.dta" in caplog.text

    def test_a_file_the_release_does_not_ship_is_named_not_gating(
            self, countries, wb, caplog):
        """Ethiopia 6161's v03 renamed one file, so acquiring it leaves v02's
        name behind (as a DVC sidecar in a clean checkout).  The release is
        held whole, so it is recorded -- and the leftover is named."""
        from lsms_library import data_access as da
        _write_source(countries, "Nigeria", "2023-24",
                      _record("Nigeria", "2023-24", **NGA_W5_QUEUED))
        data = countries / "Nigeria" / "2023-24" / "Data"
        data.mkdir(parents=True)
        (data / "old_name.dta.dvc").write_text("outs:\n- md5: 0\n")
        (data / "notes.txt").write_text("not a data file")
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            da.populate_and_push("Nigeria", "2023-24")
        assert pv.read_provenance(countries, "Nigeria", "2023-24").idno \
            == "NGA_2023_GHSP-W5_v02_M"
        assert "1 file(s)" in caplog.text and "old_name.dta.dvc" in caplog.text

    def test_a_failed_download_leaves_the_record_as_it_was(self, countries,
                                                           wb):
        from lsms_library import data_access as da
        before = _record("Nigeria", "2023-24", **NGA_W5_QUEUED)
        _write_source(countries, "Nigeria", "2023-24", before)
        wb["fail"] = True
        assert da.populate_and_push("Nigeria", "2023-24") == []
        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == before

    def test_add_wave_reacquisition_moves_the_held_keys(self, countries, wb):
        from lsms_library import data_access as da
        _write_source(countries, "Nigeria", "2023-24",
                      _record("Nigeria", "2023-24", **NGA_W5_QUEUED))
        da.add_wave("Nigeria", "6410", wave="2023-24", confirm=False,
                    push=False, verbose=False)

        prov = pv.read_provenance(countries, "Nigeria", "2023-24")
        assert prov.idno == "NGA_2023_GHSP-W5_v02_M"
        assert prov.doi == "https://doi.org/10.48529/90ec-pv69"
        assert prov.latest_idno is None and prov.latest_seen is None
        assert prov.notes == NGA_W5_QUEUED["notes"]
        assert prov.method == "source-url"           # how the id was learnt

    def test_add_wave_on_a_new_directory(self, countries, wb):
        from lsms_library import data_access as da
        da.add_wave("Nigeria", "6410", wave="2023-24", confirm=False,
                    push=False, verbose=False)
        prov = pv.read_provenance(countries, "Nigeria", "2023-24")
        assert prov.catalog_ids == ["6410"]
        assert prov.idno == "NGA_2023_GHSP-W5_v02_M"
        assert prov.doi == "https://doi.org/10.48529/90ec-pv69"
        assert prov.title == "General Household Survey, Panel 2023-2024"
        assert prov.years == "2023-2024"
        assert prov.method == "add-wave"
        assert prov.latest_idno is None

    def test_add_wave_whose_download_fails_records_no_release(self, countries,
                                                              wb):
        """The id is written first (the download routes on it); the release
        is not, because none is held."""
        from lsms_library import data_access as da
        wb["fail"] = True
        assert da.add_wave("Nigeria", "6410", wave="2023-24", confirm=False,
                           push=False, verbose=False) == []
        prov = pv.read_provenance(countries, "Nigeria", "2023-24")
        assert prov.catalog_ids == ["6410"]
        assert prov.idno is None and prov.doi is None


class TestAReStampNeverSuppliesAHeldRelease:
    """#1128 ruling (@ligon, 2026-10-20): the backfill never fills
    ``#+CATALOG_IDNO:`` from the catalog alone.  A record naming an entry but
    no release -- ``add_wave``'s routing write, or a hand-written file -- is
    stamped with the entry's facts and listed ``release-unknown``; only an
    acquisition writes the held release.  It used to be given the catalog's
    latest idno and DOI as the HELD ones (red team r1, residual 3)."""

    def _routing(self):
        return _record("Nigeria", "2023-24", catalog_id="6410",
                       url=NGA_W5["url"], method="add-wave")

    def test_a_routing_record_gets_no_release(self, bf, countries,
                                              monkeypatch, tmp_path):
        text = self._routing()
        assert "#+CATALOG_IDNO:" not in text
        _write_source(countries, "Nigeria", "2023-24", text)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])

        prov = pv.read_provenance(countries, "Nigeria", "2023-24")
        assert prov.idno is None and prov.doi is None
        assert prov.latest_idno is None and not prov.is_stale_release
        assert prov.title == NGA_W5["title"]           # entry facts written
        assert prov.years == "2023-2024"
        assert prov.method == "add-wave"
        after = pv.source_org_path(countries, "Nigeria", "2023-24").read_text()
        assert "#+CATALOG_IDNO:" not in after and "#+CATALOG_DOI:" not in after
        assert _unresolved(review) == {}
        rows = _release_unknown(review)
        assert set(rows) == {("Nigeria", "2023-24")}
        assert rows[("Nigeria", "2023-24")]["Catalog IDNO"] \
            == "NGA_2023_GHSP-W5_v02_M"

    def test_a_second_run_still_supplies_none(self, bf, countries,
                                              monkeypatch, tmp_path):
        _write_source(countries, "Nigeria", "2023-24", self._routing())
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        for _ in range(2):
            review = tmp_path / "review.org"
            bf.main(["--country", "Nigeria", "--review-file", str(review)])
        assert pv.read_provenance(countries, "Nigeria", "2023-24").idno is None
        assert set(_release_unknown(review)) == {("Nigeria", "2023-24")}

    def test_a_held_release_is_not_listed(self, bf, countries, monkeypatch,
                                         tmp_path):
        _write_source(countries, "Nigeria", "2023-24",
                      _record("Nigeria", "2023-24", **NGA_W5_HELD_V01))
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        review = tmp_path / "review.org"
        bf.main(["--country", "Nigeria", "--review-file", str(review)])
        assert _release_unknown(review) == {}
        assert pv.read_provenance(countries, "Nigeria", "2023-24").idno \
            == "NGA_2023_GHSP-W5_v01_M"

    def test_an_acquisition_still_writes_the_release(self, countries, wb):
        """The counterpart: the one writer the ruling names."""
        from lsms_library import data_access as da
        _write_source(countries, "Nigeria", "2023-24", self._routing())
        da.populate_and_push("Nigeria", "2023-24")
        assert pv.read_provenance(countries, "Nigeria", "2023-24").idno \
            == "NGA_2023_GHSP-W5_v02_M"

    def test_no_shipped_worldbank_record_lacks_a_release(self):
        """The ruling's case is latent in the shipped tree (0 of 104 WB
        records, measured 2026-10-20); this pins that it stays so."""
        lacking = []
        for path in _shipped_source_orgs():
            c, w = path.parts[-4], path.parts[-3]
            prov = pv.parse_source_org(path.read_text(), c, w)
            if prov.is_worldbank and not prov.idno:
                lacking.append(f"{c}/{w}")
        assert lacking == []


def _release_unknown(review):
    """The ``release_unknown`` table, keyed by ``(country, wave)``."""
    import pandas as pd
    from lsms_library.local_tools import df_from_orgfile
    try:
        df = df_from_orgfile(review, name="release_unknown", to_numeric=False)
    except Exception:            # an empty table has no rows to read
        return {}
    return {(r["Country"], r["Wave"]):
            {k: (None if pd.isna(v) else v) for k, v in r.items()}
            for r in df.to_dict("records")}


class TestTheRecordFollowsThePush:
    """``populate_and_push`` records the held release only after every file
    was pushed (red team r1, R5 i): a release on disk in one checkout but not
    on the remote is not held by the library."""

    def test_a_failed_push_records_no_release(self, countries, wb, monkeypatch,
                                              caplog):
        from lsms_library import data_access as da
        before = _record("Nigeria", "2023-24", **NGA_W5_QUEUED)
        _write_source(countries, "Nigeria", "2023-24", before)
        monkeypatch.setattr(da, "push_to_cache_batch",
                            lambda paths, remote=None, dvc_add=True: [])
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            assert da.populate_and_push("Nigeria", "2023-24") == []
        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == before
        assert "0 of 2 file(s) pushed" in caplog.text

    def test_a_partial_push_records_no_release(self, countries, wb,
                                               monkeypatch):
        from lsms_library import data_access as da
        before = _record("Nigeria", "2023-24", **NGA_W5_QUEUED)
        _write_source(countries, "Nigeria", "2023-24", before)
        monkeypatch.setattr(da, "push_to_cache_batch",
                            lambda paths, remote=None, dvc_add=True:
                            list(paths)[:1])
        assert len(da.populate_and_push("Nigeria", "2023-24")) == 1
        assert pv.source_org_path(countries, "Nigeria", "2023-24") \
            .read_text() == before

    def test_the_leftover_scan_is_recursive(self, countries, wb, caplog):
        """Nigeria/2023-24's Data/ is wholly nested (0 top-level entries, 106
        sidecars under subdirectories); a top-level scan saw nothing."""
        from lsms_library import data_access as da
        _write_source(countries, "Nigeria", "2023-24",
                      _record("Nigeria", "2023-24", **NGA_W5_QUEUED))
        nested = countries / "Nigeria" / "2023-24" / "Data" / "Post Harvest"
        nested.mkdir(parents=True)
        (nested / "old_name.dta.dvc").write_text("outs:\n- md5: 0\n")
        with caplog.at_level("WARNING", logger="lsms_library.data_access"):
            da.populate_and_push("Nigeria", "2023-24")
        assert pv.read_provenance(countries, "Nigeria", "2023-24").idno \
            == "NGA_2023_GHSP-W5_v02_M"
        assert "Post Harvest/old_name.dta.dvc" in caplog.text
        assert "GH #1149" in caplog.text


class TestAcquiredRelease:
    """``provenance.acquired_release`` -- the record an acquisition writes."""

    def test_doi_comes_from_the_queue_when_the_queued_release_is_acquired(self):
        existing = pv.parse_source_org(
            _record("Nigeria", "2023-24", **NGA_W5_QUEUED), "Nigeria",
            "2023-24")
        new = pv.acquired_release(
            existing, country="Nigeria", wave="2023-24", catalog_id="6410",
            idno="NGA_2023_GHSP-W5_v02_M")
        assert new.doi == "https://doi.org/10.48529/90ec-pv69"
        assert new.latest_idno is None
        # ... and is left blank rather than describe another release.
        other = pv.acquired_release(
            existing, country="Nigeria", wave="2023-24", catalog_id="6410",
            idno="NGA_2023_GHSP-W5_v03_M")
        assert other.doi is None

    def test_another_entry_keeps_only_the_prose_and_the_old_url(self):
        existing = pv.parse_source_org(
            _record("Nigeria", "2018-19", catalog_id="3827",
                    idno="NGA_2018_LSS_v01_M", notes="a human's note",
                    url="https://microdata.worldbank.org/index.php/catalog/"
                        "3827", validation=pv.VALIDATION_CONTENT),
            "Nigeria", "2018-19")
        new = pv.acquired_release(
            existing, country="Nigeria", wave="2018-19", catalog_id="3557",
            idno="NGA_2018_GHSP-W4_v03_M",
            url="https://microdata.worldbank.org/index.php/catalog/3557")
        assert new.catalog_ids == ["3557"]
        assert new.notes == "a human's note"
        assert new.validation is None           # it validated another entry
        assert new.superseded_url.endswith("/catalog/3827")
        assert new.method == "add-wave"


class TestPreservedKnownScalars:
    """Known scalar accessors stay last-value, while round trips keep facts."""

    @pytest.mark.parametrize('sources', [
        ['worldbank', 'worldbank'], ['worldbank', 'external'],
    ])
    def test_source_occurrences_cannot_erase_held_catalog_identity(self, tmp_path, sources):
        # Independent review's counterexample: retaining both source lines
        # must not turn the held catalog id into CATALOG_ID none on render.
        text = ('SOURCE\n\nhttps://microdata.worldbank.org/catalog/987654321\n\n'
                '#+CATALOG_ID: 987654321\n#+CATALOG_IDNO: held-release\n'
                + ''.join(f'#+PROVENANCE_SOURCE: {source}\n' for source in sources)
                + '#+PROVENANCE_RECORDED: 2026-10-20\n')
        _write_source(tmp_path, 'Example', '2020', text)
        record = pv.parse_source_org(text, 'Example', '2020')
        assert record.catalog_ids == ['987654321']
        if sources[-1] != pv.SOURCE_WORLDBANK:
            with pytest.raises(ValueError, match='explicit provenance curation'):
                pv.write_provenance(tmp_path, record)
            assert pv.source_org_path(tmp_path, 'Example', '2020').read_text() == text
        else:
            rendered = pv.render_source_org(record)
            assert '#+CATALOG_ID: 987654321\n' in rendered
            assert '#+CATALOG_IDNO: held-release\n' in rendered
            assert pv.parse_source_org(rendered, 'Example', '2020') == record

    def test_inconsistent_source_cannot_erase_catalog_coverage(self):
        record = pv.parse_source_org(
            'SOURCE\n\n#+CATALOG_ID: none\n#+CATALOG_COVERS: 123\n'
            '#+PROVENANCE_SOURCE: external\n', 'Example', '2020')
        assert record.covers == ['123']
        with pytest.raises(ValueError, match='explicit provenance curation'):
            pv.render_source_org(record)

    @pytest.mark.parametrize('key,values', [
        ('PROVENANCE_NOTE', ['first fact', 'second fact']),
        ('PROVENANCE_NOTE', ['same fact', 'same fact']),
        ('PROVENANCE_NOTE', ['first fact', '']),
        ('CATALOG_IDNO', ['held-one', 'held-two']),
        ('CATALOG_TITLE', ['']),
        ('PROVENANCE_RECORDED', ['']),
    ])
    def test_round_trip_keeps_each_known_scalar_occurrence(self, key, values):
        text = ('SOURCE\n\nhttps://example.org/source\n\n'
                '#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n')
        if key != 'PROVENANCE_RECORDED':
            text += '#+PROVENANCE_RECORDED: 2026-10-20\n'
        text += ''.join(f'#+{key}: {value}\n' for value in values)
        rec = pv.parse_source_org(text, 'Example', '2020')
        rendered = pv.render_source_org(rec)
        assert [line.partition(':')[2].strip() for line in rendered.splitlines()
                if line.startswith(f'#+{key}:')] == values
        assert pv.parse_source_org(rendered, 'Example', '2020') == rec
        assert pv.render_source_org(pv.parse_source_org(rendered, 'Example', '2020')) == rendered

    def test_unsafe_scalar_rewrite_is_explicit_and_does_not_touch_file(self, tmp_path):
        from dataclasses import replace
        text = ('SOURCE\n\nhttps://example.org/source\n\n'
                '#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n'
                '#+PROVENANCE_NOTE: first\n#+PROVENANCE_NOTE: second\n')
        _write_source(tmp_path, 'Example', '2020', text)
        rec = pv.parse_source_org(text, 'Example', '2020')
        with pytest.raises(ValueError, match='explicit provenance curation'):
            pv.write_provenance(tmp_path, replace(rec, note='replacement'))
        assert pv.source_org_path(tmp_path, 'Example', '2020').read_text() == text

    def test_backfill_refuses_a_scalar_reconstruction_that_loses_facts(
            self, bf, countries, monkeypatch, tmp_path):
        text = _record('Nigeria', '2023-24', **NGA_W5_HELD_V01)
        text += '#+PROVENANCE_NOTE: first\n#+PROVENANCE_NOTE: second\n'
        _write_source(countries, 'Nigeria', '2023-24', text)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        review = tmp_path / 'review.org'
        bf.main(['--country', 'Nigeria', '--review-file', str(review)])
        assert pv.source_org_path(countries, 'Nigeria', '2023-24').read_text() == text
        assert _unresolved(review)[('Nigeria', '2023-24')]['Kind'] \
            == 'restamp-would-delete'


class TestPrimarySourceRouting:
    @pytest.mark.parametrize("secondary", ["https://example.org/secondary",
                                            "https://example.org/primary"])
    def test_additional_bare_body_urls_are_preserved_as_notes(self, secondary):
        text = ("SOURCE\n\nhttps://example.org/primary\n" + secondary
                + "\n\n#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n"
                  "#+PROVENANCE_RECORDED: 2026-10-20\n")
        record = pv.parse_source_org(text, "X", "2020")
        assert record.url == "https://example.org/primary"
        assert record.notes == secondary
        rendered = pv.render_source_org(record)
        assert pv.parse_source_org(rendered, "X", "2020") == record
        assert pv.render_source_org(pv.parse_source_org(rendered, "X", "2020")) == rendered

    def test_explicit_primary_does_not_discard_a_different_body_url(self):
        text = ("SOURCE\n\nhttps://example.org/older-source\n\n"
                "#+CATALOG_ID: none\n#+CATALOG_URL: https://example.org/primary\n"
                "#+PROVENANCE_SOURCE: external\n#+PROVENANCE_RECORDED: 2026-10-20\n")
        record = pv.parse_source_org(text, "X", "2020")
        assert record.url == "https://example.org/primary"
        assert record.notes == "https://example.org/older-source"
        assert pv.parse_source_org(pv.render_source_org(record), "X", "2020") == record

    @pytest.mark.parametrize('region', ['metadata', 'notes'])
    def test_secondary_catalog_link_is_never_acquisition_routing(self, countries, region):
        from lsms_library import data_access as da
        text = ('SOURCE\n\n(source URL not recorded)\n\n'
                '#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n')
        if region == 'notes':
            text += f'\n{pv.NOTES_HEADING}\n'
        text += '#+SECONDARY_SOURCE: https://microdata.worldbank.org/catalog/6410\n'
        _write_source(countries, 'Example', '2020', text)
        assert pv.parse_source_org(text, 'Example', '2020').url is None
        assert da._read_source_url('Example', '2020') is None

    @pytest.mark.parametrize('populate', [False, True])
    def test_explicit_source_override_requires_acquisition_context(self, countries, populate):
        from lsms_library import data_access as da
        with pytest.raises(ValueError, match='acquisition mode'):
            da.get_data_file('Example/2020/Data/x.dta', populate_cache=populate,
                             _source_url='https://microdata.worldbank.org/catalog/6410')
        assert not list(countries.rglob('*.dta'))


class TestAcquisitionCompletionBoundaries:
    @pytest.mark.parametrize('receipt', ['failed', 'partial', 'duplicate', 'exception'])
    @pytest.mark.parametrize('entrypoint', ['add_wave', 'populate_and_push'])
    def test_push_must_confirm_every_requested_file(
            self, countries, wb, monkeypatch, receipt, entrypoint):
        from lsms_library import data_access as da
        before = _record('Nigeria', '2023-24', **NGA_W5_QUEUED)
        _write_source(countries, 'Nigeria', '2023-24', before)
        def push(paths, remote=None, dvc_add=True):
            if receipt == 'exception':
                raise RuntimeError('simulated push failure')
            if receipt == 'failed':
                return []
            if receipt == 'partial':
                return list(paths)[:1]
            return [paths[0], paths[0]]
        monkeypatch.setattr(da, 'push_to_cache_batch', push)
        def acquire():
            if entrypoint == 'add_wave':
                return da.add_wave('Nigeria', '6410', wave='2023-24',
                                   push=True, confirm=False, verbose=False)
            return da.populate_and_push('Nigeria', '2023-24')
        if receipt == 'exception':
            with pytest.raises(RuntimeError, match='simulated push failure'):
                acquire()
        else:
            acquire()
        assert pv.source_org_path(countries, 'Nigeria', '2023-24').read_text() == before
        assert ('Nigeria', '2023-24') not in da._ACQUIRED_RELEASES

    @pytest.mark.parametrize('absolute_receipt', [False, True])
    def test_successful_push_records_only_after_the_push(
            self, countries, wb, monkeypatch, absolute_receipt):
        from lsms_library import data_access as da
        before = _record('Nigeria', '2023-24', **NGA_W5_QUEUED)
        _write_source(countries, 'Nigeria', '2023-24', before)
        def push(paths, remote=None, dvc_add=True):
            assert pv.source_org_path(countries, 'Nigeria', '2023-24').read_text() == before
            return [countries / p for p in paths] if absolute_receipt else list(paths)
        monkeypatch.setattr(da, 'push_to_cache_batch', push)
        da.add_wave('Nigeria', '6410', wave='2023-24', push=True,
                    confirm=False, verbose=False)
        acquired = pv.read_provenance(countries, 'Nigeria', '2023-24')
        assert acquired.idno == NGA_W5_V02['idno']
        assert acquired.latest_idno is None and acquired.latest_seen is None

    def test_no_push_records_after_a_complete_download(self, countries, wb, monkeypatch):
        from lsms_library import data_access as da
        _write_source(countries, 'Nigeria', '2023-24',
                      _record('Nigeria', '2023-24', **NGA_W5_QUEUED))
        def forbidden(*args, **kwargs):
            raise AssertionError('push=False must not invoke push')
        monkeypatch.setattr(da, 'push_to_cache_batch', forbidden)
        result = da.add_wave('Nigeria', '6410', wave='2023-24', push=False,
                             confirm=False, verbose=False)
        assert sorted(p.name for p in result) == ['a.dta', 'b.dta']
        acquired = pv.read_provenance(countries, 'Nigeria', '2023-24')
        assert acquired.idno == NGA_W5_V02['idno'] and acquired.latest_idno is None

    @pytest.mark.parametrize('prior_kind', ['different-held-entry', 'unknown'])
    def test_failed_cross_entry_acquisition_preserves_existing_record(
            self, countries, wb, prior_kind):
        from lsms_library import data_access as da
        if prior_kind == 'different-held-entry':
            before = _record('Nigeria', '2023-24', **dict(NGA_W4_HELD_V03,
                             extra={'PARTNER_NOTE': ['retained fact']}))
        else:
            before = pv.render_source_org(pv.WaveProvenance(
                country='Nigeria', wave='2023-24', source=pv.SOURCE_UNKNOWN,
                note='Hand recorded fact', extra={'PARTNER_NOTE': ['retained fact']}))
        _write_source(countries, 'Nigeria', '2023-24', before)
        wb['fail'] = True
        assert da.add_wave('Nigeria', '6410', wave='2023-24', push=False,
                           confirm=False, verbose=False) == []
        assert pv.source_org_path(countries, 'Nigeria', '2023-24').read_text() == before

    def test_successful_cross_entry_acquisition_records_actual_release(self, countries, wb):
        from lsms_library import data_access as da
        before = _record('Nigeria', '2023-24', **dict(NGA_W4_HELD_V03,
                         notes='Historical source note',
                         extra={'PARTNER_NOTE': ['retained fact']}))
        _write_source(countries, 'Nigeria', '2023-24', before)
        da.add_wave('Nigeria', '6410', wave='2023-24', push=False,
                    confirm=False, verbose=False)
        acquired = pv.read_provenance(countries, 'Nigeria', '2023-24')
        assert acquired.catalog_id == '6410' and acquired.idno == NGA_W5_V02['idno']
        assert acquired.notes == 'Historical source note'
        assert acquired.extra == {'PARTNER_NOTE': ['retained fact']}

    def test_partial_extraction_cannot_record_a_complete_release(
            self, countries, wb, monkeypatch):
        import builtins
        from lsms_library import data_access as da
        before = _record('Nigeria', '2023-24', **NGA_W5_QUEUED)
        _write_source(countries, 'Nigeria', '2023-24', before)
        actual_open = builtins.open
        def fail_second(path, mode='r', *args, **kwargs):
            if mode == 'wb' and Path(path).name == 'b.dta':
                raise OSError('simulated disk failure')
            return actual_open(path, mode, *args, **kwargs)
        monkeypatch.setattr(builtins, 'open', fail_second)
        da.add_wave('Nigeria', '6410', wave='2023-24', push=False,
                    confirm=False, verbose=False)
        assert pv.source_org_path(countries, 'Nigeria', '2023-24').read_text() == before
        assert (countries / 'Nigeria/2023-24/Data/a.dta').exists()


class TestQueueFactsSurviveMissingCatalogMetadata:
    def test_unknown_held_release_does_not_erase_existing_observation(
            self, bf, countries, monkeypatch, tmp_path):
        before = _record('Nigeria', '2023-24', catalog_id='6410',
                         url=NGA_W5['url'], doi='https://doi.org/held-note',
                         latest_idno=NGA_W5_V02['idno'],
                         latest_doi=NGA_W5_V02['doi'], latest_seen='2026-10-19')
        _write_source(countries, 'Nigeria', '2023-24', before)
        _catalog(monkeypatch, bf, NGA_ROWS + [NGA_W5_V02])
        review = tmp_path / 'review.org'
        bf.main(['--country', 'Nigeria', '--review-file', str(review)])
        after = pv.read_provenance(countries, 'Nigeria', '2023-24')
        assert after.idno is None and after.doi == 'https://doi.org/held-note'
        assert (after.latest_idno, after.latest_doi, after.latest_seen) == (
            NGA_W5_V02['idno'], NGA_W5_V02['doi'], '2026-10-19')
        assert ('Nigeria', '2023-24') in _release_unknown(review)

    def test_same_queued_release_keeps_a_doi_temporarily_missing_from_catalog(
            self, bf, countries, monkeypatch, tmp_path):
        before = _record('Nigeria', '2023-24', **NGA_W5_QUEUED)
        _write_source(countries, 'Nigeria', '2023-24', before)
        _catalog(monkeypatch, bf, NGA_ROWS + [dict(NGA_W5_V02, doi=None)])
        review = tmp_path / 'review.org'
        bf.main(['--country', 'Nigeria', '--review-file', str(review)])
        after = pv.read_provenance(countries, 'Nigeria', '2023-24')
        assert after.idno == NGA_W5_QUEUED['idno']
        assert after.latest_doi == NGA_W5_QUEUED['latest_doi']
        assert after.latest_seen == NGA_W5_QUEUED['latest_seen']

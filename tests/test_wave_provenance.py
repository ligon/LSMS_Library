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

    def test_same_study_under_two_repository_ids_reads_as_held(
            self, countries, monkeypatch):
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
                      _wb("South Africa", "1993", catalog_id="297"))

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

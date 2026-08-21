"""Discovery must look in the repositories where a country's series lives.

GH #597.  ``discover_waves`` searched a single hard-coded collection
(``lsms``).  Whole household-survey series that the World Bank publishes under
*other* repositories were therefore not "not yet fetched" but **structurally
unfindable**:

* Armenia's Integrated Living Conditions Survey -- 18 annual waves, 2001-2018
  -- sits in ``central``.  ``lsms`` holds only the 1996 Household Budget Survey.
* South Africa's General Household Survey -- 21 waves, 2002-2025 -- sits in
  ``datafirst``.  ``lsms`` holds only the 1993 Integrated Household Survey.

The fix is a per-country ``repositories`` list, defaulting to ``("lsms",)``.

The tests below pin down *both* failure directions, because the fix has an
obvious wrong version.  Simply dropping the collection filter would find the
missing waves -- and bury them: a South African all-repository search returns
426 rows (quarterly labour force surveys, censuses, election studies, school
registers, media surveys), against the 22 that are actually the two series we
care about.  A missing-wave list nobody trusts is worse than none.  So the
widening is paired with an ``idno_pattern`` that pins the survey *series*, and
the noise-rejection is tested as seriously as the discovery.
"""

import pytest

from lsms_library import data_access as da
from lsms_library.data_access import (
    _COUNTRY_CATALOG,
    CountryCatalog,
    discover_waves,
)


# --- Fixtures --------------------------------------------------------------
#
# Rows are copied from the live WB catalog (2026-07) so the noise under test is
# the noise that is actually there, not noise we imagined.

ARM_LSMS = [
    {"id": "2324", "idno": "ARM_1996_HBS_v01_M", "repository": "lsms",
     "title": "Household Budget Survey 1996",
     "year_start": 1996, "year_end": 1996, "doi": "", "url": ""},
]

ARM_CENTRAL = [
    {"id": "2950", "idno": "ARM_2001_ILCS_v02_M", "repository": "central",
     "title": "Integrated Survey of Living Standards 2001",
     "year_start": 2001, "year_end": 2001, "doi": "", "url": ""},
    {"id": "2964", "idno": "ARM_2015_ILCS_v02_M", "repository": "central",
     "title": "Integrated Living Conditions Survey 2015",
     "year_start": 2015, "year_end": 2015, "doi": "", "url": ""},
    # ... and the noise that shares the `central` repository:
    {"id": "2984", "idno": "ARM_2014_LFS_v01_M", "repository": "central",
     "title": "Labor Force Survey 2014",
     "year_start": 2014, "year_end": 2014, "doi": "", "url": ""},
    {"id": "5945", "idno": "ARM_2022_GTUS_v01_M", "repository": "central",
     "title": "Special Survey on Time Use and Gender Disparities 2022",
     "year_start": 2022, "year_end": 2022, "doi": "", "url": ""},
    # A global study tagged to every country -- 28 countries carry this row.
    {"id": "7860", "idno": "WLD_2024_FINDEX_v02_M", "repository": "central",
     "title": "The Global Findex Database 2025",
     "year_start": 2024, "year_end": 2024, "doi": "", "url": ""},
]

ZAF_LSMS = [
    {"id": "297", "idno": "ZAF_1993_IHS_v01_M", "repository": "lsms",
     "title": "Integrated Household Survey 1993",
     "year_start": 1993, "year_end": 1993, "doi": "", "url": ""},
]

ZAF_DATAFIRST = [
    {"id": "2773", "idno": "ZAF_2015_GHS_v01_M", "repository": "datafirst",
     "title": "General Household Survey 2015",
     "year_start": 2015, "year_end": 2015, "doi": "", "url": ""},
    {"id": "8309", "idno": "ZAF_2025_GHS_v01_M", "repository": "datafirst",
     "title": "General Household Survey 2025",
     "year_start": 2025, "year_end": 2025, "doi": "", "url": ""},
    # The DataFirst archive is deep and mostly not ours:
    {"id": "8296", "idno": "ZAF_2026_QLFS-Q1_v01_M", "repository": "datafirst",
     "title": "Quarterly Labour Force Survey 2026",
     "year_start": 2026, "year_end": 2026, "doi": "", "url": ""},
    {"id": "8219", "idno": "ZAF_2022-2023_IES_v01_M", "repository": "datafirst",
     "title": "Income and Expenditure Survey 2022-2023",
     "year_start": 2022, "year_end": 2023, "doi": "", "url": ""},
    # THE trap: the same 1993 survey we already hold as ZAF_1993_IHS (lsms id
    # 297), re-catalogued by DataFirst under a different id and idno.
    {"id": "902", "idno": "ZAF_1993_PSLSD_v01_M", "repository": "datafirst",
     "title": "Project for Statistics on Living Standards and Development 1993",
     "year_start": 1993, "year_end": 1993, "doi": "", "url": ""},
]

LBR_LSMS = [
    {"id": "3787", "idno": "LBR_2018_NHFS_v01_M", "repository": "lsms",
     "title": "National Household Forest Survey 2018-2019",
     "year_start": 2018, "year_end": 2019, "doi": "", "url": ""},
]

LBR_CENTRAL = [
    {"id": "2563", "idno": "LBR_2014_HIES_v01_M", "repository": "central",
     "title": "Household Income and Expenditure Survey 2014-2015",
     "year_start": 2014, "year_end": 2015, "doi": "", "url": ""},
    {"id": "2986", "idno": "LBR_2016_HIES_v01_M", "repository": "central",
     "title": "Household Income and Expenditure Survey 2016",
     "year_start": 2016, "year_end": 2017, "doi": "", "url": ""},
    # `central` noise for LBR:
    {"id": "8189", "idno": "LBR_2017_MTF_v01_M", "repository": "central",
     "title": "Multi-Tier Framework for Measuring Energy Access 2017",
     "year_start": 2017, "year_end": 2017, "doi": "", "url": ""},
    {"id": "4529", "idno": "LBR_2017_PESBR_v01_M", "repository": "central",
     "title": "Survey of Public Servants 2017",
     "year_start": 2017, "year_end": 2017, "doi": "", "url": ""},
    {"id": "888", "idno": "AFR_2008_AFB-MR4_v02_M", "repository": "central",
     "title": "Afrobarometer Survey 2008",
     "year_start": 2008, "year_end": 2008, "doi": "", "url": ""},
]

_BY_COLLECTION = {
    "ARM": {"lsms": ARM_LSMS, "central": ARM_CENTRAL},
    "ZAF": {"lsms": ZAF_LSMS, "datafirst": ZAF_DATAFIRST},
    "LBR": {"lsms": LBR_LSMS, "central": LBR_CENTRAL},
}


@pytest.fixture()
def catalog(tmp_path, monkeypatch):
    """Mock the WB catalog; record which collections were queried."""
    calls: list[tuple[str, str | None]] = []

    def fake_search(code, collection="lsms"):
        calls.append((code, collection))
        return list(_BY_COLLECTION.get(code, {}).get(collection, []))

    root = tmp_path / "countries"
    root.mkdir()
    monkeypatch.setattr(da, "_COUNTRIES_DIR", root)
    monkeypatch.setattr(da, "_wb_catalog_search", fake_search)
    return calls


# ---------------------------------------------------------------------------
# The config surface
# ---------------------------------------------------------------------------

class TestRepositoryConfig:

    def test_default_is_lsms_only(self):
        """The overwhelming majority of countries stay exactly as they were."""
        assert CountryCatalog("XXX").repositories == ("lsms",)
        for name in ("Nigeria", "Uganda", "Malawi", "Ethiopia", "Tanzania"):
            assert _COUNTRY_CATALOG[name].repositories == ("lsms",), name

    def test_widened_countries_are_widened(self):
        assert _COUNTRY_CATALOG["Armenia"].repositories == ("lsms", "central")
        assert _COUNTRY_CATALOG["South Africa"].repositories == (
            "lsms", "datafirst")
        assert _COUNTRY_CATALOG["Liberia"].repositories == ("lsms", "central")

    def test_widened_countries_pin_their_series(self):
        """Widening without a series pin is the false-positive failure mode.

        This is the invariant that keeps the census trustworthy, so it is
        asserted over *every* widened country -- including any added later.
        """
        for name, spec in _COUNTRY_CATALOG.items():
            if len(spec.repositories) > 1:
                assert spec.idno_pattern, (
                    f"{name} searches {spec.repositories} but pins no series; "
                    "it will report unrelated studies as missing waves.")


# ---------------------------------------------------------------------------
# The bug: series that a lsms-only search cannot see
# ---------------------------------------------------------------------------

class TestWidenedDiscoveryFindsTheHiddenSeries:

    def test_armenia_ilcs_is_found(self, catalog):
        """18 waves of an LSMS-family survey, previously unfindable."""
        found = {e["id"] for e in discover_waves("Armenia")}
        assert {"2950", "2964"} <= found          # the ILCS, from `central`
        assert "2324" in found                    # the HBS we hold, from `lsms`

    def test_south_africa_ghs_is_found(self, catalog):
        found = {e["id"] for e in discover_waves("South Africa")}
        assert {"2773", "8309"} <= found          # the GHS, from `datafirst`
        assert "297" in found                     # the IHS we hold, from `lsms`

    def test_liberia_hies_is_found(self, catalog):
        """We hold a *forest* survey; the two HIES waves were invisible."""
        found = {e["id"] for e in discover_waves("Liberia")}
        assert {"2563", "2986"} <= found       # the HIES, from `central`
        assert "3787" in found                 # the NHFS we hold, from `lsms`

    def test_liberia_rejects_thematic_studies_in_central(self, catalog):
        found = {e["id"] for e in discover_waves("Liberia")}
        assert "8189" not in found, "energy-access framework is not a HIES wave"
        assert "4529" not in found, "survey of public servants is not ours"
        assert "888" not in found, "Afrobarometer is not ours"

    def test_liberia_nhfs_is_matched_for_identity_not_remit(self, catalog,
                                                            tmp_path):
        """The NHFS is in the pattern because it is the catalog entry backing a
        wave dir we HOLD -- discovery must be able to report it as held.

        By the remit predicate a forest-resources survey is NOT in remit.  This
        is where the two axes -- 'is this catalog row this country's?' and 'is
        this survey in remit?' -- visibly diverge, and the divergence is
        deliberate: `idno_pattern` answers the first question, not the second.
        """
        import lsms_library.provenance as pv
        src = (tmp_path / "countries" / "Liberia" / "2018-19" / "Documentation")
        src.mkdir(parents=True)
        (src / "SOURCE.org").write_text(pv.render_source_org(
            pv.WaveProvenance(country="Liberia", wave="2018-19",
                              source=pv.SOURCE_WORLDBANK, catalog_id="3787",
                              idno="LBR_2018_NHFS_v01_M",
                              url="https://microdata.worldbank.org"
                                  "/index.php/catalog/3787")))

        by_id = {e["id"]: e for e in discover_waves("Liberia")}
        # The forest survey: ours by identity, held, and NOT in remit.
        assert by_id["3787"]["local"] is True
        assert by_id["3787"]["local_waves"] == ["2018-19"]
        # The HIES waves: in remit, and NOT held.
        assert by_id["2563"]["local"] is False
        assert by_id["2986"]["local"] is False

    def test_every_configured_repository_is_searched(self, catalog):
        discover_waves("Armenia")
        assert ("ARM", "lsms") in catalog
        assert ("ARM", "central") in catalog

    def test_unwidened_country_still_searches_only_lsms(self, catalog):
        """No behaviour change for the other 34 countries."""
        discover_waves("Nigeria")
        assert catalog == [("NGA", "lsms")]


# ---------------------------------------------------------------------------
# The opposite failure: drowning the signal
# ---------------------------------------------------------------------------

class TestWideningDoesNotAdmitNoise:

    def test_armenia_rejects_non_series_studies_in_the_same_repository(
            self, catalog):
        """`central` also holds Armenian labour-force and time-use surveys,
        and global Findex rows tagged to every country.  None are ours."""
        found = {e["id"] for e in discover_waves("Armenia")}
        assert "2984" not in found, "Labour Force Survey is not an ILCS wave"
        assert "5945" not in found, "Time Use Survey is not an ILCS wave"
        assert "7860" not in found, "global Findex is not an Armenian wave"

    def test_south_africa_rejects_the_datafirst_archive(self, catalog):
        """DataFirst holds 320 ZAF rows.  Three series are ours; the rest are
        not, and the pin is the only thing standing between them and the
        census."""
        found = {e["id"] for e in discover_waves("South Africa")}
        assert "8296" not in found, "Quarterly Labour Force Survey is not ours"
        # ... while the three ruled-in series ARE admitted:
        assert {"2773", "8309"} <= found, "GHS"
        assert "8219" in found, "IES -- ruled in remit: it feeds the demand path"

    def test_a_study_we_hold_under_another_id_is_not_reported_missing(
            self, catalog):
        """ZAF_1993_PSLSD (datafirst 902) IS the survey we hold as 1993/ --
        the WB catalogued it twice, in two repositories, under two ids.

        Nothing in the catalog metadata links the two, so id-matching alone
        cannot see the duplicate.  The series pin is what keeps it out."""
        found = {e["id"] for e in discover_waves("South Africa")}
        assert "902" not in found

    def test_malawi_ihs3_duplicate_stays_invisible(self):
        """`central` id 3016 (MWI_2010_IHS-III_v01_M_v01_A_ML) is the same
        study as `lsms` id 1003, which we hold as Malawi/2010-11/.

        Malawi is not widened, so it cannot surface -- but if someone widens
        Malawi later, they must pin the series or resurrect this false
        positive.  This test documents the trap by asserting the guard."""
        spec = _COUNTRY_CATALOG["Malawi"]
        assert spec.repositories == ("lsms",) or spec.idno_pattern


# ---------------------------------------------------------------------------
# Mechanics
# ---------------------------------------------------------------------------

class TestSearchMechanics:

    def test_results_are_deduplicated_on_catalog_id(self, monkeypatch):
        """A study listed in two collections must be counted once."""
        row = {"id": "1", "idno": "ARM_2001_ILCS_v02_M", "repository": "lsms",
               "title": "x", "year_start": 2001, "year_end": 2001,
               "doi": "", "url": ""}
        monkeypatch.setattr(da, "_wb_catalog_search",
                            lambda code, collection="lsms": [dict(row)])
        out = da._wb_catalog_search_many("ARM", ("lsms", "central"))
        assert len(out) == 1

    def test_first_collection_wins(self, monkeypatch):
        """`lsms` is listed first, so its view of a shared study takes
        precedence -- that is the id our SOURCE.org files record."""
        def fake(code, collection="lsms"):
            return [{"id": "1", "idno": f"X_{collection}", "repository":
                     collection, "title": "", "year_start": 2001,
                     "year_end": 2001, "doi": "", "url": ""}]
        monkeypatch.setattr(da, "_wb_catalog_search", fake)
        out = da._wb_catalog_search_many("ARM", ("lsms", "central"))
        assert [r["idno"] for r in out] == ["X_lsms"]

    def test_explicit_collection_overrides_the_config(self, catalog):
        """The escape hatch still works, for exploration."""
        found = {e["id"] for e in discover_waves("Armenia", collection="lsms")}
        assert found == {"2324"}
        assert ("ARM", "central") not in catalog

    def test_discovery_never_writes_to_the_countries_tree(self, catalog,
                                                          tmp_path):
        """Discovery is read-only.  Writing a Documentation/SOURCE.org would
        PROMOTE a directory into a wave (Country.waves scans for it)."""
        discover_waves("Armenia")
        discover_waves("South Africa")
        assert list((tmp_path / "countries").iterdir()) == []

"""Burkina Faso ``cluster_features.District`` serves 45 provinces, not 96 strings -- GH #1059.

The #811 ``Region`` problem one level down.  The province arrives in three
spellings across the three waves -- 2014 (EMC ``province``) upper case and
unaccented, with one ``loba`` for ``Ioba``; 2018-19 (EHCVM ``s00q02``) title
case, mostly accented, with ``KOssi``, ``Kourweogo`` and ``Koulpelgo``; 2021-22
title case with ``Bale`` / ``Bazega`` / ``Seno`` unaccented -- so the pooled
column carried 96 strings for 45 provinces (measured 2026-10-23 on a cold
build of development ``9a0ed5f3c``).  Each wave is internally consistent
(45 / 45 / 44 strings, one per province present), so nothing warned.

``Burkina_Faso/_/categorical_mapping.org`` now carries a ``#+name: District``
table, auto-applied to the column by name (as ``Region`` is, PR #971).  The
Preferred Label is the official accented, title-case province name as the
data itself spells it; see the table's prose for the rule.

What the table must NOT do: map one province onto another.  On the 550
grappes shared by 2018-19 and 2021-22, three name a DIFFERENT province in
each wave (19 ``Séno``/``Yagha``, 365 ``Poni``/``Noumbiel``, 549
``Kompienga``/``Gourma``).  Whether those are the same EA is an identity
question, not a spelling one, so they must still differ after the mapping.

Two families, split as in ``tests/test_niger_region_labels.py``:

* ``TestTable`` needs NO microdata.  It fails on development, where no
  ``District`` table exists.
* ``TestServedDistrict`` builds the real table and is skipped without S3
  credentials.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import unicodedata

import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root


def _tests_conftest():
    """``tests/conftest.py``, loaded by path (the root conftest shadows it)."""
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
TABLE = "District"

#: The 45 provinces of Burkina Faso, as the table spells them.
PROVINCES = frozenset({
    "Balé", "Bam", "Banwa", "Bazèga", "Bougouriba", "Boulgou", "Boulkiemdé",
    "Comoé", "Ganzourgou", "Gnagna", "Gourma", "Houet", "Ioba", "Kadiogo",
    "Kénédougou", "Komandjoari", "Kompienga", "Kossi", "Koulpélogo",
    "Kouritenga", "Kourwéogo", "Léraba", "Loroum", "Mouhoun", "Nahouri",
    "Namentenga", "Nayala", "Noumbiel", "Oubritenga", "Oudalan", "Passoré",
    "Poni", "Sanguié", "Sanmatenga", "Séno", "Sissili", "Soum", "Sourou",
    "Tapoa", "Tuy", "Yagha", "Yatenga", "Ziro", "Zondoma", "Zoundwéogo",
})

#: Every spelling the three waves serve, measured PER WAVE on a cold build
#: of development 9a0ed5f3c (2026-10-23): 45 / 45 / 44 strings, 96 pooled.
#: A new spelling in a re-release would be served raw, which
#: ``TestServedDistrict`` catches.
RAW_SPELLINGS = {
    "2014": {
        "BALE", "BAM", "BANWA", "BAZEGA", "BOUGOURIBA", "BOULGOU",
        "BOULKIEMDE", "COMOE", "GANZOURGOU", "GNAGNA", "GOURMA",
        "HOUET", "KADIOGO", "KENEDOUGOU", "KOMANDJOARI", "KOMPIENGA",
        "KOSSI", "KOULPELOGO", "KOURITENGA", "KOURWEOGO", "LERABA",
        "LOROUM", "MOUHOUN", "NAHOURI", "NAMENTENGA", "NAYALA",
        "NOUMBIEL", "OUBRITENGA", "OUDALAN", "PASSORE", "PONI",
        "SANGUIE", "SANMATENGA", "SENO", "SISSILI", "SOUM", "SOUROU",
        "TAPOA", "TUY", "YAGHA", "YATENGA", "ZIRO", "ZONDOMA",
        "ZOUNDWEOGO", "loba",
    },
    "2018-19": {
        "Balé", "Bam", "Banwa", "Bazèga", "Bougouriba", "Boulgou",
        "Boulkiemdé", "Comoé", "Ganzourgou", "Gnagna", "Gourma",
        "Houet", "Ioba", "KOssi", "Kadiogo", "Komandjoari",
        "Kompienga", "Koulpelgo", "Kouritenga", "Kourweogo",
        "Kénédougou", "Loroum", "Léraba", "Mouhoun", "Nahouri",
        "Namentenga", "Nayala", "Noumbiel", "Oubritenga", "Oudalan",
        "Passoré", "Poni", "Sanguié", "Sanmatenga", "Sissili", "Soum",
        "Sourou", "Séno", "Tapoa", "Tuy", "Yagha", "Yatenga", "Ziro",
        "Zondoma", "Zoundwéogo",
    },
    "2021-22": {
        "Bale", "Bam", "Banwa", "Bazega", "Bougouriba", "Boulgou",
        "Boulkiemdé", "Comoé", "Ganzourgou", "Gnagna", "Gourma",
        "Houet", "Ioba", "Kadiogo", "Komandjoari", "Kossi",
        "Koulpélogo", "Kouritenga", "Kourwéogo", "Kénédougou",
        "Loroum", "Léraba", "Mouhoun", "Nahouri", "Namentenga",
        "Nayala", "Noumbiel", "Oubritenga", "Oudalan", "Passoré",
        "Poni", "Sanguié", "Sanmatenga", "Seno", "Sissili", "Soum",
        "Sourou", "Tapoa", "Tuy", "Yagha", "Yatenga", "Ziro",
        "Zondoma", "Zoundwéogo",
    },
}

#: The two rows that are more than accent / case: a lower-case L for the
#: capital I (2014; all 17 zd are in Sud-Ouest, Ioba's region), and a
#: letter dropped (2018-19).
NAMED_CORRECTIONS = {"loba": "Ioba", "Koulpelgo": "Koulpélogo"}

#: Shared 2018-19 / 2021-22 grappes that name a DIFFERENT province in each
#: wave (one grappe each).  An identity question, deliberately NOT mapped.
PROVINCE_DISAGREEMENTS = {
    ("19", "Séno", "Yagha"),
    ("365", "Poni", "Noumbiel"),
    ("549", "Kompienga", "Gourma"),
}

#: Distinct provinces served per wave.  2021-22 has no ``Kompienga`` row at
#: all (grappe 549 is the one 2018-19 Kompienga grappe in the 2021-22
#: subset, and it is coded Gourma there), so 44 is not a spelling residue.
DISTINCT_PER_WAVE = {"2014": 45, "2018-19": 45, "2021-22": 44}


def _fold(s: str) -> str:
    """Accent- and case-insensitive form of a label."""
    return "".join(ch for ch in unicodedata.normalize("NFD", s)
                   if unicodedata.category(ch) != "Mn").casefold()


@pytest.fixture(scope="module")
def district_table() -> dict[str, str]:
    """``{Alternate Spelling: Preferred Label}`` of the ``District`` org table."""
    df = df_from_orgfile(countries_root() / COUNTRY / "_" / "categorical_mapping.org",
                         name=TABLE)
    keys = df.iloc[:, 0].astype(str).str.strip()
    assert keys.is_unique, "a spelling is listed twice"
    return dict(zip(keys, df["Preferred Label"].astype(str).str.strip()))


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

class TestTable:

    def test_every_raw_spelling_of_every_wave_is_mapped(self, district_table):
        for wave, spellings in RAW_SPELLINGS.items():
            missing = spellings - set(district_table)
            assert not missing, f"{wave}: unmapped District spelling(s) {sorted(missing)}"

    def test_preferred_labels_are_exactly_the_45_provinces(self, district_table):
        assert set(district_table.values()) == PROVINCES

    def test_every_preferred_label_is_a_spelling_the_data_uses(self):
        """No label is imported from outside the data (the canonical-spelling
        rule in the table's prose)."""
        attested = set().union(*RAW_SPELLINGS.values())
        assert PROVINCES <= attested, sorted(PROVINCES - attested)

    def test_table_is_idempotent(self, district_table):
        for canon in PROVINCES:
            assert district_table.get(canon) == canon, f"{canon!r} -> {district_table.get(canon)!r}"

    def test_each_row_only_respells(self, district_table):
        """Every row maps a string onto the province it already names:
        identical up to accents and case, bar the two named corrections."""
        for raw, canon in district_table.items():
            if raw in NAMED_CORRECTIONS:
                assert canon == NAMED_CORRECTIONS[raw], (raw, canon)
            else:
                assert _fold(raw) == _fold(canon), f"{raw!r} -> {canon!r} is not a respelling"

    def test_no_province_is_mapped_onto_another(self, district_table):
        """The three cross-wave province disagreements stay disagreements."""
        for _v, before, after in sorted(PROVINCE_DISAGREEMENTS):
            assert district_table[before] == before
            assert district_table[after] == after
            assert district_table[before] != district_table[after]


# ---------------------------------------------------------------------------
# built table
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def _non_strict():
    """Warn-mode grain checks.  2014 carries a pre-existing, unrelated
    ``GrainCollapseWarning`` (GH #1060: province varies within 94 zd) that
    ``LSMS_GRAIN_STRICT=1`` would turn into an abort of the whole build."""
    prior = os.environ.pop("LSMS_GRAIN_STRICT", None)
    yield
    if prior is not None:
        os.environ["LSMS_GRAIN_STRICT"] = prior


@requires_s3
@pytest.mark.usefixtures("_non_strict")
class TestServedDistrict:

    @pytest.fixture(scope="class")
    def cluster_features(self):
        import lsms_library as ll
        return ll.Country(COUNTRY).cluster_features()

    def test_45_provinces_pooled(self, cluster_features):
        served = set(cluster_features["District"].dropna().astype(str).unique())
        assert served == PROVINCES, f"served District labels: {sorted(served)}"

    @pytest.mark.parametrize("wave", sorted(DISTINCT_PER_WAVE))
    def test_one_label_per_province_in_every_wave(self, cluster_features, wave):
        col = cluster_features.xs(wave, level="t")["District"].dropna().astype(str)
        assert set(col.unique()) <= PROVINCES, f"{wave}: {sorted(set(col.unique()) - PROVINCES)}"
        assert col.nunique() == DISTINCT_PER_WAVE[wave], (wave, col.nunique())

    def test_no_wave_serves_a_raw_spelling(self, cluster_features):
        served = set(cluster_features["District"].dropna().astype(str).unique())
        raw_only = set().union(*RAW_SPELLINGS.values()) - PROVINCES
        leaked = served & raw_only
        assert not leaked, f"raw District spelling(s) served: {sorted(leaked)}"

    def test_shared_grappes_differ_only_by_province(self, cluster_features):
        """On the shared 2018-19 / 2021-22 grappes the only remaining District
        differences are the three province disagreements."""
        flat = cluster_features.reset_index()
        a = flat[flat["t"] == "2018-19"].set_index("v")["District"]
        b = flat[flat["t"] == "2021-22"].set_index("v")["District"]
        shared = a.index.intersection(b.index)
        assert len(shared) == 550
        residue = {(v, a[v], b[v]) for v in shared if a[v] != b[v]}
        assert residue == PROVINCE_DISAGREEMENTS, sorted(residue)

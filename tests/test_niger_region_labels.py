"""Niger ``cluster_features.Region`` serves 8 labels, not 16 -- GH #811.

The eight regions arrive in four spellings across the four waves: 2011-12
title-case with accents (``Tillabéri``), 2014-15 the same plus ``Communauté
urbaine de Niamey``, 2018-19 all LOWER-CASE and unaccented (``agadez``,
``tillaberi``), 2021-22 title-case unaccented with the ``gadez`` typo.  The
``Region`` table in ``Niger/_/categorical_mapping.org`` (auto-applied to the
column by name) mapped every spelling but 2018-19's, so the served column
carried 16 labels for 8 regions (measured 2026-09-24 on the pre-fix warm
parquet: 19 raw spellings, 11 mapped onto 8 labels + 8 unmapped = 16).

Two families, split as in ``tests/test_niger_value_sentinels.py``:

* ``TestTableCoversEverySpelling`` needs NO microdata: it pins that the org
  table maps each of the 19 raw spellings, onto exactly the eight regions.
* ``TestServedRegion`` builds the real table and is skipped when Niger is
  unavailable.  It checks the served column PER WAVE, because a wave that
  serves 8 labels of its own can still pool to 16.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

COUNTRY = "Niger"
TABLE = "Region"

#: The eight regions of Niger, as the table spells them.
REGIONS = frozenset({"Agadez", "Diffa", "Dosso", "Maradi", "Niamey", "Tahoua",
                     "Tillabéri", "Zinder"})

#: Every spelling the four waves' source files carry (19), measured on the
#: pre-transformation L2 parquet 2026-09-24.  A NEW spelling in a re-release
#: would show up as a 9th served label, which ``TestServedRegion`` catches.
RAW_SPELLINGS = {
    "2011-12": {"Agadez", "Diffa", "Dosso", "Maradi", "Niamey", "Tahoua",
                "Tillabéri", "Zinder"},
    "2014-15": {"Agadez", "Diffa", "Dosso", "Maradi", "Communauté urbaine de Niamey",
                "Tahoua", "Tillaberi", "Zinder"},
    "2018-19": {"agadez", "diffa", "dosso", "maradi", "niamey", "tahoua",
                "tillaberi", "zinder"},
    "2021-22": {"gadez", "Diffa", "Dosso", "Maradi", "Niamey", "Tahoua",
                "Tillaberi", "Zinder"},
}
#: The eight rows GH #811 added: 2018-19's spellings.
LOWER_CASE_2018_19 = RAW_SPELLINGS["2018-19"]


def _root() -> Path:
    return countries_root() / COUNTRY


def _aws_creds_available() -> bool:
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (Path(__file__).parent.parent
                  / "lsms_library" / "countries" / ".dvc" / "s3_creds")
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


needs_data = pytest.mark.skipif(
    not (countries_root() / COUNTRY / "_" / "data_scheme.yml").exists()
    or os.environ.get("LSMS_SKIP_AUTH")
    or not _aws_creds_available(),
    reason=f"{COUNTRY} microdata unavailable (no config or no S3 credentials)",
)


@pytest.fixture(scope="module")
def region_table() -> dict[str, str]:
    """``{Original Label: Preferred Label}`` of the ``Region`` org table."""
    df = df_from_orgfile(_root() / "_" / "categorical_mapping.org", name=TABLE)
    return dict(zip(df["Original Label"].astype(str).str.strip(),
                    df["Preferred Label"].astype(str).str.strip()))


# ---------------------------------------------------------------------------
# config-only
# ---------------------------------------------------------------------------

class TestTableCoversEverySpelling:

    def test_every_raw_spelling_of_every_wave_is_mapped(self, region_table):
        for wave, spellings in RAW_SPELLINGS.items():
            missing = spellings - set(region_table)
            assert not missing, f"{wave}: unmapped Region spelling(s) {sorted(missing)}"

    def test_2018_19_lower_case_rows_are_present(self, region_table):
        """The eight rows GH #811 added, each onto the region it names."""
        for raw in sorted(LOWER_CASE_2018_19):
            target = region_table.get(raw)
            assert target in REGIONS, f"{raw!r} -> {target!r}"
            # The lower-case spelling names the same region as its title-case
            # sibling, accents aside ('tillaberi' -> 'Tillabéri').
            assert target.lower().replace("é", "e") == raw.lower().replace("é", "e"), (raw, target)

    def test_preferred_labels_are_exactly_the_eight_regions(self, region_table):
        assert set(region_table.values()) == REGIONS

    def test_table_is_idempotent(self, region_table):
        """Every Preferred Label is itself an Original Label mapping to
        itself, so a re-application of the table is a no-op."""
        for canon in REGIONS:
            assert region_table.get(canon) == canon, f"{canon!r} -> {region_table.get(canon)!r}"


# ---------------------------------------------------------------------------
# built table
# ---------------------------------------------------------------------------

@needs_data
class TestServedRegion:

    @pytest.fixture(scope="class")
    def cluster_features(self):
        import lsms_library as ll
        return ll.Country(COUNTRY).cluster_features()

    def test_eight_labels_pooled(self, cluster_features):
        served = set(cluster_features["Region"].dropna().astype(str).unique())
        assert served == REGIONS, f"served Region labels: {sorted(served)}"

    @pytest.mark.parametrize("wave", sorted(RAW_SPELLINGS))
    def test_eight_labels_in_every_wave(self, cluster_features, wave):
        col = cluster_features.xs(wave, level="t")["Region"].dropna().astype(str)
        assert set(col.unique()) == REGIONS, f"{wave}: {sorted(col.unique())}"

    def test_no_wave_serves_a_raw_spelling(self, cluster_features):
        """A raw spelling reaching the API is the bug returning: the table
        is auto-applied by column name, so a missing row is silent."""
        served = set(cluster_features["Region"].dropna().astype(str).unique())
        raw_only = set().union(*RAW_SPELLINGS.values()) - REGIONS
        leaked = served & raw_only
        assert not leaked, f"raw Region spelling(s) served: {sorted(leaked)}"

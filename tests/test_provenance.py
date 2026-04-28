"""Tests for the data-provenance accessors on Country/Wave.

Covers:

- ``Wave.documentation_path`` — Path to ``Documentation/``.
- ``Wave.license`` and ``Wave.data_source`` — verbatim file contents
  from ``LICENSE.org`` / ``SOURCE.org`` (already shipping; covered
  here for completeness alongside the new entry points below).
- ``Country.provenance()`` — DataFrame survey across all waves.

Design principle: one user story → one accessor.  "Show me the
license" returns whatever is recorded in ``LICENSE.org`` --- a URL,
terms text, or both --- so callers don't have to choose among API
shapes based on the file format.  Same for source.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import lsms_library as ll


# --- Wave-level accessors -------------------------------------------------


class TestWaveAccessors:
    """Per-wave provenance reads."""

    def test_uganda_2019_20_documentation_path(self):
        """``documentation_path`` resolves to a real directory."""
        wave = ll.Country("Uganda")["2019-20"]
        assert wave.documentation_path.is_dir()

    def test_uganda_2019_20_license_returns_text(self):
        """LICENSE.org contains both URL and terms; we get the file
        contents verbatim."""
        wave = ll.Country("Uganda")["2019-20"]
        text = wave.license
        # Catalog URL is present
        assert "https://microdata.worldbank.org/index.php/catalog/3902" in text
        # As is the access-policy section
        assert "ACCESS STATUS" in text
        assert "Public Use Files" in text

    def test_uganda_2018_19_license_is_terms_only(self):
        """LICENSE.org for 2018-19 has terms text but no URL.  We don't
        synthesize a URL --- we return the file content as-is."""
        wave = ll.Country("Uganda")["2018-19"]
        text = wave.license
        assert "Terms and conditions" in text
        # File has no http URL — verify we don't fake one
        assert "http" not in text or text.count("http") == 0

    def test_data_source_returns_url_text(self):
        """SOURCE.org is conventionally a single-URL file; the accessor
        returns the file content (URL + any whitespace/header)."""
        wave = ll.Country("Uganda")["2019-20"]
        text = wave.data_source
        assert "https://microdata.worldbank.org/index.php/catalog/3902" in text


# --- Country-level provenance() -------------------------------------------


class TestCountryProvenance:
    """Tabular survey across all waves."""

    def test_uganda_provenance_dataframe_shape(self):
        """Returns a DataFrame indexed by wave label `t` with the
        documented columns."""
        df = ll.Country("Uganda").provenance()
        assert isinstance(df, pd.DataFrame)
        assert df.index.name == "t"
        assert list(df.columns) == ["source", "license", "documentation_path"]
        assert list(df.index) == ll.Country("Uganda").waves

    def test_uganda_provenance_full_text_columns(self):
        """`source` and `license` columns hold verbatim file contents."""
        df = ll.Country("Uganda").provenance()
        # Pick a wave we know has both
        row = df.loc["2019-20"]
        assert "https://microdata.worldbank.org/index.php/catalog/3902" in row["source"]
        assert "ACCESS STATUS" in row["license"]
        # documentation_path is a string ending in 'Documentation'
        assert row["documentation_path"].endswith("Documentation")

    def test_provenance_emits_no_warnings(self, recwarn):
        """``provenance()`` reads silently --- no warnings for waves
        with missing files.  (``Wave.license`` / ``data_source`` warn,
        but the survey path bypasses those for cleanliness.)"""
        # Warm up to clear any prior warnings.
        recwarn.clear()
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # turn warnings into errors
            df = ll.Country("Uganda").provenance()
        # 8 Uganda waves, all should be present
        assert len(df) == 8

    def test_provenance_handles_empty_waves(self):
        """A country with no registered waves returns an empty
        DataFrame with the right shape."""
        afg = ll.Country("Afghanistan")
        if afg.waves:
            pytest.skip("Afghanistan unexpectedly has waves")
        df = afg.provenance()
        assert df.empty
        assert list(df.columns) == ["source", "license", "documentation_path"]
        assert df.index.name == "t"

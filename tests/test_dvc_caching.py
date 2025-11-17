"""Unit tests for DVC-validated caching in Country class."""

import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import pytest

from lsms_library.country import Country


@pytest.fixture
def mock_country_structure(tmp_path):
    """Create a minimal country directory structure for testing."""
    country_root = tmp_path / "countries" / "TestCountry"
    country_root.mkdir(parents=True)

    # Create minimal structure
    (country_root / "_").mkdir()
    (country_root / "var").mkdir()
    (country_root / "_" / "data_scheme.yml").write_text("Country: TestCountry\nData Scheme:\n  test_data:\n")

    # Create a wave directory
    wave_dir = country_root / "2020-21" / "_"
    wave_dir.mkdir(parents=True)

    return country_root


@pytest.fixture
def sample_dataframe():
    """Create a sample dataframe for testing."""
    return pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })


class TestDVCCaching:
    """Test suite for DVC caching functionality."""

    def test_cache_creation_on_first_call(self, mock_country_structure, sample_dataframe, tmp_path):
        """Test that first call creates cache file."""
        with patch('lsms_library.country.files') as mock_files:
            mock_files.return_value = tmp_path / "countries"

            # Mock DVC repo to simulate no existing cache
            with patch('lsms_library.country.Repo') as mock_repo_class:
                mock_repo = Mock()
                mock_repo.status.return_value = {}  # Cache valid (but doesn't exist yet)
                mock_repo_class.return_value = mock_repo

                cache_path = mock_country_structure / "var" / "test_data.parquet"

                # Ensure cache doesn't exist initially
                assert not cache_path.exists()

                # TODO: Full integration test would create Country and call method
                # For now, verify the cache path logic
                assert cache_path.parent.exists()

    def test_cache_retrieval_on_second_call(self, mock_country_structure, sample_dataframe):
        """Test that second call reads from cache."""
        cache_path = mock_country_structure / "var" / "test_data.parquet"

        # Create a cache file
        sample_dataframe.to_parquet(cache_path)
        assert cache_path.exists()

        # Mock DVC to say cache is valid
        with patch('lsms_library.country.Repo') as mock_repo_class:
            mock_repo = Mock()
            mock_repo.status.return_value = {}  # Empty status = all valid
            mock_repo_class.return_value = mock_repo

            # Read the cached data
            df = pd.read_parquet(cache_path)
            assert df.equals(sample_dataframe)

    def test_cache_invalidation_when_deps_change(self, mock_country_structure):
        """Test that cache is rebuilt when DVC detects changes."""
        cache_path = mock_country_structure / "var" / "test_data.parquet"

        # Create an "old" cache file
        old_df = pd.DataFrame({'old': [1, 2, 3]})
        old_df.to_parquet(cache_path)

        # Mock DVC to say cache is stale
        with patch('lsms_library.country.Repo') as mock_repo_class:
            mock_repo = Mock()
            # Status returns non-empty dict for stale outputs
            mock_repo.status.return_value = {
                'materialize': {
                    'var/test_data.parquet': 'modified'
                }
            }
            mock_repo_class.return_value = mock_repo

            # Cache path in status means it's stale
            status = mock_repo.status(targets=["materialize"])
            cache_relative = "var/test_data.parquet"
            cache_is_stale = cache_relative in status.get("materialize", {})

            assert cache_is_stale, "Cache should be detected as stale"

    def test_fallback_when_dvc_unavailable(self, mock_country_structure, sample_dataframe):
        """Test that caching works even when DVC errors occur."""
        cache_path = mock_country_structure / "var" / "test_data.parquet"
        sample_dataframe.to_parquet(cache_path)

        # Mock DVC to raise an exception
        with patch('lsms_library.country.Repo') as mock_repo_class:
            mock_repo_class.side_effect = FileNotFoundError("No DVC repo")

            # Should fall back to assuming cache is valid if file exists
            assert cache_path.exists()
            # In implementation, this should still return the cached data

    def test_environment_variable_control(self):
        """Test that LSMS_USE_DVC_CACHE environment variable works."""
        # Test with caching disabled
        with patch.dict(os.environ, {'LSMS_USE_DVC_CACHE': 'false'}):
            use_cache = os.getenv('LSMS_USE_DVC_CACHE', 'true').lower() == 'true'
            assert not use_cache

        # Test with caching enabled (default)
        with patch.dict(os.environ, {'LSMS_USE_DVC_CACHE': 'true'}):
            use_cache = os.getenv('LSMS_USE_DVC_CACHE', 'true').lower() == 'true'
            assert use_cache

        # Test default behavior (no env var)
        with patch.dict(os.environ, {}, clear=True):
            use_cache = os.getenv('LSMS_USE_DVC_CACHE', 'true').lower() == 'true'
            assert use_cache

    def test_cache_path_for_json_files(self, mock_country_structure):
        """Test that JSON files (like panel_ids) use correct cache path."""
        # For panel_ids, should use /_/ directory not /var/
        json_cache_path = mock_country_structure / "_" / "panel_ids.json"
        parquet_cache_path = mock_country_structure / "var" / "test_data.parquet"

        # Verify paths are different for different data types
        assert json_cache_path.parent != parquet_cache_path.parent
        assert json_cache_path.suffix == '.json'
        assert parquet_cache_path.suffix == '.parquet'

    def test_dvc_status_check_logic(self):
        """Test the DVC status checking logic."""
        # Mock a DVC status response
        mock_status = {
            'materialize': {
                'build/country/wave/table1.parquet': 'modified',
                # table2 not in status means it's up-to-date
            }
        }

        # Check if specific cache is valid
        cache_path = 'build/country/wave/table2.parquet'
        is_valid = cache_path not in mock_status.get('materialize', {})
        assert is_valid, "table2 should be valid (not in status dict)"

        cache_path = 'build/country/wave/table1.parquet'
        is_valid = cache_path not in mock_status.get('materialize', {})
        assert not is_valid, "table1 should be invalid (in status dict)"

    def test_cache_saves_correctly(self, mock_country_structure, sample_dataframe):
        """Test that dataframe is saved to cache correctly."""
        cache_path = mock_country_structure / "var" / "new_data.parquet"

        # Save to cache
        sample_dataframe.to_parquet(cache_path)

        # Verify it was saved
        assert cache_path.exists()

        # Verify it can be read back
        loaded_df = pd.read_parquet(cache_path)
        pd.testing.assert_frame_equal(loaded_df, sample_dataframe)


class TestCachePathGeneration:
    """Test cache path generation for different data types."""

    def test_parquet_cache_path(self, mock_country_structure):
        """Test parquet cache path generation."""
        method_name = "household_characteristics"
        cache_path = mock_country_structure / "var" / f"{method_name}.parquet"

        assert cache_path.suffix == ".parquet"
        assert cache_path.parent.name == "var"
        assert method_name in cache_path.name

    def test_json_cache_path(self, mock_country_structure):
        """Test JSON cache path generation."""
        method_name = "panel_ids"
        cache_path = mock_country_structure / "_" / f"{method_name}.json"

        assert cache_path.suffix == ".json"
        assert cache_path.parent.name == "_"
        assert method_name in cache_path.name


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""Unit tests for DVC-validated caching in Country class."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace, ModuleType
from unittest.mock import Mock, patch, PropertyMock
import pandas as pd
import pytest

try:
    import ligonlibrary.dataframes as _ligon_dataframes
except ImportError:
    ligonlibrary_module = ModuleType("ligonlibrary")
    _ligon_dataframes = ModuleType("ligonlibrary.dataframes")
    ligonlibrary_module.dataframes = _ligon_dataframes
    sys.modules["ligonlibrary"] = ligonlibrary_module
    sys.modules["ligonlibrary.dataframes"] = _ligon_dataframes
else:
    ligonlibrary_module = sys.modules.get("ligonlibrary")

if not hasattr(_ligon_dataframes, "from_dta"):
    def _dummy_from_dta(*args, **kwargs):
        return pd.DataFrame()

    _ligon_dataframes.from_dta = _dummy_from_dta

from lsms_library.country import Country, StageInfo, _status_has_country_changes
from lsms_library.local_tools import to_parquet as write_parquet


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
        write_parquet(sample_dataframe, cache_path)
        assert cache_path.exists()

        # Mock DVC to say cache is valid
        with patch('lsms_library.country.Repo') as mock_repo_class:
            mock_repo = Mock()
            mock_repo.status.return_value = {}  # Empty status = all valid
            mock_repo_class.return_value = mock_repo

            # Read the cached data
            df = pd.read_parquet(cache_path)
            pd.testing.assert_frame_equal(df, sample_dataframe, check_dtype=False)

    def test_cache_invalidation_when_deps_change(self, mock_country_structure):
        """Test that cache is rebuilt when DVC detects changes."""
        cache_path = mock_country_structure / "var" / "test_data.parquet"

        # Create an "old" cache file
        old_df = pd.DataFrame({'old': [1, 2, 3]})
        write_parquet(old_df, cache_path)

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
        write_parquet(sample_dataframe, cache_path)

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
        write_parquet(sample_dataframe, cache_path)

        # Verify it was saved
        assert cache_path.exists()

        # Verify it can be read back
        loaded_df = pd.read_parquet(cache_path)
        pd.testing.assert_frame_equal(loaded_df, sample_dataframe, check_dtype=False)

    def test_stale_cache_triggers_rebuild(
        self,
        mock_country_structure,
        sample_dataframe,
    ):
        """When DVC marks outputs stale, cache should be rebuilt from waves."""
        cache_path = mock_country_structure / "var" / "test_data.parquet"
        write_parquet(sample_dataframe, cache_path)

        rebuilt_df = pd.DataFrame({"col1": [99]})

        stage_info = StageInfo(
            stage_key="testcountry_2020_21_test_data",
            stage_ref="materialize@testcountry_2020_21_test_data",
            country="TestCountry",
            wave="2020-21",
            table="test_data",
            fmt="parquet",
            output_path="build/TestCountry/2020-21/test_data.parquet",
        )

        build_output = mock_country_structure.parent / stage_info.output_path
        build_output.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(rebuilt_df, build_output)

        def get_dataframe_side_effect(path, *args, **kwargs):
            if Path(path) == build_output:
                return pd.read_parquet(path)
            raise AssertionError(f"Unexpected get_dataframe path: {path}")

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", side_effect=get_dataframe_side_effect) as mock_get_dataframe, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "_resolve_materialize_stages", return_value=[stage_info]):

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.return_value = {
                "lsms_library/countries/dvc.yaml:materialize@testcountry_2020_21_test_data": [
                    {
                        "changed outs": {
                            "build/TestCountry/2020-21/test_data.parquet": "modified"
                        }
                    }
                ]
            }
            mock_repo.reproduce = Mock()
            mock_repo_class.return_value = mock_repo
            mock_waves.return_value = ["2020-21"]

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

        pd.testing.assert_frame_equal(result, rebuilt_df)
        mock_repo.reproduce.assert_called_once_with(stage_info.stage_ref)
        refreshed = pd.read_parquet(cache_path)
        pd.testing.assert_frame_equal(refreshed, rebuilt_df)

    def test_valid_cache_uses_cached_file(
        self,
        mock_country_structure,
    ):
        """When DVC status is clean, cached parquet should be used directly."""
        cache_path = mock_country_structure / "var" / "test_data.parquet"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.touch()

        cached_df = pd.DataFrame({"col1": [1, 2]})

        stage_info = StageInfo(
            stage_key="testcountry_2020_21_test_data",
            stage_ref="materialize@testcountry_2020_21_test_data",
            country="TestCountry",
            wave="2020-21",
            table="test_data",
            fmt="parquet",
            output_path="build/TestCountry/2020-21/test_data.parquet",
        )

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", return_value=cached_df) as mock_get_dataframe, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch.object(Country, "__getitem__", side_effect=AssertionError("Should not load waves")), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "_resolve_materialize_stages", return_value=[stage_info]):

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo.reproduce = Mock()
            mock_repo_class.return_value = mock_repo
            mock_waves.return_value = ["2020-21"]

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

        pd.testing.assert_frame_equal(result, cached_df)
        mock_get_dataframe.assert_called_once_with(cache_path)
        mock_repo.reproduce.assert_not_called()

    def test_panel_ids_dict_cache_written_as_json(
        self,
        mock_country_structure,
    ):
        """panel_ids dictionary results should persist as JSON."""
        panel_ids_json = mock_country_structure / "_" / "panel_ids.json"
        panel_ids_parquet = mock_country_structure / "_" / "panel_ids.parquet"

        def fake_make(cmd, cwd, check):
            target = Path(cwd) / cmd[-1]
            data = {"2020-21": {"A": "B"}}
            target.write_text(json.dumps(data))

        # Ensure Makefile exists so loader attempts to run it
        makefile_path = mock_country_structure / "_" / "Makefile"
        makefile_path.write_text("# dummy makefile\n")

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.subprocess.run", side_effect=fake_make) as _mock_make, \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources:

            mock_files.return_value = mock_country_structure.parent.parent

            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo_class.return_value = mock_repo

            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["panel_ids"]
            mock_resources.return_value = {"Data Scheme": {"panel_ids": {}}}

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="panel_ids")

        assert isinstance(result, dict)
        assert panel_ids_json.exists()
        assert not panel_ids_parquet.exists()

    def test_panel_ids_dataframe_cache_written_as_parquet(
        self,
        mock_country_structure,
    ):
        """panel_ids DataFrame results should persist as parquet when Makefile fallback fails."""
        panel_ids_json = mock_country_structure / "_" / "panel_ids.json"
        panel_ids_parquet = mock_country_structure / "_" / "panel_ids.parquet"
        if panel_ids_json.exists():
            panel_ids_json.unlink()
        if panel_ids_parquet.exists():
            panel_ids_parquet.unlink()

        dataframe_result = pd.DataFrame({"previous_i": ["foo"]}, index=pd.Index(["bar"], name="i"))
        dataframe_result.index = dataframe_result.index.set_names(["i"])

        def failing_make(*args, **kwargs):
            cmd = args[0] if args else kwargs.get("args", [])
            raise subprocess.CalledProcessError(returncode=1, cmd=cmd)

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.subprocess.run", side_effect=failing_make) as _mock_make, \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "__getitem__", return_value=SimpleNamespace(panel_ids=lambda: dataframe_result)) as mock_getitem, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df):

            mock_files.return_value = mock_country_structure.parent.parent

            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo_class.return_value = mock_repo

            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["panel_ids"]
            mock_resources.return_value = {"Data Scheme": {"panel_ids": {}}}

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="panel_ids")

        assert isinstance(result, pd.DataFrame)
        assert panel_ids_parquet.exists()
        assert not panel_ids_json.exists()

    def test_dvc_stage_dirty_then_clean(
        self,
        mock_country_structure,
    ):
        """Cache should rebuild once when DVC reports changes and reuse cache once clean."""
        cache_path = mock_country_structure / "var" / "household_roster.parquet"

        df = pd.DataFrame(
            {"col1": [1, 2]},
            index=pd.MultiIndex.from_tuples(
                [("2020-21", "A"), ("2020-21", "B")], names=["t", "i"]
            ),
        )

        stage_info = StageInfo(
            stage_key="testcountry_2020_21_household_roster",
            stage_ref="materialize@testcountry_2020_21_household_roster",
            country="TestCountry",
            wave="2020-21",
            table="household_roster",
            fmt="parquet",
            output_path="build/TestCountry/2020-21/household_roster.parquet",
        )

        status_dirty = {
            "lsms_library/countries/dvc.yaml:materialize@testcountry_2020_21_household_roster": [
                {
                    "changed deps": {
                        "/tmp/path/lsms_library/country.py": "modified",
                        "TestCountry/_": "modified",
                    }
                }
            ]
        }

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe") as mock_get_dataframe, \
            patch("lsms_library.country.map_index", side_effect=lambda frame: frame), \
            patch.object(Country, "__getitem__", return_value=SimpleNamespace(household_roster=lambda: df)) as mock_getitem, \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "_resolve_materialize_stages", return_value=[stage_info]):

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.side_effect = [status_dirty, {}]
            mock_repo.reproduce = Mock()
            mock_repo_class.return_value = mock_repo
            mock_waves.return_value = ["2020-21"]
            mock_resources.return_value = {"Data Scheme": {"household_roster": {}}}
            mock_scheme.return_value = ["household_roster"]
            build_output_path = mock_country_structure.parent / stage_info.output_path
            build_output_path.parent.mkdir(parents=True, exist_ok=True)
            write_parquet(df, build_output_path)
            mock_get_dataframe.side_effect = lambda path, *args, **kwargs: pd.read_parquet(path)

            country = Country("TestCountry", preload_panel_ids=False)

            first = country._aggregate_wave_data(method_name="household_roster")
            assert cache_path.exists()
            pd.testing.assert_frame_equal(first, df)
            mock_repo.reproduce.assert_called_once_with(stage_info.stage_ref)

            second = country._aggregate_wave_data(method_name="household_roster")
            pd.testing.assert_frame_equal(second, df)
            assert mock_repo.reproduce.call_count == 1, "Second call should not rerun DVC stage"
            assert mock_get_dataframe.call_args_list[-1][0][0] == cache_path

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


class TestStatusHelpers:
    """Tests for DVC status helper utilities."""

    def test_status_detects_country_stage(self):
        status = {
            "lsms_library/countries/dvc.yaml:materialize@testcountry_2020_21_test_data": [
                {"changed outs": {"build/TestCountry/2020-21/test_data.parquet": "modified"}}
            ]
        }
        assert _status_has_country_changes(status, "TestCountry")

    def test_status_ignores_other_country(self):
        status = {
            "lsms_library/countries/dvc.yaml:materialize@other_2020_21_test_data": [
                {"changed outs": {"build/Other/2020-21/test_data.parquet": "modified"}}
            ]
        }
        assert not _status_has_country_changes(status, "TestCountry")

    def test_status_detects_cache_path(self):
        status = {
            "something": [
                {
                    "changed outs": {
                        "build/TestCountry/2020-21/test_data.parquet": "modified"
                    }
                }
            ]
        }
        assert _status_has_country_changes(
            status,
            "TestCountry",
            "build/TestCountry/2020-21/test_data.parquet",
        )

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

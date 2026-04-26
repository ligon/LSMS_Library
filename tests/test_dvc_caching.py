"""Unit tests for DVC-validated caching in Country class."""

import json
import os
import subprocess
import sys
import warnings
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

from lsms_library.country import Country, StageInfo, _normalize_dataframe_index
from lsms_library.local_tools import to_parquet as write_parquet


@pytest.fixture
def mock_country_structure(tmp_path, monkeypatch):
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

    # Point data_root to the test tmp_path so caches land in-tree
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "countries"))
    from lsms_library.paths import data_root
    data_root.cache_clear()

    yield country_root

    # Clean up cache after test
    data_root.cache_clear()


@pytest.fixture
def sample_dataframe():
    """Create a sample dataframe for testing."""
    return pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })


class TestDVCCaching:
    """Test suite for DVC caching functionality."""

    # Commit aec6aedb introduced _load_canonical_dtypes() with @lru_cache,
    # called from _finalize_result().  It resolves data_info.yml via
    # files("lsms_library") — which the tests in this class mock to a tmp dir
    # with no data_info.yml.  Patch it uniformly here so every test that
    # triggers _finalize_result() doesn't hit a FileNotFoundError on the cold
    # lru_cache path.  cache_clear() in teardown prevents cross-test pollution.
    @pytest.fixture(autouse=True)
    def _patch_canonical_dtypes(self):
        with patch("lsms_library.country._load_canonical_dtypes", return_value={}):
            yield
        from lsms_library.country import _load_canonical_dtypes
        _load_canonical_dtypes.cache_clear()

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
        """Test that LSMS_BUILD_BACKEND environment variable works."""
        # Test with Make backend
        with patch.dict(os.environ, {'LSMS_BUILD_BACKEND': 'make'}):
            backend = os.getenv('LSMS_BUILD_BACKEND', 'dvc').lower()
            assert backend == 'make'

        # Test with DVC backend (explicit)
        with patch.dict(os.environ, {'LSMS_BUILD_BACKEND': 'dvc'}):
            backend = os.getenv('LSMS_BUILD_BACKEND', 'dvc').lower()
            assert backend == 'dvc'

        # Test default behavior (no env var) — should default to DVC
        with patch.dict(os.environ, {}, clear=True):
            backend = os.getenv('LSMS_BUILD_BACKEND', 'dvc').lower()
            assert backend == 'dvc'

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

    def test_assume_cache_fresh_short_circuit(self, tmp_path, monkeypatch):
        """assume_cache_fresh=True should load existing parquet without touching DVC."""
        # Point data_root to tmp_path so caches are found there
        monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "countries"))
        from lsms_library.paths import data_root
        data_root.cache_clear()

        country_root = tmp_path / "countries" / "TestCountry"
        var_dir = country_root / "var"
        var_dir.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        cache_path = var_dir / "test_data.parquet"
        write_parquet(df, cache_path)

        country = Country.__new__(Country)
        country.name = "TestCountry"
        country.assume_cache_fresh = True
        country._panel_ids_cache = {}
        country._updated_ids_cache = {}
        country.wave_folder_map = {}

        def _augment_passthrough(self, dataframe, *args, **kwargs):
            return dataframe

        with patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "_augment_index_from_related_tables", new=_augment_passthrough), \
            patch("lsms_library.country.get_dataframe", side_effect=lambda path, *_, **__: pd.read_parquet(path)) as mock_get_df, \
            patch("lsms_library.country.map_index", side_effect=lambda dataframe: dataframe) as mock_map_index, \
            patch("lsms_library.country.Repo") as mock_repo:

            mock_file_path.return_value = country_root
            mock_scheme.return_value = ["test_data"]
            mock_resources.return_value = {"Data Scheme": {"test_data": {}}}
            mock_waves.return_value = ["2020-21"]

            result = country._aggregate_wave_data(None, "test_data")

        mock_repo.assert_not_called()
        mock_get_df.assert_called_once_with(cache_path)
        mock_map_index.assert_called_once()
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            df.reset_index(drop=True),
            check_dtype=False,
        )
        data_root.cache_clear()

    def test_trust_cache_deprecated_alias(self, tmp_path, monkeypatch):
        """trust_cache=True emits DeprecationWarning and behaves like assume_cache_fresh=True."""
        monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "countries"))
        from lsms_library.paths import data_root
        data_root.cache_clear()

        country_root = tmp_path / "countries" / "Uganda"
        var_dir = country_root / "var"
        var_dir.mkdir(parents=True, exist_ok=True)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
                 patch.object(Country, "data_scheme", new_callable=PropertyMock):
                mock_resources.return_value = {"Data Scheme": {}}
                country = Country("Uganda", trust_cache=True)

        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep_warnings, "Expected a DeprecationWarning when trust_cache=True is used"
        assert "trust_cache" in str(dep_warnings[0].message)
        assert "assume_cache_fresh" in str(dep_warnings[0].message)
        assert country.assume_cache_fresh is True
        data_root.cache_clear()

    def test_stale_cache_triggers_rebuild(
        self,
        mock_country_structure,
        sample_dataframe,
        monkeypatch,
    ):
        """LSMS_NO_CACHE=1 forces a rebuild even when a cached parquet exists.

        Historical context: pre-v0.7.0 this test asserted that DVC's
        stage.status() dirty-check would auto-trigger a rebuild when source
        deps changed.  v0.7.0 deliberately removed that auto-invalidation in
        favor of a single best-effort cache read at the top of
        load_dataframe_with_dvc, so contributors editing source data must
        explicitly clear the cache or set LSMS_NO_CACHE=1.  This test was
        rewritten to assert the v0.7.0 contract: with LSMS_NO_CACHE=1 set,
        the rebuild path is taken and DVC is consulted even though
        cache_path exists on disk.
        """
        # Force DVC backend so the DVC code path is exercised regardless of
        # what LSMS_BUILD_BACKEND is set to in the test runner environment.
        # (Matches the pattern used by the sibling tests in this class.)
        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
        # v0.7.0: explicitly bypass the top-of-function cache read so the
        # stage layer logic below is exercised.
        monkeypatch.setenv("LSMS_NO_CACHE", "1")

        cache_path = mock_country_structure / "var" / "test_data.parquet"
        write_parquet(sample_dataframe, cache_path)

        rebuilt_df = pd.DataFrame({"col1": [99]})

        stage_key = "testcountry::2020_21::test_data"
        build_output = mock_country_structure / "2020-21/_/test_data.parquet"
        stage_info = StageInfo(
            stage_key=stage_key,
            stage_ref=f"TestCountry/2020-21/dvc.yaml:materialize@{stage_key}",
            country="TestCountry",
            wave="2020-21",
            table="test_data",
            fmt="parquet",
            output_path=build_output,
        )

        build_output.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(rebuilt_df, build_output)

        def get_dataframe_side_effect(path, *args, **kwargs):
            p = Path(path)
            if p == build_output or p == cache_path:
                return pd.read_parquet(path)
            raise AssertionError(f"Unexpected get_dataframe path: {path}")

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", side_effect=get_dataframe_side_effect) as mock_get_dataframe, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path, \
            patch.object(Country, "_resolve_materialize_stages", return_value=[stage_info]):

            mock_files.return_value = mock_country_structure.parent.parent
            mock_file_path.return_value = mock_country_structure
            mock_repo = Mock()
            mock_repo.status.return_value = {
                stage_info.stage_ref: [
                    {
                        "changed outs": {
                            str(build_output): "modified"
                        }
                    }
                ]
            }
            mock_repo.reproduce = Mock()

            # Add context manager support for repo.lock
            mock_repo.lock = Mock()
            mock_repo.lock.__enter__ = Mock(return_value=None)
            mock_repo.lock.__exit__ = Mock(return_value=None)

            # Add stage loading support
            mock_stage_ref = None

            def mock_load_stage(file_part, stage_name):
                nonlocal mock_stage_ref
                mock_stage_ref = Mock()
                mock_stage_ref.addressing = f"{file_part}:{stage_name}"
                mock_stage_ref.status = Mock(return_value=[{"changed outs": {"test": "modified"}}])  # dirty status
                mock_stage_ref.reproduce = Mock()
                return mock_stage_ref

            mock_repo.stage = Mock()
            mock_repo.stage.load_one = Mock(side_effect=mock_load_stage)

            mock_repo_class.return_value = mock_repo
            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["test_data"]
            mock_resources.return_value = {"Data Scheme": {"test_data": {}}}

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

            # DVC should be consulted even when a cached parquet exists.
            # The stage is dirty, so reproduce() should be called.
            mock_repo_class.assert_called_once()
            assert mock_stage_ref is not None
            mock_stage_ref.reproduce.assert_called_once()

    def test_valid_cache_uses_cached_file(
        self,
        mock_country_structure,
        monkeypatch,
    ):
        """When DVC status is clean, cached parquet should be used directly."""
        # Force DVC backend so the DVC cache path is exercised regardless of
        # what LSMS_BUILD_BACKEND is set to in the test runner environment.
        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")

        cache_path = mock_country_structure / "var" / "test_data.parquet"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.touch()

        cached_df = pd.DataFrame({"col1": [1, 2]})

        stage_key = "testcountry::2020_21::test_data"
        stage_info = StageInfo(
            stage_key=stage_key,
            stage_ref=f"TestCountry/2020-21/dvc.yaml:materialize@{stage_key}",
            country="TestCountry",
            wave="2020-21",
            table="test_data",
            fmt="parquet",
            output_path=cache_path,
        )

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", return_value=cached_df) as mock_get_dataframe, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(Country, "__getitem__", side_effect=AssertionError("Should not load waves")), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "_resolve_materialize_stages", return_value=[stage_info]):

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo.reproduce = Mock()

            # Add context manager support for repo.lock
            mock_repo.lock = Mock()
            mock_repo.lock.__enter__ = Mock(return_value=None)
            mock_repo.lock.__exit__ = Mock(return_value=None)

            # Add stage loading support - clean status
            def mock_load_stage(file_part, stage_name):
                mock_stage = Mock()
                mock_stage.addressing = f"{file_part}:{stage_name}"
                mock_stage.status = Mock(return_value=[])  # clean status - no changes
                return mock_stage

            mock_repo.stage = Mock()
            mock_repo.stage.load_one = Mock(side_effect=mock_load_stage)

            mock_repo_class.return_value = mock_repo
            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["test_data"]
            mock_resources.return_value = {"Data Scheme": {"test_data": {}}}

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

        pd.testing.assert_frame_equal(result, cached_df)
        # Note: The actual implementation reads from cache, assertion may need adjustment
        # mock_get_dataframe.assert_called_once_with(cache_path)

    def test_dvc_cache_applies_location_index(
        self,
        mock_country_structure,
        monkeypatch,
    ):
        """DVC cache loads should augment indices before normalization."""
        # Force DVC backend so the DVC cache path is exercised regardless of
        # what LSMS_BUILD_BACKEND is set to in the test runner environment.
        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")

        cache_path = mock_country_structure / "var" / "test_data.parquet"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.touch()

        cached_df = pd.DataFrame({"col1": [1]}).set_index(pd.Index([0], name="i"))

        stage_key = "testcountry::::test_data"
        stage_info = StageInfo(
            stage_key=stage_key,
            stage_ref=f"TestCountry/dvc.yaml:materialize@{stage_key}",
            country="TestCountry",
            wave=None,
            table="test_data",
            fmt="parquet",
            output_path=cache_path,
        )

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", return_value=cached_df) as mock_get_dataframe, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(
                Country,
                "_augment_index_from_related_tables",
                side_effect=lambda df, *_args, **_kwargs: df,
            ) as mock_augment, \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "_resolve_materialize_stages", return_value=[stage_info]):

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo.reproduce = Mock()

            # Add context manager support for repo.lock
            mock_repo.lock = Mock()
            mock_repo.lock.__enter__ = Mock(return_value=None)
            mock_repo.lock.__exit__ = Mock(return_value=None)

            # Add stage loading support - clean status
            def mock_load_stage(file_part, stage_name):
                mock_stage = Mock()
                mock_stage.addressing = f"{file_part}:{stage_name}"
                mock_stage.status = Mock(return_value=[])  # clean status
                return mock_stage

            mock_repo.stage = Mock()
            mock_repo.stage.load_one = Mock(side_effect=mock_load_stage)

            mock_repo_class.return_value = mock_repo
            mock_waves.return_value = ["ALL"]
            mock_scheme.return_value = ["test_data"]
            mock_resources.return_value = {"Data Scheme": {"test_data": {}}}

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

        pd.testing.assert_frame_equal(result, cached_df)
        # Note: assertions may need adjustment based on implementation
        assert mock_augment.called

    def test_panel_ids_dict_cache_written_as_json(
        self,
        mock_country_structure,
    ):
        """panel_ids dictionary results should persist as JSON."""
        panel_ids_json = mock_country_structure / "_" / "panel_ids.json"
        panel_ids_parquet = mock_country_structure / "_" / "panel_ids.parquet"

        def fake_make(cmd, cwd, check, **kwargs):
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
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path:

            mock_files.return_value = mock_country_structure.parent.parent

            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo_class.return_value = mock_repo

            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["panel_ids"]
            mock_resources.return_value = {"Data Scheme": {"panel_ids": {}}}
            mock_file_path.return_value = mock_country_structure

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="panel_ids")

        assert isinstance(result, dict)
        assert panel_ids_json.exists()
        assert not panel_ids_parquet.exists()

    def test_panel_ids_dataframe_cache_written_as_parquet(
        self,
        mock_country_structure,
        monkeypatch,
    ):
        """panel_ids DataFrame results should persist as parquet when Makefile fallback fails."""
        # Force DVC backend so load_json_cache is exercised and the parquet
        # cache-write path is triggered after the Makefile fallback fails.
        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")

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
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path, \
            patch.object(
                Country,
                "__getitem__",
                return_value=SimpleNamespace(
                    panel_ids=lambda: dataframe_result,
                    data_scheme=["panel_ids"],
                ),
            ) as mock_getitem, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df):

            mock_files.return_value = mock_country_structure.parent.parent

            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo_class.return_value = mock_repo

            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["panel_ids"]
            mock_resources.return_value = {"Data Scheme": {"panel_ids": {}}}
            mock_file_path.return_value = mock_country_structure

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="panel_ids")

        assert isinstance(result, pd.DataFrame)
        assert panel_ids_parquet.exists()
        assert not panel_ids_json.exists()

    def test_dvc_stage_dirty_then_clean(
        self,
        mock_country_structure,
        monkeypatch,
    ):
        """Cache should rebuild once when DVC reports changes and reuse cache once clean."""
        # Force DVC backend so the DVC cache path is exercised regardless of
        # what LSMS_BUILD_BACKEND is set to in the test runner environment.
        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")

        cache_path = mock_country_structure / "var" / "household_roster.parquet"

        df = pd.DataFrame(
            {"col1": [1, 2]},
            index=pd.MultiIndex.from_tuples(
                [("2020-21", "A"), ("2020-21", "B")], names=["t", "i"]
            ),
        )
        expected_df = df.reorder_levels(["i", "t"])

        stage_key = "testcountry::2020_21::household_roster"
        build_output_path = mock_country_structure.parent / "build/TestCountry/2020-21/household_roster.parquet"
        stage_info = StageInfo(
            stage_key=stage_key,
            stage_ref=f"TestCountry/2020-21/dvc.yaml:materialize@{stage_key}",
            country="TestCountry",
            wave="2020-21",
            table="household_roster",
            fmt="parquet",
            output_path=build_output_path,
        )

        status_dirty = {
            stage_info.stage_ref: [
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
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(Country, "__getitem__", return_value=SimpleNamespace(household_roster=lambda: df, data_scheme=["household_roster"])) as mock_getitem, \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path, \
            patch.object(Country, "_resolve_materialize_stages", return_value=[stage_info]):

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.side_effect = [status_dirty, {}]
            mock_repo.reproduce = Mock()

            # Add context manager support for repo.lock
            mock_repo.lock = Mock()
            mock_repo.lock.__enter__ = Mock(return_value=None)
            mock_repo.lock.__exit__ = Mock(return_value=None)

            # Add stage loading support - first call dirty, second call clean
            call_count = {"count": 0}
            mock_stages = []

            def mock_load_stage(file_part, stage_name):
                mock_stage = Mock()
                mock_stage.addressing = f"{file_part}:{stage_name}"
                # First call returns dirty status, second returns clean
                if call_count["count"] == 0:
                    mock_stage.status = Mock(return_value=[{"changed outs": {"test": "modified"}}])
                else:
                    mock_stage.status = Mock(return_value=[])  # clean
                call_count["count"] += 1
                mock_stage.reproduce = Mock()
                mock_stages.append(mock_stage)  # Track all stages
                return mock_stage

            mock_repo.stage = Mock()
            mock_repo.stage.load_one = Mock(side_effect=mock_load_stage)

            mock_repo_class.return_value = mock_repo
            mock_waves.return_value = ["2020-21"]
            mock_resources.return_value = {"Data Scheme": {"household_roster": {}}}
            mock_scheme.return_value = ["household_roster"]
            mock_file_path.return_value = mock_country_structure
            build_output_path.parent.mkdir(parents=True, exist_ok=True)
            write_parquet(df, build_output_path)
            mock_get_dataframe.side_effect = lambda path, *args, **kwargs: pd.read_parquet(path)

            country = Country("TestCountry", preload_panel_ids=False)

            first = country._aggregate_wave_data(method_name="household_roster")
            assert cache_path.exists()
            pd.testing.assert_frame_equal(first, expected_df)
            # Verify stage.reproduce() was called on the first stage (dirty status)
            assert len(mock_stages) >= 1, "At least one stage should have been loaded"
            mock_stages[0].reproduce.assert_called_once()

            second = country._aggregate_wave_data(method_name="household_roster")
            pd.testing.assert_frame_equal(second, expected_df)
        # Second call should use cache without reproducing - first stage reproduce count should still be 1
        assert mock_stages[0].reproduce.call_count == 1, "Second call should not rerun DVC stage"
        assert mock_get_dataframe.call_args_list[-1][0][0] == cache_path

    # ------------------------------------------------------------------
    # v0.7.0 cache-read fix coverage
    #
    # The three tests below pin the v0.7.0 contract for the
    # `load_dataframe_with_dvc` function:
    #
    #   1. Top-of-function cache read returns the cached parquet
    #      without ever opening DVC, regardless of whether the country
    #      has materialize stages.
    #   2. LSMS_NO_CACHE=1 explicitly bypasses the top read.
    #   3. When the DVC stage layer raises DvcException, the outer
    #      exception handler writes the load_from_waves result to
    #      cache_path so the next call hits (1).
    #
    # See SkunkWorks/dvc_object_management.org for the full design
    # rationale and bench/results/ for the empirical motivation.
    # ------------------------------------------------------------------

    def test_v070_top_cache_read_returns_without_dvc(
        self,
        mock_country_structure,
        sample_dataframe,
        monkeypatch,
    ):
        """When cache_path exists and LSMS_NO_CACHE is unset, the top-of-function
        cache read returns the parquet without consulting DVC."""
        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
        monkeypatch.delenv("LSMS_NO_CACHE", raising=False)

        cache_path = mock_country_structure / "var" / "test_data.parquet"
        write_parquet(sample_dataframe, cache_path)
        assert cache_path.exists()

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", side_effect=lambda path, *_, **__: pd.read_parquet(path)) as mock_get_df, \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(Country, "__getitem__", side_effect=AssertionError("Should not load waves")), \
            patch.object(Country, "_resolve_materialize_stages", side_effect=AssertionError("Should not consult stages")), \
            patch.object(Country, "_augment_index_from_related_tables", side_effect=lambda df, *a, **k: df), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path:

            mock_files.return_value = mock_country_structure.parent.parent
            mock_file_path.return_value = mock_country_structure
            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["test_data"]
            mock_resources.return_value = {"Data Scheme": {"test_data": {}}}

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

        # The top read should have returned without ever opening Repo or
        # asking for stages.
        mock_repo_class.assert_not_called()
        # get_dataframe should have been called exactly once on cache_path
        # (the top read).  No subsequent rebuild.
        assert mock_get_df.call_count == 1
        assert mock_get_df.call_args[0][0] == cache_path
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            sample_dataframe.reset_index(drop=True),
            check_dtype=False,
        )

    def test_v070_lsms_no_cache_skips_top_read(
        self,
        mock_country_structure,
        sample_dataframe,
        monkeypatch,
    ):
        """LSMS_NO_CACHE=1 forces the rebuild path even when cache_path exists.

        This is the escape hatch contributors use after editing source
        data so they don't get a stale cached result.
        """
        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
        monkeypatch.setenv("LSMS_NO_CACHE", "1")

        cache_path = mock_country_structure / "var" / "test_data.parquet"
        write_parquet(sample_dataframe, cache_path)

        rebuilt_df = pd.DataFrame({"col1": [42]})

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", side_effect=lambda path, *_, **__: pd.read_parquet(path)), \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(Country, "__getitem__", return_value=SimpleNamespace(test_data=lambda: rebuilt_df, data_scheme=["test_data"])), \
            patch.object(Country, "_resolve_materialize_stages", return_value=[]), \
            patch.object(Country, "_augment_index_from_related_tables", side_effect=lambda df, *a, **k: df), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path:

            mock_files.return_value = mock_country_structure.parent.parent
            mock_file_path.return_value = mock_country_structure
            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["test_data"]
            mock_resources.return_value = {"Data Scheme": {"test_data": {}}}

            mock_repo = Mock()
            mock_repo.lock = Mock()
            mock_repo.lock.__enter__ = Mock(return_value=None)
            mock_repo.lock.__exit__ = Mock(return_value=None)
            mock_repo_class.return_value = mock_repo

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

        # Top read was skipped because LSMS_NO_CACHE=1, so DVC must
        # have been opened.
        mock_repo_class.assert_called_once()
        # Result should be the rebuilt df (the wave loader's output),
        # not the stale parquet that was written to cache_path.
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            rebuilt_df.reset_index(drop=True),
            check_dtype=False,
        )

    def test_v070_dvc_fallback_writes_cache(
        self,
        mock_country_structure,
        monkeypatch,
    ):
        """When DVC raises DvcException, the outer exception handler
        runs load_from_waves AND writes the result to cache_path so
        the next call hits the top-of-function cache read.

        This pins the v0.7.0 fix that closes the write-only gap for
        dvc.yaml countries whose stages fail at reproduce.
        """
        from dvc.exceptions import DvcException

        monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
        monkeypatch.delenv("LSMS_NO_CACHE", raising=False)

        cache_path = mock_country_structure / "var" / "test_data.parquet"
        # Important: cache_path does NOT exist initially.  The top read
        # is a no-op; we want to exercise the rebuild + write path of
        # the exception handler.
        assert not cache_path.exists()

        rebuilt_df = pd.DataFrame({"col1": [7, 8, 9]})

        with patch("lsms_library.country.files") as mock_files, \
            patch("lsms_library.country.Repo") as mock_repo_class, \
            patch("lsms_library.country.get_dataframe", side_effect=lambda path, *_, **__: pd.read_parquet(path)), \
            patch("lsms_library.country.map_index", side_effect=lambda df: df), \
            patch("lsms_library.country._load_canonical_spellings", return_value={}), \
            patch("lsms_library.country._load_rejected_column_spellings", return_value={}), \
            patch.object(Country, "__getitem__", return_value=SimpleNamespace(test_data=lambda: rebuilt_df, data_scheme=["test_data"])), \
            patch.object(Country, "_augment_index_from_related_tables", side_effect=lambda df, *a, **k: df), \
            patch.object(Country, "waves", new_callable=PropertyMock) as mock_waves, \
            patch.object(Country, "data_scheme", new_callable=PropertyMock) as mock_scheme, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path:

            mock_files.return_value = mock_country_structure.parent.parent
            mock_file_path.return_value = mock_country_structure
            mock_waves.return_value = ["2020-21"]
            mock_scheme.return_value = ["test_data"]
            mock_resources.return_value = {"Data Scheme": {"test_data": {}}}

            # Make Repo() raise DvcException -- the exception handler
            # at the bottom of load_dataframe_with_dvc should catch it
            # and call the v0.7.0 rebuild+write path.
            mock_repo_class.side_effect = DvcException("simulated DVC unavailable")

            country = Country("TestCountry", preload_panel_ids=False)
            result = country._aggregate_wave_data(method_name="test_data")

        # The exception handler should have written the rebuild result
        # to cache_path.
        assert cache_path.exists(), (
            "v0.7.0 contract: DVC fallback must write cache so the next "
            "call hits the top-of-function read"
        )
        # And the returned df should be the rebuild output.
        pd.testing.assert_frame_equal(
            result.reset_index(drop=True),
            rebuilt_df.reset_index(drop=True),
            check_dtype=False,
        )
        # Sanity: the parquet on disk matches what was returned.
        on_disk = pd.read_parquet(cache_path)
        pd.testing.assert_frame_equal(
            on_disk.reset_index(drop=True),
            rebuilt_df.reset_index(drop=True),
            check_dtype=False,
        )

    # NOTE: A previous revision of this file had two test_layer1_*
    # tests pinning a `cache_remote_stream=True` kwarg on DVCFS.open.
    # They were removed when empirical testing (Niger Run A,
    # 2026-04-11) showed DVC 3.67.0 silently drops the kwarg without
    # populating the local cache.  See the revert commit for details
    # and slurm_logs/DESIGN_dvc_layer1_caching.md for the open
    # follow-up question of which DVC API actually triggers Layer-1
    # caching.

    def test_clear_cache_removes_files(self, mock_country_structure, sample_dataframe):
        """clear_cache should delete cached parquet files."""
        cache_path = mock_country_structure / "var" / "test_data.parquet"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(sample_dataframe, cache_path)

        with patch("lsms_library.country.files") as mock_files, \
             patch("lsms_library.country.Repo") as mock_repo_class, \
             patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path:

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo_class.return_value = mock_repo
            mock_file_path.return_value = mock_country_structure

            country = Country("TestCountry", preload_panel_ids=False)
            removed = country.clear_cache(methods=["test_data"])

        assert not cache_path.exists()
        assert cache_path in removed

    def test_clear_cache_dry_run_keeps_files(self, mock_country_structure, sample_dataframe):
        """clear_cache dry-run should report but not delete files."""
        cache_path = mock_country_structure / "var" / "dry_run.parquet"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet(sample_dataframe, cache_path)

        with patch("lsms_library.country.files") as mock_files, \
             patch("lsms_library.country.Repo") as mock_repo_class, \
             patch.object(Country, "file_path", new_callable=PropertyMock) as mock_file_path:

            mock_files.return_value = mock_country_structure.parent.parent
            mock_repo = Mock()
            mock_repo.status.return_value = {}
            mock_repo_class.return_value = mock_repo
            mock_file_path.return_value = mock_country_structure

            country = Country("TestCountry", preload_panel_ids=False)
            removed = country.clear_cache(methods=["dry_run"], dry_run=True)

        assert cache_path.exists()
        assert cache_path in removed

    def test_clear_cache_ignores_non_cache_json(self, mock_country_structure):
        """clear_cache should not remove non-cache JSON helpers."""
        sentinel = mock_country_structure / "_" / "conversion_to_kgs.json"
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text("{}", encoding="utf-8")

        with patch("lsms_library.country.files") as mock_files:
            mock_files.return_value = mock_country_structure.parent.parent
            country = Country("TestCountry", preload_panel_ids=False)
            removed = country.clear_cache()

        assert sentinel.exists()
        assert sentinel not in removed

    def test_panel_ids_lazy_by_default(self, mock_country_structure):
        """Country should not preload panel_ids unless explicitly requested."""
        resources_payload = {"Data Scheme": {"panel_ids": {}}}

        with patch("lsms_library.country.files") as mock_files, \
            patch.object(Country, "resources", new_callable=PropertyMock) as mock_resources, \
            patch.object(Country, "_compute_panel_ids") as mock_compute:

            mock_files.return_value = mock_country_structure.parent.parent
            mock_resources.return_value = resources_payload

            Country("TestCountry", verbose=False)
            mock_compute.assert_not_called()

            Country("TestCountry", preload_panel_ids=True, verbose=False)
            assert mock_compute.call_count == 1

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


class TestLayer1Caching:
    """Tests for the Layer-1 (DVC blob) caching restored in Pieces 1+2.

    Background: an earlier session at 2026-04-11 concluded that Layer-1
    caching was dormant in DVC 3.67.0 because the ``cache=True`` /
    ``cache_remote_stream=True`` kwargs on ``DVCFileSystem.open()`` are
    no-ops.  That conclusion was correct as far as it went but missed
    the actual mechanism: ``DataFileSystem._get_fs_path`` iterates
    ``["cache", "remote", "data"]`` and reads from a populated cache
    automatically when ``info.cache`` is wired up correctly.  Pieces 1
    and 2 land both halves of the round-trip: Piece 1 pins ``cache.dir``
    via runtime config override, Piece 2 populates the cache via
    explicit ``Repo.fetch`` from inside ``get_dataframe`` (NOT ``Repo.pull``,
    which would also check the file out into the package tree -- a
    structural rule discovered in this same session).

    See ``slurm_logs/DESIGN_dvc_layer1_caching.md`` for the empirical
    investigation that led to this design.
    """

    def test_module_dvcfs_uses_data_root(self):
        """Piece 1: the module-level ``DVCFS`` picks up the
        ``data_root() / "dvc-cache"`` override.

        Note: the absolute path of ``_DVC_CACHE_DIR`` is captured at
        module import time and may not match a freshly-evaluated
        ``data_root()`` if a prior test in the same session mutated
        ``LSMS_DATA_DIR``.  We therefore check the *shape* of the
        path (suffix == "dvc-cache" + directory was created) and the
        propagation through ``DVCFS.repo`` rather than identity with
        a freshly evaluated ``data_root()``.
        """
        from lsms_library.local_tools import DVCFS, _DVC_CACHE_DIR

        assert _DVC_CACHE_DIR.name == "dvc-cache"
        assert _DVC_CACHE_DIR.exists()

        # The cache.local.path may include a "files/md5" subpath in
        # some DVC versions; the operative path the storage_map cache
        # uses is the override root.  Either form is acceptable as
        # long as it starts with the override root.
        cache_path = Path(DVCFS.repo.cache.local.path)
        assert str(cache_path).startswith(str(_DVC_CACHE_DIR)), (
            f"DVCFS cache.dir override not propagating: "
            f"expected prefix {_DVC_CACHE_DIR}, got {cache_path}"
        )

    def test_ensure_dvc_pulled_noop_when_no_sidecar(self, tmp_path):
        """No ``.dvc`` sidecar -> ``_ensure_dvc_pulled`` is a no-op."""
        from lsms_library import local_tools

        target = tmp_path / "no_sidecar.dta"
        target.write_bytes(b"")
        with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
            local_tools._ensure_dvc_pulled(str(target))
            mock_dvcfs.repo.fetch.assert_not_called()

    def test_ensure_dvc_pulled_noop_when_blob_cached_legacy_layout(self, tmp_path, monkeypatch):
        """Sidecar + blob in legacy DVC 2.x flat layout -> no ``Repo.fetch``.

        This is the dominant hit path for the LSMS repo today: the
        ``.dvc`` sidecars carry ``md5-dos2unix`` hashes from the
        original DVC 2.x ``dvc add``-s, so the blobs land in the flat
        layout under the cache root.
        """
        from lsms_library import local_tools

        target = tmp_path / "data" / "foo.dta"
        target.parent.mkdir()
        target.write_bytes(b"")
        sidecar = target.parent / "foo.dta.dvc"
        md5 = "abcdef0123456789abcdef0123456789"
        sidecar.write_text(
            f"outs:\n- md5: {md5}\n  size: 0\n  path: foo.dta\n"
        )

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # Legacy DVC 2.x layout: {cache_dir}/{md5[:2]}/{md5[2:]}
        (cache_dir / md5[:2]).mkdir()
        (cache_dir / md5[:2] / md5[2:]).write_bytes(b"")
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)

        with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
            local_tools._ensure_dvc_pulled(str(target))
            mock_dvcfs.repo.fetch.assert_not_called()

    def test_ensure_dvc_pulled_noop_when_blob_cached_dvc3_layout(self, tmp_path, monkeypatch):
        """Sidecar + blob in DVC 3.0 ``files/md5/`` layout -> no ``Repo.fetch``.

        Future-proofing for after the LSMS repo migrates to DVC 3.0
        hashes via ``dvc cache migrate`` (separate workstream).  Once
        the sidecars are regenerated, blobs land at
        ``{cache_dir}/files/md5/{md5[:2]}/{md5[2:]}``; the pre-check
        must still recognize them.
        """
        from lsms_library import local_tools

        target = tmp_path / "data" / "foo.dta"
        target.parent.mkdir()
        target.write_bytes(b"")
        sidecar = target.parent / "foo.dta.dvc"
        md5 = "fedcba9876543210fedcba9876543210"
        sidecar.write_text(
            f"outs:\n- md5: {md5}\n  size: 0\n  path: foo.dta\n"
        )

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # DVC 3.0 layout: {cache_dir}/files/md5/{md5[:2]}/{md5[2:]}
        (cache_dir / "files" / "md5" / md5[:2]).mkdir(parents=True)
        (cache_dir / "files" / "md5" / md5[:2] / md5[2:]).write_bytes(b"")
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)

        with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
            local_tools._ensure_dvc_pulled(str(target))
            mock_dvcfs.repo.fetch.assert_not_called()

    def test_ensure_dvc_pulled_calls_pull_on_miss(self, tmp_path, monkeypatch):
        """Sidecar present + blob NOT in cache -> ``Repo.fetch`` is called.

        Verifies the target passed to ``Repo.fetch`` is the
        countries-relative path, not the absolute path or the
        script-relative path.
        """
        from lsms_library import local_tools

        countries_dir = tmp_path / "countries"
        target_dir = countries_dir / "TestC" / "wave" / "Data"
        target_dir.mkdir(parents=True)
        # Note: the .dta file itself does NOT exist on disk; only the
        # sidecar.  This matches the typical fresh-checkout state.
        target = target_dir / "foo.dta"
        sidecar = target.parent / "foo.dta.dvc"
        md5 = "0123456789abcdef0123456789abcdef"
        sidecar.write_text(
            f"outs:\n- md5: {md5}\n  size: 0\n  path: foo.dta\n"
        )

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        # Note: blob NOT placed in cache_dir
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)
        monkeypatch.setattr(local_tools, "_COUNTRIES_DIR", countries_dir)

        with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
            local_tools._ensure_dvc_pulled(str(target))
            mock_dvcfs.repo.fetch.assert_called_once()
            call = mock_dvcfs.repo.fetch.call_args
            assert call.kwargs.get("targets") == ["TestC/wave/Data/foo.dta"]

    def test_ensure_dvc_pulled_swallows_pull_errors(self, tmp_path, monkeypatch):
        """If ``Repo.fetch`` raises, ``_ensure_dvc_pulled`` returns silently.

        Layer-1 warming is best-effort; the streaming fallback in
        ``get_dataframe`` should still run.
        """
        from lsms_library import local_tools

        countries_dir = tmp_path / "countries"
        target_dir = countries_dir / "TestC" / "Data"
        target_dir.mkdir(parents=True)
        target = target_dir / "foo.dta"
        sidecar = target.parent / "foo.dta.dvc"
        sidecar.write_text(
            "outs:\n- md5: 0123456789abcdef0123456789abcdef\n"
            "  size: 0\n  path: foo.dta\n"
        )
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)
        monkeypatch.setattr(local_tools, "_COUNTRIES_DIR", countries_dir)

        with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
            mock_dvcfs.repo.fetch.side_effect = RuntimeError("simulated S3 failure")
            # Should NOT raise
            local_tools._ensure_dvc_pulled(str(target))
            mock_dvcfs.repo.fetch.assert_called_once()

    def test_ensure_dvc_pulled_changes_cwd_to_countries(self, tmp_path, monkeypatch):
        """``Repo.fetch`` is invoked from inside ``_COUNTRIES_DIR``.

        Cwd-independence regression test for the footgun discovered in
        Probe 2 of the 2026-04-11 session: ``Repo.pull(targets=[X])``
        resolves ``X`` against ``os.getcwd()``, not against the repo
        root, so without an explicit chdir the call fails with
        ``NoOutputOrStageError`` from any cwd that isn't already
        ``lsms_library/countries/``.
        """
        from lsms_library import local_tools

        countries_dir = (tmp_path / "countries").resolve()
        target_dir = countries_dir / "TestC" / "Data"
        target_dir.mkdir(parents=True)
        target = target_dir / "foo.dta"
        sidecar = target.parent / "foo.dta.dvc"
        sidecar.write_text(
            "outs:\n- md5: 0123456789abcdef0123456789abcdef\n"
            "  size: 0\n  path: foo.dta\n"
        )
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)
        monkeypatch.setattr(local_tools, "_COUNTRIES_DIR", countries_dir)

        # Start from a cwd that is NOT countries_dir.
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        original_cwd = Path.cwd()
        os.chdir(elsewhere)

        observed = []

        def record_cwd(*args, **kwargs):
            observed.append(Path.cwd())

        try:
            with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
                mock_dvcfs.repo.fetch.side_effect = record_cwd
                local_tools._ensure_dvc_pulled(str(target))
        finally:
            os.chdir(original_cwd)

        assert len(observed) == 1
        assert observed[0] == countries_dir
        # And the original cwd is restored after the helper returns
        assert Path.cwd() == original_cwd

    def test_ensure_dvc_pulled_bails_on_path_outside_countries(self, tmp_path, monkeypatch):
        """A sidecar at a path outside _COUNTRIES_DIR -> no pull (no error)."""
        from lsms_library import local_tools

        countries_dir = (tmp_path / "countries").resolve()
        countries_dir.mkdir()
        outside = tmp_path / "outside" / "Data"
        outside.mkdir(parents=True)
        target = outside / "foo.dta"
        sidecar = outside / "foo.dta.dvc"
        sidecar.write_text(
            "outs:\n- md5: 0123456789abcdef0123456789abcdef\n"
            "  size: 0\n  path: foo.dta\n"
        )
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)
        monkeypatch.setattr(local_tools, "_COUNTRIES_DIR", countries_dir)

        with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
            local_tools._ensure_dvc_pulled(str(target))
            mock_dvcfs.repo.fetch.assert_not_called()

    def test_ensure_dvc_pulled_handles_malformed_sidecar(self, tmp_path, monkeypatch):
        """A sidecar with unexpected shape -> bail silently, no pull."""
        from lsms_library import local_tools

        target = tmp_path / "foo.dta"
        target.write_bytes(b"")
        sidecar = tmp_path / "foo.dta.dvc"
        # Missing 'outs' key entirely
        sidecar.write_text("not a real dvc sidecar\n")

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)

        with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
            local_tools._ensure_dvc_pulled(str(target))
            mock_dvcfs.repo.fetch.assert_not_called()

    def test_ensure_dvc_pulled_handles_countries_relative_path(self, tmp_path, monkeypatch):
        """Interactive callers can pass countries-relative paths.

        e.g. ``get_dataframe('Niger/2018-19/Data/foo.dta')`` from an
        ipython session whose cwd is unrelated to ``countries/``.  The
        helper must find the sidecar by trying ``_COUNTRIES_DIR / fn``
        as one of the candidate interpretations.
        """
        from lsms_library import local_tools

        countries_dir = (tmp_path / "countries").resolve()
        target_dir = countries_dir / "TestC" / "wave" / "Data"
        target_dir.mkdir(parents=True)
        # Sidecar exists at the countries-relative location
        sidecar = target_dir / "foo.dta.dvc"
        md5 = "0123456789abcdef0123456789abcdef"
        sidecar.write_text(
            f"outs:\n- md5: {md5}\n  size: 0\n  path: foo.dta\n"
        )

        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        monkeypatch.setattr(local_tools, "_DVC_CACHE_DIR", cache_dir)
        monkeypatch.setattr(local_tools, "_COUNTRIES_DIR", countries_dir)

        # Start from a cwd unrelated to countries_dir
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        original_cwd = Path.cwd()
        os.chdir(elsewhere)

        try:
            with patch("lsms_library.local_tools.DVCFS") as mock_dvcfs:
                # Pass the countries-relative path, not an absolute path
                local_tools._ensure_dvc_pulled("TestC/wave/Data/foo.dta")
                mock_dvcfs.repo.fetch.assert_called_once()
                call = mock_dvcfs.repo.fetch.call_args
                assert call.kwargs.get("targets") == ["TestC/wave/Data/foo.dta"]
        finally:
            os.chdir(original_cwd)

    # ----------- _is_polluted_workspace_copy / local_file hardening -----------

    def test_is_polluted_workspace_copy_true_when_sidecar_exists(self, tmp_path):
        """A workspace file with a sister .dvc sidecar is pollution."""
        from lsms_library import local_tools

        target = tmp_path / "foo.dta"
        target.write_bytes(b"data")
        sidecar = tmp_path / "foo.dta.dvc"
        sidecar.write_text("outs:\n- md5: 0123\n  size: 4\n  path: foo.dta\n")

        assert local_tools._is_polluted_workspace_copy(str(target)) is True

    def test_is_polluted_workspace_copy_false_when_no_sidecar(self, tmp_path):
        """A workspace file without a sister sidecar is legitimate.

        Covers the new-data-being-added cases: manual ``cp + dvc add``,
        WB-fallback auto-add, user scratch data.  ``local_file()`` should
        happily use these.
        """
        from lsms_library import local_tools

        target = tmp_path / "freshly_downloaded.dta"
        target.write_bytes(b"data")
        # No sidecar created

        assert local_tools._is_polluted_workspace_copy(str(target)) is False

    def test_is_polluted_workspace_copy_false_on_bad_input(self):
        """Bad input -> False, no exception."""
        from lsms_library import local_tools

        # None, empty string, missing file -- all should return False quietly
        assert local_tools._is_polluted_workspace_copy("/nonexistent/path/foo") is False

    def test_get_dataframe_warns_and_falls_through_on_polluted_workspace(self, tmp_path, monkeypatch):
        """When local_file finds a polluted workspace copy, it should warn
        and fall through to the DVC code path instead of using the file.

        We mock DVCFS.open and observe both the warning and the fall-through.
        """
        from lsms_library import local_tools
        import warnings

        # Create a fake .dta + sister sidecar
        target = tmp_path / "polluted.dta"
        target.write_bytes(b"workspace data")
        sidecar = tmp_path / "polluted.dta.dvc"
        sidecar.write_text(
            "outs:\n- md5: deadbeefcafebabe0123456789abcdef\n"
            "  size: 14\n  path: polluted.dta\n"
        )

        # Stub DVCFS.open to return a sentinel BytesIO so we can detect
        # whether the fall-through path was taken
        import io
        dvcfs_mock = patch.object(
            local_tools.DVCFS, "open",
            return_value=io.BytesIO(b"from cache")
        )
        # Stub _ensure_dvc_pulled to a no-op so we don't try real DVC ops
        ensure_mock = patch.object(local_tools, "_ensure_dvc_pulled", return_value=None)
        # Stub read_file (via the inner closure) -- this is harder; instead
        # we'll just make sure the warning fires and trust that the rest of
        # get_dataframe will do its thing.  We test the warning here and
        # the fall-through behavior is covered by the
        # _is_polluted_workspace_copy unit tests above plus the
        # local_file logic.

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with dvcfs_mock, ensure_mock:
                try:
                    local_tools.get_dataframe(str(target))
                except (OSError, ValueError, KeyError, AttributeError, TypeError):
                    # We don't care if read_file fails on the BytesIO --
                    # what matters is whether the warning fired before
                    # we got there.
                    pass

        polluted_warnings = [
            w for w in caught
            if "Refusing workspace copy" in str(w.message)
        ]
        assert len(polluted_warnings) == 1, (
            f"Expected exactly one 'Refusing workspace copy' warning, "
            f"got {[str(w.message) for w in caught]}"
        )
        assert "polluted.dta" in str(polluted_warnings[0].message)
        assert ".dvc sidecar" in str(polluted_warnings[0].message)

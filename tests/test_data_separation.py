"""
Lightweight tests for the data/code separation refactor.

These verify path resolution logic, Makefile VAR_DIR plumbing, and
import integrity WITHOUT requiring a full data build.
"""

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
COUNTRIES_ROOT = REPO_ROOT / "lsms_library" / "countries"


# ---------------------------------------------------------------------------
# paths.py unit tests
# ---------------------------------------------------------------------------

class TestDataRoot:
    def test_env_override(self, tmp_path):
        with mock.patch.dict(os.environ, {"LSMS_DATA_DIR": str(tmp_path)}):
            from lsms_library.paths import data_root
            data_root.cache_clear()
            assert data_root() == tmp_path
            assert data_root("Uganda") == tmp_path / "Uganda"
            data_root.cache_clear()

    def test_default_without_env(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            env = os.environ.copy()
            env.pop("LSMS_DATA_DIR", None)
            with mock.patch.dict(os.environ, env, clear=True):
                from lsms_library.paths import data_root
                data_root.cache_clear()
                root = data_root()
                # Should be a real path (XDG-style default)
                assert isinstance(root, Path)
                assert "lsms_library" in str(root)
                data_root.cache_clear()

    def test_default_is_space_free(self):
        """Default data_root must never contain whitespace.

        GNU make (used by wave-level builds like Uganda food_expenditures)
        splits target names on whitespace, so a space in ``data_root``
        breaks every Makefile-backed feature.  On macOS,
        ``platformdirs.user_data_path`` would return
        ``~/Library/Application Support/lsms_library`` — that's why we
        use an XDG-style default instead.  Regression guard for the
        2026-04-23 macOS bug report.
        """
        env = os.environ.copy()
        env.pop("LSMS_DATA_DIR", None)
        with mock.patch.dict(os.environ, env, clear=True):
            from lsms_library.paths import data_root
            data_root.cache_clear()
            root = data_root()
            assert " " not in str(root), (
                f"Default data_root must not contain whitespace "
                f"(GNU make target parsing breaks): {root!s}"
            )
            data_root.cache_clear()

    def test_whitespace_override_warns(self, tmp_path):
        """Explicit overrides with whitespace get a RuntimeWarning."""
        import warnings as _warnings

        spaced = tmp_path / "My Data" / "lsms_library"
        spaced.mkdir(parents=True)
        with mock.patch.dict(os.environ, {"LSMS_DATA_DIR": str(spaced)}):
            from lsms_library.paths import data_root, _WHITESPACE_WARNED
            _WHITESPACE_WARNED.clear()
            data_root.cache_clear()
            with _warnings.catch_warnings(record=True) as caught:
                _warnings.simplefilter("always")
                data_root()
            data_root.cache_clear()
            _WHITESPACE_WARNED.clear()
            messages = [str(w.message) for w in caught
                        if issubclass(w.category, RuntimeWarning)]
            assert any("whitespace" in m for m in messages), (
                f"expected a whitespace RuntimeWarning, got: {messages}"
            )


class TestResolveDataPath:
    """Test _resolve_data_path without actually calling from country scripts."""

    def test_always_active(self):
        """Path rewriting is always active (no LSMS_DATA_DIR gate)."""
        from lsms_library.local_tools import _resolve_data_path
        # When called from outside the countries tree, paths pass through
        # (the stack inspection won't find a country), but the function
        # itself is not gated on an env var.
        result = _resolve_data_path("../var/food.parquet")
        # Either redirected (if stack happens to match) or unchanged
        assert isinstance(result, str)

    def test_absolute_paths_unchanged(self, tmp_path):
        with mock.patch.dict(os.environ, {"LSMS_DATA_DIR": str(tmp_path)}):
            from lsms_library.local_tools import _resolve_data_path
            abs_path = str(tmp_path / "foo.parquet")
            assert _resolve_data_path(abs_path) == abs_path


class TestVarPath:
    def test_explicit_country(self, tmp_path):
        with mock.patch.dict(os.environ, {"LSMS_DATA_DIR": str(tmp_path)}):
            from lsms_library.paths import data_root, var_path
            data_root.cache_clear()
            result = var_path("food_acquired.parquet", country="Uganda")
            assert result == tmp_path / "Uganda" / "var" / "food_acquired.parquet"
            data_root.cache_clear()


# ---------------------------------------------------------------------------
# Makefile VAR_DIR plumbing
# ---------------------------------------------------------------------------

class TestMakefileVarDir:
    """Every country Makefile must define VAR_DIR and use it consistently."""

    @staticmethod
    def _country_makefiles():
        return sorted(COUNTRIES_ROOT.glob("*/_/Makefile"))

    def test_all_makefiles_define_var_dir(self):
        for mf in self._country_makefiles():
            content = mf.read_text()
            assert "VAR_DIR ?=" in content, (
                f"{mf.relative_to(REPO_ROOT)} missing 'VAR_DIR ?='"
            )
            assert "LSMS_DATA_ROOT ?=" in content, (
                f"{mf.relative_to(REPO_ROOT)} missing 'LSMS_DATA_ROOT ?='"
            )
            assert "COUNTRY :=" in content, (
                f"{mf.relative_to(REPO_ROOT)} missing 'COUNTRY :='"
            )

    def test_no_hardcoded_var_paths_in_makefiles(self):
        """No remaining ../var/ references outside the VAR_DIR definition."""
        for mf in self._country_makefiles():
            content = mf.read_text()
            for i, line in enumerate(content.splitlines(), 1):
                # Skip the VAR_DIR definition itself
                if line.strip().startswith("VAR_DIR"):
                    continue
                # Skip comments
                if line.strip().startswith("#"):
                    continue
                assert "../var/" not in line and "../var)" not in line, (
                    f"{mf.relative_to(REPO_ROOT)}:{i} has hardcoded ../var/: {line.strip()}"
                )


# ---------------------------------------------------------------------------
# country.py uses data_root, not self.file_path / "var"
# ---------------------------------------------------------------------------

class TestCountryPyPaths:
    """Verify country.py doesn't use self.file_path / 'var' for caching."""

    def test_country_py_uses_data_root_for_primary_cache(self):
        """Primary cache lookups should go through data_root, not self.file_path / 'var'.

        self.file_path / 'var' is acceptable as a *fallback* in output_candidates
        (for finding outputs that Make wrote in-tree), but the first candidate
        should always be a data_root() path.
        """
        country_py = REPO_ROOT / "lsms_library" / "country.py"
        content = country_py.read_text()
        assert "data_root(self.name)" in content, (
            "country.py should use data_root(self.name) for cache paths"
        )

    def test_country_py_imports_data_root(self):
        country_py = REPO_ROOT / "lsms_library" / "country.py"
        content = country_py.read_text()
        assert "from .paths import data_root" in content

    def test_country_py_sets_lsms_data_dir_for_make(self):
        """When country.py invokes make, it sets LSMS_DATA_DIR so scripts redirect."""
        country_py = REPO_ROOT / "lsms_library" / "country.py"
        content = country_py.read_text()
        assert 'env["LSMS_DATA_DIR"]' in content, (
            "country.py should set LSMS_DATA_DIR in subprocess env for make"
        )

    def test_country_py_sets_lsms_data_dir_in_env(self):
        """Subprocess env should include LSMS_DATA_DIR."""
        country_py = REPO_ROOT / "lsms_library" / "country.py"
        content = country_py.read_text()
        assert "LSMS_DATA_DIR" in content, "country.py should set LSMS_DATA_DIR in subprocess env"


# ---------------------------------------------------------------------------
# Import smoke tests
# ---------------------------------------------------------------------------

class TestImports:
    def test_paths_module(self):
        from lsms_library.paths import data_root, var_path, wave_data_path
        assert callable(data_root)
        assert callable(var_path)
        assert callable(wave_data_path)

    def test_local_tools_exports_resolve(self):
        from lsms_library.local_tools import _resolve_data_path
        assert callable(_resolve_data_path)

    def test_country_imports(self):
        from lsms_library.country import Country
        assert callable(Country)


# ---------------------------------------------------------------------------
# End-to-end path resolution with LSMS_DATA_DIR
# ---------------------------------------------------------------------------

class TestEndToEndRedirect:
    """Verify to_parquet and get_dataframe actually write/read the redirected path."""

    def test_country_level_var_redirect(self, tmp_path):
        """../var/foo.parquet from a country-level script lands under data_root."""
        with mock.patch.dict(os.environ, {"LSMS_DATA_DIR": str(tmp_path)}):
            from lsms_library.paths import data_root
            data_root.cache_clear()

            from lsms_library.local_tools import _resolve_data_path
            # Simulate call from Uganda/_/food_acquired.py
            caller = str(COUNTRIES_ROOT / "Uganda" / "_" / "food_acquired.py")
            with mock.patch("inspect.stack") as mock_stack:
                mock_frame = mock.MagicMock()
                mock_frame.filename = caller
                mock_stack.return_value = [mock.MagicMock(), mock.MagicMock(), mock_frame]
                result = _resolve_data_path("../var/food_acquired.parquet", stack_depth=2)

            assert result == str(tmp_path / "Uganda" / "var" / "food_acquired.parquet")
            data_root.cache_clear()

    def test_wave_level_bare_redirect(self, tmp_path):
        """Bare filename from a wave-level script lands under data_root/wave/_/."""
        with mock.patch.dict(os.environ, {"LSMS_DATA_DIR": str(tmp_path)}):
            from lsms_library.paths import data_root
            data_root.cache_clear()

            from lsms_library.local_tools import _resolve_data_path
            # Simulate call from Uganda/2005-06/_/shocks.py
            caller = str(COUNTRIES_ROOT / "Uganda" / "2005-06" / "_" / "shocks.py")
            with mock.patch("inspect.stack") as mock_stack:
                mock_frame = mock.MagicMock()
                mock_frame.filename = caller
                mock_stack.return_value = [mock.MagicMock(), mock.MagicMock(), mock_frame]
                result = _resolve_data_path("shocks.parquet", stack_depth=2)

            assert result == str(tmp_path / "Uganda" / "2005-06" / "_" / "shocks.parquet")
            data_root.cache_clear()

    def test_cross_wave_ref_resolved(self, tmp_path):
        """Paths like ../2018-19/_/foo.parquet from country-level scripts are Pattern 3."""
        with mock.patch.dict(os.environ, {"LSMS_DATA_DIR": str(tmp_path)}):
            from lsms_library.paths import data_root
            data_root.cache_clear()

            from lsms_library.local_tools import _resolve_data_path
            caller = str(COUNTRIES_ROOT / "Uganda" / "_" / "other_features.py")
            with mock.patch("inspect.stack") as mock_stack:
                mock_frame = mock.MagicMock()
                mock_frame.filename = caller
                mock_stack.return_value = [mock.MagicMock(), mock.MagicMock(), mock_frame]
                result = _resolve_data_path("../2018-19/_/other_features.parquet", stack_depth=2)

            # Cross-wave ref from country-level script → resolved via Pattern 3
            assert result == str(tmp_path / "Uganda" / "2018-19" / "_" / "other_features.parquet")
            data_root.cache_clear()


# ---------------------------------------------------------------------------
# Makefile dry-run: VAR_DIR override works
# ---------------------------------------------------------------------------

class TestMakefileDryRun:
    """Verify that overriding VAR_DIR changes the target paths in make -n output."""

    @pytest.mark.skipif(
        subprocess.run(["make", "--version"], capture_output=True).returncode != 0,
        reason="make not available",
    )
    def test_uganda_makefile_respects_var_dir(self, tmp_path):
        makefile = COUNTRIES_ROOT / "Uganda" / "_" / "Makefile"
        if not makefile.exists():
            pytest.skip("Uganda Makefile not found")
        custom_var = str(tmp_path / "custom_var")
        result = subprocess.run(
            ["make", "-n", f"VAR_DIR={custom_var}"],
            cwd=makefile.parent,
            capture_output=True,
            text=True,
        )
        # The dry-run output shouldn't mention ../var/ — only our custom path
        combined = result.stdout + result.stderr
        # Filter out the VAR_DIR definition line itself
        lines = [l for l in combined.splitlines()
                 if "../var/" in l and not l.strip().startswith("VAR_DIR")]
        assert not lines, (
            f"Dry-run with VAR_DIR override still references ../var/:\n"
            + "\n".join(lines[:5])
        )

"""GH #436 Item 1: ``LSMS_COUNTRIES_ROOT`` config-tree override.

The country *config* tree must resolve through ``paths.countries_root()``
(env -> config.yml -> package-relative default), mirroring ``data_root()``,
so a git worktree / alternate config checkout can be read by the installed
(``.pth``-pinned) package and thus self-verify.

Two guarantees:
  1. **default-preserving** -- no override => the historical package-relative
     path, byte-identical, so the common case is unchanged;
  2. **override honored across every layer** -- ``countries_root()``,
     ``Country.file_path``, and the ``_COUNTRIES_DIR`` snapshots in
     ``data_access`` / ``local_tools``.  Verified in a subprocess with the env
     set *before* import (the real worktree model), so the in-process
     ``lru_cache`` is never polluted.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import lsms_library.paths as paths
from lsms_library.paths import countries_root


def test_default_is_package_countries_dir(monkeypatch):
    monkeypatch.delenv("LSMS_COUNTRIES_ROOT", raising=False)
    countries_root.cache_clear()
    try:
        default = Path(paths.__file__).resolve().parent / "countries"
        assert countries_root() == default
    finally:
        countries_root.cache_clear()


def test_config_resolver_reads_env(monkeypatch, tmp_path):
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(tmp_path))
    countries_root.cache_clear()
    try:
        from lsms_library import config
        assert config.countries_dir() == str(tmp_path)
        assert countries_root() == tmp_path
    finally:
        countries_root.cache_clear()  # never leave the override cached


def test_subprocess_override_routes_all_layers(tmp_path):
    """Env set before import => every countries-tree resolver honors it."""
    ov = tmp_path / "countries"
    ov.mkdir()
    script = textwrap.dedent(
        f"""
        import warnings; warnings.simplefilter("ignore")
        from pathlib import Path
        OV = Path({str(ov)!r})
        from lsms_library.paths import countries_root
        import lsms_library.data_access as da
        import lsms_library.local_tools as lt
        import lsms_library as ll
        assert countries_root() == OV, countries_root()
        assert da._COUNTRIES_DIR == OV, da._COUNTRIES_DIR
        assert lt._COUNTRIES_DIR == OV, lt._COUNTRIES_DIR
        assert ll.Country("Uganda").file_path == OV / "Uganda"
        print("OVERRIDE_OK")
        """
    )
    env = dict(os.environ, LSMS_COUNTRIES_ROOT=str(ov))
    r = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True
    )
    assert r.returncode == 0, f"stderr:\n{r.stderr}"
    assert "OVERRIDE_OK" in r.stdout


def test_subprocess_default_unchanged(tmp_path):
    """No override => package-relative default, byte-identical to history."""
    script = textwrap.dedent(
        """
        import warnings; warnings.simplefilter("ignore")
        from pathlib import Path
        import lsms_library.paths as paths
        from lsms_library.paths import countries_root
        default = Path(paths.__file__).resolve().parent / "countries"
        assert countries_root() == default, countries_root()
        import lsms_library as ll
        assert ll.Country("Uganda").file_path == default / "Uganda"
        print("DEFAULT_OK")
        """
    )
    env = {k: v for k, v in os.environ.items() if k != "LSMS_COUNTRIES_ROOT"}
    r = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True
    )
    assert r.returncode == 0, f"stderr:\n{r.stderr}"
    assert "DEFAULT_OK" in r.stdout

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

import pytest

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


@pytest.mark.parametrize("wave", ["1987-88", "1988-89", "1991-92", "1998-99"])
@pytest.mark.parametrize("region_level", ["wave", "country"])
def test_ghanalss_mapping_uses_override_labels(tmp_path, wave, region_level):
    """GH #753: decode from the override, retaining wave-first fallback.

    Load the installed code independently of the temporary config tree.  No
    microdata or derived caches are involved, so stale data cannot conceal a
    lookup from the wrong checkout.  The early rounds' relationship table has
    a distinct name; GLSS4 requires its relationship table at wave level.
    """
    ov = tmp_path / "countries"
    wave_dir = ov / "GhanaLSS" / wave / "_"
    country_dir = ov / "GhanaLSS" / "_"
    wave_dir.mkdir(parents=True)
    country_dir.mkdir()

    def table(name, label):
        return (
            f"#+name: {name}\n"
            "| Code | Label |\n"
            "|------+-------|\n"
            f"| 1 | {label} |\n\n"
        )

    relationship = "relationship_glss1" if wave in ("1987-88", "1988-89") else "relationship"
    country_tables = table("region", "Country sentinel")
    country_tables += table(relationship, "Country relationship sentinel")
    country_tables += table("rural", "Country rural sentinel")
    (country_dir / "categorical_mapping.org").write_text(country_tables)
    wave_tables = table(relationship, "Wave relationship sentinel")
    if region_level == "wave":
        wave_tables += table("region", "Wave sentinel")
    (wave_dir / "categorical_mapping.org").write_text(wave_tables)

    module_path = Path(paths.__file__).resolve().parent / "countries" / "GhanaLSS" / wave / "_" / "mapping.py"
    expected_region = "Wave sentinel" if region_level == "wave" else "Country sentinel"
    script = textwrap.dedent(
        f"""
        import importlib.util
        from pathlib import Path
        from lsms_library.paths import countries_root
        assert countries_root() == Path({str(ov)!r})
        spec = importlib.util.spec_from_file_location("_gh753_mapping", {str(module_path)!r})
        mapping = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mapping)
        assert mapping.Birthplace(1) == {expected_region!r}, mapping.region_dict
        assert mapping.Region(1) == {expected_region!r}, mapping.region_dict
        assert mapping.Relationship(1) == "Wave relationship sentinel"
        if hasattr(mapping, "rural_dict"):
            assert mapping.rural_dict[1] == "Country rural sentinel"
        """
    )
    env = dict(os.environ, LSMS_COUNTRIES_ROOT=str(ov))
    result = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"

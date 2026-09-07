"""GH #808: the script-path predicate behind ``_assert_built_required_columns``.

The guard (GH #479) only runs for script-path tables.  Before #808 a table
counted as script-path when it declared ``materialize: make`` or had a
country-level ``_/{table}.py``; GhanaLSS ``food_acquired`` was covered only by
the second clause, through the country-level concatenator that #808 retired.
The table is still script-built -- by seven per-wave ``{wave}/_/food_acquired.py``
scripts -- so the predicate now also looks for wave-level scripts, resolving
each wave's folder through ``Country.__getitem__`` (which honours
``wave_folder_map``) rather than ``file_path / wave``.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from lsms_library.country import Country as _CountryCls


def _fake_country(tmp_path: Path, waves: dict[str, str]):
    """A stand-in with the two attributes the predicate touches.

    ``waves`` maps wave label -> folder name, so a label whose folder differs
    (``'2008-09' -> '2008-15'``) exercises the ``wave_folder_map`` path.
    """
    root = tmp_path / "Fixtureland"
    (root / "_").mkdir(parents=True)
    for folder in set(waves.values()):
        (root / folder / "_").mkdir(parents=True, exist_ok=True)

    class _Fake:
        file_path = root

        def __getitem__(self, w):          # Country.__getitem__ -> Wave
            return SimpleNamespace(file_path=root / waves[w])

    fake = _Fake()
    fake.waves = list(waves)
    fake._is_script_path = _CountryCls._is_script_path.__get__(fake)
    return fake, root


def test_yaml_only_table_is_not_script_path(tmp_path):
    fake, _ = _fake_country(tmp_path, {"2010": "2010", "2012": "2012"})
    assert fake._is_script_path("housing", None, fake.waves) is False


def test_make_backend_is_script_path(tmp_path):
    fake, _ = _fake_country(tmp_path, {"2010": "2010"})
    assert fake._is_script_path("housing", "make", fake.waves) is True


def test_country_level_script_is_script_path(tmp_path):
    fake, root = _fake_country(tmp_path, {"2010": "2010"})
    (root / "_" / "housing.py").write_text("# concatenator\n")
    assert fake._is_script_path("housing", None, fake.waves) is True


def test_wave_level_script_alone_is_script_path(tmp_path):
    """The GhanaLSS food_acquired shape after #808: wave scripts, nothing else."""
    fake, root = _fake_country(tmp_path, {"2010": "2010", "2012": "2012"})
    (root / "2012" / "_" / "food_acquired.py").write_text("# wave script\n")
    assert fake._is_script_path("food_acquired", None, fake.waves) is True
    # Only the waves actually being built are consulted.
    assert fake._is_script_path("food_acquired", None, ["2010"]) is False


def test_wave_folder_resolved_through_getitem(tmp_path):
    """A label whose folder is mapped (Tanzania '2008-09' -> '2008-15') must be
    found; ``file_path / label`` does not exist and must not be what is probed."""
    fake, root = _fake_country(tmp_path, {"2008-09": "2008-15", "2010-11": "2008-15"})
    (root / "2008-15" / "_" / "sample.py").write_text("# multi-round script\n")
    assert not (root / "2008-09").exists()
    assert fake._is_script_path("sample", None, ["2008-09"]) is True

"""The runtime never writes into ``countries_root()`` (GH #831, #914, #803).

``countries_root()`` is the *config* tree: reviewed YAML, wave scripts, the
committed panel crosswalk.  Everything the library *produces* belongs under
``data_root()``.  Three issues are the same bug wearing different hats:

* **#803** -- a wave script run from the wrong checkout wrote its parquet
  in-tree, and the reader *preferred* it to re-running the script.  Fixed: an
  in-tree parquet is now warned about and ignored.
* **#914** -- a Make rule regenerated the committed ``panel_ids.json`` into the
  package tree whenever a wheel install happened to leave the script a
  millisecond newer.  Fixed: the crosswalk is a source, not a target.
* **#831** -- ``get_data_file`` materialised *every* fetched raw file into the
  config tree: the DVC branch wrote the blob to ``_COUNTRIES_DIR / path`` and
  the World Bank branch extracted zips there.  Two sweeps found 433 such
  copies (1.31 GB), 33 of them without a ``.dvc`` sidecar -- and a sidecar-less
  copy *shadows the DVC blob silently*, because the reader's chain is
  local -> DVC -> WB and the stray wins at step one.

All three share one user-visible symptom on a shared install: the package
directory is not writable by the user running the code, so the write raises
``PermissionError`` from several layers inside a call that was only trying to
read data.

The acquisition path is the deliberate exception: ``add_wave`` and
``populate_and_push`` pass ``populate_cache=True`` and extract into the config
tree precisely so the maintainer can ``dvc add`` what landed.  That is checked
here too, so a future "just never write there" simplification cannot silently
break wave acquisition.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from lsms_library import data_access
from lsms_library.paths import countries_root, data_root


REL = Path("Uganda/2013-14/Data/GSEC1.dta")


def test_read_path_destination_is_the_cache_not_the_package():
    dest = data_access._fetch_destination(REL, populate_cache=False)
    assert data_root() in dest.parents, dest
    assert countries_root() not in dest.parents, (
        f"a read-path fetch would write into the config tree: {dest}"
    )
    # Layout mirrors the config tree, so the file is findable by eye.
    assert dest == data_root() / REL


def test_acquisition_path_still_targets_the_config_tree():
    """`add_wave` dvc-adds what it extracted; it must land in the checkout."""
    dest = data_access._fetch_destination(REL, populate_cache=True)
    assert dest == countries_root() / REL


def test_a_file_already_in_the_config_tree_is_still_READ(tmp_path, monkeypatch):
    """Read from the tree, write to the cache.

    A maintainer's checkout legitimately holds real data, and every pre-#831
    fetch put files there.  Those must keep working -- the fix removes a write,
    not a read.
    """
    fake_countries = tmp_path / "countries"
    fake_data = tmp_path / "data"
    target = fake_countries / REL
    target.parent.mkdir(parents=True)
    target.write_bytes(b"stata")

    monkeypatch.setattr(data_access, "_COUNTRIES_DIR", fake_countries)
    monkeypatch.setattr(data_access, "data_root", lambda *a, **k: fake_data)

    got = data_access.get_data_file(REL)
    assert got == target, got


def test_the_cache_copy_wins_over_a_refetch(tmp_path, monkeypatch):
    """A file already materialised under data_root() short-circuits."""
    fake_countries = tmp_path / "countries"
    fake_data = tmp_path / "data"
    cached = fake_data / REL
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b"stata")
    (fake_countries / REL).parent.mkdir(parents=True)

    monkeypatch.setattr(data_access, "_COUNTRIES_DIR", fake_countries)
    monkeypatch.setattr(data_access, "data_root", lambda *a, **k: fake_data)

    got = data_access.get_data_file(REL)
    assert got == cached, got


def test_a_readonly_config_tree_does_not_break_a_read(tmp_path, monkeypatch):
    """The #831 / #914 symptom: site-packages is not writable.

    With nothing on disk and no credentials the fetch still returns None rather
    than raising PermissionError -- the point is that no write into the config
    tree is even attempted.
    """
    fake_countries = tmp_path / "countries"
    fake_data = tmp_path / "data"
    (fake_countries / REL).parent.mkdir(parents=True)
    fake_countries.chmod(0o555)
    try:
        monkeypatch.setattr(data_access, "_COUNTRIES_DIR", fake_countries)
        monkeypatch.setattr(data_access, "data_root", lambda *a, **k: fake_data)
        monkeypatch.setattr(data_access, "permissions", lambda p: {})
        assert data_access.get_data_file(REL) is None
    finally:
        fake_countries.chmod(0o755)


def test_no_write_site_in_get_data_file_targets_the_config_tree():
    """Static guard: the read path must not name ``_COUNTRIES_DIR`` as a sink.

    ``_fetch_destination`` is the single place that decides, so a new write
    added straight to ``_COUNTRIES_DIR / path`` inside the fetch would
    reintroduce #831 while every behavioural test above still passed.
    """
    import inspect

    src = inspect.getsource(data_access.get_data_file)
    # The only permitted mention is the read-side candidate and the
    # acquisition-only data_dir, both commented as such.
    offenders = [
        line.strip() for line in src.splitlines()
        if "_COUNTRIES_DIR" in line
        and "intree_path" not in line
        and "data_dir" not in line
    ]
    assert not offenders, (
        "get_data_file names the config tree outside the two sanctioned "
        f"sites; route it through _fetch_destination instead:\n  " +
        "\n  ".join(offenders)
    )

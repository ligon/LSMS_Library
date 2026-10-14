"""GH #970: a cold Design-B ``panel_ids`` read never shells out to ``make``.

``Country._aggregate_wave_data`` -> ``load_json_cache`` used to run
``make -s <method>.json`` in ``countries/{C}/_`` when none of its four cached
candidates existed, with no ``env=`` (the one shell-out in ``country.py`` not
routed through ``_script_subprocess_env``).  Since GH #914 no country Makefile
has a ``.json`` target, so the call could only fail: Malawi and Nigeria
(Design B, crosswalk derived from per-wave ``data_info.yml``) got a
``CalledProcessError``, a misleading ``"Makefile build failed ... Falling back
to wave aggregation"`` warning, and *then* the wave aggregation that was always
going to run.  Where ``make`` is absent (Windows, GH #964/#968) the same path
raised ``FileNotFoundError`` first.

The fix goes straight to ``load_from_waves`` when no cached candidate exists.
These tests pin that on a private, empty ``LSMS_DATA_DIR``:

* no ``subprocess`` call of any kind is made on the way to the crosswalk;
* no ``"Makefile build failed"`` warning is logged;
* ``Country('Malawi').panel_ids`` still returns the dict, and the result is
  written to the JSON-tier cache under the private root (never in-tree).

The per-wave extraction is monkeypatched (``Wave.grab_data``) so the test is
hermetic -- no microdata, no DVC, no S3.  Malawi is the Design-B country with
three waves declaring ``panel_ids`` (2013-14, 2016-17, 2019-20).
"""
from __future__ import annotations

import logging
from collections.abc import Mapping
import shutil
import subprocess
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import country as country_mod
from lsms_library.paths import countries_root

COUNTRY = "Malawi"


def _fake_panel_ids_frame(wave: str) -> pd.DataFrame:
    """A ``(t, i) -> previous_i`` frame shaped like Malawi's real one."""
    rows = {
        "2013-14": [("0001-001", "101012150028"), ("0002-001", "101015620053")],
        "2016-17": [("0010-001", "0001-001"), ("0011-001", "0002-001")],
        "2019-20": [("0100-001", "0010-001")],
    }[wave]
    idx = pd.MultiIndex.from_tuples([(wave, i) for i, _ in rows], names=["t", "i"])
    return pd.DataFrame({"previous_i": [p for _, p in rows]}, index=idx)


@pytest.fixture
def cold_private_root(tmp_path, monkeypatch):
    """An empty ``LSMS_DATA_DIR`` with every cached candidate absent, a
    serial in-process build, and a ``Wave.grab_data`` that serves a small
    ``panel_ids`` frame instead of reading microdata."""
    droot = tmp_path / "data"
    droot.mkdir()
    monkeypatch.setenv("LSMS_DATA_DIR", str(droot))
    monkeypatch.setenv("LSMS_BUILD_WORKERS", "1")
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    monkeypatch.delenv("LSMS_BUILD_BACKEND", raising=False)

    real_grab = country_mod.Wave.grab_data

    def grab_data(self, request):
        if request == "panel_ids":
            return _fake_panel_ids_frame(self.year)
        return real_grab(self, request)

    monkeypatch.setattr(country_mod.Wave, "grab_data", grab_data)

    # The in-tree committed JSON must not exist for a Design-B country, or the
    # cached-candidate lookup would short-circuit before the path under test.
    intree = Path(countries_root()) / COUNTRY / "_"
    assert not (intree / "panel_ids.json").exists(), "Malawi is Design B (#894)"
    assert not (intree / "panel_ids.parquet").exists(), "in-tree parquet (GH #803)"
    return droot


def _forbid_subprocess(monkeypatch):
    def boom(*args, **kwargs):  # pragma: no cover - only reached on regression
        raise AssertionError(f"subprocess call on the panel_ids path: {args!r}")

    for name in ("run", "Popen", "check_output", "check_call", "call"):
        monkeypatch.setattr(subprocess, name, boom)


def test_cold_design_b_panel_ids_never_shells_out(cold_private_root, monkeypatch, caplog):
    _forbid_subprocess(monkeypatch)
    caplog.set_level(logging.WARNING, logger="lsms_library.country")

    c = ll.Country(COUNTRY)
    result = c.panel_ids  # a property, not a method

    assert isinstance(result, Mapping) and len(result) > 0, "Design B must still derive the crosswalk"
    assert isinstance(c.updated_ids, Mapping) and len(c.updated_ids) > 0
    offending = [r.getMessage() for r in caplog.records if "Makefile build failed" in r.getMessage()]
    assert not offending, offending

    # The derived crosswalk is cached under the PRIVATE root, never in-tree.
    cached = list((cold_private_root / COUNTRY / "_").glob("panel_ids.*"))
    assert cached, "the derived crosswalk must be cached under LSMS_DATA_DIR"
    assert not list((Path(countries_root()) / COUNTRY / "_").glob("panel_ids.*"))


def test_cold_design_b_panel_ids_with_no_make_on_path(cold_private_root, monkeypatch, caplog):
    """GH #968: ``make`` absent must take the no-make path cleanly."""
    real_which = shutil.which

    def which(cmd, *a, **k):
        return None if cmd == "make" else real_which(cmd, *a, **k)

    monkeypatch.setattr(shutil, "which", which)

    calls: list[tuple] = []

    def no_make(*args, **kwargs):
        calls.append(args)
        raise FileNotFoundError(2, "No such file or directory: 'make'")

    for name in ("run", "Popen", "check_output", "check_call", "call"):
        monkeypatch.setattr(subprocess, name, no_make)
    caplog.set_level(logging.WARNING, logger="lsms_library.country")

    result = ll.Country(COUNTRY).panel_ids
    assert isinstance(result, Mapping) and len(result) > 0
    assert not [r for r in caplog.records if "Makefile build failed" in r.getMessage()]
    # ``run_make_target`` would catch the FileNotFoundError and downgrade it
    # to a *different* warning ("Makefile execution failed"), so the only
    # check that cannot be fooled is: no subprocess was attempted at all.
    assert calls == [], calls

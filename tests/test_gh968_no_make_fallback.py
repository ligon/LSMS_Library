"""GH #968: a wave's script-path table builds without ``make``.

``Wave.grab_data`` used to call ``make`` unguarded, so on a machine without it
every wave-script table raised ``FileNotFoundError`` (Uganda ``shocks``,
measured 2026-09-26).  The call now goes through
``country._build_wave_script_target``, which runs the wave script directly
when ``make`` is not installed.

Data-free: a two-file fake country in ``tmp_path`` whose Makefile has the same
wave rule as the real ones, ``(cd $(@D) && python ./$(<F))``.
"""
from __future__ import annotations

import shutil
import textwrap
import warnings
from pathlib import Path

import pytest

from lsms_library import _build_registry as R
from lsms_library import country as C

SCRIPT = textwrap.dedent("""\
    import pathlib
    here = pathlib.Path(__file__).resolve()
    here.with_suffix(".parquet").write_text(here.parent.parent.name, encoding="utf-8")
""")

MAKEFILE = textwrap.dedent("""\
    ../%/_/things.parquet: ../%/_/things.py
    \t(cd $(@D) && python ./$(<F))
""")


@pytest.fixture
def fake_country(tmp_path: Path) -> Path:
    (tmp_path / "Fake" / "_").mkdir(parents=True)
    (tmp_path / "Fake" / "_" / "Makefile").write_text(MAKEFILE, encoding="utf-8")
    wave = tmp_path / "Fake" / "2020-21" / "_"
    wave.mkdir(parents=True)
    (wave / "things.py").write_text(SCRIPT, encoding="utf-8")
    return tmp_path / "Fake"


def _target() -> Path:
    return Path("2020-21") / "_" / "things.parquet"


def test_without_make_the_wave_script_runs_directly(fake_country, monkeypatch):
    monkeypatch.setattr(C.shutil, "which", lambda *a, **k: None)
    monkeypatch.setattr(C, "_NO_MAKE_WARNED", False)
    with pytest.warns(RuntimeWarning, match="make is not on PATH"):
        C._build_wave_script_target(fake_country / "_", _target())
    out = fake_country / "2020-21" / "_" / "things.parquet"
    assert out.read_text(encoding="utf-8") == "2020-21"


def test_the_no_make_warning_is_issued_once(fake_country, monkeypatch):
    monkeypatch.setattr(C.shutil, "which", lambda *a, **k: None)
    monkeypatch.setattr(C, "_NO_MAKE_WARNED", False)
    with pytest.warns(RuntimeWarning):
        C._build_wave_script_target(fake_country / "_", _target())
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        C._build_wave_script_target(fake_country / "_", _target())


def test_without_make_or_script_the_error_names_the_script(fake_country, monkeypatch):
    monkeypatch.setattr(C.shutil, "which", lambda *a, **k: None)
    (fake_country / "2020-21" / "_" / "things.py").unlink()
    with pytest.raises(FileNotFoundError, match="things.py"):
        C._build_wave_script_target(fake_country / "_", _target())


@pytest.mark.skipif(shutil.which("make") is None, reason="needs GNU make")
def test_with_make_the_makefile_rule_builds_it(fake_country):
    C._build_wave_script_target(fake_country / "_", _target())
    out = fake_country / "2020-21" / "_" / "things.parquet"
    assert out.read_text(encoding="utf-8") == "2020-21"


def test_the_runner_is_not_folded_into_any_build_fingerprint():
    """Editing the invocation must not cold-rebuild the corpus.  Asserted
    structurally, as tests/test_null_read_guard.py does for its audit."""
    assert "lsms_library.country._build_wave_script_target" in R._EXCLUDED_CALLABLES
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    # Keyed on the part's own qualname: grab_data's source legitimately
    # contains the CALL, and that is not a leak of the runner's body.
    leaked = [p.split("=")[0] for p in parts
              if p.split("=")[0].endswith("._build_wave_script_target")]
    assert not leaked, f"runner source leaked into the build fingerprint: {leaked}"

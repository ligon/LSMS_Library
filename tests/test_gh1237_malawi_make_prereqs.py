"""GH #1237: every Malawi per-wave Make rule depends on its wave script.

Malawi's per-wave rules use the data-root target form
``$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/<table>.parquet``.  Until #1237 they named
only ``malawi.py`` (``food_acquired``'s named nothing), so under
``LSMS_BUILD_BACKEND=make`` an edit to ``<wave>/_/<table>.py`` never rebuilt an
existing wave parquet: make judged it current, and the country concatenator's
``var/<table>.parquet`` (whose prerequisites are those wave parquets) stayed
current with it.  This is the GH #866 class -- make is not a judge of whether
its own output is current -- at wave grain.

The tests ask GNU make itself, never a text match on the rule:

* ``-W <wave script>`` ("pretend this file was just edited") against a root in
  which the target already exists and is newer than every source.  Without the
  prerequisite make answers "up to date"; with it make prints the recipe.
* A pattern-rule prerequisite that does not exist makes the rule inapplicable
  for that stem.  So every wave a rule is asked for (the waves named in the
  table's ``*_parquet`` variable) must carry the script, and a cold ``make -n``
  must run it there; a wave WITHOUT the script answers "No rule to make target"
  (previously the recipe ran and ``python`` failed on the missing file -- the
  same non-zero exit to ``run_make_target``).

``LSMS_COUNTRIES_ROOT`` selects the Makefile under test, so the same file can
be pointed at another checkout's config tree.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from lsms_library.paths import countries_root

needs_make = pytest.mark.skipif(shutil.which("make") is None,
                                reason="GNU make not on PATH")

_RULE = re.compile(
    r"^\$\(LSMS_DATA_ROOT\)/\$\(COUNTRY\)/%/_/(?P<table>\w+)\.parquet\s*:",
    re.MULTILINE)

# The ten per-wave rules at #1237: the nine the issue names plus
# food_acquired's (which had no prerequisite at all).  Pinned so a rule that
# disappears, or a new one, is a visible change rather than a silent skip.
EXPECTED_TABLES = {
    "food_acquired", "food_coping", "months_food_inadequate",
    "crop_production", "plot_inputs", "livestock", "anthropometry",
    "plot_labor", "people_last7days", "community_prices",
}


def _make_dir() -> Path:
    countries_root.cache_clear()
    return countries_root() / "Malawi" / "_"


def _per_wave_tables() -> list[str]:
    text = (_make_dir() / "Makefile").read_text()
    return sorted({m.group("table") for m in _RULE.finditer(text)})


def _wave_dirs() -> list[str]:
    return sorted(p.name for p in _make_dir().parent.iterdir()
                  if p.is_dir() and re.fullmatch(r"[12]\d{3}-\d{2}", p.name))


def _make(args: list[str], root: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["make", f"LSMS_DATA_ROOT={root}", *args],
                          cwd=_make_dir(), capture_output=True, text=True)


def _requested_waves(table: str, root: Path) -> list[str]:
    """The waves whose parquet the country target asks for, as make expands
    the table's ``<table>_parquet`` variable."""
    res = _make(["-s", "--eval=print-%: ; @echo $($*)",
                 f"print-{table}_parquet"], root)
    assert res.returncode == 0, res.stderr
    prefix = f"{root}/Malawi/"
    waves = []
    for path in res.stdout.split():
        assert path.startswith(prefix) and path.endswith(f"/_/{table}.parquet"), path
        waves.append(path[len(prefix):].split("/")[0])
    assert waves, f"{table}_parquet expands to nothing"
    return waves


def _cases() -> list[tuple[str, str]]:
    if shutil.which("make") is None:
        return []
    root = Path("/nonexistent-gh1237-root")
    return [(t, w) for t in _per_wave_tables() for w in _requested_waves(t, root)]


def _warm_target(root: Path, table: str, wave: str) -> Path:
    """A stand-in wave parquet newer than every Malawi source in the checkout:
    make only compares mtimes, so an empty file is a warm cache to it."""
    target = root / "Malawi" / wave / "_" / f"{table}.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch()
    newest = max(p.stat().st_mtime for p in _make_dir().parent.glob("*/*")
                 if p.is_file())
    stamp = max(time.time(), newest + 1)
    os.utime(target, (stamp, stamp))
    return target


@needs_make
def test_per_wave_rules_are_the_expected_ten():
    assert set(_per_wave_tables()) == EXPECTED_TABLES


@needs_make
@pytest.mark.parametrize("table,wave", _cases())
def test_wave_script_edit_rebuilds_existing_wave_parquet(tmp_path, table, wave):
    target = _warm_target(tmp_path, table, wave)
    script = f"../{wave}/_/{table}.py"
    recipe = f"cd ../{wave}/_/ && python {table}.py"

    # Control: nothing edited, the stand-in is current.
    plain = _make(["-n", str(target)], tmp_path)
    assert plain.returncode == 0, plain.stderr
    assert recipe not in plain.stdout, plain.stdout

    # The wave script "just edited": the rule must depend on it.
    edited = _make(["-n", "-W", script, str(target)], tmp_path)
    assert edited.returncode == 0, edited.stderr
    assert recipe in edited.stdout, (
        f"{table} {wave}: make judged {target.name} current after an edit "
        f"to {script} (GH #1237)\n{edited.stdout}{edited.stderr}")


@needs_make
@pytest.mark.parametrize("table,wave", _cases())
def test_requested_wave_has_script_and_builds_cold(tmp_path, table, wave):
    # A missing prerequisite would make the pattern rule inapplicable here.
    assert (_make_dir().parent / wave / "_" / f"{table}.py").is_file()
    target = tmp_path / "Malawi" / wave / "_" / f"{table}.parquet"
    res = _make(["-n", str(target)], tmp_path)
    assert res.returncode == 0, res.stderr
    assert f"cd ../{wave}/_/ && python {table}.py" in res.stdout
    assert not (tmp_path / "Malawi").exists(), "a dry-run Make probe wrote data"


@needs_make
def test_wave_without_script_has_no_rule(tmp_path):
    missing = [(t, w) for t in _per_wave_tables() for w in _wave_dirs()
               if not (_make_dir().parent / w / "_" / f"{t}.py").exists()]
    # 2004-05 carries only food_acquired; 2016-17 has no community_prices.
    assert ("plot_inputs", "2004-05") in missing
    assert ("community_prices", "2016-17") in missing
    for table, wave in missing:
        target = tmp_path / "Malawi" / wave / "_" / f"{table}.parquet"
        res = _make(["-n", str(target)], tmp_path)
        assert res.returncode != 0, (table, wave, res.stdout)
        assert "No rule to make target" in res.stderr, (table, wave, res.stderr)
        assert f"python {table}.py" not in res.stdout

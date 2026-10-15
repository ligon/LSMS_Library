"""The panel crosswalk is a REVIEWED SOURCE, not a build product (GH #914, #894).

``panel_ids.json`` / ``updated_ids.json`` are committed to the config tree and
shipped in the wheel.  Nine countries persist the crosswalk that way (design A);
Malawi and Nigeria derive theirs from per-wave YAML into a cache parquet
(design B).  This module pins two things:

**#914 -- nothing regenerates the crosswalk on the runtime build path.**  Until
this landed each of the nine Makefiles carried

    panel_ids.json updated_ids.json: panel_ids.py
            python panel_ids.py

so ``make`` regenerated the JSON whenever ``panel_ids.py`` was the newer file --
and on a pip install *which of the two is newer is decided by the millisecond
the wheel was unpacked*.  Measured on the real 0.13.0 wheel: the zip stores
``.json`` before ``.py`` alphabetically, and 5 of the 9 countries straddled a
filesystem tick and landed with the script ~1 ms ahead (Uganda, Niger,
Burkina_Faso, Ethiopia, Tanzania); the other 4 landed on the same nanosecond and
were fine.  Pure luck, which is why the bug is intermittent across deployments.
The recipe then ran ``python panel_ids.py`` with cwd inside ``site-packages``
and the script wrote a bare relative path, so on any shared install (a
JupyterHub, a system Python, anything installed as root) it died with
``PermissionError`` raised three layers inside a ``make`` subprocess.

``Country._finalize_result`` calls ``id_walk(df, self.updated_ids)`` on *every*
table (``country.py:3107``), so the rule was reachable from any read of any of
the nine countries -- Uganda additionally routed it through
``$(parquet) $(var): panel_ids.json``, which is why the report's traceback shows
``make -s ../2005-06/_/food_acquired.parquet``.

``test_make_never_regenerates_the_crosswalk`` reproduces the bug's *trigger*
rather than its spelling: it makes the script strictly newer than the JSON and
asserts ``make`` still has nothing to do.  A future edit that reintroduces a
rule under any spelling fails it.  ``-n`` writes nothing and ``os.utime`` does
not change file content, so the test is read-only with respect to git.

**#894 -- a twelfth country cannot pick the other design by accident.**
``test_every_panel_ids_country_declares_its_design`` enumerates the countries
declaring ``panel_ids`` and requires each to be design A (tracked JSON on disk)
or named in ``DESIGN_B_PENDING``.  Converting Malawi and Nigeria to A is #894's
remaining work; listing them here makes the deferral visible in code.
"""
from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path

import pytest

from lsms_library.paths import countries_root

# The nine countries that persist the crosswalk as committed JSON (design A).
DESIGN_A = [
    "Burkina_Faso", "Ethiopia", "EthiopiaRHS", "GhanaLSS", "Mali",
    "Niger", "Senegal", "Tanzania", "Uganda",
]

# Design B (per-wave YAML -> cache parquet only).  #894 item 2 proposes giving
# these two a committed JSON so the crosswalk has a diff surface at review time;
# that is behaviour-neutral at the API but needs its own verification, so it is
# deliberately NOT done here.  Listed rather than grandfathered by silence.
DESIGN_B_PENDING = {"Malawi", "Nigeria"}


def _root() -> Path:
    return countries_root()


def _panel_ids_countries() -> list[str]:
    """Every country whose data_scheme declares panel_ids."""
    found = []
    for cdir in sorted(_root().iterdir()):
        scheme = cdir / "_" / "data_scheme.yml"
        if not scheme.is_file():
            continue
        # Read as text: data_scheme.yml uses `!make` tags that a bare
        # yaml.safe_load refuses, and we only need to know the key is declared.
        for line in scheme.read_text(errors="replace").splitlines():
            if re.match(r"^\s*panel_ids\s*:", line):
                found.append(cdir.name)
                break
    return found


# --------------------------------------------------------------------------
# GH #914 -- the crosswalk is never regenerated on the build path
# --------------------------------------------------------------------------

@pytest.mark.parametrize("country", DESIGN_A)
def test_make_never_regenerates_the_crosswalk(country, tmp_path):
    """The bug's trigger: script newer than JSON must NOT provoke a rebuild.

    This is the condition a pip install produces by accident.  Before the fix
    ``make -n panel_ids.json`` printed ``python panel_ids.py`` here; it must now
    print nothing to run.
    """
    cdir = _root() / country / "_"
    script, js = cdir / "panel_ids.py", cdir / "panel_ids.json"
    assert script.is_file() and js.is_file()

    original = (script.stat().st_atime, script.stat().st_mtime)
    try:
        # Make the script strictly newer than both JSONs -- the worst case.
        future = time.time() + 60
        os.utime(script, (future, future))
        for target in ("panel_ids.json", "updated_ids.json"):
            proc = subprocess.run(
                ["make", "-n", target], cwd=cdir,
                capture_output=True, text=True, timeout=120,
            )
            assert proc.returncode == 0, (
                f"{country}: make -n {target} failed:\n{proc.stderr}"
            )
            assert "panel_ids.py" not in proc.stdout, (
                f"{country}: `make {target}` wants to run the generator even "
                f"though the JSON is a committed source (GH #914).  Output:\n"
                f"{proc.stdout}"
            )
    finally:
        os.utime(script, original)


@pytest.mark.parametrize("country", DESIGN_A)
def test_the_maintainer_target_still_regenerates(country):
    """Control for the test above: `make panel-ids` must still run the script.

    Without this, a Makefile that simply deleted the rule would pass the
    regression test while losing the ability to regenerate at all.
    """
    cdir = _root() / country / "_"
    proc = subprocess.run(
        ["make", "-n", "panel-ids"], cwd=cdir,
        capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, f"{country}: no `panel-ids` target:\n{proc.stderr}"
    assert "panel_ids.py" in proc.stdout, (
        f"{country}: `make panel-ids` does not run the generator:\n{proc.stdout}"
    )


@pytest.mark.parametrize("country", DESIGN_A)
def test_no_makefile_declares_the_json_as_a_rule_target(country):
    """Static guard: the JSON must never appear on the left of a rule.

    Belt-and-suspenders for the behavioural test above -- this one names the
    exact construct to avoid, so the failure message teaches the rule.
    """
    text = (_root() / country / "_" / "Makefile").read_text()
    for lineno, line in enumerate(text.splitlines(), 1):
        if line.startswith("#") or not line.strip():
            continue
        target_part = line.split(":", 1)[0] if ":" in line else ""
        if re.search(r"\b(panel_ids|updated_ids)\.json\b", target_part):
            pytest.fail(
                f"{country}/_/Makefile:{lineno} declares a committed source as "
                f"a rule target (GH #914):\n    {line}\n"
                f"The crosswalk is regenerated by `make panel-ids`, not by the "
                f"build path."
            )


@pytest.mark.parametrize("country", DESIGN_A)
def test_the_generator_writes_beside_itself_not_into_cwd(country):
    """A bare relative write lands wherever the caller happened to be.

    Under `make` that is the package's country directory; the whole point of
    #914 is that this is not writable on a shared install.  Resolving off
    ``__file__`` also means a maintainer running the script from elsewhere
    updates the tracked files rather than scattering copies.
    """
    text = (_root() / country / "_" / "panel_ids.py").read_text()
    for bad in ("open('panel_ids.json'", 'open("panel_ids.json"',
                "open('updated_ids.json'", 'open("updated_ids.json"'):
        assert bad not in text, (
            f"{country}/_/panel_ids.py writes a bare relative path ({bad}...), "
            f"which resolves against the process's cwd (GH #914).  Write "
            f"relative to Path(__file__).parent instead."
        )


@pytest.mark.parametrize("country", DESIGN_A)
def test_clean_does_not_delete_the_committed_sources(country):
    """`make clean` must not remove a tracked, reviewed source.

    Three countries used to `rm -f panel_ids.json updated_ids.json` in `clean`,
    which -- now that there is no rule to remake them -- would leave the country
    unbuildable until a `git checkout`.
    """
    text = (_root() / country / "_" / "Makefile").read_text()
    for lineno, line in enumerate(text.splitlines(), 1):
        if line.startswith("\t") and "rm " in line and ".json" in line:
            assert not re.search(r"\b(panel_ids|updated_ids)\.json\b", line), (
                f"{country}/_/Makefile:{lineno} deletes a committed source:\n"
                f"    {line.strip()}"
            )


# --------------------------------------------------------------------------
# GH #894 -- one convention, declared
# --------------------------------------------------------------------------

def test_every_panel_ids_country_declares_its_design():
    """No country may persist the crosswalk a third way, or drift silently.

    Design A is the convention (see CLAUDE.md): the crosswalk is a
    harmonization decision, small and identical for every user, so it belongs
    in the config tree where a change shows up as a reviewable diff.
    """
    declared = _panel_ids_countries()
    assert declared, "no country declares panel_ids -- the probe is broken"

    unclassified = []
    for country in declared:
        has_json = (_root() / country / "_" / "panel_ids.json").is_file()
        if has_json and country not in DESIGN_B_PENDING:
            continue
        if country in DESIGN_B_PENDING and not has_json:
            continue
        unclassified.append((country, has_json))

    assert not unclassified, (
        "these countries declare panel_ids but match neither design (GH #894):\n"
        + "\n".join(f"  {c}: committed JSON present={j}" for c, j in unclassified)
        + "\nEither ship a committed _/panel_ids.json (design A, the "
          "convention) or add the country to DESIGN_B_PENDING with a reason."
    )


def test_design_a_roster_is_exactly_the_countries_shipping_json():
    """The DESIGN_A list above must not drift from what is on disk."""
    on_disk = {c for c in _panel_ids_countries()
               if (_root() / c / "_" / "panel_ids.json").is_file()}
    assert on_disk == set(DESIGN_A), (
        f"DESIGN_A is stale: on disk {sorted(on_disk)}, "
        f"listed {sorted(DESIGN_A)}"
    )


@pytest.mark.parametrize("country", DESIGN_A)
def test_both_crosswalk_files_ship(country):
    """`updated_ids.json` is read separately from `panel_ids.json`.

    ``_compute_panel_ids`` aggregates them independently, so a country that
    ships one and not the other fails at read time, not at build time.
    """
    cdir = _root() / country / "_"
    for name in ("panel_ids.json", "updated_ids.json"):
        assert (cdir / name).is_file(), f"{country} ships no {name}"

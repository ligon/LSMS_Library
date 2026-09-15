"""GH #803: a parquet inside the config tree (``countries_root()``) is never an input.

The mechanism this pins (attribution in ``slurm_logs/gh797/attribution/REPORT.org``):

1. a hashed-input edit moves ``Country._table_cache_hash``; the L2-country
   parquet grades ``stale`` and the read descends into ``load_from_waves``;
2. ``_evict_hashless_wave_caches`` evicts hashless wave parquets under
   ``data_root()`` ONLY;
3. for a script-path table ``Wave.grab_data`` used to consider two candidates,
   ``data_root()/{wave}/_/{table}.parquet`` and the IN-TREE
   ``{countries_root()}/{C}/{wave}/_/{table}.parquet``.  The v0.8.0 gate skips
   a candidate only when it grades ``stale``; a parquet with no embedded hash
   grades ``legacy`` and is trusted.  So a hashless in-tree artefact was read
   INSTEAD of running the wave script;
4. the stale frames were concatenated and the L2-country parquet written with
   the correct, current hash -- fresh forever, never rebuilt.

The in-tree artefacts come from the WRITE side of the ``.pth`` trap:
``to_parquet`` -> ``_resolve_data_path`` redirects to ``data_root()`` only when
the *calling script's file* is under the imported package's
``countries_root()``; a script run from any other checkout writes the literal
relative path, i.e. next to itself.  109 such files, 14 countries, on the
main checkout on 2026-08-24.

What is pinned here:

- ``Wave._script_parquet_candidates`` (the ONLY read locations for a
  script-path wave parquet) never names a path under ``countries_root()``;
- a planted hashless in-tree parquet is ignored by ``Wave.grab_data`` and by
  the Country-level build: the wave script RUNS and its output is served;
- ...also when the artefact is NEWER than the script (Make's own timestamp
  check would otherwise call the target up to date and run nothing);
- a hashless parquet UNDER ``data_root()`` is still trusted (``legacy``) --
  that is the v0.8.0 upgrade path and must not change;
- the write-side trap itself (a script that writes in-tree) is LOUD: the
  build fails with ``RuntimeError`` and an ``InTreeParquetWarning`` names the
  file, instead of the in-tree frame being served.

Fixture design.  A fake country under ``LSMS_COUNTRIES_ROOT`` (in-process,
the ``test_gh323_site4_dfs_merge`` pattern) with a private ``LSMS_DATA_DIR``.
The fake wave script deliberately does NOT import ``lsms_library``: Make runs
it with the fake ``LSMS_COUNTRIES_ROOT`` inherited, and a child process
importing the package under a config tree that has no ``.dvc/`` fails at
import with ``dvc.config.ConfigError`` (GH #802).  It therefore writes with
pandas to exactly the path a correctly redirected ``to_parquet`` would choose
(``$LSMS_DATA_DIR/{C}/{wave}/_/{table}.parquet`` -- ``grab_data`` and
``run_make_target.build_env`` both export ``LSMS_DATA_DIR`` to the child).
The READ side is what is under test.
"""
from __future__ import annotations

import os
import time
import warnings
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

import lsms_library.country as C
from lsms_library.country import InTreeParquetWarning, _INTREE_ARTEFACTS_WARNED
from lsms_library.paths import countries_root, data_root

COUNTRY = "Fakeland"
WAVE = "2001"
TABLE = "build_me"

_SCHEME = f"""\
Country: {COUNTRY}
Waves: ['{WAVE}']
Data Scheme:
  {TABLE}:
    index: (t, i)
    x: float
    materialize: make
"""

# Same shape as the real country Makefiles (GhanaLSS/_/Makefile): the wave
# target is named IN-TREE (``../%/_/<table>.parquet``); the recipe cd's into
# the wave dir and runs the script, which writes wherever its own redirect
# says.  A ``%.parquet: %.py`` catch-all mirrors the real files too.
_MAKEFILE = """\
../%/_/build_me.parquet: ../%/_/build_me.py
\tcd $(@D) && python build_me.py

%.parquet: %.py
\tcd $(@D) && python $(<F)
"""

# A correctly redirected write (see module docstring for why not to_parquet).
# The RAN sentinel is the observable that the script executed.
_SCRIPT_REDIRECTED = """\
import os
from pathlib import Path
import pandas as pd
out = Path(os.environ["LSMS_DATA_DIR"]) / "Fakeland" / "2001" / "_" / "build_me.parquet"
out.parent.mkdir(parents=True, exist_ok=True)
df = pd.DataFrame({"t": ["2001", "2001"], "i": ["h1", "h2"], "x": [1.0, 2.0]}).set_index(["t", "i"])
df.to_parquet(out)
Path(__file__).with_name("RAN").write_text("1")
"""

# The write-side trap: ``to_parquet`` from a checkout that is not the imported
# package falls through ``_resolve_data_path`` and writes the literal relative
# path -- next to the script.  Emulated by writing to cwd.
_SCRIPT_INTREE = """\
from pathlib import Path
import pandas as pd
df = pd.DataFrame({"t": ["2001"], "i": ["trap"], "x": [-1.0]}).set_index(["t", "i"])
df.to_parquet("build_me.parquet")
Path(__file__).with_name("RAN").write_text("1")
"""

SCRIPT_FRAME = pd.DataFrame({"t": ["2001", "2001"], "i": ["h1", "h2"],
                             "x": [1.0, 2.0]}).set_index(["t", "i"])
PLANTED = pd.DataFrame({"t": ["2001"] * 3, "i": ["p1", "p2", "p3"],
                        "x": [99.0, 99.0, 99.0]}).set_index(["t", "i"])


def _hashless(path: Path, df: pd.DataFrame = PLANTED) -> None:
    """Write a parquet with NO ``lsms_cache_hash`` -- the artefact class."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    md = pq.read_schema(path).metadata or {}
    assert b"lsms_cache_hash" not in md


@pytest.fixture
def fakeland(tmp_path, monkeypatch):
    croot = tmp_path / "countries"
    droot = tmp_path / "data"
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(croot))
    monkeypatch.setenv("LSMS_DATA_DIR", str(droot))
    monkeypatch.setenv("LSMS_ISSUES_LOG", str(tmp_path / "issues.log"))
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    _INTREE_ARTEFACTS_WARNED.clear()
    assert countries_root() == croot and data_root() == droot

    c = croot / COUNTRY
    (c / "_").mkdir(parents=True)
    (c / WAVE / "_").mkdir(parents=True)
    (c / "_" / "data_scheme.yml").write_text(_SCHEME)
    (c / "_" / "Makefile").write_text(_MAKEFILE)

    class Fake:
        country_dir = c
        wave_dir = c / WAVE / "_"
        script = c / WAVE / "_" / f"{TABLE}.py"
        ran = c / WAVE / "_" / "RAN"
        intree = c / WAVE / "_" / f"{TABLE}.parquet"
        external = droot / COUNTRY / WAVE / "_" / f"{TABLE}.parquet"

        def build(self, script=_SCRIPT_REDIRECTED, plant=True, planted_newer=False):
            self.script.write_text(script)
            if plant:
                _hashless(self.intree)
                now = time.time()
                stamp = now + 3600 if planted_newer else now - 3600
                os.utime(self.intree, (stamp, stamp))
                # make the script strictly older than a "newer" artefact and
                # strictly newer than an "older" one
                os.utime(self.script, (now, now))
            return C.Country(COUNTRY)

    try:
        yield Fake()
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()
        _INTREE_ARTEFACTS_WARNED.clear()


def _x_values(df: pd.DataFrame) -> set[float]:
    return set(df["x"].astype(float).tolist())


# ---------------------------------------------------------------------------
# Unit level: the candidate list
# ---------------------------------------------------------------------------

def test_candidates_are_under_data_root_only(fakeland):
    w = fakeland.build()[WAVE]
    cands = w._script_parquet_candidates(TABLE)
    assert cands, "no read candidates at all"
    for p in cands:
        assert p.is_relative_to(data_root()), p
        assert not p.is_relative_to(countries_root()), f"in-tree candidate: {p}"
    assert fakeland.intree not in cands


@pytest.mark.parametrize("country,wave", [("GhanaLSS", "1991-92"), ("Uganda", "2005-06"),
                                          ("Tanzania", "2008-09")])
def test_real_country_candidates_never_in_tree(country, wave):
    """Config-only (no data): the real trees name no in-tree read location."""
    w = C.Country(country)[wave]
    cands = w._script_parquet_candidates("food_acquired")
    assert cands
    for p in cands:
        assert p.is_relative_to(data_root()), p
        assert not p.is_relative_to(countries_root()), f"in-tree candidate: {p}"


# ---------------------------------------------------------------------------
# Behaviour: the planted artefact is ignored and the script runs
# ---------------------------------------------------------------------------

def test_wave_grab_data_runs_script_and_ignores_planted(fakeland):
    """The replica in miniature: cold data_root, hashless in-tree parquet
    (older than the script), script-path table.  Pre-#803 the artefact was
    read as ``legacy`` and the script never ran."""
    c = fakeland.build(plant=True, planted_newer=False)
    before = fakeland.intree.stat()
    with pytest.warns(InTreeParquetWarning, match="ignoring in-tree parquet"):
        df = c[WAVE].build_me()
    assert fakeland.ran.exists(), "wave script did not run"
    assert fakeland.external.exists(), "script output not under data_root"
    assert _x_values(df) == {1.0, 2.0}
    assert 99.0 not in _x_values(df), "planted in-tree content was served"
    after = fakeland.intree.stat()
    assert (before.st_mtime, before.st_size) == (after.st_mtime, after.st_size), \
        "in-tree artefact was touched"


def test_planted_newer_than_script_still_rebuilds(fakeland):
    """Make stats the target by its in-tree name.  An artefact NEWER than the
    script makes the target look up to date, so without ``-B`` nothing runs
    and the wave comes back empty.  The artefact must not be an input to
    Make's decision either."""
    c = fakeland.build(plant=True, planted_newer=True)
    with pytest.warns(InTreeParquetWarning):
        df = c[WAVE].build_me()
    assert fakeland.ran.exists(), "wave script did not run (Make called the in-tree artefact up to date)"
    assert _x_values(df) == {1.0, 2.0}


def test_country_level_build_ignores_planted(fakeland):
    """Through ``_aggregate_wave_data`` -> ``load_from_waves``: the same
    planted artefact, and the L2-country parquet that gets written must carry
    the script's content, not the artefact's."""
    c = fakeland.build(plant=True, planted_newer=True)
    with pytest.warns(InTreeParquetWarning):
        df = c.build_me()
    assert fakeland.ran.exists()
    assert _x_values(df) == {1.0, 2.0}
    var = data_root(COUNTRY) / "var" / f"{TABLE}.parquet"
    assert var.exists()
    assert _x_values(pd.read_parquet(var)) == {1.0, 2.0}
    assert b"lsms_cache_hash" in (pq.read_schema(var).metadata or {}), \
        "L2-country parquet written without a hash"
    # the artefact is still there, still hashless, still ignored
    assert fakeland.intree.exists()


def test_hashless_under_data_root_is_still_trusted(fakeland):
    """The v0.8.0 upgrade path is unchanged: a hashless parquet UNDER
    data_root grades ``legacy`` and is read without running the script."""
    c = fakeland.build(plant=False)
    _hashless(fakeland.external, SCRIPT_FRAME)
    with warnings.catch_warnings():
        warnings.simplefilter("error", InTreeParquetWarning)
        df = c[WAVE].build_me()
    assert not fakeland.ran.exists(), "script ran although a legacy data_root parquet was present"
    assert _x_values(df) == {1.0, 2.0}


def test_write_side_trap_is_loud_not_served(fakeland):
    """A script that writes IN-TREE (the write-side .pth trap) used to have
    its output picked up by the second candidate loop / by
    ``run_make_target``'s in-tree output candidates.  Now: nothing under
    data_root appears, the build fails loudly, and the warning names the
    artefact.  Country-level in-tree ``var/`` and ``_/`` parquets (the
    Guyana ``assets`` / Nigeria ``var/`` class) are planted too and must not
    be served either."""
    c = fakeland.build(script=_SCRIPT_INTREE, plant=False)
    _hashless(fakeland.country_dir / "var" / f"{TABLE}.parquet")
    _hashless(fakeland.country_dir / "_" / f"{TABLE}.parquet")
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        with pytest.raises(RuntimeError, match="Could not materialize"):
            c.build_me()
    assert fakeland.ran.exists(), "script did not run"
    assert fakeland.intree.exists(), "trap script did not write in-tree (test is vacuous)"
    assert not fakeland.external.exists()
    named = {str(w.message) for w in rec if issubclass(w.category, InTreeParquetWarning)}
    assert any(str(fakeland.intree) in m for m in named), named
    assert any(str(fakeland.country_dir / "var" / f"{TABLE}.parquet") in m for m in named), named
    var = data_root(COUNTRY) / "var" / f"{TABLE}.parquet"
    assert not var.exists(), "an L2-country parquet was written from in-tree content"


# ---------------------------------------------------------------------------
# Cache-hash cost of the warning: zero, and pinned
# ---------------------------------------------------------------------------

def test_warning_helper_is_not_folded_into_any_build_fingerprint():
    """``_warn_intree_parquet_artefact`` is reached from ``grab_data`` (tagged)
    and ``run_make_target`` (nested in the tagged ``_aggregate_wave_data``),
    so without its ``_EXCLUDED_CALLABLES`` entry re-wording the warning would
    move every table's fingerprint and cold-rebuild the corpus.  Asserted
    structurally (the ``test_null_read_guard`` shape) so it still means
    something after any legitimate hash change elsewhere."""
    from lsms_library import _build_registry as R
    seen, parts = set(), []
    for _qn, (fn, _tables) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    assert "lsms_library.country._warn_intree_parquet_artefact" not in seen
    leaked = [p[:80] for p in parts if "_warn_intree_parquet_artefact=" in p]
    assert not leaked, f"warning helper source leaked into the build fingerprint: {leaked}"
    # ...while the candidate list, which DOES decide what is read, is folded.
    assert any("_script_parquet_candidates=" in p for p in parts), \
        "_script_parquet_candidates is build logic and must be versioned"


def test_rewording_the_warning_moves_no_hash(monkeypatch):
    """Behavioural twin: perturb the helper's source the way
    ``test_dynamically_dispatched_framework_helpers_are_versioned`` perturbs a
    versioned helper, and assert the fingerprint and a real country's table
    hash do NOT move."""
    import inspect
    from lsms_library import _build_registry as R
    target = inspect.unwrap(C._warn_intree_parquet_artefact)
    c = C.Country("Uganda")
    R.clear_caches()
    fp_before = R.build_transforms_fingerprint("food_acquired")
    h_before = c._table_cache_hash("food_acquired", c.waves)
    real = inspect.getsource
    monkeypatch.setattr(inspect, "getsource",
                        lambda o: real(o) + "\n_reworded = 1\n" if o is target else real(o))
    R.clear_caches()
    assert R.build_transforms_fingerprint("food_acquired") == fp_before
    assert c._table_cache_hash("food_acquired", c.waves) == h_before

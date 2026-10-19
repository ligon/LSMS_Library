"""GH #1039: ``LSMS_BUILD_BACKEND=make`` builds a ``materialize: make`` table
that has wave scripts but NO country-level rule from its waves.

Before the fix the make backend sent every ``materialize: make`` table straight
to ``run_make_target(method_name)`` -- the COUNTRY target.  Where the country
has neither a Make rule for ``$(VAR_DIR)/<t>.parquet`` nor a ``_/<t>.py``
script, that target cannot exist, so:

* cold, the call raised ``Could not materialize`` (Tanzania ``food_acquired``
  since #836 deleted its dead concatenator), or -- in a country with no
  Makefile at all -- ``run_make_target`` returned an empty frame and the
  required-column guard raised instead;
* warm, ``make`` answered "nothing to be done" for the EXISTING
  ``var/<t>.parquet`` it has no rule for, and that file was served as the
  "rebuild" (the make backend's half of #866).

Ruling (@ligon, 2026-10-18): such a table falls back to ``load_from_waves`` --
the framework is authoritative (#788).  A table whose country level DOES have a
builder (a rule, even a multi-target one, or a script) keeps the legacy route,
so a country builder that exists and is broken still fails loudly.

Measured census (2026-10-19, ``make -n`` per target under an empty root): 72
``materialize: make`` cells have no country builder, all with wave scripts;
``Country._has_country_builder`` agrees with that census on all 171 cells.

Fixture: a fake country under ``LSMS_COUNTRIES_ROOT`` with a private
``LSMS_DATA_DIR`` (the ``test_gh803_intree_parquet_never_read`` pattern).  The
fake scripts do not import ``lsms_library`` (GH #802) and write with pandas to
exactly where a redirected ``to_parquet`` would.
"""
from __future__ import annotations

import inspect
import shutil
import warnings
from pathlib import Path

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library import _build_registry as R
from lsms_library.paths import countries_root, data_root

COUNTRY = "Fakeland"
WAVE = "2001"
TABLE = "build_me"

needs_make = pytest.mark.skipif(shutil.which("make") is None,
                                reason="GNU make not on PATH")

_SCHEME = f"""\
Country: {COUNTRY}
Waves: ['{WAVE}']
Data Scheme:
  {TABLE}:
    index: (t, i)
    x: float
    materialize: make
"""

_PREAMBLE = """\
LSMS_DATA_ROOT ?= $(LSMS_DATA_DIR)
VAR_DIR ?= $(LSMS_DATA_ROOT)/Fakeland/var
"""

# The shape of Tanzania/_/Makefile after #836: a wave rule keyed on the
# in-tree name, the ``%.parquet: %.py`` catch-all, and NO country rule.
_MAKEFILE_WAVE_ONLY = _PREAMBLE + """\
../%/_/build_me.parquet: ../%/_/build_me.py
\tcd $(@D) && python build_me.py

%.parquet: %.py
\tcd $(@D) && python $(<F)
"""

# Uganda's ``nutrition.parquet fct.parquet: nutrition.py ...`` shape: a
# multi-target country rule whose recipe is NOT named after the table, so no
# ``_/build_me.py`` exists and a regex for one would miss the rule.
_MAKEFILE_MULTI_TARGET = _MAKEFILE_WAVE_ONLY + """\
$(VAR_DIR)/other.parquet $(VAR_DIR)/build_me.parquet: maker.py
\tpython maker.py
"""

# A country rule whose prerequisite has no rule: make names the PREREQUISITE
# ("..., needed by ..."), which is a rule for the target, not its absence.
_MAKEFILE_UNMAKEABLE_PREREQ = _MAKEFILE_WAVE_ONLY + """\
$(VAR_DIR)/build_me.parquet: $(VAR_DIR)/nowhere.parquet
\tpython maker.py
"""

_WAVE_SCRIPT = """\
import os
from pathlib import Path
import pandas as pd
out = Path(os.environ["LSMS_DATA_DIR"]) / "Fakeland" / "2001" / "_" / "build_me.parquet"
out.parent.mkdir(parents=True, exist_ok=True)
pd.DataFrame({"t": ["2001", "2001"], "i": ["h1", "h2"], "x": [1.0, 2.0]}
             ).set_index(["t", "i"]).to_parquet(out)
"""

# A COUNTRY-level builder (rule recipe or ``_/build_me.py``): distinct content
# so the test can tell which builder answered.
_COUNTRY_SCRIPT = """\
import os
from pathlib import Path
import pandas as pd
out = Path(os.environ["LSMS_DATA_DIR"]) / "Fakeland" / "var" / "build_me.parquet"
out.parent.mkdir(parents=True, exist_ok=True)
pd.DataFrame({"t": ["2001"], "i": ["c1"], "x": [7.0]}).set_index(["t", "i"]).to_parquet(out)
"""

WAVE_X = {1.0, 2.0}
COUNTRY_X = {7.0}
POISON_X = {99.0}


@pytest.fixture
def fakeland(tmp_path, monkeypatch):
    croot = tmp_path / "countries"
    droot = tmp_path / "data"
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(croot))
    monkeypatch.setenv("LSMS_DATA_DIR", str(droot))
    monkeypatch.setenv("LSMS_ISSUES_LOG", str(tmp_path / "issues.log"))
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "make")
    monkeypatch.setenv("LSMS_BUILD_WORKERS", "1")
    monkeypatch.delenv("LSMS_NO_CACHE", raising=False)
    monkeypatch.delenv("VAR_DIR", raising=False)
    monkeypatch.delenv("LSMS_DATA_ROOT", raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    assert countries_root() == croot and data_root() == droot

    c = croot / COUNTRY
    (c / "_").mkdir(parents=True)
    (c / WAVE / "_").mkdir(parents=True)
    (c / "_" / "data_scheme.yml").write_text(_SCHEME)
    (c / WAVE / "_" / f"{TABLE}.py").write_text(_WAVE_SCRIPT)

    class Fake:
        country_dir = c
        var = droot / COUNTRY / "var" / f"{TABLE}.parquet"
        wave_out = droot / COUNTRY / WAVE / "_" / f"{TABLE}.parquet"

        def build(self, makefile: str | None = _MAKEFILE_WAVE_ONLY,
                  country_script: str | None = None,
                  maker: bool = False) -> C.Country:
            if makefile is not None:
                (c / "_" / "Makefile").write_text(makefile)
            if country_script is not None:
                (c / "_" / f"{TABLE}.py").write_text(country_script)
            if maker:
                (c / "_" / "maker.py").write_text(_COUNTRY_SCRIPT)
            return C.Country(COUNTRY)

        def plant_var(self) -> None:
            """A warm, WRONG ``var/`` parquet -- what make used to serve."""
            self.var.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({"t": ["2001"] * 3, "i": ["p1", "p2", "p3"], "x": [99.0] * 3}
                         ).set_index(["t", "i"]).to_parquet(self.var)

    try:
        yield Fake()
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


def _x(df: pd.DataFrame) -> set[float]:
    return set(df["x"].astype(float).tolist())


def _intree_parquets(root: Path) -> list[Path]:
    return sorted(root.rglob("*.parquet"))


# ---------------------------------------------------------------------------
# The cells the ruling is about: no country builder -> build from the waves
# ---------------------------------------------------------------------------

@needs_make
def test_no_country_rule_builds_from_waves_cold(fakeland):
    """Tanzania ``food_acquired``'s shape.  Before: ``Could not materialize``."""
    c = fakeland.build()
    df = c.build_me()
    assert _x(df) == WAVE_X
    assert c._has_country_builder(TABLE) is False
    assert fakeland.wave_out.exists(), "the wave script did not run"
    # The make backend bypasses the parquet tiers: nothing is cached at var/.
    assert not fakeland.var.exists(), "the make backend wrote an L2-country parquet"
    assert not _intree_parquets(fakeland.country_dir), "a parquet landed in the config tree"


def test_no_makefile_builds_from_waves_cold(fakeland):
    """The 32 census cells in countries with no ``_/Makefile`` (Albania,
    Benin, ...).  Before: ``run_make_target`` returned an EMPTY frame and the
    required-column guard raised."""
    c = fakeland.build(makefile=None)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # "Makefile not found" from the wave probe
        df = c.build_me()
    assert _x(df) == WAVE_X
    assert c._has_country_builder(TABLE) is False
    assert not fakeland.var.exists()


@needs_make
def test_no_country_rule_does_not_serve_a_warm_var_parquet(fakeland):
    """The make backend's half of #866.  A rule-less EXISTING target is
    "nothing to be done" to make, so the old route served whatever sat at
    ``var/`` as the rebuild.  Now the waves are rebuilt and served, and the
    planted file is left exactly as it was (the make backend writes no L2)."""
    c = fakeland.build()
    fakeland.plant_var()
    before = fakeland.var.stat()
    df = c.build_me()
    assert _x(df) == WAVE_X, f"served {_x(df)}; {POISON_X} is the planted var/ parquet"
    after = fakeland.var.stat()
    assert (before.st_mtime_ns, before.st_size) == (after.st_mtime_ns, after.st_size)


# ---------------------------------------------------------------------------
# No wave produced rows: the no-builder route never asks the country fallback
# (red team r1, verdict 6.4)
# ---------------------------------------------------------------------------

BARREN = "2002"   # a second wave that does not carry the table

_SCHEME_TWO_WAVES = _SCHEME.replace(f"Waves: ['{WAVE}']", f"Waves: ['{WAVE}', '{BARREN}']")


@pytest.fixture
def fakeland2(fakeland):
    """Fakeland with a second wave, ``2002``, that lacks the table -- the
    shape of Senegal ``livestock`` (2018-19 has it, 2021-22 does not)."""
    (fakeland.country_dir / "_" / "data_scheme.yml").write_text(_SCHEME_TWO_WAVES)
    (fakeland.country_dir / BARREN / "_").mkdir(parents=True)
    return fakeland


def _barren(c: C.Country) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return c.build_me(waves=[BARREN])


def _assert_empty_and_shaped(df: pd.DataFrame) -> None:
    assert len(df) == 0, f"served {len(df)} rows for a wave that lacks the table: {_x(df)}"
    assert "x" in df.columns
    assert {"t", "i"} <= set(df.index.names)


@needs_make
@pytest.mark.parametrize("backend", ["make", "dvc"])
def test_barren_subset_is_empty_on_both_backends(fakeland2, monkeypatch, backend):
    """Senegal ``livestock(waves=['2021-22'])`` under the make backend served
    the whole ``var/`` -- 9,404 rows of 2018-19 -- because the country
    fallback asked make for a target it has no rule for.  The default backend
    serves 0.  Both must serve 0; the make backend must not touch ``var/``.

    (The default backend's 0 is reached a different way here: the planted
    hashless parquet grades stale, the barren subset falls through to the
    country fallback, and ``_restrict_to_waves`` filters what make hands
    back -- CLAUDE.md's "accidentally correct" #866 case.  Only the make side
    is this fix.)"""
    monkeypatch.setenv("LSMS_BUILD_BACKEND", backend)
    c = fakeland2.build()
    fakeland2.plant_var()
    before = fakeland2.var.stat()
    _assert_empty_and_shaped(_barren(c))
    if backend == "make":
        after = fakeland2.var.stat()
        assert (before.st_mtime_ns, before.st_size) == (after.st_mtime_ns, after.st_size)


def test_barren_subset_is_empty_without_a_makefile(fakeland2):
    """The 5 barren cells in countries with no ``_/Makefile`` (Albania x4,
    Tajikistan): before, ``Could not materialize ... no cached parquet was
    found`` -- false when ``var/`` exists, and not what the default backend
    serves (0 rows)."""
    c = fakeland2.build(makefile=None)
    fakeland2.plant_var()
    _assert_empty_and_shaped(_barren(c))


@needs_make
def test_full_request_with_no_wave_rows_still_fails_loudly(fakeland2):
    """A request for EVERY wave that produced nothing is a failed build, as on
    the default backend -- not an empty frame, and not the planted
    ``var/``."""
    (fakeland2.country_dir / WAVE / "_" / f"{TABLE}.py").unlink()
    c = fakeland2.build()
    fakeland2.plant_var()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with pytest.raises(RuntimeError, match=f"Could not materialize {COUNTRY}/{TABLE}"):
            c.build_me()
    assert fakeland2.var.exists(), "the make backend deleted var/"


@needs_make
def test_barren_subset_with_a_country_builder_keeps_the_legacy_fallback(fakeland2):
    """Only the NO-builder route is cut off from the country fallback.  A
    country script still answers a barren subset exactly as before (whether
    it should is #1039-a's business)."""
    c = fakeland2.build(country_script=_COUNTRY_SCRIPT)
    assert _x(_barren(c)) == COUNTRY_X


def test_empty_declared_frame_has_the_declared_shape():
    entry = {"index": "(t, v, i)", "x": "float", "y": {"type": "str", "optional": True},
             "materialize": "make"}
    df = C._empty_declared_frame(entry)
    assert len(df) == 0
    assert list(df.index.names) == ["t", "v", "i"]
    assert list(df.columns) == ["x", "y"]
    assert all(str(df.index.get_level_values(n).dtype) == "str" for n in df.index.names)
    assert list(C._empty_declared_frame({"x": "float"}).columns) == ["x"]
    assert C._empty_declared_frame(None).empty


# ---------------------------------------------------------------------------
# A country builder that EXISTS keeps the legacy route
# ---------------------------------------------------------------------------

@needs_make
def test_multi_target_country_rule_keeps_the_make_route(fakeland):
    """Uganda ``fct``'s shape: the rule names two targets and runs a script
    that is not ``_/<table>.py``.  It must be found -- by asking make, not by
    looking for a script -- and it must answer."""
    c = fakeland.build(makefile=_MAKEFILE_MULTI_TARGET, maker=True)
    assert not (fakeland.country_dir / "_" / f"{TABLE}.py").exists()
    df = c.build_me()
    assert _x(df) == COUNTRY_X, "the country rule did not answer"
    assert c._has_country_builder(TABLE) is True


@needs_make
def test_unmakeable_prerequisite_is_a_rule_not_its_absence(fakeland):
    """``No rule to make target '<prereq>', needed by '<target>'`` names the
    prerequisite: the target HAS a rule.  Reading it as absence would hand a
    broken country rule's table to the waves silently."""
    c = fakeland.build(makefile=_MAKEFILE_UNMAKEABLE_PREREQ, maker=True)
    assert c._has_country_builder(TABLE) is True


@needs_make
def test_country_script_keeps_the_make_route(fakeland):
    """A ``_/<table>.py`` is a country builder (the ``%.parquet: %.py``
    catch-all and ``try_script`` both run it).  Whether such a script should
    exist is #788's business, not this route's."""
    c = fakeland.build(country_script=_COUNTRY_SCRIPT)
    df = c.build_me()
    assert _x(df) == COUNTRY_X, "the country script did not answer"
    assert c._has_country_builder(TABLE) is True
    assert not _intree_parquets(fakeland.country_dir / "_"), \
        "the in-tree catch-all target materialised in the config tree"


def test_default_backend_is_unchanged(fakeland, monkeypatch):
    """The predicate is never consulted off the make backend."""
    monkeypatch.delenv("LSMS_BUILD_BACKEND")
    c = fakeland.build()
    called = []
    monkeypatch.setattr(C.Country, "_has_country_builder",
                        lambda self, t: called.append(t) or True)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = c.build_me()
    assert _x(df) == WAVE_X
    assert called == []


# ---------------------------------------------------------------------------
# The real trees (config only, no microdata)
# ---------------------------------------------------------------------------

@needs_make
@pytest.mark.parametrize("country,table,expected", [
    ("Tanzania", "food_acquired", False),    # the issue's cell: wave rule only (#836)
    ("Tanzania", "shocks", True),            # $(VAR_DIR)/shocks.parquet rule + script
    ("Uganda", "fct", True),                 # multi-target rule, no _/fct.py
    ("Burkina_Faso", "plot_features", True), # script, no var rule (catch-all + try_script)
    ("Nigeria", "livestock", True),          # script, no rule (#866's no-rule cell)
    ("GhanaAHIES", "sample", False),         # wave rules only, incl. data_root spellings
    ("Togo", "livestock", False),            # dead catch-all (needs malawi.py)
    ("Albania", "shocks", False),            # no Makefile
])
def test_real_country_builders(country, table, expected):
    c = C.Country(country, preload_panel_ids=False)
    entry = c._materialization_entry(table)
    assert str(entry.get("materialize", "")).lower() == "make", entry
    assert c._has_country_builder(table) is expected


# ---------------------------------------------------------------------------
# Cache cost of the predicate: nil, and pinned
# ---------------------------------------------------------------------------

_PROBE = [("Tanzania", "food_acquired"), ("Tanzania", "shocks"), ("Uganda", "fct"),
          ("Albania", "shocks"), ("GhanaAHIES", "sample")]


def _hashes():
    R.clear_caches()
    btf = {t: R.build_transforms_fingerprint(t) for _c, t in _PROBE}
    tch = {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for c, t in _PROBE:
            co = C.Country(c, preload_panel_ids=False)
            tch[(c, t)] = co._table_cache_hash(t, co.waves)
    return btf, tch


def _probe_edit(monkeypatch, target):
    real = inspect.getsource
    monkeypatch.setattr(inspect, "getsource",
                        lambda o: real(o) + "\n_redteam_probe = 1\n"
                        if inspect.unwrap(o) is target else real(o))


def test_the_predicate_is_not_folded_into_any_build_fingerprint():
    qn = "lsms_library.country.Country._has_country_builder"
    assert qn in R._EXCLUDED_CALLABLES
    seen, parts = set(), []
    for _qn, (fn, _t) in R._BUILD_TRANSFORMS.items():
        parts += R._closure_parts(fn, seen)
    assert not [p[:80] for p in parts if p.split("=")[0] == qn]


def test_editing_the_predicate_moves_no_hash(monkeypatch):
    """Measured: a logic edit to the predicate moves none of the probed
    fingerprints or table hashes.  Positive control: the same edit to the
    tagged ``_aggregate_wave_data`` (where the routing condition lives) moves
    all of them -- which is why the routing change itself is
    corpus-invalidating."""
    btf0, tch0 = _hashes()
    _probe_edit(monkeypatch, C.Country._has_country_builder)
    btf1, tch1 = _hashes()
    assert btf0 == btf1 and tch0 == tch1
    monkeypatch.undo()
    _probe_edit(monkeypatch, inspect.unwrap(C.Country._aggregate_wave_data))
    btf2, tch2 = _hashes()
    assert all(btf0[t] != btf2[t] for t in btf0)
    assert all(tch0[k] != tch2[k] for k in tch0)
    monkeypatch.undo()
    R.clear_caches()

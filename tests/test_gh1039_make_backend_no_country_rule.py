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

import copy
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

    Before GH #1099 the default backend reached 0 accidentally: the planted
    hashless parquet fell through to the country fallback and the requested
    wave restriction filtered what make handed back.  Both backends now
    recognize absence without asking the country fallback."""
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


@pytest.mark.parametrize("backend", ["dvc", "make"])
@pytest.mark.parametrize("cache_state", ["cold", "all", "partial", "stale", "no_cache"])
def test_absent_subset_is_typed_and_preserves_other_cached_waves(
        fakeland2, monkeypatch, backend, cache_state):
    """GH #1099: absence is independent of the cache, whose other waves survive.

    The earlier default-backend test plants an existing country parquet, so
    make can accidentally serve it and the final restriction returns zero.
    These cases include a genuinely cold root and a coverage-short manifest.
    """
    monkeypatch.setenv("LSMS_BUILD_BACKEND", backend)
    c = fakeland2.build(makefile=None)
    before = None
    expected = pd.DataFrame({"t": [WAVE], "i": ["h1"], "x": [2.0]}).set_index(["t", "i"])
    if cache_state != "cold":
        fakeland2.var.parent.mkdir(parents=True, exist_ok=True)
        C.to_parquet(
            expected, fakeland2.var,
            cache_hash="old-hash" if cache_state == "stale" else c._table_cache_hash(TABLE, c.waves),
            cache_waves=[WAVE] if cache_state == "partial" else c.waves,
        )
        before = fakeland2.var.read_bytes()
    if cache_state == "no_cache":
        monkeypatch.setenv("LSMS_NO_CACHE", "1")

    result = _barren(c)
    expected = c._finalize_result(expected.iloc[:0], c._materialization_entry(TABLE), TABLE)
    pd.testing.assert_frame_equal(result, expected)
    if before is None:
        assert not fakeland2.var.exists(), "an absent-wave answer populated var/"
    else:
        assert fakeland2.var.read_bytes() == before, "an empty subset replaced other waves' cache"


def test_absent_subset_keeps_required_source_failure(fakeland2, monkeypatch):
    """A declared wave with an unreadable source is not an absent table."""
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    c = fakeland2.build(makefile=None)

    def unavailable(self, request):
        raise FileNotFoundError("required source is unavailable")

    monkeypatch.setattr(C.Wave, "grab_data", unavailable)
    with pytest.raises(FileNotFoundError, match="required source is unavailable"):
        c.build_me(waves=[WAVE])


def test_absent_subset_does_not_accept_unknown_table_or_wave(fakeland2, monkeypatch):
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    c = fakeland2.build(makefile=None)
    with pytest.raises(AttributeError, match="not_a_table"):
        c.not_a_table(waves=[BARREN])
    with pytest.raises(KeyError, match="2099 is not a valid wave"):
        c.build_me(waves=["2099"])


def test_absent_subset_does_not_turn_a_failed_full_build_into_empty(fakeland2, monkeypatch):
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    (fakeland2.country_dir / WAVE / "_" / f"{TABLE}.py").unlink()
    c = fakeland2.build(makefile=None)
    with pytest.raises(RuntimeError, match="Could not materialize"):
        c.build_me()


@pytest.mark.parametrize("assume_fresh", [False, True])
def test_covered_subset_cache_hit_does_not_construct_waves(
        fakeland2, monkeypatch, assume_fresh):
    """GH #1099's absence check must remain behind both cache-hit gates."""
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    c = fakeland2.build(makefile=None)
    c.assume_cache_fresh = assume_fresh
    frame = pd.DataFrame({"t": [WAVE], "i": ["h1"], "x": [2.0]}).set_index(["t", "i"])
    fakeland2.var.parent.mkdir(parents=True, exist_ok=True)
    cache_hash = c._table_cache_hash(TABLE, c.waves)
    C.to_parquet(frame, fakeland2.var, cache_hash=cache_hash, cache_waves=c.waves)
    # Hash computation already inspects wave config by design; isolate any
    # additional declaration traversal introduced on the cache-hit path.
    monkeypatch.setattr(C.Country, "_table_cache_hash", lambda self, *args: cache_hash)

    def no_wave(self, wave):
        raise AssertionError("a covered cache hit constructed a wave")

    monkeypatch.setattr(C.Country, "__getitem__", no_wave)
    result = c.build_me(waves=[WAVE])
    expected = c._finalize_result(frame, c._materialization_entry(TABLE), TABLE)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("backend", ["dvc", "make"])
@pytest.mark.parametrize("country_makefile", [False, True])
def test_absent_subset_without_make_preserves_unknown_country_rules(
        fakeland2, monkeypatch, backend, country_makefile):
    """No make cannot prove a rule absent; no Makefile can (GH #1099 review).

    The make backend keeps its existing no-executable-builder policy, while
    the new default-backend guard must preserve an uninspectable country rule.
    """
    monkeypatch.setenv("LSMS_BUILD_BACKEND", backend)
    c = fakeland2.build(
        makefile=_MAKEFILE_MULTI_TARGET if country_makefile else None,
        maker=country_makefile,
    )
    real_which, real_run = C.shutil.which, C.subprocess.run

    def no_make(command, *args, **kwargs):
        return None if command == "make" else real_which(command, *args, **kwargs)

    def run_without_make(command, *args, **kwargs):
        if isinstance(command, list) and command and command[0] == "make":
            raise FileNotFoundError("make is unavailable")
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(C.shutil, "which", no_make)
    monkeypatch.setattr(C.subprocess, "run", run_without_make)
    if country_makefile and backend == "dvc":
        with pytest.raises(RuntimeError, match="Could not materialize"):
            _barren(c)
    else:
        _assert_empty_and_shaped(_barren(c))
    assert not fakeland2.var.exists()


# ---------------------------------------------------------------------------
# GH #1218 / PR #1221 follow-up: the constructed empty frame is never cached
# ---------------------------------------------------------------------------
#
# A default-backend request asks ``absent_subset_frame`` twice: at the top of
# ``load_dataframe_with_dvc`` and again inside ``load_from_waves(build_waves)``.
# If ``_has_country_builder`` answered True and then False (its ``make -n``
# probe answers True on OSError/timeout), the constructed empty frame came back
# from the second check and was WRITTEN to var/ with the union coverage
# manifest.  Review repro on development, Iraq ``shocks``: 8,812 rows of 2012,
# then one empty 2006-07 request, then 0 rows of 2012.

def _var_waves(fake) -> list[str] | None:
    from lsms_library.local_tools import read_parquet_cache_waves
    return read_parquet_cache_waves(fake.var)


def _populate_one_wave(fake) -> bytes:
    """Step 1: the populated wave builds and is cached with coverage [WAVE]."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert _x(C.Country(COUNTRY).build_me(waves=[WAVE])) == WAVE_X
    assert fake.var.exists() and _var_waves(fake) == [WAVE]
    return fake.var.read_bytes()


def _assert_populated_wave_survives(fake, before: bytes | None) -> None:
    """Step 3: the populated wave still serves its rows from a fresh instance."""
    if before is not None:
        assert fake.var.read_bytes() == before, \
            f"var/ was rewritten; coverage now {_var_waves(fake)}"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        again = C.Country(COUNTRY).build_me(waves=[WAVE])
    assert _x(again) == WAVE_X and len(again) == 2, \
        f"the populated wave now serves {len(again)} rows"


def test_constructed_empty_is_marked_never_cache():
    frame = C._empty_declared_frame({"index": "(t, i)", "x": "float"})
    assert frame.attrs.get(C._CONSTRUCTED_EMPTY_ATTR) is True
    assert C._is_constructed_empty(frame)
    # A built frame that happens to be empty is not a constructed answer.
    assert not C._is_constructed_empty(pd.DataFrame({"x": pd.Series([], dtype=float)}))
    assert not C._is_constructed_empty({}) and not C._is_constructed_empty(None)


@pytest.mark.parametrize("write_site", ["no_stage", "dvc_fallback"])
def test_flaky_predicate_cannot_cache_the_constructed_empty(
        fakeland2, monkeypatch, caplog, write_site):
    """The reviewer's forced race, hermetic: the predicate says True to the
    first absence check and False to the second.  At BOTH write sites of
    ``load_dataframe_with_dvc`` the constructed empty frame must be served and
    not written; the populated wave's cache and rows must survive."""
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    fakeland2.build(makefile=None)

    class _NoStageRepo:          # a DVC repo with no materialize stages
        def __init__(self, *a, **k):
            pass

        def close(self):
            pass

    def _no_repo(*a, **k):
        raise C.DvcException("not a DVC repo (test)")

    monkeypatch.setattr(C, "Repo", _NoStageRepo if write_site == "no_stage" else _no_repo)
    monkeypatch.setattr(C.Country, "_resolve_materialize_stages", lambda self, m, w: [])

    before = _populate_one_wave(fakeland2)

    real = C.Country._has_country_builder
    calls = []

    def flaky(self, method_name):
        calls.append(method_name)
        return True if len(calls) == 1 else real(self, method_name)

    monkeypatch.setattr(C.Country, "_has_country_builder", flaky)
    caplog.clear()
    with caplog.at_level("WARNING"):
        _assert_empty_and_shaped(_barren(C.Country(COUNTRY)))
    monkeypatch.setattr(C.Country, "_has_country_builder", real)
    assert calls == [TABLE, TABLE], f"the race did not happen: {calls}"
    fell_back = any("DVC unavailable for build_me" in r.getMessage() for r in caplog.records)
    assert fell_back is (write_site == "dvc_fallback"), "wrong write site exercised"
    assert _var_waves(fakeland2) == [WAVE]
    _assert_populated_wave_survives(fakeland2, before)


@needs_make
def test_one_request_cannot_get_two_predicate_answers(fakeland2, monkeypatch):
    """The other half: the predicate is memoised per instance, so a transient
    probe failure (OSError -> the fail-safe True) is the answer for the whole
    request rather than flipping to False on the second absence check.  Real
    ``make -n`` probe on a wave-only Makefile; only the first probe fails."""
    monkeypatch.setenv("LSMS_BUILD_BACKEND", "dvc")
    fakeland2.build()                      # wave-only Makefile: no country rule
    _populate_one_wave(fakeland2)

    real_run = C.subprocess.run
    probes = []

    def first_probe_fails(command, *args, **kwargs):
        if isinstance(command, list) and command[:2] == ["make", "-n"]:
            probes.append(command[-1])
            if len(probes) == 1:
                raise OSError(11, "Resource temporarily unavailable (test)")
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(C.subprocess, "run", first_probe_fails)
    c = C.Country(COUNTRY)
    _assert_empty_and_shaped(_barren(c))
    assert len(probes) == 1, f"one request probed make {len(probes)} times"
    assert c._has_country_builder(TABLE) is True, "the memo did not hold"
    assert len(probes) == 1
    # The request rebuilt the union (the fail-safe route); var/ now covers
    # both waves and still holds the populated wave's rows.
    assert _var_waves(fakeland2) == [WAVE, BARREN]
    _assert_populated_wave_survives(fakeland2, None)
    # A fresh instance probes again, and without the fault finds no rule.
    monkeypatch.setattr(C.subprocess, "run", real_run)
    assert C.Country(COUNTRY)._has_country_builder(TABLE) is False


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


def test_empty_declared_frame_keeps_interleaved_optional_declaration_order():
    entry = {
        'index': '(t, i)',
        'Unit': {'type': 'str', 'optional': True},
        'Quantity': 'float',
        'Flag': {'type': 'bool', 'optional': True},
        'Count': 'int',
        'Factor': {'type': 'float', 'optional': True},
        'Label': {'type': 'str', 'optional': False},
        't': 'str',
        'i': {'type': 'str', 'optional': True},
        'materialize': 'make',
        'backend': 'make',
        'aggregation': {'Quantity': 'sum'},
        1: 'str',
    }
    frame = C._empty_declared_frame(entry)
    # A source declaration is the oracle; requiredness is not a sort key.
    assert list(frame.columns) == ['Unit', 'Quantity', 'Flag', 'Count',
                                   'Factor', 'Label']
    assert frame.empty and list(frame.index.names) == ['t', 'i']
    assert all(str(frame.index.get_level_values(n).dtype) == 'str'
               for n in frame.index.names)
    # GH #1218 intentionally extends this empty-only dtype contract; the
    # declaration order and index contract from GH #1215 stay the same.
    assert list(frame.dtypes) == [pd.StringDtype(), pd.Float64Dtype(),
                                  pd.BooleanDtype(), pd.Int64Dtype(),
                                  pd.Float64Dtype(), pd.StringDtype()]


@pytest.mark.parametrize('entry,columns,levels', [
    ({'Z': 'float', 'A': 'str'}, ['Z', 'A'], [None]),
    ({'Z': {'type': 'float', 'optional': True},
      'A': {'type': 'str', 'optional': True}}, ['Z', 'A'], [None]),
    ({'index': '(t, i)', 't': 'str', 'i': 'str'}, [], ['t', 'i']),
    ({'index': '(t, i)', 'Z': 'float',
      'A': {'type': 'str', 'optional': False}}, ['Z', 'A'], ['t', 'i']),
    (None, [], [None]),
    ('not a mapping', [], [None]),
])
def test_empty_declared_order_preserves_existing_shapes(entry, columns, levels):
    frame = C._empty_declared_frame(entry)
    assert frame.empty
    assert list(frame.columns) == columns
    assert list(frame.index.names) == levels


# GH #1218: type and optional membership are independent declarations.
@pytest.mark.parametrize('form', ['scalar', 'required', 'implicit-required', 'optional'])
@pytest.mark.parametrize('declared,expected', [
    ('str', pd.StringDtype()), ('string', pd.StringDtype()),
    ('bool', pd.BooleanDtype()), ('int', pd.Int64Dtype()),
    ('float', pd.Float64Dtype()),
    ('datetime', 'calendar'), ('date', 'calendar'), ('timestamp', 'calendar'),
])
def test_constructed_empty_uses_existing_explicit_nullable_types(form, declared, expected):
    spec = declared if form == 'scalar' else {
        'type': declared, 'optional': form == 'optional'}
    if form == 'implicit-required':
        spec.pop('optional')
    entry = {'index': '(t, i)', 'Local': spec}
    before = copy.deepcopy(entry)
    frame = C._empty_declared_frame(entry)
    if expected == 'calendar':
        # The library's stored date resolution (GH #1045: build-time dates
        # are datetime64[us]), not pandas' inference on an empty object
        # column, which is [s] (PR #1221 follow-up).
        expected = 'datetime64[us]'
    pd.testing.assert_series_equal(
        frame.Local, pd.Series([], index=frame.index, dtype=expected, name='Local'),
        check_exact=True)
    assert entry == before, 'empty construction mutated a source declaration'


@pytest.mark.parametrize('declared', [
    None, 'unknown', {'type': 'unknown', 'optional': True},
    {'optional': True}, {'type': None, 'optional': True},
    {'type': ['a', 'b'], 'optional': True},
    {'type': {'name': 'float'}, 'optional': True},
    {'type': True, 'optional': True}, {'type': 1, 'optional': False},
])
def test_constructed_empty_does_not_guess_missing_or_unknown_types(declared):
    entry = {'index': '(t, i)', 'Local': declared}
    before = copy.deepcopy(entry)
    frame = C._empty_declared_frame(entry)
    assert frame.empty and list(frame.columns) == ['Local']
    assert frame.Local.dtype == object
    assert entry == before


@pytest.mark.parametrize('labels', [[], ['a', 'b']])
def test_constructed_empty_keeps_existing_bare_list_string_semantics(labels):
    frame = C._empty_declared_frame({'Local': labels})
    pd.testing.assert_series_equal(
        frame.Local, pd.Series([], dtype=pd.StringDtype(), name='Local'),
        check_exact=True)


def test_constructed_empty_payload_types_never_recast_declared_index_names():
    entry = {'index': ['LocalInt', 'LocalString'],
             'LocalInt': {'type': 'int', 'optional': False},
             'LocalString': {'type': 'bool', 'optional': True},
             'Payload': {'type': 'float', 'optional': True}}
    frame = C._empty_declared_frame(entry)
    expected = pd.MultiIndex.from_arrays(
        [pd.Series([], dtype='str'), pd.Series([], dtype='str')],
        names=['LocalInt', 'LocalString'])
    pd.testing.assert_index_equal(frame.index, expected, exact=True)
    for actual, original in zip(frame.index.codes, expected.codes):
        assert actual.dtype == original.dtype
    for actual, original in zip(frame.index.levels, expected.levels):
        assert type(actual.dtype) is type(original.dtype)
        assert actual.dtype.storage == original.dtype.storage
        assert type(actual.dtype.na_value) is type(original.dtype.na_value)
    assert list(frame.columns) == ['Payload']
    assert frame.Payload.dtype == pd.Float64Dtype()


def test_constructed_empty_canonical_dtype_still_wins_in_finalize(fakeland2):
    country = fakeland2.build(makefile=None)
    entry = {'index': '(t, i)', 'Age': {'type': 'bool', 'optional': True}}
    frame = C._empty_declared_frame(entry)
    assert frame.Age.dtype == pd.BooleanDtype()
    expected = C._SCHEME_DTYPE_MAP[C._load_canonical_dtypes()['household_roster']['Age']]
    assert expected != frame.Age.dtype, 'fixture must conflict with canonical Age'
    result = country._finalize_result(frame, entry, 'household_roster')
    assert result.empty and result.Age.dtype == expected


@pytest.mark.parametrize('optional', [False, True])
def test_populated_dict_coercion_and_optional_absence_remain_unchanged(optional):
    frame = pd.DataFrame({'Local': [1.25, None]})
    frame.attrs['marker'] = ('native', 'unchanged')
    expected = frame.copy(deep=True)
    C._enforce_declared_dtypes(frame, {
        'Local': {'type': 'bool', 'optional': optional},
        'Absent': {'type': 'str', 'optional': True},
    })
    pd.testing.assert_frame_equal(frame, expected, check_exact=True)
    assert frame.attrs == expected.attrs
    assert 'Absent' not in frame.columns


_LOCAL_OPTIONAL_FIELDS = """\
    Unit_purchased: {type: str, optional: true}
    Received_as_gift: {type: bool, optional: true}
    Quantity_received_as_gift: {type: float, optional: true}
    Unit_received_as_gift: {type: str, optional: true}
"""


@needs_make
@pytest.mark.parametrize('backend', ['dvc', 'make'])
def test_optional_type_declarations_do_not_require_populated_fields(
        fakeland2, monkeypatch, backend):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    scheme = _SCHEME_TWO_WAVES.replace(
        '    x: float\n', '    x: float\n' + _LOCAL_OPTIONAL_FIELDS)
    (fakeland2.country_dir / '_' / 'data_scheme.yml').write_text(scheme)
    country = fakeland2.build()
    frame = country.build_me(waves=[WAVE])
    assert _x(frame) == WAVE_X
    assert list(frame.columns) == ['x']
    assert frame.x.dtype == pd.Float64Dtype()


@pytest.fixture
def typed_fakeland(fakeland2):
    """Identical explicit country-local types on both framework heads."""
    scheme = _SCHEME_TWO_WAVES.replace(
        '    x: float\n', '    x: float\n' + _LOCAL_OPTIONAL_FIELDS)
    (fakeland2.country_dir / '_' / 'data_scheme.yml').write_text(scheme)
    script = _WAVE_SCRIPT.replace('"x": [1.0, 2.0]', '''"x": [1.0, 2.0],
    "Unit_purchased": pd.Series(["bag", pd.NA], dtype="string"),
    "Received_as_gift": pd.Series([True, pd.NA], dtype="boolean"),
    "Quantity_received_as_gift": pd.Series([2.0, pd.NA], dtype="Float64"),
    "Unit_received_as_gift": pd.Series(["tin", pd.NA], dtype="string")''')
    (fakeland2.country_dir / WAVE / '_' / f'{TABLE}.py').write_text(script)
    return fakeland2


def _assert_local_empty_types(frame):
    assert frame.empty
    assert list(frame.columns) == [
        'x', 'Unit_purchased', 'Received_as_gift',
        'Quantity_received_as_gift', 'Unit_received_as_gift']
    assert list(frame.dtypes) == [pd.Float64Dtype(), pd.StringDtype(),
                                  pd.BooleanDtype(), pd.Float64Dtype(),
                                  pd.StringDtype()]


@needs_make
@pytest.mark.parametrize('backend', ['dvc', 'make'])
@pytest.mark.parametrize('prior', ['cold', 'supported', 'all'])
def test_constructed_empty_country_local_types_across_no_builder_routes(
        typed_fakeland, monkeypatch, backend, prior):
    monkeypatch.setenv('LSMS_BUILD_BACKEND', backend)
    country = typed_fakeland.build()
    if prior != 'cold':
        populated = country.build_me(waves=[WAVE] if prior == 'supported' else None)
        assert _x(populated) == WAVE_X
        assert populated.Unit_purchased.dtype == pd.StringDtype()
        assert populated.Received_as_gift.dtype == pd.BooleanDtype()
    before = {p: (p.read_bytes(), p.stat().st_mode, p.stat().st_mtime_ns)
              for p in typed_fakeland.country_dir.parent.parent.rglob('*.parquet')}
    _assert_local_empty_types(_barren(country))
    after = {p: (p.read_bytes(), p.stat().st_mode, p.stat().st_mtime_ns)
             for p in typed_fakeland.country_dir.parent.parent.rglob('*.parquet')}
    assert after == before, 'absent-wave request changed a populated cache'


@needs_make
def test_constructed_empty_after_declared_wave_produces_no_rows(typed_fakeland):
    # This wave declares the table, so absent_subset_frame cannot short-circuit.
    # Its real script writes a shaped empty result; load_from_waves reaches the
    # separate no-country-builder/no-results constructor.
    script = _WAVE_SCRIPT.replace('["2001", "2001"]', '[]').replace(
        '["h1", "h2"]', '[]').replace('[1.0, 2.0]', '[]')
    (typed_fakeland.country_dir / WAVE / '_' / f'{TABLE}.py').write_text(script)
    country = typed_fakeland.build()
    _assert_local_empty_types(country.build_me(waves=[WAVE]))
    assert typed_fakeland.wave_out.exists(), 'declared wave script did not run'
    assert pd.read_parquet(typed_fakeland.wave_out).empty
    assert not typed_fakeland.var.exists()


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


def test_full_default_backend_build_does_not_probe_country_builder(fakeland, monkeypatch):
    """The predicate is unnecessary for a full default-backend wave build."""
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

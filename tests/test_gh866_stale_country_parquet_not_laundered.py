"""GH #866 -- a STALE country parquet must not be handed back by ``make``.

``run_make_target(method_name, wave=None)`` runs ``make`` on
``<data_root>/<C>/var/<table>.parquet`` and returns the first candidate that
``exists()``.  With the target already on disk make has two ways to do nothing
and still exit 0 -- no rule for it, or a rule that mtime-skips -- and both were
read as a successful build.  ``try_script`` (the only caller of a country's
``_/<table>.py``) was never reached, the stale frame was served, and the
descent wrote it back stamped with the CURRENT expected hash.  That re-stamp is
what makes the defect permanent: every later read grades ``fresh``.

Reproduced before the fix: ``Nigeria/livestock`` 23,196 rows -> 5 rows served
and re-stamped with the correct hash.

Two tiers, as in ``test_gh809_hashless_country_parquet.py``: source guards that
cost nothing, then cache-gated delivered tests.
"""

import ast
import shutil
from pathlib import Path

import pytest

from lsms_library.country import Country, JSON_CACHE_METHODS
from lsms_library.local_tools import (
    _atomic_write_table, data_root, read_parquet_cache_hash,
)

SRC = Path(Country.__module__.replace('.', '/') + '.py')
if not SRC.exists():                                  # installed, not in-tree
    import lsms_library.country as _c
    SRC = Path(_c.__file__)
SOURCE = SRC.read_text()


# --------------------------------------------------------------------------
# The live surface, measured per-target with `make -n` against a root where
# the target is absent.  BOTH forms are country-only -- the country fallback
# is reached only when no wave produced rows -- which is what keeps the
# removal away from the 13 Make rules that consume a var/ parquet as a
# PREREQUISITE (deleting Tanzania's rule-less `cluster_features.parquet` would
# brick that country's whole food chain).
# --------------------------------------------------------------------------
RULE_LESS = [                       # make: "... is up to date", exit 0
    ("Nigeria", "crop_production"),
    ("Nigeria", "livestock"),
    ("Nigeria", "people_last7days"),
    ("Nigeria", "plot_inputs"),
    ("Nigeria", "plot_labor"),
]
MTIME_SKIP = [                      # a rule exists; make judges it current
    ("Ethiopia", "nutrition"),
    ("EthiopiaRHS", "community_prices"),
    ("GhanaLSS", "nutrition"),
    ("Nigeria", "anthropometry"),
]
SURFACE = RULE_LESS + MTIME_SKIP


# ==========================================================================
# Tier 1 -- source guards
# ==========================================================================

def _load_from_waves_node():
    tree = ast.parse(SOURCE)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "load_from_waves":
            return node
    raise AssertionError("load_from_waves not found")


def test_load_from_waves_takes_the_flag_and_defaults_it_off():
    node = _load_from_waves_node()
    names = [a.arg for a in node.args.kwonlyargs]
    assert "evict_country_cache" in names, (
        "the eviction must be opt-in per call site, not inferred inside "
        "load_from_waves: LSMS_NO_CACHE skips the read gate entirely, so no "
        "freshness value is in scope there to key off."
    )
    default = node.args.kw_defaults[names.index("evict_country_cache")]
    assert isinstance(default, ast.Constant) and default.value is False, (
        "default must be False -- the JSON tier and the LSMS_BUILD_BACKEND=make "
        "routes call load_from_waves too and must never delete a var/ parquet."
    )


def test_the_eviction_precedes_the_country_fallback():
    """Order is the whole fix.  After the call, make cannot report the target
    up to date and cannot mtime-skip it, because it is not there."""
    body = SOURCE[SOURCE.index("def load_from_waves("):]
    body = body[:body.index("\n        def load_json_cache(")]
    evict = body.index("self._evict_stale_country_cache(method_name)")
    fallback = body.index("country_fallback = run_make_target(method_name, wave=None)")
    assert evict < fallback, (
        "the removal must happen BEFORE run_make_target(wave=None); after it "
        "the stale file has already been served."
    )


def test_exactly_the_country_tier_call_sites_pass_the_flag():
    """Pins the call-site table.  `load_dataframe_with_dvc` is the only place
    that has decided not to serve `var/<t>.parquet`; the JSON tier targets
    `_/<t>.json`, and LSMS_BUILD_BACKEND=make is documented as a *bypass* of
    the parquet tiers, not an invalidation of them."""
    calls = [ln for ln in SOURCE.splitlines() if "load_from_waves(" in ln
             and "def load_from_waves(" not in ln]
    passing = [ln for ln in calls if "evict_country_cache=" in ln]
    assert len(calls) == 6, f"call sites moved: {calls}"
    assert len(passing) == 3, (
        f"expected exactly the 3 load_dataframe_with_dvc sites to evict, got "
        f"{len(passing)}:\n" + "\n".join(passing)
    )


def test_the_rebuild_failure_is_still_raised_not_swallowed():
    """Loud, not silent.  With the stale file gone, a country whose script is
    missing or broken now fails instead of serving stale data -- that is the
    intended outcome and must not be softened into an empty frame."""
    body = SOURCE[SOURCE.index("country_fallback = run_make_target(method_name, wave=None)"):]
    body = body[:body.index("\n        def load_json_cache(")]
    assert "raise _rebuild_failure_error(self.name, method_name)" in body


def test_the_helper_never_deletes_a_json_tier_cache(tmp_path, monkeypatch):
    """`panel_ids.json` is a committed SOURCE (GH #914).  No JSON-tier caller
    passes the flag today, but reachability arguments are how #914 got broken
    once already -- so assert *never deletes*, not *never reached*."""
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
    victim = tmp_path / "Uganda" / "var" / "panel_ids.parquet"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"not really a parquet")
    for name in sorted(JSON_CACHE_METHODS):
        Country("Uganda")._evict_stale_country_cache(name)
    assert victim.exists(), "a JSON_CACHE_METHODS name must never be evicted here"


def test_the_helper_is_a_noop_on_an_absent_parquet(tmp_path, monkeypatch):
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
    Country("Uganda")._evict_stale_country_cache("livestock")   # must not raise


# ==========================================================================
# Tier 2 -- delivered
# ==========================================================================

def _warm(country: str, table: str) -> Path | None:
    p = data_root() / country / "var" / f"{table}.parquet"
    return p if p.exists() else None


@pytest.mark.parametrize("country,table", SURFACE,
                         ids=[f"{c}/{t}" for c, t in SURFACE])
def test_a_poisoned_country_parquet_is_REBUILT_not_served(country, table):
    """The repro, inverted.

    Poison the CONTENT as well as the hash, so a row count alone cannot pass:
    the assertion is that the frame served after the poisoning equals the
    frame served before it, which is only true if the country script actually
    re-ran.  Before the fix the poisoned 5-row frame came back and was
    re-stamped with the correct hash.
    """
    import pyarrow.parquet as pq

    p = _warm(country, table)
    if p is None:
        pytest.skip(f"{country}/{table} not warm")

    ref = getattr(Country(country), table)()
    backup = p.with_suffix(".parquet.gh866bak")
    shutil.copyfile(p, backup)
    try:
        tbl = pq.read_table(str(p))
        md = dict(tbl.schema.metadata or {})
        md[b"lsms_cache_hash"] = b"BOGUSHASH866"
        _atomic_write_table(tbl.slice(0, 5).replace_schema_metadata(md), p)
        assert read_parquet_cache_hash(p) == "BOGUSHASH866"
        assert pq.read_table(str(p)).num_rows == min(5, tbl.num_rows)

        served = getattr(Country(country), table)()
        assert served.equals(ref), (
            "the poisoned frame was served (or a different one was): make "
            "handed back the file the gate refused instead of rebuilding"
        )

        stamped = read_parquet_cache_hash(p)
        assert stamped not in (None, "BOGUSHASH866"), (
            "the rebuild did not re-stamp -> every later read rebuilds forever"
        )
        again = getattr(Country(country), table)()
        assert again.equals(ref)
        assert read_parquet_cache_hash(p) == stamped, "hash churns between reads"
    finally:
        shutil.move(str(backup), str(p))


def test_a_wave_produced_table_is_never_evicted(monkeypatch):
    """Scoped by control flow, not by luck.

    `Togo/plot_features` is rule-less too, but its waves produce rows, so the
    country fallback -- and therefore the eviction -- is unreachable.  That is
    what keeps the removal away from the 13 Make rules that consume a var/
    parquet as a prerequisite.
    """
    p = _warm("Togo", "plot_features")
    if p is None:
        pytest.skip("Togo/plot_features not warm")

    evicted = []
    original = Country._evict_stale_country_cache

    def spy(self, method_name):
        evicted.append((self.name, method_name))
        return original(self, method_name)

    monkeypatch.setattr(Country, "_evict_stale_country_cache", spy)
    monkeypatch.setenv("LSMS_NO_CACHE", "1")          # force the descent
    Country("Togo").plot_features()
    assert ("Togo", "plot_features") not in evicted, (
        "a wave-produced table reached the country fallback -- the predicate "
        "that keeps this fix away from prerequisite parquets has broken"
    )

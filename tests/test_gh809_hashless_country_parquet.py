"""A hashless country parquet is rebuilt, not trusted and re-stamped (GH #809).

The v0.8.0 gate has a `legacy` grade: a parquet carrying no `lsms_cache_hash`
predates stamping, so it is trusted once and re-stamped, and no upgrade triggers
a corpus rebuild. That is right for a parquet written before v0.8.0 existed. It
is wrong for a country-level `var/<table>.parquet` that a `make` recipe wrote
moments ago -- a country script's `to_parquet('../var/x.parquet')` passes no
`cache_hash=`, so its output is hashless too, and the two are indistinguishable
at the gate.

**The re-stamp is what makes it permanent.** Trusting unverified content once
would be a transient defect; writing the *current expected hash* onto it means
every later read grades `fresh`, so the content is served indefinitely and the
evidence that anything was ever wrong is destroyed. That is why a census finds
nothing: a laundered parquet is byte-indistinguishable from a legitimate build
(measured 2026-09-14: 0 of 376 warm country parquets are hashless). #808 is the
instance that was caught, and only because its symptom -- GhanaLSS
`change_id`'s `_0` panel-id convention beating `updated_ids` -- was visible in
the served data.

**Scope is the point of this module.** The fix applies at the COUNTRY-tier read
gate only. The two wave-tier gates must keep `legacy`, because script-path
L2-wave parquets are written hashless *by design*; grading those stale would
rebuild them on every read forever. They are handled instead by
`_evict_hashless_wave_caches` before each rebuild descent (GH #479). So
`cache_freshness` itself is deliberately unchanged -- a test here pins that,
because "simplify by moving it into cache_freshness" is the obvious wrong
refactor and it would be a permanent performance regression rather than a
visible failure.
"""
from __future__ import annotations

import inspect
import os
import shutil
from pathlib import Path

import pytest

from lsms_library import country as country_mod
from lsms_library.country import Country
from lsms_library.local_tools import (
    cache_freshness, read_parquet_cache_hash, _atomic_write_table,
)
from lsms_library.paths import data_root


# --------------------------------------------------------------------------
# The wave tier's contract is UNCHANGED -- pin it
# --------------------------------------------------------------------------

def test_cache_freshness_itself_still_grades_hashless_as_legacy(tmp_path):
    """`cache_freshness` is shared with two wave-tier call sites.

    Moving the #809 rule inside it would grade every script-written L2-wave
    parquet stale on every read -- a permanent rebuild loop, not a one-time
    re-warm. The country gate narrows the grade at its own call site instead.
    """
    import pandas as pd
    p = tmp_path / "x.parquet"
    pd.DataFrame({"a": [1]}).to_parquet(p)
    assert read_parquet_cache_hash(p) is None
    assert cache_freshness(p, "deadbeef") == "legacy"
    assert cache_freshness(p, None) == "unverifiable"


def test_the_wave_tier_still_trusts_once_and_stamps():
    """`Wave.grab_data` keeps the legacy branch; only the country gate changed."""
    src = inspect.getsource(country_mod.Wave.grab_data)
    assert "stamp_parquet_hash" in src, (
        "the wave tier lost its trust-once-and-stamp; #809 is country-tier only"
    )


# --------------------------------------------------------------------------
# The country tier no longer trusts, and no longer stamps
# --------------------------------------------------------------------------

def test_the_country_gate_converts_legacy_to_stale():
    src = inspect.getsource(country_mod.Country._aggregate_wave_data)
    assert 'freshness == "legacy"' in src and 'freshness = "stale"' in src, (
        "the country-tier gate no longer downgrades a hashless parquet (GH #809)"
    )


def test_the_country_gate_never_stamps_a_parquet_it_served():
    """Stamping unverified content is what made the defect permanent."""
    src = inspect.getsource(country_mod.Country._aggregate_wave_data)
    assert "stamp_parquet_hash" not in src, (
        "the country-tier read path stamps a hash onto a parquet it did not "
        "verify -- that is the laundering #809 is about"
    )


# --------------------------------------------------------------------------
# Delivered: a de-stamped parquet rebuilds ONCE, then serves
# --------------------------------------------------------------------------

def _warm(country: str, table: str) -> Path | None:
    p = data_root() / country / "var" / f"{table}.parquet"
    return p if p.exists() else None


@pytest.mark.parametrize("country,table", [
    ("China", "cluster_features"),      # YAML path
    ("Cambodia", "food_acquired"),      # make path -- the class #809 names
])
def test_a_destamped_parquet_rebuilds_once_and_does_not_loop(country, table):
    """The loop is the risk worth measuring, not the rebuild.

    If the rebuild descent wrote the country parquet hashless too, "hashless is
    stale" would rebuild on every read forever. It does not: all three
    country-parquet writes in the descent pass `cache_hash=`.
    """
    import pyarrow.parquet as pq

    p = _warm(country, table)
    if p is None:
        pytest.skip(f"{country}/{table} not warm")

    ref = getattr(Country(country), table)()
    backup = p.with_suffix(".parquet.gh809bak")
    shutil.copyfile(p, backup)
    try:
        tbl = pq.read_table(str(p))
        md = dict(tbl.schema.metadata or {})
        md.pop(b"lsms_cache_hash", None)
        _atomic_write_table(tbl.replace_schema_metadata(md), p)
        assert read_parquet_cache_hash(p) is None

        first = getattr(Country(country), table)()
        stamped = read_parquet_cache_hash(p)
        assert stamped is not None, "the rebuild did not stamp -> every read loops"

        second = getattr(Country(country), table)()
        assert read_parquet_cache_hash(p) == stamped, "hash churns between reads"

        assert first.equals(ref), "the rebuild changed the served data"
        assert second.equals(ref)
    finally:
        shutil.copyfile(backup, p)
        backup.unlink(missing_ok=True)

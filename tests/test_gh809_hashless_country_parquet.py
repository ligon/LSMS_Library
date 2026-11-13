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
import ast
import textwrap
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
    """`Wave.grab_data` keeps the legacy branch; only the country gate changed.

    GH #1040 moved the YAML-path L2-wave read gate out of the tagged
    `grab_data` into `Wave._read_l2_wave_cache`.  Read the UNION, as
    `_country_read_path_source` below does for #908 -- and require that
    `grab_data` still calls the gate, so the union cannot be satisfied by a
    helper nothing reaches.  (Delivered behaviour: tests/
    test_gh1040_wave_read_gates.py::test_site1_legacy_is_trusted_once_and_stamped.)
    """
    grab = inspect.getsource(country_mod.Wave.grab_data)
    assert "self._read_l2_wave_cache(" in grab, "grab_data no longer calls the wave gate"
    src = grab + inspect.getsource(country_mod.Wave._read_l2_wave_cache)
    assert "stamp_parquet_hash" in src, (
        "the wave tier lost its trust-once-and-stamp; #809 is country-tier only"
    )


# --------------------------------------------------------------------------
# The country tier no longer trusts, and no longer stamps
# --------------------------------------------------------------------------

def _country_read_path_source() -> str:
    # Only the actual read gates, not fresh_make_country's verified producer
    # stamp. Caller linkage/cache-hit branches are pinned independently below.
    return (inspect.getsource(Country._read_l2_country_cache)
            + inspect.getsource(Country._read_assumed_fresh_cache))


def test_country_cache_hit_branches_reach_read_gate_and_never_write():
    tree = ast.parse(textwrap.dedent(inspect.getsource(Country._aggregate_wave_data)))
    routes = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    for route_name, hit_name in [('load_dataframe_with_dvc', 'cached_df'),
                                  ('fresh_make_country', 'reused')]:
        route = routes[route_name]
        gates = [n for n in ast.walk(route) if isinstance(n, ast.Call)
                 and ast.unparse(n.func) == 'self._read_l2_country_cache']
        assert len(gates) == 1
        hits = [n for n in ast.walk(route) if isinstance(n, ast.If)
                and ((route_name == 'load_dataframe_with_dvc' and ast.unparse(n.test) == 'miss_reason is None')
                     or (route_name == 'fresh_make_country' and ast.unparse(n.test) == 'reused is not None'))]
        assert len(hits) == 1 and gates[0].lineno < hits[0].lineno
        hit = ast.Module(body=hits[0].body, type_ignores=[])
        assert any(isinstance(n, ast.Return) and hit_name in ast.unparse(n.value) for n in ast.walk(hit))
        forbidden = {'stamp_parquet_hash', 'to_parquet', '_atomic_write_table', 'replace', 'unlink', 'write_bytes', 'write_text'}
        calls = [n.func.id if isinstance(n.func, ast.Name) else n.func.attr
                 for n in ast.walk(hit) if isinstance(n, ast.Call) and isinstance(n.func, (ast.Name, ast.Attribute))]
        assert forbidden.isdisjoint(calls)


def test_the_country_gate_converts_legacy_to_stale():
    src = _country_read_path_source()
    assert 'freshness == "legacy"' in src and 'freshness = "stale"' in src, (
        "the country-tier gate no longer downgrades a hashless parquet (GH #809)"
    )


def test_the_country_gate_never_stamps_a_parquet_it_served():
    """Stamping unverified content is what made the defect permanent."""
    src = _country_read_path_source()
    assert "stamp_parquet_hash" not in src, (
        "the country-tier read path stamps a hash onto a parquet it did not "
        "verify -- that is the laundering #809 is about"
    )


# --------------------------------------------------------------------------
# Delivered: a de-stamped parquet rebuilds ONCE, then serves
# --------------------------------------------------------------------------

@pytest.fixture
def private_country_root(tmp_path):
    """A private ``LSMS_DATA_DIR`` holding a COPY of one country's warm cache.

    The test below de-stamps ``var/<table>.parquet`` and forces a rebuild,
    which (since GH #866) deletes the file first.  In the shared root that
    window is visible to every other xdist worker reading the cell -- a
    different file, so ``--dist=loadfile`` does not separate them (GH #778).
    Copy the country's subtree into ``tmp_path``, symlink ``dvc-cache`` to the
    real blob store, and flip the root with a before/finally
    ``data_root.cache_clear()`` (the discipline of
    ``test_gh866...::private_helper_root``).  Yields
    ``isolate(country, table) -> Path``, skipping before any copy if cold.
    """
    shared = data_root()
    root = tmp_path / "data"
    root.mkdir()
    with pytest.MonkeyPatch.context() as patch:
        def isolate(country: str, table: str) -> Path:
            src = shared / country / "var" / f"{table}.parquet"
            if not src.exists():
                pytest.skip(f"{country}/{table} not warm")
            blobs = shared / "dvc-cache"
            if blobs.exists() and not (root / "dvc-cache").exists():
                (root / "dvc-cache").symlink_to(blobs.resolve(), target_is_directory=True)
            shutil.copytree(shared / country, root / country, symlinks=True)
            patch.setenv("LSMS_DATA_DIR", str(root))
            data_root.cache_clear()
            assert data_root() == root
            return root / country / "var" / f"{table}.parquet"
        try:
            yield isolate
        finally:
            patch.undo()
            data_root.cache_clear()
            shutil.rmtree(root, ignore_errors=True)


@pytest.mark.parametrize("country,table", [
    ("China", "cluster_features"),      # YAML path
    ("Cambodia", "food_acquired"),      # make path -- the class #809 names
])
def test_a_destamped_parquet_rebuilds_once_and_does_not_loop(country, table,
                                                              private_country_root):
    """The loop is the risk worth measuring, not the rebuild.

    If the rebuild descent wrote the country parquet hashless too, "hashless is
    stale" would rebuild on every read forever. It does not: all three
    country-parquet writes in the descent pass `cache_hash=`.
    """
    import pyarrow.parquet as pq

    p = private_country_root(country, table)

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


def test_fresh_invented_country_cache_hit_preserves_bytes_and_never_stamps(tmp_path, monkeypatch):
    from types import SimpleNamespace, MethodType
    import pandas as pd
    from lsms_library import local_tools as LT
    path = tmp_path / 'fresh.parquet'
    frame = pd.DataFrame({'x': [1.0]}, index=pd.MultiIndex.from_tuples([('w1', 'h1')], names=['t', 'i']))
    LT.to_parquet(frame, path, absolute_path=True, cache_hash='current', cache_waves=['w1'])
    expected = LT.get_dataframe(path)
    before = path.read_bytes(), path.stat().st_mtime_ns
    receiver = SimpleNamespace(name='Invented', waves=['w1'], wave_folder_map={})
    receiver._table_cache_hash = lambda table, waves: 'current'
    for name in ['_cache_covers_waves', '_restrict_to_waves', '_grain_reports_for_waves']:
        setattr(receiver, name, MethodType(getattr(Country, name), receiver))
    def refuse(*args, **kwargs):
        pytest.fail('cache hit attempted a write/stamp')
    monkeypatch.setattr(country_mod, 'stamp_parquet_hash', refuse)
    monkeypatch.setattr(country_mod, 'to_parquet', refuse)
    for _ in range(2):
        actual, covered, reason = Country._read_l2_country_cache(receiver, 'facts', path, ['w1'])
        assert covered == ['w1'] and reason is None
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        assert (path.read_bytes(), path.stat().st_mtime_ns) == before

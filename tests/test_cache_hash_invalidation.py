"""Tests for v0.8.0 L2 cache content-hash invalidation.

Covers the primitives in ``local_tools`` (file hashing, source
fingerprinting, parquet metadata stamp/read, freshness classification,
atomic writes) and the wiring in ``country.py`` (per-wave / per-country
input hashes, trust-once-then-stamp migration, stale-not-served).

The unit tests are fully offline.  The country-level integration tests
seed a temporary ``LSMS_DATA_DIR`` with synthetic parquets and never
touch the network or the user's real cache.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from lsms_library import local_tools as lt


# --------------------------------------------------------------------------
# Unit: cached_file_hash
# --------------------------------------------------------------------------
def test_cached_file_hash_content_sensitive(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("v1")
    h1 = lt.cached_file_hash(f)
    assert h1 == lt.cached_file_hash(f)  # same content -> same hash
    # A same-length edit must still change the hash (no stale memo).
    f.write_text("v2")
    h2 = lt.cached_file_hash(f)
    assert h2 != h1
    assert lt.cached_file_hash(tmp_path / "missing") is None


# --------------------------------------------------------------------------
# Unit: source_fingerprint
# --------------------------------------------------------------------------
def test_source_fingerprint_prefers_sidecar(tmp_path):
    src = tmp_path / "data.dta"
    src.write_bytes(b"\x00" * 2048)
    fp_raw = lt.source_fingerprint(src)
    assert fp_raw.startswith("raw:")

    (tmp_path / "data.dta.dvc").write_text(
        "outs:\n- md5: deadbeef\n  size: 2048\n  path: data.dta\n"
    )
    fp_dvc = lt.source_fingerprint(src)
    assert fp_dvc.startswith("dvc:")
    assert fp_dvc != fp_raw  # sidecar takes precedence and changes the value

    assert lt.source_fingerprint(tmp_path / "nope.dta") == "missing:nope.dta"


def test_source_fingerprint_tracks_sidecar_md5(tmp_path):
    src = tmp_path / "x.dta"
    src.write_bytes(b"abc")
    sidecar = tmp_path / "x.dta.dvc"
    sidecar.write_text("outs:\n- md5: aaa\n  path: x.dta\n")
    fp1 = lt.source_fingerprint(src)
    os.utime(sidecar, None)
    sidecar.write_text("outs:\n- md5: bbb\n  path: x.dta\n")
    fp2 = lt.source_fingerprint(src)
    assert fp1 != fp2  # a re-`dvc add` (md5 change) flips the fingerprint


# --------------------------------------------------------------------------
# Unit: scan_script_data_refs
# --------------------------------------------------------------------------
def test_scan_script_data_refs(tmp_path):
    s = tmp_path / "food_acquired.py"
    s.write_text(
        "import x\n"
        "get_dataframe('../Data/foo.dta')\n"
        "pd.read_csv('bar.csv')\n"
        "df.to_parquet('out.parquet')\n"   # also a data suffix -> picked up
        "# baz.tab in a comment is ignored\n"
        "name = 'not_a_file'\n"
    )
    refs = set(lt.scan_script_data_refs(s))
    assert "../Data/foo.dta" in refs
    assert "bar.csv" in refs
    assert "not_a_file" not in refs
    # comment content is not a literal
    assert "baz.tab" not in refs


# --------------------------------------------------------------------------
# Unit: to_parquet roundtrip + metadata + atomicity
# --------------------------------------------------------------------------
def _frame():
    return pd.DataFrame(
        {"t": ["2019", "2019"], "i": ["a", "b"], "x": [1.0, 2.0]}
    ).set_index(["t", "i"])


def test_to_parquet_roundtrip_with_and_without_hash(tmp_path):
    df = _frame()

    p_plain = tmp_path / "plain.parquet"
    lt.to_parquet(df, p_plain, absolute_path=True)
    assert lt.read_parquet_cache_hash(p_plain) is None
    assert pd.read_parquet(p_plain).equals(df)

    p_hash = tmp_path / "hashed.parquet"
    lt.to_parquet(df, p_hash, absolute_path=True, cache_hash="H1")
    assert lt.read_parquet_cache_hash(p_hash) == "H1"
    assert pd.read_parquet(p_hash).equals(df)


def test_to_parquet_leaves_no_tmp_files(tmp_path):
    df = _frame()
    lt.to_parquet(df, tmp_path / "a.parquet", absolute_path=True)
    lt.to_parquet(df, tmp_path / "b.parquet", absolute_path=True, cache_hash="H")
    assert not list(tmp_path.glob("*.tmp.*"))


def test_to_parquet_overwrite_is_atomic_replace(tmp_path):
    p = tmp_path / "a.parquet"
    lt.to_parquet(_frame(), p, absolute_path=True, cache_hash="H1")
    # Overwrite with new data + new hash; must fully replace, no desync.
    df2 = pd.DataFrame({"t": ["2020"], "i": ["z"], "x": [9.0]}).set_index(["t", "i"])
    lt.to_parquet(df2, p, absolute_path=True, cache_hash="H2")
    assert lt.read_parquet_cache_hash(p) == "H2"
    assert pd.read_parquet(p).equals(df2)


# --------------------------------------------------------------------------
# Unit: cache_freshness classification
# --------------------------------------------------------------------------
def test_cache_freshness_classifications(tmp_path):
    p_legacy = tmp_path / "legacy.parquet"
    lt.to_parquet(_frame(), p_legacy, absolute_path=True)
    p_hashed = tmp_path / "hashed.parquet"
    lt.to_parquet(_frame(), p_hashed, absolute_path=True, cache_hash="H1")

    assert lt.cache_freshness(p_hashed, None) == "unverifiable"
    assert lt.cache_freshness(p_hashed, "H1") == "fresh"
    assert lt.cache_freshness(p_hashed, "H2") == "stale"
    assert lt.cache_freshness(p_legacy, "H1") == "legacy"


def test_stamp_parquet_hash_migrates_legacy(tmp_path):
    p = tmp_path / "legacy.parquet"
    lt.to_parquet(_frame(), p, absolute_path=True)
    assert lt.read_parquet_cache_hash(p) is None
    assert lt.stamp_parquet_hash(p, "HX") is True
    assert lt.read_parquet_cache_hash(p) == "HX"
    # Data is preserved across the metadata rewrite.
    assert pd.read_parquet(p).equals(_frame())
    # Idempotent.
    assert lt.stamp_parquet_hash(p, "HX") is True
    assert lt.stamp_parquet_hash(p, None) is False


def test_read_parquet_cache_hash_on_garbage(tmp_path):
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not a parquet")
    assert lt.read_parquet_cache_hash(bad) is None
    assert lt.cache_freshness(bad, "H") == "legacy"  # unreadable -> no stored hash


# --------------------------------------------------------------------------
# Integration helpers (synthetic temp cache; offline)
# --------------------------------------------------------------------------
@pytest.fixture
def temp_data_dir(monkeypatch):
    """Point the library at an empty temporary data root.

    ``paths.data_root`` is ``@lru_cache``-d, so we clear its cache after
    setting the env var (and again on teardown) to avoid leaking a temp
    path into later tests.
    """
    from lsms_library.paths import data_root
    d = Path(tempfile.mkdtemp(prefix="lsms_hashtest_"))
    monkeypatch.setenv("LSMS_DATA_DIR", str(d))
    data_root.cache_clear()
    assert str(data_root()) == str(d)
    yield d
    shutil.rmtree(d, ignore_errors=True)
    data_root.cache_clear()


def _make_country(name="Albania"):
    import lsms_library as ll
    return ll.Country(name)


def test_input_hash_is_computable_and_stable(temp_data_dir):
    c = _make_country()
    waves = c.waves
    assert waves, "fixture country should have waves"
    h_country = c._table_cache_hash("housing", waves)
    assert isinstance(h_country, str) and len(h_country) == 64
    assert h_country == c._table_cache_hash("housing", waves)  # stable
    # Different table -> different hash.
    h_other = c._table_cache_hash("sample", waves)
    assert h_other != h_country
    # Per-wave hashes are computable.
    wh = c[waves[0]]._input_hash("housing")
    assert isinstance(wh, str) and len(wh) == 64


def test_schema_version_is_a_real_lever(temp_data_dir, monkeypatch):
    """Bumping LSMS_CACHE_SCHEMA must change the composed hash."""
    c = _make_country()
    waves = c.waves
    before = c[waves[0]]._input_hash("housing")
    monkeypatch.setattr(lt, "LSMS_CACHE_SCHEMA", lt.LSMS_CACHE_SCHEMA + 1)
    # country.py imported the constant by value; patch it there too.
    import lsms_library.country as country_mod
    monkeypatch.setattr(country_mod, "LSMS_CACHE_SCHEMA", lt.LSMS_CACHE_SCHEMA)
    after = c[waves[0]]._input_hash("housing")
    assert after != before


def test_legacy_country_parquet_is_trust_once_stamped(temp_data_dir):
    """A pre-hash L2-country parquet is read (not rebuilt) and stamped so
    the next read is guarded."""
    c = _make_country()
    waves = c.waves
    expected = c._table_cache_hash("housing", waves)

    # Seed a synthetic legacy var/ parquet (no embedded hash).
    var_p = temp_data_dir / c.name / "var" / "housing.parquet"
    var_p.parent.mkdir(parents=True, exist_ok=True)
    sentinel = pd.DataFrame(
        {"i": ["h1", "h2"], "t": [waves[0], waves[0]],
         "v": ["c1", "c2"], "Roof": ["Grass", "Iron Sheets"]}
    ).set_index(["i", "t", "v"])
    lt.to_parquet(sentinel, var_p, absolute_path=True)  # legacy: no hash
    assert lt.read_parquet_cache_hash(var_p) is None

    df = c.housing()
    # The fast path served our seeded data (proves no rebuild from source).
    assert "Roof" in df.columns
    assert set(df["Roof"]) <= {"Grass", "Iron Sheets"}
    # ... and migrated the parquet to the current hash.
    assert lt.read_parquet_cache_hash(var_p) == expected
    assert lt.cache_freshness(var_p, expected) == "fresh"


def test_stale_country_parquet_is_not_served(temp_data_dir):
    """A wrong-stamped (stale) L2-country parquet must NOT be returned;
    the read falls through to a rebuild attempt."""
    c = _make_country()
    waves = c.waves

    var_p = temp_data_dir / c.name / "var" / "housing.parquet"
    var_p.parent.mkdir(parents=True, exist_ok=True)
    SENTINEL = "DO_NOT_SERVE_ME"
    stale = pd.DataFrame(
        {"i": ["h1"], "t": [waves[0]], "v": ["c1"], "Roof": [SENTINEL]}
    ).set_index(["i", "t", "v"])
    # Stamp a deliberately wrong hash so the parquet classifies as stale.
    lt.to_parquet(stale, var_p, absolute_path=True, cache_hash="WRONGHASH")
    assert lt.cache_freshness(var_p, c._table_cache_hash("housing", waves)) == "stale"

    # Reading now must rebuild from source.  Albania housing has no local
    # source/Makefile here, so the rebuild yields no sentinel rows (it may
    # return empty, warn, or raise) -- the contract under test is simply
    # that the STALE sentinel is never served.
    try:
        df = c.housing()
    except Exception:
        return  # rebuild attempted and failed loudly -> stale was rejected
    if isinstance(df, pd.DataFrame) and "Roof" in df.columns:
        assert SENTINEL not in set(df["Roof"].astype(str))


def test_country_level_scripts_are_in_the_hash(temp_data_dir, monkeypatch):
    """Regression for CRITICAL-1: editing a country-level concatenator
    (``{country}/_/{table}.py``) or the country module
    (``{country}/_/{name}.py``) must affect the hash.  We prove they are
    inputs by recording which paths the hasher is asked for."""
    import lsms_library.country as country_mod
    asked: list[str] = []
    real = country_mod.cached_file_hash

    def recording(path):
        asked.append(str(path))
        return real(path)

    monkeypatch.setattr(country_mod, "cached_file_hash", recording)

    c = country_mod.Country("Uganda")
    c._table_cache_hash("food_acquired", c.waves)

    base = str(c.file_path / "_")
    assert f"{base}/food_acquired.py" in asked, "country-level table script not hashed"
    assert f"{base}/uganda.py" in asked, "country module not hashed"


def test_evict_hashless_wave_caches_targets_only_hashless(temp_data_dir):
    """Regression for CRITICAL-2 (unit): eviction deletes hashless
    (script-written) L2-wave parquets but spares stamped (YAML) ones."""
    import lsms_library as ll
    c = ll.Country("Albania")
    table = "food_acquired"
    root = temp_data_dir / "Albania"

    hashless = root / "2002" / "_" / f"{table}.parquet"
    hashless.parent.mkdir(parents=True)
    lt.to_parquet(_frame(), hashless, absolute_path=True)  # no hash -> script-like

    stamped = root / "2003" / "_" / f"{table}.parquet"
    stamped.parent.mkdir(parents=True)
    lt.to_parquet(_frame(), stamped, absolute_path=True, cache_hash="WH")  # YAML-like

    c._evict_hashless_wave_caches(table)

    assert not hashless.exists(), "hashless script wave parquet should be evicted"
    assert stamped.exists(), "stamped YAML wave parquet must be preserved"


def test_stale_country_read_evicts_hashless_wave_parquet(temp_data_dir):
    """Regression for CRITICAL-2 (integration): a stale L2-country read
    evicts hashless wave parquets so the rebuild descent can't reuse
    them."""
    import lsms_library as ll
    c = ll.Country("Albania")
    waves = c.waves

    # Use a table Albania actually declares so the read reaches the gate.
    table = "housing"
    var_p = temp_data_dir / "Albania" / "var" / f"{table}.parquet"
    var_p.parent.mkdir(parents=True, exist_ok=True)
    lt.to_parquet(_frame(), var_p, absolute_path=True, cache_hash="WRONGHASH")  # stale
    assert lt.cache_freshness(var_p, c._table_cache_hash(table, waves)) == "stale"

    wave_p = temp_data_dir / "Albania" / waves[0] / "_" / f"{table}.parquet"
    wave_p.parent.mkdir(parents=True, exist_ok=True)
    lt.to_parquet(_frame(), wave_p, absolute_path=True)  # hashless script parquet
    assert wave_p.exists()

    try:
        c.housing()
    except Exception:
        pass  # rebuild may fail offline; the contract is the eviction
    assert not wave_p.exists(), "stale read must evict the hashless wave parquet"


def test_assume_cache_fresh_skips_hash_and_serves_stale(temp_data_dir):
    """assume_cache_fresh is the documented escape: it bypasses the hash
    check and serves whatever parquet is present."""
    import lsms_library as ll
    c0 = ll.Country("Albania")
    waves = c0.waves
    var_p = temp_data_dir / "Albania" / "var" / "housing.parquet"
    var_p.parent.mkdir(parents=True, exist_ok=True)
    SENTINEL = "ESCAPE_HATCH"
    stale = pd.DataFrame(
        {"i": ["h1"], "t": [waves[0]], "v": ["c1"], "Roof": [SENTINEL]}
    ).set_index(["i", "t", "v"])
    lt.to_parquet(stale, var_p, absolute_path=True, cache_hash="WRONGHASH")

    c = ll.Country("Albania", assume_cache_fresh=True)
    df = c.housing()
    # The escape hatch returns the (stale) cached parquet verbatim.
    assert SENTINEL in set(df["Roof"].astype(str))

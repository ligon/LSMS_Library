"""Ordinary L2 country hashes must account for country-script DVC literals."""

import os
from types import SimpleNamespace

import pytest

from lsms_library import country as C, local_tools as L

from tests.test_gh1288_zero_exit_freshness import (
    OTHER, TABLE, WAVES, file_state, model, payload,
)


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
def test_country_sidecar_drift_rejects_ordinary_warm_cache(model):
    """Use the real hash and read gate, without calling any source producer."""
    script = model.source / '_/facts.py'
    script.write_text("SOURCE = '../Data/facts.csv'\n")
    sidecar = model.source / 'Data/facts.csv.dvc'
    sidecar.parent.mkdir()
    sidecar.write_text('outs:\n- md5: ' + 'a' * 32 + '\n')
    c = model.country
    full_hash = c._table_cache_hash(TABLE, WAVES)
    other_hash = c._table_cache_hash(OTHER, WAVES)
    wave_hashes = [c[w]._input_hash(TABLE) for w in WAVES]
    assert c._table_cache_hash(TABLE, WAVES[:1]) == full_hash
    model.seed(model.selected, payload(), current=True, waves=WAVES)
    stored = file_state(model.selected)
    frame, covered, reason = c._read_l2_country_cache(TABLE, model.selected, WAVES[:1])
    assert frame is not None and covered == WAVES and reason is None

    before = sidecar.stat()
    sidecar.write_text('outs:\n- md5: ' + 'b' * 32 + '\n')
    os.utime(sidecar, ns=(before.st_atime_ns, before.st_mtime_ns))

    frame, _, reason = c._read_l2_country_cache(TABLE, model.selected, WAVES[:1])
    assert frame is None and reason.startswith('STALE:')
    assert c._table_cache_hash(TABLE, WAVES) != full_hash
    assert c._table_cache_hash(TABLE, WAVES) == c._table_cache_hash(TABLE, WAVES[:1])
    assert c._table_cache_hash(OTHER, WAVES) == other_hash
    assert [c[w]._input_hash(TABLE) for w in WAVES] == wave_hashes
    assert file_state(model.selected) == stored
    assert not model.calls  # the read gate must not invoke any producer


def _sidecar(path, char='a'):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('outs:\n- md5: ' + char * 32 + '\n')


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
def test_raw_only_outside_and_json_inputs_do_not_move_parquet_hash(model):
    script = model.source / '_/facts.py'
    local_raw = model.source / 'Data/unmanaged.csv'
    local_raw.parent.mkdir()
    local_raw.write_text('old invented contents')
    outside = model.source.parent / 'outside.csv.dvc'
    _sidecar(outside)
    panel = model.source / '_/panel_ids.py'
    panel.write_text("SOURCE = '../Data/panel.csv'\n")
    panel_sidecar = model.source / 'Data/panel.csv.dvc'
    _sidecar(panel_sidecar)
    script.write_text("LOCAL = '../Data/unmanaged.csv'\nOUTSIDE = '../../outside.csv'\n"
                      "INVALID = '../Data/invalid\\x00.csv'\n")
    hash_table = model.country._table_cache_hash
    fact = hash_table(TABLE, WAVES)
    panel_hash = hash_table('panel_ids', WAVES)
    assert fact and panel_hash

    local_raw.write_text('replaced invented contents')
    _sidecar(outside, 'b')
    _sidecar(panel_sidecar, 'b')
    assert hash_table(TABLE, WAVES) == fact
    assert hash_table('panel_ids', WAVES) == panel_hash


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
def test_escaping_sidecar_symlink_is_not_read(model):
    script = model.source / '_/facts.py'
    script.write_text("SOURCE = '../Data/facts.csv'\n")
    outside = model.source.parent / 'outside.csv.dvc'
    sidecar = model.source / 'Data/facts.csv.dvc'
    sidecar.parent.mkdir()
    _sidecar(outside)
    try:
        sidecar.symlink_to(outside)
    except OSError:
        pytest.skip('environment does not allow symlinks')
    hash_table = model.country._table_cache_hash
    previous = hash_table(TABLE, WAVES)
    _sidecar(outside, 'b')
    assert hash_table(TABLE, WAVES) == previous


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
def test_script_edit_refreshes_literal_discovery(model):
    script = model.source / '_/facts.py'
    first = model.source / 'Data/one.csv.dvc'
    second = model.source / 'Data/two.csv.dvc'
    _sidecar(first)
    _sidecar(second)
    script.write_text("SOURCE = '../Data/one.csv'\n")
    hash_table = model.country._table_cache_hash
    initial = hash_table(TABLE, WAVES)
    script.write_text("SOURCE = '../Data/two.csv'\n")
    updated = hash_table(TABLE, WAVES)
    assert updated != initial
    _sidecar(first, 'b')
    assert hash_table(TABLE, WAVES) == updated
    _sidecar(second, 'b')
    assert hash_table(TABLE, WAVES) != updated


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
def test_non_utf8_country_script_keeps_content_invalidation(model):
    script = model.source / '_/facts.py'
    one = b"# coding: latin-1\nNOTE = 'caf\xe9'\n"
    two = b"# coding: latin-1\nNOTE = 'caf\xe8'\n"
    for src in (one, two):
        compile(src, str(script), 'exec')
    hash_table = model.country._table_cache_hash
    script.write_bytes(one)
    previous = hash_table(TABLE, WAVES)
    script.write_bytes(two)
    following = hash_table(TABLE, WAVES)
    assert previous and following and previous != following


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
def test_unencodable_literal_does_not_hide_other_sidecars(model):
    script = model.source / '_/facts.py'
    script.write_text("INVALID = '../Data/00\\ud800.csv'\nSOURCE = '../Data/facts.csv'\n")
    sidecar = model.source / 'Data/facts.csv.dvc'
    _sidecar(sidecar)
    hash_table = model.country._table_cache_hash
    previous = hash_table(TABLE, WAVES)
    _sidecar(sidecar, 'b')
    following = hash_table(TABLE, WAVES)
    assert previous and following and previous != following


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
def test_filesystem_surrogateescape_sidecar_has_valid_hash(model):
    if os.name != 'posix' or os.fsencode('\udcff') != b'\xff':
        pytest.skip('environment lacks POSIX byte filenames')
    script = model.source / '_/facts.py'
    script.write_text("SOURCE = '../Data/raw\\udcff.csv'\n")
    sidecar = model.source / 'Data/raw\udcff.csv.dvc'
    _sidecar(sidecar)
    hash_table = model.country._table_cache_hash
    previous = hash_table(TABLE, WAVES)
    _sidecar(sidecar, 'b')
    following = hash_table(TABLE, WAVES)
    assert previous and following and previous != following


@pytest.mark.parametrize('model', ['Alpha'], indirect=True)
@pytest.mark.parametrize('no_repo', [False, True], ids=['no-stages', 'repo-unavailable'])
def test_default_country_only_rebuild_replaces_stale_payload(model, monkeypatch, no_repo):
    """Both default writers reach the existing country-only producer, not the old file."""
    script = model.source / '_/facts.py'
    script.write_text("SOURCE = '../Data/facts.csv'\n")
    sidecar = model.source / 'Data/facts.csv.dvc'
    _sidecar(sidecar)
    for wave in WAVES:
        root = model.source / wave / '_'
        (root / 'facts.py').unlink()
        (root / 'data_info.yml').write_text('support: null\n')
    c = model.country
    assert all(TABLE not in c[w].data_scheme for w in WAVES)
    assert not c._resolve_materialize_stages(TABLE, WAVES)
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'dvc')

    def repo(*args):
        if no_repo:
            raise FileNotFoundError('invented missing DVC repo')
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(C, 'Repo', repo)
    model.seed(model.selected, payload(values=(900., 901.)), current=True, waves=WAVES)
    prior_hash = L.read_parquet_cache_hash(model.selected)
    assert model.load()['x'].tolist() == [900., 901.]
    assert not model.calls
    _sidecar(sidecar, 'b')
    model.country_frame = payload(values=(30., 40.))
    result = model.load()
    assert result['x'].tolist() == [30., 40.]
    assert [e[0] for e in model.events] == ['country']
    assert L.read_parquet_cache_waves(model.selected) == WAVES
    rebuilt_hash = L.read_parquet_cache_hash(model.selected)
    assert rebuilt_hash == c._table_cache_hash(TABLE, WAVES) != prior_hash
    calls = len(model.calls)
    assert model.load()['x'].tolist() == [30., 40.]
    assert len(model.calls) == calls

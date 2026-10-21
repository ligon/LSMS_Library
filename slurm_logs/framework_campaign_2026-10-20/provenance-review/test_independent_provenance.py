"""Reviewer-owned hermetic controls, prepared before the final source freeze."""
from collections import Counter
from dataclasses import replace
import importlib.util
import io
import os
from pathlib import Path
import subprocess
import urllib.error
import urllib.request
import zipfile

import pytest
import lsms_library as ll
from lsms_library import provenance as pv, data_access as da

TOP = Path('/local/job40255479/worktrees/1115-1128-provenance')
COUNTRY, WAVE = 'ReviewLand', '2020'
OLD_ID, NEW_ID = '987654320', '987654321'
OLD_RELEASE, NEW_RELEASE = 'RVW_2020_v01', 'RVW_2020_v02'


@pytest.fixture(autouse=True)
def identity_and_no_network(monkeypatch):
    assert Path(ll.__file__).resolve().is_relative_to(TOP)
    assert Path.cwd().resolve() == TOP
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == TOP / 'lsms_library/countries'
    expected = os.environ['REVIEW_FROZEN_HEAD']
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == expected
    def forbid(*args, **kwargs):
        raise AssertionError('Independent review forbids unmocked network')
    monkeypatch.setattr(urllib.request, 'urlopen', forbid)


def record(catalog_id=NEW_ID, held=OLD_RELEASE, **kwargs):
    return pv.render_source_org(pv.WaveProvenance(
        country=COUNTRY, wave=WAVE, source=pv.SOURCE_WORLDBANK,
        catalog_id=catalog_id, idno=held,
        url=f'https://microdata.worldbank.org/index.php/catalog/{catalog_id}',
        latest_idno=NEW_RELEASE, latest_seen='2026-10-19',
        recorded='2026-10-18', notes='A human paragraph.\n\n#+IN_NOTE: retained.',
        extra={'REVIEW_FACT': ['first', '', 'first']}, **kwargs))


def archive(*names):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as handle:
        for name in names:
            handle.writestr('release/' + name, b'new synthetic release')
    return buffer.getvalue()


@pytest.fixture
def rig(tmp_path, monkeypatch):
    root = tmp_path / 'config'
    root.mkdir()
    source = pv.source_org_path(root, COUNTRY, WAVE)
    source.parent.mkdir(parents=True)
    source.write_text(record())
    monkeypatch.setattr(da, '_COUNTRIES_DIR', root)
    monkeypatch.setattr(da, 'permissions', lambda *args, **kwargs: {'wb_api': 'read'})
    monkeypatch.setattr(da.config, 'microdata_api_key', lambda: 'synthetic-key')
    monkeypatch.setattr(da, '_get_catalog_idno', lambda url: NEW_RELEASE)
    monkeypatch.setattr(da, '_find_stata_zip_url', lambda *args: ('https://review.invalid/data.zip', 'data.zip'))
    monkeypatch.setattr(da, '_wb_catalog_search', lambda *args, **kwargs: [])
    state = {'zip': archive('a.dta', 'b.dta'), 'fail': False, 'receipt': 'all', 'push_calls': 0}
    def open_zip(*args, **kwargs):
        if state['fail']:
            raise urllib.error.URLError('review synthetic unavailable')
        return io.BytesIO(state['zip'])
    monkeypatch.setattr(urllib.request, 'urlopen', open_zip)
    def push(paths, remote=None, dvc_add=True):
        state['push_calls'] += 1
        assert source.read_text() == state['before'], 'SOURCE changed before push completed'
        mode = state['receipt']
        if mode == 'raises':
            raise RuntimeError('review synthetic push failure')
        if mode == 'none':
            return []
        if mode == 'partial':
            return list(paths)[:1]
        if mode == 'duplicate':
            return [paths[0], paths[0]]
        if mode == 'wrong':
            return [Path('unrelated-a.dta'), Path('unrelated-b.dta')]
        return [root / path for path in paths]
    monkeypatch.setattr(da, 'push_to_cache_batch', push)
    da._ACQUIRED_RELEASES.pop((COUNTRY, WAVE), None)
    state.update(root=root, source=source, before=source.read_text())
    yield state
    da._ACQUIRED_RELEASES.pop((COUNTRY, WAVE), None)


def acquire(rig, entrypoint='add_wave', push=True):
    if entrypoint == 'populate_and_push':
        return da.populate_and_push(COUNTRY, WAVE)
    return da.add_wave(COUNTRY, NEW_ID, wave=WAVE, confirm=False, push=push, verbose=False)


@pytest.mark.parametrize('mode', ['none', 'partial', 'duplicate', 'wrong', 'raises'])
@pytest.mark.parametrize('entrypoint', ['add_wave', 'populate_and_push'])
def test_each_incomplete_push_keeps_exact_source(rig, mode, entrypoint):
    rig['receipt'] = mode
    if mode == 'raises':
        with pytest.raises(RuntimeError, match='review synthetic push failure'):
            acquire(rig, entrypoint)
    else:
        acquire(rig, entrypoint)
    assert rig['source'].read_text() == rig['before']
    assert (COUNTRY, WAVE) not in da._ACQUIRED_RELEASES


@pytest.mark.parametrize('push', [False, True])
def test_complete_same_entry_download_commits_held_and_clears_queue(rig, push):
    assert len(acquire(rig, push=push)) == 2
    after = pv.read_provenance(rig['root'], COUNTRY, WAVE)
    assert after.idno == NEW_RELEASE
    assert after.latest_idno is None and after.latest_seen is None
    assert after.notes == 'A human paragraph.\n\n#+IN_NOTE: retained.'
    assert after.extra == {'REVIEW_FACT': ['first', '', 'first']}
    assert rig['push_calls'] == int(push)


def test_failed_cross_entry_keeps_every_original_byte(rig):
    rig['source'].write_text(record(catalog_id=OLD_ID, held='DIFFERENT_ENTRY_v01',
                                    note='A recorded primary-entry fact'))
    before = rig['source'].read_bytes()
    rig.update(fail=True, before=before.decode())
    assert acquire(rig, push=False) == []
    assert rig['source'].read_bytes() == before


def test_empty_download_does_not_claim_a_release(rig):
    rig['zip'] = archive('README.txt')
    assert acquire(rig, push=False) == []
    assert rig['source'].read_text() == rig['before']


def test_existing_skipped_file_prevents_release_claim(rig):
    data = rig['root'] / COUNTRY / WAVE / 'Data'
    data.mkdir()
    (data / 'a.dta').write_bytes(b'old synthetic bytes')
    acquire(rig, push=False)
    assert rig['source'].read_text() == rig['before']
    assert (data / 'a.dta').read_bytes() == b'old synthetic bytes'


def test_repeated_empty_unknown_metadata_and_note_keywords_survive():
    text = (record() + '#+REVIEW_REPEAT: one\n#+REVIEW_REPEAT:\n'
            '#+REVIEW_REPEAT: one\n#+REVIEW_AFTER: two\n')
    # These additions are notes (below NOTES_HEADING), hence must remain prose.
    original = pv.parse_source_org(text, COUNTRY, WAVE)
    rendered = pv.render_source_org(original)
    assert pv.parse_source_org(rendered, COUNTRY, WAVE) == original
    assert Counter(text.splitlines())['#+REVIEW_REPEAT: one'] == Counter(rendered.splitlines())['#+REVIEW_REPEAT: one']
    assert '#+REVIEW_REPEAT:\n#+REVIEW_REPEAT: one' in rendered


@pytest.mark.parametrize('values', [['worldbank', 'worldbank'], ['worldbank', 'external']])
def test_scalar_occurrences_cannot_silently_discard_held_catalog_identity(values):
    text = ('SOURCE\n\nhttps://microdata.worldbank.org/catalog/987654321\n\n'
            '#+CATALOG_ID: 987654321\n#+CATALOG_IDNO: held-release\n'
            + ''.join(f'#+PROVENANCE_SOURCE: {value}\n' for value in values)
            + '#+PROVENANCE_RECORDED: 2026-10-20\n')
    parsed = pv.parse_source_org(text, COUNTRY, WAVE)
    try:
        rendered = pv.render_source_org(parsed)
    except ValueError:
        return  # Explicit ambiguity refusal is within the approved contract.
    assert '#+CATALOG_ID: 987654321' in rendered
    assert '#+CATALOG_IDNO: held-release' in rendered
    assert [value for key, value in pv._KEYWORD_RE.findall(rendered)
            if key == 'PROVENANCE_SOURCE'] == values


def load_backfill():
    spec = importlib.util.spec_from_file_location('review_backfill', TOP / 'scripts/backfill_wave_provenance.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_unknown_held_release_keeps_prior_observation():
    bf = load_backfill()
    before = pv.parse_source_org(record(held=None, doi='https://doi.org/held-evidence'), COUNTRY, WAVE)
    proposal = replace(before, idno=NEW_RELEASE, doi='https://doi.org/latest')
    after = bf.release_unknown(pv.render_source_org(before), proposal)
    assert after.idno is None and after.doi == 'https://doi.org/held-evidence'
    assert after.latest_idno == NEW_RELEASE and after.latest_seen == '2026-10-19'


def test_queue_first_seen_survives_identical_observation_and_changes_for_new_release():
    bf = load_backfill()
    old = pv.parse_source_org(record(), COUNTRY, WAVE)
    latest = replace(old, idno=NEW_RELEASE, doi='https://doi.org/new')
    same = bf.latest_release(pv.render_source_org(old), latest, '2026-10-20')
    assert (same.idno, same.latest_idno, same.latest_seen) == (OLD_RELEASE, NEW_RELEASE, '2026-10-19')
    changed = bf.latest_release(pv.render_source_org(old), replace(latest, idno='RVW_2020_v03'), '2026-10-20')
    assert (changed.idno, changed.latest_idno, changed.latest_seen) == (OLD_RELEASE, 'RVW_2020_v03', '2026-10-20')
    resolved = bf.latest_release(pv.render_source_org(old), replace(latest, idno=OLD_RELEASE), '2026-10-20')
    assert resolved.latest_idno is None and resolved.latest_seen is None

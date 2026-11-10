"""Published-document transport only: synthetic HTTP bytes and DVC receipts."""
import hashlib
import io
import json
from pathlib import Path
import urllib.error
import urllib.request

import pytest

from lsms_library import data_access as da, provenance as pv
from tests.test_wave_provenance import countries, wb


def resource(rid='1', filename='questionnaire.pdf', **extra):
    return dict(resource_id=rid, filename=filename, title='Published questionnaire',
                type='Document, Questionnaire [doc/qst]',
                _links={'download': f'https://synthetic.invalid/{rid}'}, **extra)


class Response(io.BytesIO):
    headers = {'Content-Type': 'application/octet-stream'}


@pytest.fixture
def docs(tmp_path, monkeypatch):
    root = tmp_path / 'countries'
    directory = root / 'Invented' / 'a' / 'Documentation'
    state = dict(resources=[resource()], bodies={'https://synthetic.invalid/1': b'%PDF invented'},
                 calls=[], pushes=[], html='<div class="resources"></div>')
    monkeypatch.setattr(da, '_COUNTRIES_DIR', root)
    monkeypatch.setattr(da, '_get_catalog_idno', lambda url: 'INVENTED_v01')
    monkeypatch.setattr(da.config, 'microdata_api_key', lambda: None)

    def urlopen(req, **kwargs):
        url = req.full_url
        state['calls'].append(url)
        if '/api/resources/' in url:
            result = state.get('api', {'resources': state['resources']})
        elif url.endswith('/related-materials'):
            result = state['html']
        else:
            result = state['bodies'][url]
        if isinstance(result, Exception):
            raise result
        if isinstance(result, dict):
            result = json.dumps(result)
        return Response(result.encode() if isinstance(result, str) else result)

    def push(paths, **kwargs):
        state['pushes'].append((list(paths), kwargs))
        return state['push'](paths) if 'push' in state else list(paths)

    monkeypatch.setattr(urllib.request, 'urlopen', urlopen)
    monkeypatch.setattr(da, 'push_to_cache_batch', push)
    state.update(root=root, directory=directory)
    return state


def fetch(**kwargs):
    return da.fetch_documentation('Invented', 'a', '10', **kwargs)


def saved(docs):
    return json.loads((docs['directory'] / 'DOCUMENTS-10.json').read_text())


def test_default_document_batch_and_exact_persisted_receipt(docs):
    report = fetch(remote='invented-remote')
    assert report == saved(docs)
    assert report['status'] == 'complete' and report['held']['1']['status'] == 'pushed'
    assert report['held']['1']['sha256'] == hashlib.sha256(b'%PDF invented').hexdigest()
    assert docs['pushes'] == [([Path('Invented/a/Documentation/questionnaire.pdf')], {'remote': 'invented-remote'})]
    assert docs['calls'][-1] == 'https://synthetic.invalid/1'
    assert not list(docs['root'].rglob('*.dta')) and not da._ACQUIRED_RELEASES.get(('Invented', 'a'))


def test_push_false_is_local_only_without_dvc(docs):
    report = fetch(push=False)
    assert not docs['pushes'] and report['held']['1']['status'] == 'local-only'
    assert not list(docs['directory'].glob('*.dvc'))


@pytest.mark.parametrize('receipt', ['empty', 'partial', 'duplicate', 'foreign', 'exception'])
def test_push_confirmation_is_exact_and_incomplete_is_explicit(docs, receipt):
    docs['resources'].append(resource('2', 'manual.pdf'))
    docs['bodies']['https://synthetic.invalid/2'] = b'published manual'
    def push(paths):
        if receipt == 'exception':
            raise OSError('invented failed push')
        return {'empty': [], 'partial': list(paths)[:1], 'duplicate': [paths[0], paths[0]],
                'foreign': [Path('Invented/a/Data/foreign.dta')]}[receipt]
    docs['push'] = push
    report = fetch()
    assert report['status'] == 'incomplete'
    assert sum(r['status'] == 'pushed' for r in report['resources']) == (1 if receipt == 'partial' else 0)
    assert all(r['status'] in ('pushed', 'push_failed') for r in report['resources'])
    assert report == saved(docs)


@pytest.mark.parametrize('api', [{'resources': []}, {'wrong': []}, urllib.error.URLError('API down')])
def test_recognized_empty_is_distinct_from_unknown_or_failed_discovery(docs, api):
    docs['api'] = api
    docs['html'] = '<html>unknown layout/login</html>'
    report = fetch()
    assert report['status'] == ('not_listed' if api == {'resources': []} else 'discovery_failed')
    assert not docs['pushes'] and not report['resources']


def test_related_materials_fallback_and_explicit_generated_ddi(docs):
    docs['api'] = urllib.error.HTTPError('https://invented.invalid', 400, 'IDNO-NOT-FOUND', {}, None)
    docs['html'] = '''<div class="resources"><span class="resource-info" id="11">Own PH questionnaire</span>
    <a class="download" href="/catalog/10/download/11" data-filename="own.xls"
       data-dctype="Document, Questionnaire [doc/qst]"></a></div>
    <a class="download" href="/catalog/10/pdf-documentation" title="Documentation in PDF"
       data-filename="ddi-10.pdf" data-dctype="report"></a>'''
    docs['bodies'].update({'https://microdata.worldbank.org/catalog/10/download/11': b'xls bytes',
                           'https://microdata.worldbank.org/catalog/10/pdf-documentation': b'pdf bytes'})
    report = fetch(push=False)
    assert report['status'] == 'complete' and set(report['held']) == {'11', 'ddi'}
    assert report['held']['11']['title'] == 'Own PH questionnaire'
    assert report['held']['ddi']['generated_ddi'] is True
    assert len(report['discovery']['attempts']) == 1


def test_non_document_resources_are_visible_but_never_downloaded(docs):
    docs['resources'] = [dict(resource('2', 'stata.zip'), type='Data [data/zip]')]
    report = fetch()
    assert report['status'] == 'not_listed' and report['resources'][0]['status'] == 'not_selected'
    assert len(docs['calls']) == 1 and not docs['pushes']


@pytest.mark.parametrize('where', ['api', 'html'])
def test_unknown_resource_types_are_discovery_failure_not_not_listed(docs, where):
    if where == 'api':
        docs['api'] = {'resources': [dict(resource(), type='Unrecognized category')]}
        docs['html'] = '<html>unrecognized fallback layout</html>'
    else:
        docs['api'] = urllib.error.URLError('API unavailable')
        docs['html'] = '''<div class="resources"><a class="download"
            href="/catalog/10/download/11" data-filename="own.pdf"
            data-dctype="Unrecognized category"></a></div>'''
    report = fetch()
    assert report['status'] == 'discovery_failed' and report['discovery']['status'] == 'discovery_failed'
    assert not docs['pushes'] and not report['held']
    assert all('/api/resources/' in u or u.endswith('/related-materials') for u in docs['calls'])


@pytest.mark.parametrize('broken', ['empty-href', 'wrong-catalog', 'missing-link', 'unclosed-block'])
def test_incomplete_known_html_resource_is_not_successful_empty_listing(docs, broken):
    docs['api'] = urllib.error.URLError('API unavailable')
    href = '' if broken == 'empty-href' else '/catalog/20/download/11' if broken == 'wrong-catalog' else '/catalog/10/download/11'
    link = '' if broken == 'missing-link' else f'''<a class="download" data-filename="own.pdf"
        data-dctype="Document [doc/qst]" href="{href}"></a>'''
    close = '' if broken == 'unclosed-block' else '</div>'
    docs['html'] = '<div class="resources"><span class="resource-info" id="11">Own form</span>' + link + close
    report = fetch()
    assert report['status'] == 'discovery_failed' and not report['resources']
    assert not docs['pushes'] and len(docs['calls']) == 2


def test_genuine_empty_html_and_unrelated_catalog_export_links_stay_empty(docs):
    docs['api'] = urllib.error.URLError('API unavailable')
    docs['html'] = '''<a class="download" href="/metadata/export/10/ddi"
        data-filename="metadata-10.xml" data-dctype="metadata"></a><div class="resources"></div>'''
    report = fetch()
    assert report['status'] == 'not_listed' and not report['resources'] and not docs['pushes']


def test_download_failure_preserves_old_held_bytes_and_receipt_identity(docs):
    first = fetch(push=False)
    before = (docs['directory'] / 'questionnaire.pdf').read_bytes()
    docs['bodies']['https://synthetic.invalid/1'] = urllib.error.URLError('document offline')
    report = fetch(push=False)
    assert report['status'] == 'incomplete' and report['resources'][0]['status'] == 'download_failed'
    assert report['held'] == first['held']
    assert (docs['directory'] / 'questionnaire.pdf').read_bytes() == before


def test_same_resource_is_downloaded_again_not_assumed_remote_fresh(docs):
    first = fetch(push=False)
    path = docs['directory'] / 'questionnaire.pdf'
    inode = path.stat().st_ino
    assert fetch(push=False)['held'] == first['held'] and path.stat().st_ino == inode
    assert docs['calls'].count('https://synthetic.invalid/1') == 2
    docs['bodies']['https://synthetic.invalid/1'] = b'published revision'
    report = fetch(push=False)
    assert report['held']['1']['sha256'] != first['held']['1']['sha256']
    assert path.read_bytes() == b'published revision'


@pytest.mark.parametrize('name', ['../escape.pdf', '/escape.pdf', 'SOURCE.org', 'LICENSE',
                                  'DOCUMENTS-20.json', '.gitignore', 'own.pdf.dvc'])
def test_unsafe_or_reserved_resource_never_changes_source_or_terms(docs, name):
    directory = docs['directory']; directory.mkdir(parents=True)
    (directory / 'SOURCE.org').write_bytes(b'own source facts')
    (directory / 'LICENSE').write_bytes(b'actual terms')
    docs['resources'][0]['filename'] = name
    report = fetch()
    assert report['status'] == 'incomplete' and not docs['pushes']
    assert (directory / 'SOURCE.org').read_bytes() == b'own source facts'
    assert (directory / 'LICENSE').read_bytes() == b'actual terms'


@pytest.mark.parametrize('kind', ['different-catalog', 'unowned', 'symlink', 'late-case-collision'])
def test_ownership_collision_refuses_before_any_document_placement(docs, kind):
    directory = docs['directory']; directory.mkdir(parents=True)
    target = directory / 'questionnaire.pdf'
    if kind == 'different-catalog':
        target.write_bytes(b'other catalog')
        (directory / 'DOCUMENTS-20.json').write_text(json.dumps({'catalog_id': '20', 'held': {
            '77': {'path': target.name, 'sha256': hashlib.sha256(target.read_bytes()).hexdigest()}}}))
    elif kind == 'unowned':
        target.write_bytes(b'unattributed document')
    elif kind == 'symlink':
        other = directory.parent / 'other.pdf'; other.write_bytes(b'original')
        target.symlink_to(other)
    else:
        docs['resources'].append(resource('2', 'QUESTIONNAIRE.pdf'))
    before = {p: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    report = fetch()
    assert report['status'] == 'incomplete' and not docs['pushes']
    assert {p: p.read_bytes() for p in before} == before
    if kind == 'late-case-collision':
        assert not target.exists()


def test_unknown_existing_receipt_is_preserved_not_overwritten(docs):
    directory = docs['directory']; directory.mkdir(parents=True)
    receipt = directory / 'DOCUMENTS-10.json'; receipt.write_text('not valid JSON')
    report = fetch()
    assert report['status'] == 'incomplete' and report['receipt_error']
    assert receipt.read_text() == 'not valid JSON'


@pytest.mark.parametrize('raw_hash', [True, False])
@pytest.mark.parametrize('same_bytes', [True, False])
@pytest.mark.parametrize('workspace', [False, True])
def test_held_sidecar_without_receipt_needs_exact_declared_bytes(docs, raw_hash, same_bytes, workspace):
    directory = docs['directory']; directory.mkdir(parents=True)
    document = directory / 'questionnaire.pdf'
    digest = hashlib.md5(b'%PDF invented' if same_bytes else b'other bytes').hexdigest()
    sidecar = document.with_name(document.name + '.dvc')
    sidecar.write_text(f'outs:\n- md5: {digest}\n' + ('  hash: md5\n' if raw_hash else '') + '  path: questionnaire.pdf\n')
    if workspace:
        document.write_bytes(b'%PDF invented')
    before = sidecar.read_bytes()
    report = fetch(push=False)
    assert report['status'] == ('complete' if same_bytes else 'incomplete')
    assert document.exists() == (same_bytes or workspace) and sidecar.read_bytes() == before
    if workspace:
        assert document.read_bytes() == b'%PDF invented'


def test_document_revision_atomic_replace_preserves_hardlinked_old_blob(docs):
    fetch(push=False)
    document = docs['directory'] / 'questionnaire.pdf'
    blob = docs['root'] / 'old-blob'
    import os
    os.link(document, blob)
    original = blob.read_bytes()
    docs['bodies']['https://synthetic.invalid/1'] = b'new public document bytes'
    report = fetch(push=False)
    assert report['status'] == 'complete' and document.read_bytes() == b'new public document bytes'
    assert blob.read_bytes() == original and document.stat().st_ino != blob.stat().st_ino


@pytest.mark.parametrize('workspace', [False, True])
@pytest.mark.parametrize('raw_hash', [False, True])
@pytest.mark.parametrize('declared_path', [None, 'foreign.pdf', '../foreign.pdf'])
def test_sidecar_must_declare_exact_own_document_path(docs, workspace, raw_hash, declared_path):
    directory = docs['directory']; directory.mkdir(parents=True)
    document = directory / 'questionnaire.pdf'
    sidecar = document.with_name(document.name + '.dvc')
    declaration = 'outs:\n- md5: ' + hashlib.md5(b'%PDF invented').hexdigest() + '\n'
    if raw_hash:
        declaration += '  hash: md5\n'
    if declared_path is not None:
        declaration += f'  path: {declared_path}\n'
    sidecar.write_text(declaration)
    if workspace:
        document.write_bytes(b'%PDF invented')
    before = sidecar.read_bytes()
    report = fetch()
    assert report['status'] == 'incomplete' and not docs['pushes']
    assert sidecar.read_bytes() == before and document.exists() == workspace
    if workspace:
        assert document.read_bytes() == b'%PDF invented'


@pytest.mark.parametrize('workspace', [False, True])
@pytest.mark.parametrize('raw_hash', [False, True])
@pytest.mark.parametrize('pointer', ['prior', 'incoming', 'unrelated'])
def test_owned_receipt_does_not_authorize_unrelated_changed_sidecar(docs, workspace, raw_hash, pointer):
    prior = b'%PDF prior\r\nbody\r\n'
    incoming = b'%PDF revision\r\nnew\r\n'
    docs['bodies']['https://synthetic.invalid/1'] = prior
    held = fetch(push=False)['held']
    document = docs['directory'] / 'questionnaire.pdf'
    if not workspace:
        document.unlink()
    selected = {'prior': prior, 'incoming': incoming, 'unrelated': b'unrelated\r\npointer\r\n'}[pointer]
    checksum = hashlib.md5(selected if raw_hash else selected.replace(b'\r\n', b'\n')).hexdigest()
    sidecar = document.with_name(document.name + '.dvc')
    sidecar.write_text(f'outs:\n- md5: {checksum}\n' + ('  hash: md5\n' if raw_hash else '') + '  path: questionnaire.pdf\n')
    before = sidecar.read_bytes()
    docs['bodies']['https://synthetic.invalid/1'] = incoming
    report = fetch()
    allowed = pointer == 'incoming' or (pointer == 'prior' and (raw_hash or workspace))
    assert report['status'] == ('complete' if allowed else 'incomplete')
    assert len(docs['pushes']) == int(allowed) and sidecar.read_bytes() == before
    if allowed:
        assert document.read_bytes() == incoming and report['held']['1']['sha256'] == hashlib.sha256(incoming).hexdigest()
    else:
        assert report['held'] == held and document.exists() == workspace
        if workspace:
            assert document.read_bytes() == prior


@pytest.mark.parametrize('convention', ['sha256', 'unknown'])
def test_owned_receipt_unknown_hash_convention_is_unverifiable(docs, convention):
    held = fetch(push=False)['held']
    document = docs['directory'] / 'questionnaire.pdf'
    sidecar = document.with_name(document.name + '.dvc')
    sidecar.write_text(f'outs:\n- md5: {held["1"]["md5"]}\n  hash: {convention}\n  path: questionnaire.pdf\n')
    before = sidecar.read_bytes(), document.read_bytes()
    report = fetch()
    assert report['status'] == 'incomplete' and not docs['pushes']
    assert (sidecar.read_bytes(), document.read_bytes()) == before and report['held'] == held


@pytest.mark.parametrize('workspace', [False, True])
def test_invalid_prior_raw_checksum_needs_verified_prior_workspace(docs, workspace):
    prior = b'prior bytes'
    docs['bodies']['https://synthetic.invalid/1'] = prior
    fetch(push=False)
    document = docs['directory'] / 'questionnaire.pdf'
    receipt = docs['directory'] / 'DOCUMENTS-10.json'
    held = json.loads(receipt.read_text()); held['held']['1']['md5'] = 'not-a-valid-raw-md5'
    receipt.write_text(json.dumps(held))
    if not workspace:
        document.unlink()
    sidecar = document.with_name(document.name + '.dvc')
    sidecar.write_text(f'outs:\n- md5: {hashlib.md5(prior).hexdigest()}\n  hash: md5\n  path: questionnaire.pdf\n')
    before = sidecar.read_bytes()
    docs['bodies']['https://synthetic.invalid/1'] = b'incoming revision'
    report = fetch()
    assert report['status'] == ('complete' if workspace else 'incomplete')
    assert len(docs['pushes']) == int(workspace) and sidecar.read_bytes() == before
    assert document.exists() == workspace


def test_explicit_legacy_revision_uses_verified_bytes_not_raw_receipt_md5(docs):
    prior = b'prior\r\nbytes\r\n'
    docs['bodies']['https://synthetic.invalid/1'] = prior
    fetch(push=False)
    document = docs['directory'] / 'questionnaire.pdf'
    sidecar = document.with_name(document.name + '.dvc')
    checksum = hashlib.md5(prior.replace(b'\r\n', b'\n')).hexdigest()
    assert checksum != hashlib.md5(prior).hexdigest()
    sidecar.write_text(f'outs:\n- md5: {checksum}\n  hash: md5-dos2unix\n  path: questionnaire.pdf\n')
    docs['bodies']['https://synthetic.invalid/1'] = b'incoming\r\nrevision\r\n'
    report = fetch()
    assert report['status'] == 'complete' and len(docs['pushes']) == 1
    assert document.read_bytes() == b'incoming\r\nrevision\r\n'


def test_atomic_receipt_failure_preserves_previous_receipt(docs, monkeypatch):
    fetch(push=False)
    receipt = docs['directory'] / 'DOCUMENTS-10.json'; before = receipt.read_bytes()
    original = da.os.replace
    def fail_receipt(src, dest):
        if Path(dest) == receipt:
            raise OSError('invented receipt failure')
        return original(src, dest)
    monkeypatch.setattr(da.os, 'replace', fail_receipt)
    report = fetch(push=False)
    assert report['status'] == 'incomplete' and report['receipt_error']
    assert receipt.read_bytes() == before and not list(docs['directory'].glob('.lsms-*'))


def test_failed_one_document_still_reports_and_publishes_independent_member(docs):
    docs['resources'].append(resource('2', 'manual.pdf'))
    docs['bodies']['https://synthetic.invalid/2'] = urllib.error.URLError('manual offline')
    report = fetch()
    assert report['status'] == 'incomplete'
    assert [r['status'] for r in report['resources']] == ['pushed', 'download_failed']
    assert set(report['held']) == {'1'} and len(docs['pushes'][0][0]) == 1


@pytest.mark.parametrize('documentation', [True, False])
def test_add_wave_default_adjunct_and_escape_preserve_raw_return_and_release(countries, wb, monkeypatch, documentation):
    calls = []
    monkeypatch.setattr(da, 'fetch_documentation', lambda *a, **k: calls.append((a, k)))
    kwargs = {} if documentation else {'documentation': False}
    result = da.add_wave('Nigeria', '6410', wave='2023-24', push=False, confirm=False, verbose=False, **kwargs)
    assert sorted(p.name for p in result) == ['a.dta', 'b.dta']
    assert pv.read_provenance(countries, 'Nigeria', '2023-24').idno == 'NGA_2023_GHSP-W5_v02_M'
    assert calls == ([(('Nigeria', '2023-24', '6410'), {'push': False, 'remote': None})] if documentation else [])


def test_raw_success_and_real_document_discovery_failure_are_independent(countries, wb, monkeypatch, caplog):
    monkeypatch.setattr(da, '_documentation_resources', lambda *a: ([], {'status': 'discovery_failed'}))
    # wb normally stubs the adjunct for unrelated raw tests. Restore only this
    # actual public document method to exercise its failure/receipt boundary.
    monkeypatch.setattr(da, 'fetch_documentation', REAL_FETCH)
    result = da.add_wave('Nigeria', '6410', wave='2023-24', push=False, confirm=False, verbose=False)
    assert sorted(p.name for p in result) == ['a.dta', 'b.dta']
    assert pv.read_provenance(countries, 'Nigeria', '2023-24').idno == 'NGA_2023_GHSP-W5_v02_M'
    receipt = json.loads((countries / 'Nigeria/2023-24/Documentation/DOCUMENTS-6410.json').read_text())
    assert receipt['status'] == 'discovery_failed' and 'incomplete documentation' in caplog.text


REAL_FETCH = da.fetch_documentation

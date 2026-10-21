"""Related identity/coverage refusal controls; first-round evidence is untouched."""
from collections import Counter
from dataclasses import replace
import os
from pathlib import Path
import subprocess

import pytest
import lsms_library as ll
from lsms_library import provenance as pv

TOP = Path('/local/job40255479/worktrees/1115-1128-provenance')


@pytest.fixture(autouse=True)
def identity():
    assert Path.cwd() == TOP
    assert Path(ll.__file__).resolve().is_relative_to(TOP)
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == TOP / 'lsms_library/countries'
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == os.environ['REVIEW_FROZEN_HEAD']


@pytest.mark.parametrize('source', ['external', 'unknown'])
@pytest.mark.parametrize('holdings', ['identity', 'coverage', 'both'])
def test_conflict_refusal_preserves_destination_bytes(tmp_path, source, holdings):
    ids = '987654321, 987654322' if holdings != 'coverage' else 'none'
    text = f'SOURCE\n\n#+CATALOG_ID: {ids}\n'
    if holdings != 'identity':
        text += '#+CATALOG_COVERS: 987654323, 987654324\n'
    text += ('#+CATALOG_IDNO: held-release\n#+PROVENANCE_SOURCE: worldbank\n'
             f'#+PROVENANCE_SOURCE: {source}\n')
    path = pv.source_org_path(tmp_path, 'ReviewLand', '2020')
    path.parent.mkdir(parents=True)
    path.write_text(text)
    before = path.read_bytes()
    parsed = pv.parse_source_org(text, 'ReviewLand', '2020')
    assert parsed.catalog_ids or parsed.covers
    with pytest.raises(ValueError, match='explicit provenance curation'):
        pv.write_provenance(tmp_path, parsed)
    assert path.read_bytes() == before


def test_consistent_repeated_source_keeps_multi_identity_and_coverage():
    text = ('SOURCE\n\nhttps://microdata.worldbank.org/catalog/987654321\n\n'
            '#+CATALOG_ID: 987654321\n#+CATALOG_ID: 987654322\n'
            '#+CATALOG_COVERS: 987654323\n#+CATALOG_COVERS: 987654324\n'
            '#+PROVENANCE_SOURCE: worldbank\n#+PROVENANCE_SOURCE: worldbank\n'
            '#+PROVENANCE_RECORDED: 2026-10-20\n')
    parsed = pv.parse_source_org(text, 'ReviewLand', '2020')
    rendered = pv.render_source_org(parsed)
    back = pv.parse_source_org(rendered, 'ReviewLand', '2020')
    assert back == parsed
    assert back.catalog_ids == ['987654321', '987654322']
    assert back.covers == ['987654323', '987654324']
    assert rendered.count('#+PROVENANCE_SOURCE: worldbank') == 2


def test_repeated_empty_unknown_record_fields_keep_occurrence_counts():
    text = ('SOURCE\n\nhttps://example.invalid/data\n\n'
            '#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n'
            '#+PROVENANCE_RECORDED: 2026-10-20\n'
            '#+PARTNER: one\n#+PARTNER:\n#+PARTNER: one\n#+AFTER: two\n')
    parsed = pv.parse_source_org(text, 'ReviewLand', '2020')
    rendered = pv.render_source_org(parsed)
    keys = lambda s: Counter(pv._KEYWORD_RE.findall(s))
    # Canonical rendering may add CATALOG_URL for the existing primary body
    # URL. Every original keyword/value occurrence must survive that addition.
    assert not (keys(text) - keys(rendered))
    assert pv.parse_source_org(rendered, 'ReviewLand', '2020') == parsed


def test_changing_a_preserved_scalar_refuses_before_file_write(tmp_path):
    text = ('SOURCE\n\nhttps://example.invalid/data\n\n'
            '#+CATALOG_ID: none\n#+PROVENANCE_SOURCE: external\n'
            '#+PROVENANCE_NOTE: first\n#+PROVENANCE_NOTE: second\n')
    path = pv.source_org_path(tmp_path, 'ReviewLand', '2020')
    path.parent.mkdir(parents=True)
    path.write_text(text)
    parsed = pv.parse_source_org(text, 'ReviewLand', '2020')
    with pytest.raises(ValueError, match='explicit provenance curation'):
        pv.write_provenance(tmp_path, replace(parsed, note='new value'))
    assert path.read_bytes() == text.encode()


def test_external_non_wb_release_identifiers_remain_supported():
    text = ('SOURCE\n\nhttps://example.invalid/national-data\n\n'
            '#+CATALOG_ID: none\n#+CATALOG_IDNO: NATIONAL_RELEASE_v1\n'
            '#+PROVENANCE_SOURCE: external\n#+PROVENANCE_RECORDED: 2026-10-20\n')
    parsed = pv.parse_source_org(text, 'ReviewLand', '2020')
    assert pv.parse_source_org(pv.render_source_org(parsed), 'ReviewLand', '2020') == parsed

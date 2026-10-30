"""Forty NEW real-GNU-Make/private-command controls; source prepared only.

No project imports, serialized survey frames or actual producers execute in this
carrier. The real Makefile expands its own dynamic prerequisites and chains an
invented stdlib producer/driver command. Actual producer+Country behavior remains
covered separately by the existing typed composition and whole-route carriers.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from tests._niger_make_fixture import NigerMake, TABLES, WAVES, PROJECT


@pytest.fixture(params=TABLES)
def recipe(request, tmp_path):
    return NigerMake(tmp_path, request.param,
                     PROJECT / 'lsms_library/countries/Niger/_/Makefile')


def receipt(path):
    info = path.stat()
    return (info.st_ino, info.st_mtime_ns, info.st_size,
            hashlib.sha256(path.read_bytes()).hexdigest())


def producers(harness):
    return [event for event in harness.events()
            if event['stage'] in ('produce', 'native', 'yaml', 'compiler')]


def alias(harness):
    source = harness.root / 'alias.mk'
    source.write_text('.PHONY: requested\nrequested: ' + str(harness.target) + '\n')
    return ('-f', source)


def test_current_native_and_yaml_prerequisites_rebuild_a_warm_target(recipe):
    h = recipe
    h.install(mechanisms={WAVES[1]: 'yaml', WAVES[2]: 'both'})
    cold = h.run()
    assert cold.returncode == 0, cold.stdout + cold.stderr
    assert [(e['stage'], e.get('folder')) for e in producers(h)] == [
        ('produce', None), ('native', WAVES[0]), ('yaml', WAVES[1]),
        ('native', WAVES[2]), ('native', WAVES[3]), ('compiler', None)]
    before, events = receipt(h.target), producers(h)
    warm = h.run()
    assert warm.returncode == 0, warm.stderr
    assert receipt(h.target) == before and producers(h) == events
    # Ask GNU make itself: both YAML metadata and native script prerequisites
    # must invalidate an existing country target; never override its deps var.
    for dependency in (h.root / 'Niger' / WAVES[1] / '_/data_info.yml',
                       h.root / 'Niger' / WAVES[0] / '_' / (h.table + '.py')):
        count = sum(e['stage'] == 'compiler' for e in h.events())
        edited = h.run(extra=('-W', dependency))
        assert edited.returncode == 0, edited.stderr
        assert sum(e['stage'] == 'compiler' for e in h.events()) == count + 1
    assert {e['table'] for e in h.events() if e['stage'] == 'dependencies'} == {h.table}


def test_failed_discovery_refuses_even_when_country_target_is_newer(recipe):
    h = recipe
    assert h.run().returncode == 0
    h.target.write_bytes(b'invented stale country marker')
    before, earlier = receipt(h.target), producers(h)
    (h.root / 'Niger' / WAVES[-1] / '_/data_info.yml').write_text('{bad JSON')
    failed = h.run()
    assert failed.returncode != 0
    assert 'injected discovery/producer failure' in failed.stderr
    assert 'Niger producer discovery failed' in failed.stderr
    assert producers(h) == earlier
    assert receipt(h.target) == before


def test_unrelated_missing_target_has_clean_no_rule_without_discovery(recipe):
    h = recipe
    (h.build / 'wave_producers.py').unlink()  # unrelated fixture needs no helper
    unrelated = h.var / 'community_prices.parquet'
    missing = h.run(goal=unrelated, extra=('-n',))
    assert missing.returncode != 0
    assert 'No rule to make target' in missing.stderr
    assert len([line for line in missing.stderr.splitlines() if line.strip()]) == 1
    assert 'wave_producers' not in missing.stderr
    assert h.events() == [] and not unrelated.exists()


def test_transitive_requested_target_keeps_dynamic_prerequisites(recipe):
    h = recipe
    extra = alias(h)
    cold = h.run(goal='requested', extra=extra)
    assert cold.returncode == 0, cold.stderr
    before = sum(e['stage'] == 'compiler' for e in h.events())
    changed = h.run(goal='requested', extra=(*extra, '-W', h.build / 'data_scheme.yml'))
    assert changed.returncode == 0, changed.stderr
    assert sum(e['stage'] == 'compiler' for e in h.events()) == before + 1
    assert {e['table'] for e in h.events() if e['stage'] == 'dependencies'} == {h.table}


def test_relative_var_and_other_directory_do_not_broaden_builder(recipe):
    h = recipe
    built = h.run(relative=True)
    assert built.returncode == 0 and h.target.exists(), built.stderr
    earlier = list(h.events())
    other = h.var.parent / 'not-the-var-directory' / (h.table + '.parquet')
    for relative in (False, True):
        goal = os.path.relpath(other, h.build) if relative else other
        missing = h.run(goal=goal, extra=('-n',), relative=relative)
        assert missing.returncode != 0 and 'No rule to make target' in missing.stderr
        assert len([line for line in missing.stderr.splitlines() if line.strip()]) == 1
        assert h.events() == earlier and not other.exists()


def test_mapped_yaml_without_native_script_and_native_priority(recipe):
    h = recipe
    for folder in ('mapped-folder', 'mapped #$ folder'):
        h.install(waves=(WAVES[0], '2025-26'),
                  folders={'2025-26': folder},
                  mechanisms={WAVES[0]: 'both', '2025-26': 'yaml'})
        earlier = len(producers(h))
        result = h.run(extra=('-W', h.build / 'data_scheme.yml'))
        assert result.returncode == 0, result.stdout + result.stderr
        assert [(e['stage'], e.get('folder')) for e in producers(h)[earlier:]] == [
            ('produce', None), ('native', WAVES[0]), ('yaml', folder), ('compiler', None)]
        assert not (h.root / 'Niger' / folder / '_' / (h.table + '.py')).exists()
        assert (h.data / 'Niger' / folder / '_' / (h.table + '.parquet')).stat().st_size == 0


def test_registration_change_ignores_retired_and_unregistered_sources(recipe):
    h = recipe
    assert h.run().returncode == 0
    h.install(waves=(WAVES[0], '2025-26'), mechanisms={'2025-26': 'yaml'})
    assert (h.root / 'Niger' / WAVES[1] / '_' / (h.table + '.py')).exists()
    earlier = len(producers(h))
    result = h.run(extra=('-W', h.build / 'data_scheme.yml'))
    assert result.returncode == 0, result.stderr
    assert [(e['stage'], e.get('folder')) for e in producers(h)[earlier:]] == [
        ('produce', None), ('native', WAVES[0]), ('yaml', '2025-26'), ('compiler', None)]


def test_matching_transitive_discovery_failure_stays_terminal(recipe):
    h = recipe
    extra = alias(h)
    assert h.run(goal='requested', extra=extra).returncode == 0
    before, earlier = receipt(h.target), producers(h)
    (h.root / 'Niger' / WAVES[-1] / '_/data_info.yml').write_text('{bad JSON')
    failed = h.run(goal='requested', extra=extra)
    assert failed.returncode != 0 and 'Niger producer discovery failed' in failed.stderr
    assert producers(h) == earlier and receipt(h.target) == before

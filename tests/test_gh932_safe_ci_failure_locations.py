"""Static public CI failure names, invented JUnit; never import survey code."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/private_ci_check.py'
SECRET = 'FAKE_PRIVATE_PARAMETER_MESSAGE_932'
spec = importlib.util.spec_from_file_location('private_ci_locations_932', SCRIPT)
wrapper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wrapper)
pytestmark = pytest.mark.skipif(shutil.which('git') is None, reason='Git not installed')


def _git(root, *args, input=None):
    result = subprocess.run(['git', '-c', 'core.fsmonitor=false', '-C', str(root), *args],
        input=input, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return result.stdout.decode().strip()


def _commit(root):
    _git(root, 'add', 'tests')
    _git(root, '-c', 'user.name=Invented', '-c', 'user.email=invented@example.invalid',
         '-c', 'commit.gpgsign=false', 'commit', '-qm', 'public test source')
    return _git(root, 'rev-parse', 'HEAD')


@pytest.fixture
def public_repo(tmp_path):
    root = tmp_path / 'public-repository'
    (root / 'tests').mkdir(parents=True)
    _git(root, 'init', '-q')
    source = root / 'tests/test_visible.py'
    source.write_text('def test_top():\n    pass\n\nclass TestPublic:\n'
                      '    def test_method(self):\n        pass\n')
    return root, _commit(root), source


def _xml(tmp_path, cases):
    path = tmp_path / 'private.xml'
    path.write_text('<testsuite>' + cases + '</testsuite>')
    return path


def test_only_exact_declared_file_function_names_and_numeric_outcomes(public_repo, tmp_path):
    root, revision, _ = public_repo
    path = _xml(tmp_path, f'''
      <testcase classname="tests.test_visible" name="test_top[{SECRET}]"><failure message="{SECRET}">{SECRET}</failure></testcase>
      <testcase file="tests/test_visible.py" classname="tests.test_visible" name="test_top"><failure/></testcase>
      <testcase classname="tests.test_visible.TestPublic" name="test_method[{SECRET}]"><error>{SECRET}</error></testcase>
      <testcase classname="tests.test_visible" name="test_top"><skipped message="{SECRET}"/></testcase>
      <testcase name="test_visible"><error message="{SECRET}"/></testcase>
    ''')
    locations, unknown = wrapper._junit_failure_locations(path, root, revision)
    assert locations == {('failures', 'tests/test_visible.py', 'test_top'): 2,
                         ('errors', 'tests/test_visible.py', 'test_method'): 1}
    assert unknown == {'failures': 0, 'errors': 1}
    assert SECRET not in repr((locations, unknown))


@pytest.mark.parametrize('attributes', [
    'file="../tests/test_visible.py" name="test_top"',
    'file="/tests/test_visible.py" name="test_top"',
    'file="tests\\test_visible.py" name="test_top"',
    'file="tests/test_missing.py" name="test_top"',
    'file="tests/test_visible.py" classname="other.test_visible" name="test_top"',
    'classname="tests.test_visible.MissingClass" name="test_method"',
    'classname="tests.test_visible" name="test_missing"',
    'classname="tests.test_visible" name="test_top&#10;' + SECRET + '"',
    'classname="tests.test_visible.' + SECRET + '" name="test_method"',
])
def test_untrusted_or_mismatched_identities_are_count_only(public_repo, tmp_path, attributes):
    root, revision, _ = public_repo
    path = _xml(tmp_path, '<testcase ' + attributes + '><failure/></testcase>')
    assert wrapper._junit_failure_locations(path, root, revision) == (
        {}, {'failures': 1, 'errors': 0})


def test_committed_symlink_is_not_a_static_test_source(public_repo, tmp_path):
    root, revision, _ = public_repo
    # A valid Python blob in mode120000 would fool a content-only AST check.
    blob = _git(root, 'hash-object', '-w', '--stdin', input=b'def test_link():\n    pass\n')
    _git(root, 'update-index', '--add', '--cacheinfo', '120000,' + blob + ',tests/test_link.py')
    _git(root, '-c', 'user.name=Invented', '-c', 'user.email=invented@example.invalid',
         '-c', 'commit.gpgsign=false', 'commit', '-qm', 'invented symlink entry')
    revision = _git(root, 'rev-parse', 'HEAD')
    path = _xml(tmp_path, '<testcase classname="tests.test_link" name="test_link"><failure/></testcase>')
    assert wrapper._junit_failure_locations(path, root, revision) == ({}, {'failures': 1, 'errors': 0})


def test_fixed_revision_ignores_worktree_and_later_head_changes(public_repo, tmp_path):
    root, original, source = public_repo
    assert wrapper._source_head(root) == original
    source.write_text('def test_added_after_child_started():\n    pass\n')
    newer = _commit(root)
    assert newer != original
    path = _xml(tmp_path, '<testcase classname="tests.test_visible" name="test_added_after_child_started"><failure/></testcase>')
    assert wrapper._junit_failure_locations(path, root, original) == ({}, {'failures': 1, 'errors': 0})
    assert wrapper._junit_failure_locations(path, root, newer)[0] == {
        ('failures', 'tests/test_visible.py', 'test_added_after_child_started'): 1}


@pytest.mark.parametrize('limit', ['source', 'xml'])
def test_source_and_xml_limits_fail_closed(public_repo, tmp_path, monkeypatch, limit):
    root, revision, _ = public_repo
    path = _xml(tmp_path, '<testcase classname="tests.test_visible" name="test_top"><failure/></testcase>')
    if limit == 'source':
        monkeypatch.setattr(wrapper, '_MAX_SOURCE_BYTES', 1)
        assert wrapper._junit_failure_locations(path, root, revision) == ({}, {'failures': 1, 'errors': 0})
    else:
        monkeypatch.setattr(wrapper, '_MAX_XML_BYTES', 1)
        assert wrapper._junit_failure_locations(path, root, revision) is None


@pytest.mark.parametrize('xml', ['', '<broken', '<unexpected/>',
    '<!DOCTYPE testsuite [<!ENTITY x "' + SECRET + '">]><testsuite/>'])
def test_malformed_or_dtd_diagnostics_never_forward_xml(public_repo, tmp_path, xml, capsys):
    root, revision, _ = public_repo
    path = tmp_path / 'private.xml';path.write_text(xml)
    assert wrapper._junit_failure_locations(path, root, revision) is None
    output = capsys.readouterr()
    assert output.out == output.err == ''


def test_location_budget_keeps_numeric_unmapped_counts(public_repo, tmp_path, monkeypatch):
    root, revision, source = public_repo
    source.write_text('def test_one(): pass\ndef test_two(): pass\ndef test_three(): pass\n')
    revision = _commit(root)
    monkeypatch.setattr(wrapper, '_MAX_PUBLIC_LOCATIONS', 2)
    path = _xml(tmp_path, ''.join('<testcase classname="tests.test_visible" name="test_' + name + '"><failure/></testcase>'
                                 for name in ['one', 'two', 'three', 'one']))
    locations, unknown = wrapper._junit_failure_locations(path, root, revision)
    assert locations == {('failures', 'tests/test_visible.py', 'test_one'): 2,
                         ('failures', 'tests/test_visible.py', 'test_two'): 1}
    assert unknown == {'failures': 1, 'errors': 0}


def test_missing_git_identity_is_count_only(public_repo, tmp_path):
    root, _, _ = public_repo
    path = _xml(tmp_path, '<testcase classname="tests.test_visible" name="test_top"><failure/><error/></testcase>')
    assert wrapper._junit_failure_locations(path, root, None) == ({}, {'failures': 0, 'errors': 1})


def test_ast_warnings_are_private(public_repo, tmp_path, capsys):
    root, _, source = public_repo
    source.write_text('def test_warning():\n    return (1 is "' + SECRET + '")\n')
    revision = _commit(root)
    path = _xml(tmp_path, '<testcase classname="tests.test_visible" name="test_warning"><failure/></testcase>')
    assert wrapper._junit_failure_locations(path, root, revision)[0] == {
        ('failures', 'tests/test_visible.py', 'test_warning'): 1}
    output = capsys.readouterr()
    assert output.out == output.err == ''


def test_real_wrapper_optin_preserves_stream_privacy_cleanup_status_and_static_names(tmp_path):
    private = tmp_path / 'private-temp';private.mkdir()
    xml = (f'<testsuite><testcase classname="tests.test_private_ci_check" '
           f'name="test_startup_failure_does_not_publish_command_or_exception[{SECRET}]">'
           f'<failure message="{SECRET}">{SECRET}</failure></testcase>'
           f'<testcase classname="{SECRET}" name="{SECRET}"><error/></testcase></testsuite>')
    code = ('import pathlib,sys; pathlib.Path(sys.argv[1]).write_text(sys.argv[2]); '
            f'print({SECRET!r}); print({SECRET!r},file=sys.stderr); sys.exit(7)')
    env = os.environ.copy();env.update(TMPDIR=str(private), TMP=str(private), TEMP=str(private))
    result = subprocess.run([sys.executable, str(SCRIPT), '--stage', 'data-tests',
        '--junit', 'results.xml', '--public-test-failures', '--', sys.executable,
        '-c', code, '{junit}', xml], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 7 and result.stderr == '' and SECRET not in result.stdout
    assert result.stdout == ('private-ci-check stage=data-tests exit=7\n'
        'junit tests=2 passed=0 failures=1 errors=1 skipped=0\n'
        'junit failures tests/test_private_ci_check.py::test_startup_failure_does_not_publish_command_or_exception count=1\n'
        'junit unmapped failures=0 errors=1\n')
    assert list(private.iterdir()) == []


def test_optin_requires_junit_before_child_executes(tmp_path):
    marker = tmp_path / 'must-not-run'
    result = subprocess.run([sys.executable, str(SCRIPT), '--stage', 'data-tests',
        '--public-test-failures', '--', sys.executable, '-c',
        'from pathlib import Path;Path(' + repr(str(marker)) + ').touch()'],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 125 and not marker.exists()
    assert result.stdout == 'private-ci-check stage=data-tests exit=125\n' and result.stderr == ''

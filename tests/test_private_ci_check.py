"""Credentialed CI publishes no child output: fake payloads, real subprocesses."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
WRAPPER = ROOT / "scripts" / "private_ci_check.py"
SECRET = "FAKE_PRIVATE_ROW_42_COORD_12_345"


def _invoke(tmp_path, command, *, junit=None, stage="data-tests"):
    temporary = tmp_path / "user temp with spaces"
    temporary.mkdir(exist_ok=True)
    env = os.environ.copy()
    env.update(TMPDIR=str(temporary), TMP=str(temporary), TEMP=str(temporary),
               PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    argv = [sys.executable, str(WRAPPER), "--stage", stage]
    if junit is not None:
        argv += ["--junit", junit]
    result = subprocess.run(argv + ["--"] + command, cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=60)
    assert SECRET not in result.stdout
    assert SECRET not in result.stderr
    assert result.stderr == ""
    assert list(temporary.iterdir()) == [], "private command output was not removed"
    return result


@pytest.mark.parametrize("status", [0, 1, 7, 42])
def test_child_streams_are_private_and_exit_status_is_preserved(tmp_path, status):
    code = ("import sys, logging, warnings; "
            f"print({SECRET!r}); print({SECRET!r}, file=sys.stderr); "
            f"logging.error({SECRET!r}); warnings.warn({SECRET!r}); "
            f"sys.exit({status})")
    result = _invoke(tmp_path, [sys.executable, "-c", code])
    assert result.returncode == status
    assert result.stdout == f"private-ci-check stage=data-tests exit={status}\n"


@pytest.mark.parametrize("statement", [
    f"assert False, {SECRET!r}",
    f"raise RuntimeError({SECRET!r})",
    f"raise ExceptionGroup({SECRET!r}, [ValueError({SECRET!r})])",
])
def test_uncaught_python_failures_never_publish_their_message(tmp_path, statement):
    result = _invoke(tmp_path, [sys.executable, "-c", statement])
    assert result.returncode == 1
    assert "Traceback" not in result.stdout


def test_startup_failure_does_not_publish_command_or_exception(tmp_path):
    result = _invoke(tmp_path, [str(tmp_path / SECRET)])
    assert result.returncode == 125
    assert result.stdout == "private-ci-check stage=data-tests exit=125\n"


@pytest.mark.parametrize("stage", [SECRET, "bad\nstage"])
def test_invalid_stage_is_not_an_output_channel(tmp_path, stage):
    result = _invoke(tmp_path, [sys.executable, "-c", "pass"], stage=stage)
    assert result.returncode == 125
    assert result.stdout == "private-ci-check stage=ci-check exit=125\n"


def test_arguments_with_spaces_and_shell_metacharacters_remain_literal(tmp_path):
    directory = tmp_path / "command directory with spaces"
    directory.mkdir()
    program = directory / "program with spaces.py"
    expected = ["a b", 'quote"inside', "$HOME; echo " + SECRET,
                "$(echo " + SECRET + ")", r"C:\path with spaces\file.txt", "a&b|c"]
    program.write_text("import json, sys\n"
                       f"assert sys.argv[1:] == {expected!r}\n"
                       f"print({SECRET!r})\n")
    result = _invoke(tmp_path, [sys.executable, str(program), *expected])
    assert result.returncode == 0


def test_private_directory_modes_and_xml_are_cleaned(tmp_path):
    # On Windows the hosted user's temp/profile ACL is the privacy boundary;
    # POSIX mode bits are checked only where they have that meaning.
    probe = tmp_path / "probe.json"
    code = ("import json, os, pathlib, stat, sys; p=pathlib.Path(sys.argv[1]); "
            "p.write_text('<testsuite><testcase/></testsuite>'); "
            "info={'directory': str(p.parent), 'mode':stat.S_IMODE(p.parent.stat().st_mode),"
            "'log_mode':stat.S_IMODE(os.fstat(1).st_mode)}; "
            f"pathlib.Path({str(probe)!r}).write_text(json.dumps(info)); print({SECRET!r})")
    result = _invoke(tmp_path, [sys.executable, "-c", code, "{junit}"], junit="result.xml")
    assert result.returncode == 0
    info = json.loads(probe.read_text())
    assert not Path(info["directory"]).exists()
    if os.name == "posix":
        assert info["mode"] == 0o700
        assert info["log_mode"] == 0o600


def _xml_command(xml, status):
    return [sys.executable, "-c",
            "import pathlib, sys; pathlib.Path(sys.argv[1]).write_text(sys.argv[2]); "
            f"print({SECRET!r}); sys.exit({status})", "{junit}", xml]


@pytest.mark.parametrize("status", [0, 3])
def test_junit_reports_only_outcome_counts_not_any_xml_payload(tmp_path, status):
    xml = (f'<testsuites name="{SECRET}" tests="{SECRET}"><testsuite>'
           f'<properties><property name="{SECRET}" value="{SECRET}"/></properties>'
           f'<testcase classname="{SECRET}" name="{SECRET}[{SECRET}]"/>'
           f'<testcase name="{SECRET}"><failure message="{SECRET}">{SECRET}</failure>'
           f'<system-out>{SECRET}</system-out><system-err>{SECRET}</system-err></testcase>'
           f'<testcase><error message="{SECRET}"/></testcase>'
           f'<testcase><skipped message="{SECRET}"/></testcase>'
           '</testsuite></testsuites>')
    result = _invoke(tmp_path, _xml_command(xml, status), junit="result.xml")
    assert result.returncode == status
    assert result.stdout == (f"private-ci-check stage=data-tests exit={status}\n"
                             "junit tests=4 passed=1 failures=1 errors=1 skipped=1\n")


@pytest.mark.parametrize("xml", [
    "", "<" + SECRET, f"<unexpected>{SECRET}</unexpected>",
    f'<!DOCTYPE testsuite [<!ENTITY secret "{SECRET}">]>'
    '<testsuite><testcase name="&secret;"/></testsuite>',
])
def test_malformed_or_unexpected_xml_does_not_change_child_status(tmp_path, xml):
    result = _invoke(tmp_path, _xml_command(xml, 7), junit="result.xml")
    assert result.returncode == 7
    assert result.stdout.endswith("junit unavailable\n")


def test_missing_junit_is_safe_and_preserves_success(tmp_path):
    result = _invoke(tmp_path, [sys.executable, "-c", f"print({SECRET!r})"],
                     junit="missing.xml")
    assert result.returncode == 0
    assert result.stdout.endswith("junit unavailable\n")


@pytest.mark.parametrize("filename", ["../" + SECRET, "..\\" + SECRET, ".", ".."])
def test_junit_path_cannot_escape_the_private_directory(tmp_path, filename):
    result = _invoke(tmp_path, [sys.executable, "-c", "raise AssertionError"],
                     junit=filename)
    assert result.returncode == 125


def test_junit_placeholder_without_output_path_is_refused(tmp_path):
    result = _invoke(tmp_path, [sys.executable, "-c", "pass", "{junit}"])
    assert result.returncode == 125


def test_posix_signal_failure_remains_a_failure(tmp_path):
    if os.name != "posix":
        return  # Windows exit-code behavior is exercised by ordinary status tests.
    result = _invoke(tmp_path, [sys.executable, "-c",
                              "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"])
    assert result.returncode == 128 + signal.SIGTERM
    assert "exit=143" in result.stdout


_INSTAFAIL_MODES = [False]
if importlib.util.find_spec("pytest_instafail") is not None:
    _INSTAFAIL_MODES.append(True)


@pytest.mark.parametrize("instafail", _INSTAFAIL_MODES)
def test_real_pytest_failure_routes_and_warning_capture_remain_private(tmp_path, instafail):
    # Isolate this child from the repository's conftest and all project/data IO.
    config = tmp_path / "pytest.ini"
    config.write_text("[pytest]\n")
    source = f'''import logging, sys, warnings
import pytest
SECRET = {SECRET!r}
def test_assert():
    print(SECRET)
    print(SECRET, file=sys.stderr)
    logging.error(SECRET)
    warnings.warn(SECRET)
    assert False, SECRET
def test_exception():
    raise ValueError(SECRET)
@pytest.fixture
def setup_error():
    raise RuntimeError(SECRET)
def test_setup(setup_error):
    pass
@pytest.fixture
def teardown_error():
    yield
    raise RuntimeError(SECRET)
def test_teardown(teardown_error):
    pass
def test_skip():
    pytest.skip(SECRET)
def test_xfail():
    pytest.xfail(SECRET)
@pytest.mark.parametrize('value', [1], ids=[SECRET])
def test_parameter_id(value):
    assert False, SECRET
def test_warning_capture():
    with pytest.warns(UserWarning, match=SECRET):
        warnings.warn(SECRET)
'''
    test = tmp_path / "test_fake_private.py"
    test.write_text(source)
    command = [sys.executable, "-m", "pytest", "-c", str(config), str(test),
               "--tb=short", "--assert=plain", "--disable-warnings", "--junitxml={junit}"]
    if instafail:
        command += ["-p", "pytest_instafail", "--instafail"]
    result = _invoke(tmp_path, command, junit="results.xml")
    assert result.returncode == 1
    assert "failures=3 errors=2 skipped=2" in result.stdout
    assert result.stdout.count("\n") == 2


def test_pytest_collection_error_is_private(tmp_path):
    test = tmp_path / "test_collection.py"
    test.write_text(f"raise RuntimeError({SECRET!r})\n")
    config = tmp_path / "pytest.ini"
    config.write_text("[pytest]\n")
    result = _invoke(tmp_path, [sys.executable, "-m", "pytest", "-c", str(config),
                               str(test), "--junitxml={junit}"], junit="result.xml")
    assert result.returncode == 2
    assert "errors=1" in result.stdout


def test_real_pytest_success_keeps_warning_capture_and_success_status(tmp_path):
    test = tmp_path / "test_pass.py"
    test.write_text("import pytest, warnings\n"
                    "def test_warning_capture():\n"
                    f"    with pytest.warns(UserWarning, match={SECRET!r}):\n"
                    f"        warnings.warn({SECRET!r})\n")
    config = tmp_path / "pytest.ini"
    config.write_text("[pytest]\n")
    result = _invoke(tmp_path, [sys.executable, "-m", "pytest", "-c", str(config),
                               str(test), "--junitxml={junit}"], junit="result.xml")
    assert result.returncode == 0
    assert "tests=1 passed=1 failures=0 errors=0 skipped=0" in result.stdout


def test_workflow_wraps_each_credentialed_data_command_without_changing_coverage():
    jobs = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"]
    steps = {step["name"]: step for step in jobs["data-tests"]["steps"] if "name" in step}
    build = steps["Build Uganda tables"]
    assert build["working-directory"] == "lsms_library/countries/Uganda/_"
    assert "private_ci_check.py" in build["run"]
    assert "--stage data-prebuild -- poetry run make -j2" in build["run"]
    full = steps["Run full test suite"]["run"]
    assert "private_ci_check.py --stage data-tests --junit results.xml --" in full
    assert "poetry run pytest --tb=short --assert=plain --disable-warnings" in full
    assert "--timeout=600 --timeout-method=thread --durations=25" in full
    assert "--junitxml={junit}" in full
    windows = next(step for step in jobs["windows-script-build"]["steps"]
                   if step.get("name") == "Cold-build and compare with Linux")
    assert windows["shell"] == "cmd /C CALL {0}"
    assert 'python "%GITHUB_WORKSPACE%\\scripts\\private_ci_check.py"' in windows["run"]
    assert '--stage windows-script-build -- python "%GITHUB_WORKSPACE%\\scripts\\windows_build_check.py" Uganda:shocks Uganda:assets' in windows["run"]

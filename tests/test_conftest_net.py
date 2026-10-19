"""Exercise the missing-credentials hook through real pytest sessions (GH #907)."""
from pathlib import Path

import pytest

pytest_plugins = ["pytester"]


@pytest.fixture
def isolated_net(pytester, monkeypatch):
    # Copy the production test plugin, not a second implementation of its
    # hook.  Its credential file is pinned to a path that does not exist
    # (GH #1031), so "no credentials" does not depend on where the copy sits:
    # a copy that finds lsms_library/config.py beside it would otherwise read
    # the developer's (or CI's) real ``<config dir>/s3_creds``.
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("LSMS_SKIP_AUTH", "1")
    monkeypatch.setenv("LSMS_S3_CREDS", str(pytester.path / "no_s3_creds"))
    pytester.makeconftest(Path(__file__).with_name("conftest.py").read_text())
    return pytester


@pytest.mark.parametrize("phase", ["call", "setup"])
@pytest.mark.parametrize("warnings_as_errors", [False, True])
def test_unmarked_missing_credentials_skip_loudly(
    isolated_net, phase, warnings_as_errors,
):
    if phase == "setup":
        source = """
            import pytest
            from botocore.exceptions import NoCredentialsError
            @pytest.fixture
            def needs_data():
                raise NoCredentialsError()
            def test_probe(needs_data):
                raise AssertionError("setup should not complete")
        """
    else:
        source = """
            from botocore.exceptions import NoCredentialsError
            def test_probe():
                raise NoCredentialsError()
        """
    isolated_net.makepyfile(test_probe=source)
    args = ["-q"]
    if warnings_as_errors:
        args += ["-W", "error::UserWarning"]
    result = isolated_net.runpytest_subprocess(*args)
    result.assert_outcomes(skipped=1, warnings=1)
    result.stdout.fnmatch_lines([
        "*UnmarkedS3TestWarning: test_probe.py::test_probe*RUN time*",
    ])
    assert result.ret == 0


def test_marked_test_skips_before_execution_without_net_warning(isolated_net):
    isolated_net.makepyfile(test_probe="""
        import pytest
        @pytest.mark.requires_s3
        def test_probe():
            raise AssertionError("marked test must not execute")
    """)
    result = isolated_net.runpytest_subprocess("-q")
    result.assert_outcomes(skipped=1, warnings=0)
    assert "UnmarkedS3TestWarning" not in result.stdout.str()


@pytest.mark.parametrize("credentials_present", [False, True])
def test_real_failures_are_not_hidden(isolated_net, monkeypatch, credentials_present):
    if credentials_present:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-only-not-a-real-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-only-not-a-real-secret")
        source = """
            from botocore.exceptions import NoCredentialsError
            def test_probe():
                raise NoCredentialsError()
        """
    else:
        source = """
            def test_probe():
                raise ValueError("a genuine data defect")
        """
    isolated_net.makepyfile(test_probe=source)
    result = isolated_net.runpytest_subprocess("-q")
    result.assert_outcomes(failed=1, warnings=0)
    assert "UnmarkedS3TestWarning" not in result.stdout.str()

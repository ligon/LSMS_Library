"""S3 reader credentials must be EXPLICIT, not merely a ``credentialpath``.

``dvc_s3`` implements the ``credentialpath`` remote option as::

    shared_creds = config.get("credentialpath")
    if shared_creds:
        os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", shared_creds)

which an ambient AWS environment defeats two different ways:

1. ``setdefault`` -- an already-exported ``AWS_SHARED_CREDENTIALS_FILE``
   means our path is discarded outright and never consulted at all.
2. Even when it does apply, the shared *credentials file* ranks BELOW
   ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` in botocore's
   resolution order, so exported keys win.

Either way a contributor with AWS credentials exported for unrelated work
reads this bucket as *themselves*, gets 403 on every blob, and is told
only ``PermissionError: Forbidden`` from four layers inside s3fs.  That
cost a full debugging session to identify once; these tests keep the fix
from being quietly undone.

``access_key_id`` / ``secret_access_key`` become the ``key`` / ``secret``
handed directly to ``S3FileSystem``
(``dvc_s3.S3FileSystem._prepare_credentials``).  Those are explicit
credentials and outrank both env-var mechanisms.

The fallback matters as much as the fix: with no readable credentials
file we must emit NOTHING and leave ``credentialpath`` alone, so a user
whose own AWS credentials or instance role legitimately grant access is
not locked out by us forcing empty keys.
"""
import configparser

import pytest

from lsms_library import local_tools


FAKE = "AKIAIOSFODNN7EXAMPLE"
FAKE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"


def _write_creds(path, *, section="default", token=None, body=None):
    if body is not None:
        path.write_text(body)
        return path
    cp = configparser.ConfigParser()
    cp[section] = {"aws_access_key_id": FAKE, "aws_secret_access_key": FAKE_SECRET}
    if token:
        cp[section]["aws_session_token"] = token
    with open(path, "w") as fh:
        cp.write(fh)
    return path


@pytest.fixture
def creds_at(tmp_path, monkeypatch):
    """Point the library's creds path at a temp file we control."""
    def _point(**kw):
        p = tmp_path / "s3_creds"
        if kw.pop("write", True):
            _write_creds(p, **kw)
        monkeypatch.setenv("LSMS_S3_CREDS", str(p))
        return p
    return _point


class TestExplicitCredentials:

    def test_parses_a_well_formed_file(self, creds_at):
        creds_at()
        got = local_tools._s3_explicit_credentials()
        assert got == {"access_key_id": FAKE, "secret_access_key": FAKE_SECRET}

    def test_carries_a_session_token_when_present(self, creds_at):
        creds_at(token="FQoGZXIvYXdzEXAMPLE")
        assert local_tools._s3_explicit_credentials()["session_token"] == \
            "FQoGZXIvYXdzEXAMPLE"

    def test_reads_a_non_default_profile(self, creds_at):
        creds_at(section="ligonresearch")
        assert local_tools._s3_explicit_credentials()["access_key_id"] == FAKE

    @pytest.mark.parametrize("case", ["missing", "empty", "malformed", "incomplete"])
    def test_falls_back_silently_rather_than_forcing_empty_keys(self, creds_at, case):
        """No readable credentials -> emit nothing, leave credentialpath alone.

        Returning e.g. {"access_key_id": None} would override a user's own
        working AWS credentials with nothing and lock them out.
        """
        if case == "missing":
            creds_at(write=False)
        elif case == "empty":
            creds_at(body="")
        elif case == "malformed":
            creds_at(body="this is not ini\n\x00 nor anything else")
        else:
            creds_at(body="[default]\naws_access_key_id = %s\n" % FAKE)  # no secret
        assert local_tools._s3_explicit_credentials() == {}


class TestRemoteConfig:

    def test_config_is_not_credentialpath_alone(self, creds_at):
        """The whole point: explicit keys, because credentialpath loses."""
        creds_at()
        fs = local_tools._build_dvcfs()
        rc = fs.repo.config["remote"]["ligonresearch_s3"]
        assert rc["access_key_id"] == FAKE
        assert rc["secret_access_key"] == FAKE_SECRET
        # credentialpath is retained as the no-credentials-file fallback
        assert "credentialpath" in rc

    def test_refresh_is_a_noop_once_explicit(self, creds_at):
        creds_at()
        assert local_tools.refresh_s3_credentials() is False

    def test_refresh_declines_when_there_is_nothing_to_apply(self, creds_at):
        creds_at(write=False)
        assert local_tools.refresh_s3_credentials() is False

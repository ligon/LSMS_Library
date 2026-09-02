"""The S3 read-cache unlock must not require a `gpg` binary (GH #741).

A valid World Bank API key is the library's real access gate, but until #741 it
was *necessary and not sufficient*: the unlock still shelled out to `gpg`, which
is not pip-installable and ships on neither Windows nor macOS by default.  The
user saw `NoCredentialsError` from botocore three layers down, naming nothing
that was actually wrong.

These tests need no data, no API key and no network, so unlike most of this
suite they actually run in CI -- which matters, because CI is exactly the
environment that would otherwise never exercise a credential path.

Nothing here asserts on decrypted credential VALUES.  Shape only.
"""
import base64
import shutil
import warnings

import pytest

from lsms_library import data_access as da
from lsms_library.paths import countries_root

DVC_DIR = countries_root() / ".dvc"
ENC = DVC_DIR / "s3_reader_creds.enc"
GPG = DVC_DIR / "s3_reader_creds.gpg"

PASSPHRASE = base64.b64decode(da._S3_UNLOCK_PASSPHRASE).decode()


def _looks_like_aws_creds(text: str) -> bool:
    return text.strip().startswith("[") and "aws_access_key_id" in text


class TestFernetPath:
    def test_enc_file_is_shipped(self):
        """If this is missing from the wheel the whole fix is inert."""
        assert ENC.exists(), f"{ENC} is missing -- pip users get no fast path"

    def test_enc_decrypts_without_gpg(self):
        plain = da._fernet_decrypt(ENC, PASSPHRASE)
        assert plain is not None, "pure-Python decryption failed"
        assert _looks_like_aws_creds(plain)

    def test_wrong_passphrase_returns_none(self):
        assert da._fernet_decrypt(ENC, "not the passphrase") is None

    def test_key_derivation_matches_the_generator(self):
        """`scripts/reencrypt_s3_creds.py` must derive the identical key.

        Drift here bricks the unlock for every pip user at once, and the
        symptom is an unrelated-looking botocore error.
        """
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_reencrypt", countries_root().parents[1] / "scripts" / "reencrypt_s3_creds.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.derive_fernet_key(PASSPHRASE) == da._derive_fernet_key(PASSPHRASE)
        assert mod._S3_UNLOCK_PASSPHRASE == da._S3_UNLOCK_PASSPHRASE


@pytest.mark.skipif(shutil.which("gpg") is None, reason="gpg binary not installed")
def test_enc_and_gpg_carry_the_same_credentials():
    """The rotation guard.

    Two encrypted copies of one secret drift the moment someone rotates the
    credentials and regenerates only one.  Nothing else would catch it: both
    files decrypt fine, and the wrong one silently wins.
    """
    assert da._fernet_decrypt(ENC, PASSPHRASE) == da._gpg_decrypt(GPG, PASSPHRASE)


class TestAutoUnlockWithoutGpg:
    """The #741 scenario itself: a valid key, and no gpg on the machine."""

    @pytest.fixture
    def isolated_creds(self, tmp_path, monkeypatch):
        """Point s3_creds at a temp file so we never touch the real one."""
        target = tmp_path / "s3_creds"
        monkeypatch.setattr(da.config, "s3_creds_path", lambda: target)
        monkeypatch.setattr(da, "_sync_legacy_dvc_creds", lambda *a, **k: True)
        return target

    def test_unlock_succeeds_with_gpg_unavailable(self, isolated_creds, monkeypatch):
        monkeypatch.setattr(da, "_gpg_decrypt", lambda *a, **k: None)
        assert da._auto_unlock_s3(DVC_DIR) is True
        assert _looks_like_aws_creds(isolated_creds.read_text())

    def test_failure_of_both_paths_warns_legibly(self, isolated_creds, monkeypatch):
        """A silent False here is what sent people to inspect their AWS config."""
        monkeypatch.setattr(da, "_gpg_decrypt", lambda *a, **k: None)
        monkeypatch.setattr(da, "_fernet_decrypt", lambda *a, **k: None)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert da._auto_unlock_s3(DVC_DIR) is False
        msgs = [str(w.message) for w in caught]
        assert msgs, "failure was silent"
        blob = "\n".join(msgs)
        assert "cryptography" in blob and "gpg" in blob, blob
        assert "741" in blob, "the warning should point at the issue"
        assert not isolated_creds.exists()

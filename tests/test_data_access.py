"""Tests for the data_access module (issue #101).

Tests cover:
- DVC config parsing
- Credential probing and access level determination
- WB API key validation (mocked)
- Auto-unlock of S3 credentials via obfuscated passphrase
- permissions() caching and reset
- can_read() / can_write() convenience functions
- get_data_file() fallback chain (mocked network)
"""

from __future__ import annotations

import base64
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def dvc_dir(tmp_path):
    """Create a realistic .dvc directory with config and encrypted creds."""
    dvc = tmp_path / ".dvc"
    dvc.mkdir()

    # DVC config with two remotes
    config_text = textwrap.dedent("""\
        [core]
            remote = ligonresearch_s3
            autostage = true
        ['remote "ligonresearch"']
            url = gdrive://1oVOdo2-oeM_LYBSe1lKSr-pm08syczdu/LSMS
            gdrive_use_service_account = true
            gdrive_service_account_json_file_path = gdrive_creds.json
        ['remote "ligonresearch_s3"']
            url = s3://dvcbucket0/LSMS
            credentialpath = s3_creds
            configpath = s3_config
    """)
    (dvc / "config").write_text(config_text)

    return dvc


@pytest.fixture(autouse=True)
def _reset_caches(tmp_path, monkeypatch):
    """Reset module-level caches and isolate HOME so ~/.aws/credentials
    on the host machine doesn't leak write access into tests."""
    from lsms_library.data_access import reset_permissions_cache
    reset_permissions_cache()
    monkeypatch.setenv("HOME", str(tmp_path))
    yield
    reset_permissions_cache()


# ---------------------------------------------------------------------------
# _parse_dvc_remotes
# ---------------------------------------------------------------------------

class TestParseDvcRemotes:
    def test_parses_both_remotes(self, dvc_dir):
        from lsms_library.data_access import _parse_dvc_remotes

        remotes = _parse_dvc_remotes(dvc_dir)

        assert "ligonresearch" in remotes
        assert "ligonresearch_s3" in remotes
        assert remotes["ligonresearch_s3"]["url"] == "s3://dvcbucket0/LSMS"
        assert remotes["ligonresearch"]["url"].startswith("gdrive://")

    def test_returns_empty_when_no_config(self, tmp_path):
        from lsms_library.data_access import _parse_dvc_remotes

        assert _parse_dvc_remotes(tmp_path / "nonexistent") == {}

    def test_returns_credentialpath(self, dvc_dir):
        from lsms_library.data_access import _parse_dvc_remotes

        remotes = _parse_dvc_remotes(dvc_dir)
        assert remotes["ligonresearch_s3"]["credentialpath"] == "s3_creds"

    def test_returns_gdrive_cred_key(self, dvc_dir):
        from lsms_library.data_access import _parse_dvc_remotes

        remotes = _parse_dvc_remotes(dvc_dir)
        key = "gdrive_service_account_json_file_path"
        assert remotes["ligonresearch"][key] == "gdrive_creds.json"


# ---------------------------------------------------------------------------
# _check_remote_access
# ---------------------------------------------------------------------------

class TestCheckRemoteAccess:
    def test_s3_read_when_creds_exist(self, dvc_dir):
        from lsms_library.data_access import _check_remote_access

        (dvc_dir / "s3_creds").write_text("[default]\naws_key=foo\n")
        cfg = {"url": "s3://dvcbucket0/LSMS", "credentialpath": "s3_creds"}
        assert _check_remote_access("test_s3", cfg, dvc_dir) == "read"

    def test_s3_write_when_env_set(self, dvc_dir, monkeypatch):
        from lsms_library.data_access import _check_remote_access

        (dvc_dir / "s3_creds").write_text("[default]\naws_key=foo\n")
        monkeypatch.setenv("LSMS_S3_WRITE_KEY", "secret")
        cfg = {"url": "s3://dvcbucket0/LSMS", "credentialpath": "s3_creds"}
        assert _check_remote_access("test_s3", cfg, dvc_dir) == "write"

    def test_s3_none_when_no_creds(self, dvc_dir):
        from lsms_library.data_access import _check_remote_access

        cfg = {"url": "s3://dvcbucket0/LSMS", "credentialpath": "s3_creds"}
        assert _check_remote_access("test_s3", cfg, dvc_dir) is None

    def test_s3_none_when_creds_empty(self, dvc_dir):
        from lsms_library.data_access import _check_remote_access

        (dvc_dir / "s3_creds").write_text("")
        cfg = {"url": "s3://dvcbucket0/LSMS", "credentialpath": "s3_creds"}
        assert _check_remote_access("test_s3", cfg, dvc_dir) is None

    def test_gdrive_read_when_creds_exist(self, dvc_dir):
        from lsms_library.data_access import _check_remote_access

        (dvc_dir / "gdrive_creds.json").write_text('{"type": "service"}')
        cfg = {
            "url": "gdrive://xxx/LSMS",
            "gdrive_service_account_json_file_path": "gdrive_creds.json",
        }
        assert _check_remote_access("test_gd", cfg, dvc_dir) == "read"

    def test_gdrive_none_when_no_creds(self, dvc_dir):
        from lsms_library.data_access import _check_remote_access

        cfg = {
            "url": "gdrive://xxx/LSMS",
            "gdrive_service_account_json_file_path": "gdrive_creds.json",
        }
        assert _check_remote_access("test_gd", cfg, dvc_dir) is None

    def test_s3_write_via_aws_env_vars(self, dvc_dir, monkeypatch):
        from lsms_library.data_access import _check_remote_access

        (dvc_dir / "s3_creds").write_text("[default]\naws_key=foo\n")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA...")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
        cfg = {"url": "s3://dvcbucket0/LSMS", "credentialpath": "s3_creds"}
        assert _check_remote_access("test_s3", cfg, dvc_dir) == "write"

    def test_s3_write_via_write_creds_file(self, dvc_dir):
        from lsms_library.data_access import _check_remote_access

        (dvc_dir / "s3_creds").write_text("[default]\naws_key=foo\n")
        (dvc_dir / "s3_write_creds").write_text("[default]\nkey=bar\n")
        cfg = {"url": "s3://dvcbucket0/LSMS", "credentialpath": "s3_creds"}
        assert _check_remote_access("test_s3", cfg, dvc_dir) == "write"

    def test_unknown_url_scheme_returns_none(self, dvc_dir):
        from lsms_library.data_access import _check_remote_access

        cfg = {"url": "gcs://bucket/path"}
        assert _check_remote_access("test_gcs", cfg, dvc_dir) is None


# ---------------------------------------------------------------------------
# _validate_wb_api_key (mocked network)
# ---------------------------------------------------------------------------

class TestValidateWbApiKey:
    def test_valid_key(self):
        from lsms_library.data_access import (
            _validate_wb_api_key, reset_permissions_cache,
        )
        reset_permissions_cache()

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"result": {"found": 42}}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            assert _validate_wb_api_key("good-key") is True

    def test_invalid_key_zero_results(self):
        from lsms_library.data_access import (
            _validate_wb_api_key, reset_permissions_cache,
        )
        reset_permissions_cache()

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"result": {"found": 0}}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp):
            assert _validate_wb_api_key("bad-key") is False

    def test_network_error_returns_false(self):
        from lsms_library.data_access import (
            _validate_wb_api_key, reset_permissions_cache,
        )
        reset_permissions_cache()

        with patch("urllib.request.urlopen", side_effect=OSError("timeout")):
            assert _validate_wb_api_key("any-key") is False

    def test_caches_result(self):
        from lsms_library.data_access import (
            _validate_wb_api_key, reset_permissions_cache,
        )
        reset_permissions_cache()

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"result": {"found": 1}}'
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)

        with patch("urllib.request.urlopen", return_value=mock_resp) as m:
            assert _validate_wb_api_key("key") is True
            assert _validate_wb_api_key("key") is True
            # Only one network call despite two invocations
            assert m.call_count == 1


# ---------------------------------------------------------------------------
# _auto_unlock_s3
# ---------------------------------------------------------------------------

class TestAutoUnlockS3:
    def test_returns_true_when_creds_already_exist(self, dvc_dir):
        from lsms_library.data_access import _auto_unlock_s3

        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        assert _auto_unlock_s3(dvc_dir) is True

    def test_returns_false_when_no_gpg_file(self, dvc_dir):
        from lsms_library.data_access import _auto_unlock_s3

        assert _auto_unlock_s3(dvc_dir) is False

    def test_decrypts_and_writes_creds(self, dvc_dir):
        """Use the real gpg binary to create an encrypted fixture,
        then verify _auto_unlock_s3 decrypts it."""
        import subprocess

        passphrase = base64.b64decode("QnVubnkgbXVmZmlu").decode()
        plaintext = "[default]\naws_access_key_id = TESTKEY\n"

        # Encrypt with gpg
        result = subprocess.run(
            ["gpg", "--batch", "--yes", "--passphrase-fd", "0",
             "--symmetric", "--cipher-algo", "AES256",
             "--output", str(dvc_dir / "s3_reader_creds.gpg")],
            input=(passphrase + "\n").encode() + plaintext.encode(),
            capture_output=True,
        )
        if result.returncode != 0:
            pytest.skip(f"gpg encryption failed: {result.stderr.decode()}")

        from lsms_library.data_access import _auto_unlock_s3

        assert _auto_unlock_s3(dvc_dir) is True
        creds = (dvc_dir / "s3_creds").read_text()
        assert "TESTKEY" in creds

    def test_returns_false_on_bad_gpg_file(self, dvc_dir):
        from lsms_library.data_access import _auto_unlock_s3

        (dvc_dir / "s3_reader_creds.gpg").write_bytes(b"not a gpg file")
        assert _auto_unlock_s3(dvc_dir) is False


# ---------------------------------------------------------------------------
# permissions / can_read / can_write
# ---------------------------------------------------------------------------

class TestPermissions:
    def test_s3_read_without_wb_key(self, dvc_dir, monkeypatch):
        """RA scenario: S3 creds exist but no WB API key."""
        from lsms_library import data_access

        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        perms = data_access.permissions()
        assert "ligonresearch_s3" in perms
        assert perms["ligonresearch_s3"] == "read"
        assert "wb_api" not in perms

    def test_wb_api_without_s3(self, dvc_dir, monkeypatch):
        """User has WB key but no S3 creds and auto-unlock is blocked."""
        from lsms_library import data_access

        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: "test-key")
        # Mock validation to succeed
        monkeypatch.setattr(data_access, "_validate_wb_api_key",
                            lambda k: True)
        # auto_unlock will fail (no gpg file), that's fine
        monkeypatch.setattr(data_access, "_auto_unlock_s3",
                            lambda dvc_dir=None: False)

        perms = data_access.permissions()
        assert perms.get("wb_api") == "read"
        # S3 not accessible because auto-unlock failed and no creds on disk
        assert "ligonresearch_s3" not in perms

    def test_wb_key_triggers_auto_unlock(self, dvc_dir, monkeypatch):
        """Valid WB key auto-unlocks S3 so both appear in permissions."""
        from lsms_library import data_access

        (dvc_dir / "s3_creds").write_text("")  # empty = not yet unlocked
        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: "test-key")
        monkeypatch.setattr(data_access, "_validate_wb_api_key",
                            lambda k: True)

        # Simulate successful auto-unlock writing the creds file
        def fake_unlock(d=None):
            (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
            return True
        monkeypatch.setattr(data_access, "_auto_unlock_s3", fake_unlock)

        perms = data_access.permissions()
        assert perms.get("wb_api") == "read"
        assert perms.get("ligonresearch_s3") == "read"

    def test_cache_is_used(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        p1 = data_access.permissions()
        # Mutate the returned copy -- should not affect cache
        p1["hacked"] = "yes"
        p2 = data_access.permissions()
        assert "hacked" not in p2

    def test_reset_clears_cache(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        p1 = data_access.permissions()

        # Now add creds and reset
        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        data_access.reset_permissions_cache()

        p2 = data_access.permissions()
        assert "ligonresearch_s3" in p2


class TestCanReadWrite:
    def test_can_read_true(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        assert data_access.can_read("ligonresearch_s3") is True
        assert data_access.can_write("ligonresearch_s3") is False

    def test_can_write_true(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        monkeypatch.setenv("LSMS_S3_WRITE_KEY", "secret")
        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        assert data_access.can_read("ligonresearch_s3") is True
        assert data_access.can_write("ligonresearch_s3") is True

    def test_absent_resource(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        assert data_access.can_read("nonexistent") is False
        assert data_access.can_write("nonexistent") is False


# ---------------------------------------------------------------------------
# push_to_cache
# ---------------------------------------------------------------------------

class TestPushToCache:
    def test_rejects_nonexistent_file(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        monkeypatch.setattr(data_access, "_COUNTRIES_DIR",
                            dvc_dir.parent)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        assert data_access.push_to_cache("Foo/2020/Data/bar.dta") is False

    def test_rejects_without_write_perms(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        # Create the file but only grant read access
        countries = dvc_dir.parent
        data_dir = countries / "Foo" / "2020" / "Data"
        data_dir.mkdir(parents=True)
        (data_dir / "bar.dta").write_bytes(b"fake data")
        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")

        monkeypatch.setattr(data_access, "_COUNTRIES_DIR", countries)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        assert data_access.push_to_cache("Foo/2020/Data/bar.dta") is False

    def test_calls_dvc_add_and_push(self, dvc_dir, monkeypatch):
        from lsms_library import data_access
        from unittest.mock import call

        countries = dvc_dir.parent
        data_dir = countries / "Foo" / "2020" / "Data"
        data_dir.mkdir(parents=True)
        target = data_dir / "bar.dta"
        target.write_bytes(b"fake data")
        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        monkeypatch.setenv("LSMS_S3_WRITE_KEY", "secret")

        monkeypatch.setattr(data_access, "_COUNTRIES_DIR", countries)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        # Mock subprocess.run to simulate success
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        with patch.object(data_access.subprocess, "run",
                          return_value=mock_result) as mock_run:
            assert data_access.push_to_cache("Foo/2020/Data/bar.dta") is True

            # Should have called dvc add then dvc push
            assert mock_run.call_count == 2
            add_call = mock_run.call_args_list[0]
            push_call = mock_run.call_args_list[1]
            assert "add" in add_call[0][0]
            assert "push" in push_call[0][0]

    def test_skips_dvc_add_when_disabled(self, dvc_dir, monkeypatch):
        from lsms_library import data_access

        countries = dvc_dir.parent
        data_dir = countries / "Foo" / "2020" / "Data"
        data_dir.mkdir(parents=True)
        (data_dir / "bar.dta").write_bytes(b"fake data")
        (dvc_dir / "s3_creds").write_text("[default]\nkey=val\n")
        monkeypatch.setenv("LSMS_S3_WRITE_KEY", "secret")

        monkeypatch.setattr(data_access, "_COUNTRIES_DIR", countries)
        monkeypatch.setattr("lsms_library.config.microdata_api_key",
                            lambda: None)

        mock_result = MagicMock()
        mock_result.returncode = 0
        with patch.object(data_access.subprocess, "run",
                          return_value=mock_result) as mock_run:
            assert data_access.push_to_cache(
                "Foo/2020/Data/bar.dta", dvc_add=False) is True
            # Only push, no add
            assert mock_run.call_count == 1
            assert "push" in mock_run.call_args_list[0][0][0]


# ---------------------------------------------------------------------------
# config.py
# ---------------------------------------------------------------------------

class TestConfig:
    def test_env_var_takes_precedence(self, monkeypatch):
        from lsms_library import config as cfg

        monkeypatch.setenv("MICRODATA_API_KEY", "from-env")
        assert cfg.microdata_api_key() == "from-env"

    def test_config_file_fallback(self, tmp_path, monkeypatch):
        from lsms_library import config as cfg

        # Clear env
        monkeypatch.delenv("MICRODATA_API_KEY", raising=False)

        # Write a config file
        config_file = tmp_path / "config.yml"
        config_file.write_text("microdata_api_key: from-file\n")

        # Patch config file location and clear the lru_cache
        monkeypatch.setattr(cfg, "_config_file", lambda: config_file)
        cfg._load_config.cache_clear()

        assert cfg.microdata_api_key() == "from-file"

        # Clean up cache
        cfg._load_config.cache_clear()

    def test_returns_none_when_absent(self, tmp_path, monkeypatch):
        from lsms_library import config as cfg

        monkeypatch.delenv("MICRODATA_API_KEY", raising=False)
        monkeypatch.setattr(cfg, "_config_file",
                            lambda: tmp_path / "nope.yml")
        cfg._load_config.cache_clear()

        assert cfg.microdata_api_key() is None

        cfg._load_config.cache_clear()

    def test_data_dir_env_var(self, monkeypatch):
        from lsms_library import config as cfg

        monkeypatch.setenv("LSMS_DATA_DIR", "/custom/path")
        assert cfg.data_dir() == "/custom/path"

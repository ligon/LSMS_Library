"""GH #1001: importing the package writes NO credential into ``countries_root()``.

``_sync_legacy_dvc_creds`` used to copy the user-config ``s3_creds`` into
``countries_root()/.dvc/s3_creds`` at import time (and from
``_auto_unlock_s3``), for "legacy ``dvc.api.open()`` wave scripts" -- of
which none remain (GH #810).  The reader passes ``config.s3_creds_path()``
to ``DVCFileSystem`` itself (``local_tools._build_dvcfs``), so the copy
served nothing and broke CLAUDE.md's "``countries_root()`` is READ-ONLY at
runtime" invariant (#803/#914/#831).

A failed write used to be swallowed (``except OSError: return False``), so
"import does not raise ``PermissionError``" passes with or without the copy
and proves nothing.  The discriminating assertion is on the WRITABLE tree:
after import, the tree is byte-for-byte what it was before.

Each case runs a fresh interpreter, because ``_COUNTRIES_DIR`` is an
import-time snapshot of ``LSMS_COUNTRIES_ROOT``.  Everything the import
might write -- config dir, data dir, countries root -- is a tmp path, so
neither the developer's real credentials nor CI's in-tree
``.dvc/s3_creds`` (which the ``data-tests`` job writes into the checkout)
can mask the result: the real tree is never the countries root here.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
REAL_DVC = REPO / "lsms_library" / "countries" / ".dvc"

FAKE_CREDS = (
    "[default]\n"
    "aws_access_key_id = AKIAIOSFODNN7EXAMPLE\n"
    "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"
)

#: What a shipped wheel carries in ``countries/.dvc/`` -- the committed
#: files, and NOT the gitignored ``s3_creds`` a developer clone may hold.
_SHIPPED = ("config", "s3_config", "s3_reader_creds.enc", "s3_reader_creds.gpg")


def _countries_tree(root: Path) -> Path:
    countries = root / "countries"
    dvc = countries / ".dvc"
    dvc.mkdir(parents=True)
    for name in _SHIPPED:
        if (REAL_DVC / name).exists():
            shutil.copy2(REAL_DVC / name, dvc / name)
    # DVC's Repo makes ``.dvc/tmp`` with exist_ok=True: a no-op on a
    # read-only tree only if the directory is already there, as in a wheel.
    (dvc / "tmp").mkdir()
    assert (dvc / "config").exists(), "the remote stanza needs .dvc/config"
    assert not (dvc / "s3_creds").exists()
    return countries


def _snapshot(root: Path) -> dict[str, str]:
    """Path -> sha256 of every file under *root* (directories map to "dir").

    Digests, never contents: a failing comparison must not print a
    credential into the test log.

    ``.dvc/tmp/`` is excluded: it is DVC's own scratch directory (constructing
    the ``DVCFileSystem`` drops a ``btime`` marker there on a writable tree,
    and tolerates a read-only one).  That is a separate runtime write, not a
    credential, and is reported as a residual of #1001 rather than pinned.
    """
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = Path(dirpath).relative_to(root)
        if rel_dir.parts[:2] == (".dvc", "tmp"):
            continue
        for d in dirnames:
            out[str(rel_dir / d) + "/"] = "dir"
        for f in filenames:
            out[str(rel_dir / f)] = hashlib.sha256(Path(dirpath, f).read_bytes()).hexdigest()
    return out


def _chmod_tree(root: Path, writable: bool) -> None:
    for dirpath, dirnames, filenames in os.walk(root):
        for name in [*dirnames, *filenames, ""]:
            p = Path(dirpath, name) if name else Path(dirpath)
            mode = p.stat().st_mode
            if writable:
                p.chmod(mode | stat.S_IWUSR)
            else:
                p.chmod(mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))


def _import(countries: Path, config_dir: Path, data_dir: Path):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("AWS_", "LSMS_", "MICRODATA"))}
    env.update(
        LSMS_COUNTRIES_ROOT=str(countries),
        LSMS_CONFIG_DIR=str(config_dir),
        LSMS_DATA_DIR=str(data_dir),
        PYTHONPATH=str(REPO),
        PYTHONSAFEPATH="1",
        # LSMS_SKIP_AUTH deliberately UNSET: it short-circuits the branch
        # under test.  No WB key either -- a key validation is a network
        # call, and the S3 unlock does not need one at import.
    )
    code = (
        "import lsms_library, os\n"
        "from lsms_library.paths import countries_root\n"
        f"assert os.path.realpath(countries_root()) == os.path.realpath({str(countries)!r}), countries_root()\n"
        f"assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath({str(REPO)!r}) + os.sep)\n"
        "print('IMPORTED')\n"
    )
    return subprocess.run([sys.executable, "-c", code], env=env, cwd=str(data_dir),
                          capture_output=True, text=True, timeout=300)


@pytest.fixture
def layout(tmp_path):
    countries = _countries_tree(tmp_path / "pkg")
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    yield countries, config_dir, data_dir
    _chmod_tree(tmp_path / "pkg", writable=True)  # let tmp_path clean up


def test_user_config_creds_are_not_mirrored_into_the_tree(layout):
    """The ``creds_file.exists()`` import branch: the one that used to copy."""
    countries, config_dir, data_dir = layout
    (config_dir / "s3_creds").write_text(FAKE_CREDS)
    before = _snapshot(countries)

    res = _import(countries, config_dir, data_dir)

    assert res.returncode == 0 and "IMPORTED" in res.stdout, res.stderr
    assert not (countries / ".dvc" / "s3_creds").exists(), (
        "import mirrored user-config credentials into countries_root()")
    assert _snapshot(countries) == before, "import wrote into countries_root()"


def test_auto_unlock_writes_the_user_config_only(layout):
    """No user creds yet: ``_auto_unlock_s3`` decrypts into the config dir."""
    countries, config_dir, data_dir = layout
    if not (countries / ".dvc" / "s3_reader_creds.enc").exists():
        pytest.skip("no s3_reader_creds.enc in this checkout")
    before = _snapshot(countries)

    res = _import(countries, config_dir, data_dir)

    assert res.returncode == 0 and "IMPORTED" in res.stdout, res.stderr
    assert "aws_access_key_id" in (config_dir / "s3_creds").read_text()
    assert _snapshot(countries) == before, "auto-unlock wrote into countries_root()"


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
@pytest.mark.parametrize("user_creds", [True, False], ids=["user-creds", "auto-unlock"])
def test_read_only_countries_root_imports_cleanly(layout, user_creds):
    """A read-only installed package: import works, nothing tries to write."""
    countries, config_dir, data_dir = layout
    if user_creds:
        (config_dir / "s3_creds").write_text(FAKE_CREDS)
    before = _snapshot(countries)
    _chmod_tree(countries, writable=False)

    res = _import(countries, config_dir, data_dir)

    assert res.returncode == 0 and "IMPORTED" in res.stdout, res.stderr
    assert "PermissionError" not in res.stderr, res.stderr
    assert "s3_creds" not in res.stderr, res.stderr
    assert (config_dir / "s3_creds").exists()
    _chmod_tree(countries, writable=True)
    assert _snapshot(countries) == before


# ---------------------------------------------------------------------------
# The two consumers that used to lean on the in-tree copy
# ---------------------------------------------------------------------------


def _load_tests_conftest():
    """``tests/conftest.py`` as a plain module (it must not be imported by name)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_gh1001_tests_conftest", Path(__file__).with_name("conftest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("valid", [True, False])
def test_requires_s3_gate_reads_the_user_config_creds(tmp_path, monkeypatch, valid):
    """User-config-only credentials must not skip ``requires_s3`` tests.

    The legacy in-tree path is pointed at nothing, so only the
    ``config.s3_creds_path()`` lookup can make the gate open.
    """
    conftest = _load_tests_conftest()
    for var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(conftest, "_LEGACY_INTREE_CREDS", tmp_path / "absent")
    creds = tmp_path / "s3_creds"
    creds.write_text(FAKE_CREDS if valid else "[default]\n")
    monkeypatch.setenv("LSMS_S3_CREDS", str(creds))

    assert conftest.aws_creds_available() is valid


def test_cli_remote_reads_get_the_user_config_creds(tmp_path, monkeypatch):
    """``dvc status --cloud`` resolves ``credentialpath`` in-tree; hand it ours."""
    from lsms_library import data_access as da

    creds = tmp_path / "s3_creds"
    creds.write_text(FAKE_CREDS)
    monkeypatch.setattr(da.config, "s3_creds_path", lambda: creds)
    monkeypatch.delenv("AWS_SHARED_CREDENTIALS_FILE", raising=False)

    seen = {}

    class _Result:
        stdout, stderr, returncode = "", "", 0

    def _fake(cmd, **kw):
        seen.update(kw)
        return _Result()

    monkeypatch.setattr(da, "_run_dvc_with_lock_retry", _fake)
    assert da.unpushed_blobs() == []
    assert seen["env"]["AWS_SHARED_CREDENTIALS_FILE"] == str(creds)

    # An exported file is the user's explicit choice and wins.
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "theirs"))
    assert da._reader_credentials_env() is None
    # No user-config creds: inherit the environment unchanged.
    monkeypatch.delenv("AWS_SHARED_CREDENTIALS_FILE")
    creds.unlink()
    assert da._reader_credentials_env() is None

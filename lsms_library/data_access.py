"""Data access layer with resource-based permissions and multi-source fallback.

Provides :func:`get_data_file` --- the single entry point for obtaining a
raw data file (e.g. ``Uganda/2013-14/Data/GSEC1.dta``).  It tries
sources in order:

1. **Local file** --- already on disk (e.g. from a previous download).
2. **DVC remotes** --- for each remote defined in ``.dvc/config`` that the
   user has read credentials for.
3. **World Bank Microdata Library** --- download the Stata zip from the
   NADA API and extract the requested file.  Requires ``MICRODATA_API_KEY``.

The :func:`permissions` function returns a dict of
``{resource_name: access_level}`` where resource names come from DVC
remote names (parsed from ``.dvc/config``) plus ``"wb_api"`` for the
World Bank Microdata Library, and access levels are ``"read"`` or
``"write"``.

When a valid ``MICRODATA_API_KEY`` is detected, the module automatically
decrypts the S3 read credentials (``s3_reader_creds.gpg``) so that DVC
can stream data without the user having to run ``ll.authenticate()``
interactively.

Environment variables
---------------------
MICRODATA_API_KEY
    World Bank Microdata Library API key.  Proves the user has accepted
    the WB terms of use.
LSMS_S3_WRITE_KEY
    If set, grants write access to S3 remotes.
LSMS_SKIP_AUTH
    If ``"1"``/``"true"``/``"yes"``, skip the interactive GPG passphrase
    prompt on import.
"""

from __future__ import annotations

import base64
import configparser
import logging
import os
import re
import subprocess
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

from . import config

logger = logging.getLogger(__name__)

_COUNTRIES_DIR = Path(__file__).resolve().parent / "countries"

# Base64-obfuscated passphrase for s3_reader_creds.gpg.
# The real access gate is WB API key validation; this just keeps the
# passphrase from being grep-able as a plaintext string.
_S3_UNLOCK_PASSPHRASE = "QnVubnkgbXVmZmlu"


# ---------------------------------------------------------------------------
# DVC config parsing
# ---------------------------------------------------------------------------

def _parse_dvc_remotes(dvc_dir: Path | None = None,
                       ) -> dict[str, dict[str, str]]:
    """Parse ``.dvc/config`` and return ``{remote_name: {key: value}}``.

    Handles the configparser quirk where section names like
    ``['remote "ligonresearch_s3"']`` may include surrounding single
    quotes.
    """
    if dvc_dir is None:
        dvc_dir = _COUNTRIES_DIR / ".dvc"

    cfg_path = dvc_dir / "config"
    if not cfg_path.exists():
        return {}

    cfg = configparser.ConfigParser()
    cfg.read(cfg_path)

    remotes: dict[str, dict[str, str]] = {}
    for section in cfg.sections():
        m = re.match(r"""'?remote\s+["'](.+?)["']'?""", section)
        if m:
            name = m.group(1)
            remotes[name] = dict(cfg.items(section))
    return remotes


# ---------------------------------------------------------------------------
# Credential probing
# ---------------------------------------------------------------------------

def _check_remote_access(remote_name: str, remote_cfg: dict[str, str],
                         dvc_dir: Path) -> str | None:
    """Determine access level for a single DVC remote.

    Returns ``"read"``, ``"write"``, or ``None``.

    For S3 remotes, write access is detected via:
    - ``LSMS_S3_WRITE_KEY`` env var (value is the AWS secret access key;
      paired with ``LSMS_S3_WRITE_KEY_ID`` for the access key id), or
    - ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars
      (standard boto3 credentials), or
    - A ``s3_write_creds`` file alongside ``s3_creds`` in the DVC dir.
    """
    url = remote_cfg.get("url", "")

    if url.startswith("s3://"):
        cred_path = remote_cfg.get("credentialpath")
        has_read = False
        if cred_path:
            cred_file = dvc_dir / cred_path
            has_read = cred_file.exists() and cred_file.stat().st_size > 0

        if not has_read:
            return None

        # Check for write credentials (several forms)
        if os.environ.get("LSMS_S3_WRITE_KEY"):
            return "write"
        if (os.environ.get("AWS_ACCESS_KEY_ID")
                and os.environ.get("AWS_SECRET_ACCESS_KEY")):
            return "write"
        write_creds = dvc_dir / "s3_write_creds"
        if write_creds.exists() and write_creds.stat().st_size > 0:
            return "write"

        return "read"

    if url.startswith("gdrive://"):
        cred_key = "gdrive_service_account_json_file_path"
        cred_path = remote_cfg.get(cred_key)
        if cred_path:
            cred_file = dvc_dir / cred_path
            if cred_file.exists() and cred_file.stat().st_size > 0:
                return "read"
        return None

    # Unknown remote type --- no opinion
    return None


# ---------------------------------------------------------------------------
# WB API key validation (session-cached)
# ---------------------------------------------------------------------------

_wb_key_validated: bool | None = None


def _validate_wb_api_key(api_key: str) -> bool:
    """Lightweight check that *api_key* is accepted by the WB.

    Performs a cheap catalog search and caches the result for the
    session so subsequent calls are free.
    """
    global _wb_key_validated
    if _wb_key_validated is not None:
        return _wb_key_validated

    import json
    import urllib.request

    url = "https://microdata.worldbank.org/index.php/api/catalog/search?ps=1"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        _wb_key_validated = data.get("result", {}).get("found", 0) > 0
    except Exception:
        _wb_key_validated = False

    return _wb_key_validated


# ---------------------------------------------------------------------------
# GPG decryption helpers
# ---------------------------------------------------------------------------

def _gpg_decrypt(gpg_file: Path, passphrase: str) -> str | None:
    """Decrypt a symmetric-GPG file, trying python-gnupg then ``gpg(1)``.

    Returns the decrypted text, or ``None`` on failure.
    """
    # Try the python-gnupg library first
    try:
        import gnupg
        g = gnupg.GPG()
        decrypted = g.decrypt(gpg_file.read_bytes(), passphrase=passphrase)
        if decrypted.ok:
            return str(decrypted)
    except ImportError:
        pass
    except Exception as exc:
        logger.debug("python-gnupg decryption failed: %s", exc)

    # Subprocess fallback (works when only the gpg binary is installed)
    for extra_args in ([], ["--pinentry-mode", "loopback"]):
        try:
            result = subprocess.run(
                ["gpg", "--batch", "--yes", "--passphrase-fd", "0",
                 "--decrypt", str(gpg_file)] + extra_args,
                input=passphrase.encode(),
                capture_output=True,
                timeout=30,
            )
            if result.returncode == 0:
                return result.stdout.decode()
        except Exception as exc:
            logger.debug("gpg subprocess error: %s", exc)

    return None


def _auto_unlock_s3(dvc_dir: Path | None = None) -> bool:
    """Decrypt ``s3_reader_creds.gpg`` using the obfuscated passphrase.

    Called automatically when a valid WB API key is present.  The API
    key validation is the real access gate; the obfuscated passphrase
    just keeps the value from being trivially discoverable via grep.

    Returns ``True`` if ``s3_creds`` was written (or already exists).
    """
    if dvc_dir is None:
        dvc_dir = _COUNTRIES_DIR / ".dvc"

    creds_file = dvc_dir / "s3_creds"
    if creds_file.exists() and creds_file.stat().st_size > 0:
        return True

    gpg_file = dvc_dir / "s3_reader_creds.gpg"
    if not gpg_file.exists():
        return False

    passphrase = base64.b64decode(_S3_UNLOCK_PASSPHRASE).decode()
    decrypted = _gpg_decrypt(gpg_file, passphrase)
    if decrypted is None:
        logger.warning("Auto-unlock of S3 read credentials failed "
                       "(GPG decryption error).")
        return False

    try:
        creds_file.write_text(decrypted)
    except OSError as exc:
        logger.warning("Could not write s3_creds: %s", exc)
        return False

    logger.info("Auto-unlocked S3 read credentials via WB API key.")
    return True


# ---------------------------------------------------------------------------
# Permissions (public API)
# ---------------------------------------------------------------------------

_cached_permissions: dict[str, str] | None = None


def permissions(path: str | Path | None = None) -> dict[str, str]:
    """Return ``{resource_name: access_level}`` for available resources.

    Resource names come from DVC remote names defined in
    ``.dvc/config`` (e.g. ``"ligonresearch_s3"``, ``"ligonresearch"``)
    plus ``"wb_api"`` for the World Bank Microdata Library.

    Access levels are ``"read"`` or ``"write"``.  Resources the user
    cannot reach are absent from the dict.

    Parameters
    ----------
    path : str or Path, optional
        Path to a specific data file.  Reserved for future per-file /
        per-LICENSE access checks.  Currently unused.

    Returns
    -------
    dict[str, str]
    """
    global _cached_permissions
    if _cached_permissions is not None:
        return dict(_cached_permissions)

    perms: dict[str, str] = {}
    dvc_dir = _COUNTRIES_DIR / ".dvc"

    # --- WB API key (and auto-unlock of S3 creds) -----------------------
    api_key = config.microdata_api_key()
    if api_key and _validate_wb_api_key(api_key):
        perms["wb_api"] = "read"
        # A valid WB key proves ToU acceptance, which is what the GPG
        # passphrase was gating.  Auto-decrypt S3 read creds so DVC
        # works without an interactive authenticate() call.
        _auto_unlock_s3(dvc_dir)

    # --- DVC remotes (checked *after* auto-unlock so freshly written
    #     creds are visible) ----------------------------------------------
    remotes = _parse_dvc_remotes(dvc_dir)
    for name, remote_cfg in remotes.items():
        level = _check_remote_access(name, remote_cfg, dvc_dir)
        if level is not None:
            perms[name] = level

    _cached_permissions = perms
    return dict(perms)


def can_read(resource: str, path: str | Path | None = None) -> bool:
    """Check whether *resource* is readable in the current session."""
    return permissions(path).get(resource) in ("read", "write")


def can_write(resource: str, path: str | Path | None = None) -> bool:
    """Check whether *resource* is writable in the current session."""
    return permissions(path).get(resource) == "write"


def reset_permissions_cache() -> None:
    """Clear cached permissions (e.g. after new credentials are written)."""
    global _cached_permissions, _wb_key_validated
    _cached_permissions = None
    _wb_key_validated = None


# ---------------------------------------------------------------------------
# World Bank NADA download helpers
# ---------------------------------------------------------------------------

def _read_source_url(country: str, wave: str) -> str | None:
    """Read the catalog URL from SOURCE.org."""
    source = _COUNTRIES_DIR / country / wave / "Documentation" / "SOURCE.org"
    if not source.exists():
        return None
    try:
        text = source.read_text()
        m = re.search(r'https?://[^\s\]\)]+', text)
        return m.group(0).rstrip("/") if m else None
    except Exception:
        return None


def _extract_catalog_id(url: str) -> str | None:
    m = re.search(r'/catalog/(\d+)', url)
    return m.group(1) if m else None


def _get_catalog_idno(catalog_url: str) -> str | None:
    """Scrape the NADA string idno from the catalog HTML page."""
    import urllib.request

    try:
        req = urllib.request.Request(catalog_url)
        req.add_header("User-Agent", "lsms_library")
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        m = re.search(r'data-idno="([^"]+)"', html)
        return m.group(1) if m else None
    except Exception:
        return None


def _find_stata_zip_url(api_base: str, idno: str,
                        api_key: str) -> tuple[str, str] | None:
    """Find the Stata zip download URL from the NADA resources API.

    Returns ``(download_url, filename)`` or ``None``.
    """
    import json
    import urllib.request

    url = f"{api_base}/api/resources/{idno}"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("Accept", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        logger.debug("NADA resources API error for %s: %s", idno, e)
        return None

    resources = data.get("resources", [])
    for r in resources:
        filename = (r.get("filename") or "").lower()
        title = (r.get("title") or "").lower()
        if ("stata" in filename or "stata" in title) and filename.endswith(".zip"):
            download_url = r.get("_links", {}).get("download")
            if not download_url:
                rid = r.get("resource_id") or r.get("id")
                sid = r.get("survey_id", "")
                download_url = (f"{api_base}/catalog/{sid}"
                                f"/download/{rid}/{r.get('filename', '')}")
            return download_url, r.get("filename", f"{idno}.zip")

    return None


def _download_and_extract(download_url: str, zip_filename: str,
                          target_file: str, dest_path: Path,
                          api_key: str) -> bool:
    """Download a zip from the WB and extract a specific file.

    Parameters
    ----------
    download_url : str
        URL to download the Stata zip.
    zip_filename : str
        Name of the zip file (for logging).
    target_file : str
        The .dta/.csv filename to extract (basename, case-insensitive).
    dest_path : Path
        Where to write the extracted file.
    api_key : str
        NADA API key for authentication.

    Returns
    -------
    bool
        True if the file was extracted successfully.
    """
    import urllib.request

    req = urllib.request.Request(download_url)
    req.add_header("X-API-KEY", api_key)

    target_lower = target_file.lower()

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        logger.info("Downloading %s ...", zip_filename)
        with urllib.request.urlopen(req, timeout=300) as resp:
            with open(tmp_path, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)

        with zipfile.ZipFile(tmp_path) as zf:
            for name in zf.namelist():
                if Path(name).name.lower() == target_lower:
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(name) as src, open(dest_path, "wb") as dst:
                        while True:
                            chunk = src.read(65536)
                            if not chunk:
                                break
                            dst.write(chunk)
                    logger.info("Extracted %s -> %s", name, dest_path)
                    return True

        logger.warning("File %s not found in %s", target_file, zip_filename)
        return False
    except Exception as e:
        logger.error("Download/extract failed: %s", e)
        return False
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _extract_all_from_zip(download_url: str, zip_filename: str,
                          dest_dir: Path, api_key: str) -> list[Path]:
    """Download a zip and extract ALL data files.  Returns list of paths."""
    import urllib.request

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name

    extracted: list[Path] = []
    try:
        logger.info("Downloading %s (full zip) ...", zip_filename)
        req = urllib.request.Request(download_url)
        req.add_header("X-API-KEY", api_key)
        with urllib.request.urlopen(req, timeout=600) as resp:
            with open(tmp_path, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)

        data_suffixes = {".dta", ".csv", ".sav", ".dta.gz"}
        with zipfile.ZipFile(tmp_path) as zf:
            for name in zf.namelist():
                if any(name.lower().endswith(s) for s in data_suffixes):
                    basename = Path(name).name
                    dest = dest_dir / basename
                    if not dest.exists():
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(name) as src, open(dest, "wb") as dst:
                            while True:
                                chunk = src.read(65536)
                                if not chunk:
                                    break
                                dst.write(chunk)
                        extracted.append(dest)
                        logger.info("  Extracted %s", basename)
        return extracted
    except Exception as e:
        logger.error("Full zip download/extract failed: %s", e)
        return extracted
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Push to cache
# ---------------------------------------------------------------------------

def push_to_cache(path: str | Path,
                  remote: str | None = None,
                  dvc_add: bool = True) -> bool:
    """Push a local data file to a DVC remote.

    Runs ``dvc add`` on the file (creating a ``.dvc`` pointer), then
    ``dvc push`` to upload it to the remote.  Requires write access to
    the target remote.

    Parameters
    ----------
    path : str or Path
        Relative path like ``Uganda/2013-14/Data/GSEC1.dta``.
        Interpreted relative to the countries directory.
    remote : str, optional
        DVC remote name to push to.  Defaults to the ``core.remote``
        configured in ``.dvc/config``.
    dvc_add : bool
        If True (default), run ``dvc add`` before pushing.  Set to
        False if the file is already DVC-tracked.

    Returns
    -------
    bool
        True if the push succeeded.
    """
    path = Path(path)
    abs_path = _COUNTRIES_DIR / path

    if not abs_path.exists():
        logger.error("Cannot push non-existent file: %s", abs_path)
        return False

    # Check write permission on at least one DVC remote
    perms = permissions()
    if remote:
        if perms.get(remote) != "write":
            logger.error("No write access to remote %r (have: %s)",
                         remote, perms.get(remote))
            return False
    else:
        writable = [r for r, lvl in perms.items()
                    if r != "wb_api" and lvl == "write"]
        if not writable:
            logger.error("No writable DVC remotes. Set LSMS_S3_WRITE_KEY "
                         "or provide write credentials.")
            return False

    try:
        if dvc_add:
            result = subprocess.run(
                ["dvc", "add", str(abs_path)],
                cwd=str(_COUNTRIES_DIR),
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode != 0:
                logger.error("dvc add failed: %s", result.stderr.strip())
                return False
            logger.info("dvc add: %s", abs_path)

        push_cmd = ["dvc", "push", str(abs_path) + ".dvc"]
        if remote:
            push_cmd.extend(["-r", remote])
        result = subprocess.run(
            push_cmd,
            cwd=str(_COUNTRIES_DIR),
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode != 0:
            logger.error("dvc push failed: %s", result.stderr.strip())
            return False

        logger.info("Pushed to cache: %s", path)
        return True
    except Exception as exc:
        logger.error("push_to_cache error: %s", exc)
        return False


def populate_and_push(country: str, wave: str,
                      remote: str | None = None) -> list[Path]:
    """Download all data files for a wave from WB and push to DVC cache.

    Convenience wrapper: downloads the full Stata zip via the WB NADA
    API, extracts all data files, then ``dvc add`` + ``dvc push`` each
    one.  Requires both ``wb_api`` read access and write access to a
    DVC remote.

    Parameters
    ----------
    country : str
        Country name (directory name), e.g. ``"Uganda"``.
    wave : str
        Wave identifier, e.g. ``"2013-14"``.
    remote : str, optional
        DVC remote to push to.  Defaults to the configured default.

    Returns
    -------
    list[Path]
        Paths of files successfully pushed.
    """
    # Use a dummy filename to trigger the download; populate_cache=True
    # extracts everything from the zip.
    dummy = f"{country}/{wave}/Data/_probe_.dta"
    get_data_file(dummy, populate_cache=True)

    data_dir = _COUNTRIES_DIR / country / wave / "Data"
    if not data_dir.exists():
        logger.warning("No Data directory after download: %s", data_dir)
        return []

    pushed: list[Path] = []
    data_suffixes = {".dta", ".csv", ".sav"}
    for f in sorted(data_dir.iterdir()):
        if f.suffix.lower() not in data_suffixes:
            continue
        rel = f.relative_to(_COUNTRIES_DIR)
        if push_to_cache(rel, remote=remote, dvc_add=True):
            pushed.append(f)
        else:
            logger.warning("Failed to push %s", rel)

    return pushed


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def get_data_file(path: str | Path,
                  populate_cache: bool = False) -> Path | None:
    """Obtain a raw data file, trying local -> DVC remotes -> World Bank.

    Parameters
    ----------
    path : str or Path
        Relative path like ``Uganda/2013-14/Data/GSEC1.dta``.
        Interpreted relative to the countries directory.
    populate_cache : bool
        If True and the file is downloaded from the WB, also extract
        all files from the zip (not just the one requested).

    Returns
    -------
    Path or None
        Absolute path to the file on local disk, or ``None`` if
        unavailable.
    """
    path = Path(path)
    abs_path = _COUNTRIES_DIR / path
    target_filename = path.name

    # Parse country/wave from the path
    parts = path.parts
    if len(parts) < 3:
        logger.error("Path too short, expected Country/Wave/Data/file: %s",
                      path)
        return None
    country = parts[0]
    wave = parts[1]

    # 1. Local file already exists
    if abs_path.exists():
        logger.debug("Local hit: %s", abs_path)
        return abs_path

    perms = permissions(path)
    logger.debug("Permissions: %s", perms)

    # 2. Try DVC (uses whichever remote .dvc/config designates as default).
    #    We gate on whether *any* DVC remote is readable.
    dvc_readable = any(
        level in ("read", "write")
        for resource, level in perms.items()
        if resource != "wb_api"
    )
    if dvc_readable:
        try:
            from dvc.api import DVCFileSystem
            fs = DVCFileSystem(os.fspath(_COUNTRIES_DIR))
            dvc_path = str(path)
            if fs.exists(dvc_path):
                abs_path.parent.mkdir(parents=True, exist_ok=True)
                fs.get_file(dvc_path, str(abs_path))
                logger.info("Fetched from DVC: %s", path)
                return abs_path
        except Exception as e:
            logger.debug("DVC fetch failed: %s", e)

    # 3. Try World Bank download
    if "wb_api" not in perms:
        logger.warning("No permissions available to fetch %s", path)
        return None

    api_key = config.microdata_api_key()
    if not api_key:
        logger.warning("No MICRODATA_API_KEY set; cannot fetch from "
                        "World Bank.")
        return None

    source_url = _read_source_url(country, wave)
    if not source_url:
        logger.warning("No SOURCE.org found for %s/%s", country, wave)
        return None

    if "worldbank.org" not in source_url:
        logger.info("Non-World Bank source for %s/%s, skipping WB download.",
                     country, wave)
        return None

    catalog_id = _extract_catalog_id(source_url)
    if not catalog_id:
        logger.warning("Could not extract catalog ID from %s", source_url)
        return None

    idno = _get_catalog_idno(source_url)
    if not idno:
        logger.warning("Could not resolve idno for catalog %s", catalog_id)
        return None

    parsed = urlparse(source_url)
    api_base = f"{parsed.scheme}://{parsed.netloc}/index.php"

    result = _find_stata_zip_url(api_base, idno, api_key)
    if not result:
        logger.warning("No Stata zip found for %s (%s)", idno, source_url)
        return None

    download_url, zip_filename = result
    data_dir = _COUNTRIES_DIR / country / wave / "Data"

    if populate_cache:
        extracted = _extract_all_from_zip(download_url, zip_filename,
                                          data_dir, api_key)
        if abs_path.exists():
            return abs_path
        for f in extracted:
            if f.name.lower() == target_filename.lower():
                return f
        return None
    else:
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        if _download_and_extract(download_url, zip_filename,
                                  target_filename, abs_path, api_key):
            return abs_path
        return None

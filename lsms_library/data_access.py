"""Data access layer with tiered permissions and multi-source fallback.

Provides :func:`get_data_file` — the single entry point for obtaining a
raw data file (e.g. ``Uganda/2013-14/Data/GSEC1.dta``).  It tries
sources in order:

1. **Local file** — already on disk (e.g. from a previous download).
2. **S3 cache via DVC** — if the user has S3 read credentials
   (decrypted via ``ll.authenticate()`` *or* validated via a WB API key).
3. **World Bank Microdata Library** — download the Stata zip from the
   NADA API and extract the requested file.  Requires ``MICRODATA_API_KEY``.

The :func:`permission_to_read` function encapsulates the credential
check and is designed to be extended for per-file or per-license
access tiers.

Environment variables
---------------------
MICRODATA_API_KEY
    World Bank Microdata Library API key.  Proves the user has accepted
    the WB terms of use.
LSMS_SKIP_AUTH
    If ``"1"``/``"true"``/``"yes"``, skip the interactive GPG passphrase
    prompt on import.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
import zipfile
from enum import Enum, auto
from pathlib import Path
from urllib.parse import urlparse

from . import config

logger = logging.getLogger(__name__)

_COUNTRIES_DIR = Path(__file__).resolve().parent / "countries"


# ---------------------------------------------------------------------------
# Permission tiers
# ---------------------------------------------------------------------------

class AccessTier(Enum):
    """Data access tiers, from most to least restrictive."""
    NONE = auto()       # No credentials at all
    WB_API = auto()     # Has a valid WB Microdata API key
    S3_READ = auto()    # Can read the S3 cache (via decrypted creds or validated API key)
    S3_WRITE = auto()   # Can write to the S3 cache (trusted maintainers)


def _has_s3_read_creds() -> bool:
    """Check whether decrypted S3 read credentials exist."""
    creds = _COUNTRIES_DIR / ".dvc" / "s3_creds"
    return creds.exists() and creds.stat().st_size > 0


def _has_s3_write_creds() -> bool:
    """Check whether S3 write credentials are available."""
    # Write credentials could be in a separate file or env var.
    # For now, check for an env var that the maintainer sets.
    return bool(os.environ.get("LSMS_S3_WRITE_KEY"))


def _has_wb_api_key() -> bool:
    """Check whether a World Bank Microdata API key is set."""
    return bool(config.microdata_api_key())


def _validate_wb_api_key(api_key: str) -> bool:
    """Lightweight check that the API key is accepted by the WB."""
    import urllib.request
    import json

    url = "https://microdata.worldbank.org/index.php/api/catalog/search?ps=1"
    req = urllib.request.Request(url)
    req.add_header("X-API-KEY", api_key)
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        return data.get("result", {}).get("found", 0) > 0
    except Exception:
        return False


# Cache the validation result for the session
_wb_key_validated: bool | None = None


def permission_to_read(path: str | Path | None = None) -> AccessTier:
    """Determine the current user's data access tier.

    Parameters
    ----------
    path : str or Path, optional
        Path to a specific data file (e.g. ``Uganda/2013-14/Data/GSEC1.dta``).
        Reserved for future per-file / per-LICENSE access checks.
        Currently unused — access tier is determined globally.

    Returns
    -------
    AccessTier
        The highest access tier available to the current user.
    """
    global _wb_key_validated

    # Highest tier: S3 write
    if _has_s3_write_creds() and _has_s3_read_creds():
        return AccessTier.S3_WRITE

    # S3 read via decrypted GPG credentials
    if _has_s3_read_creds():
        return AccessTier.S3_READ

    # WB API key — validates on first check, caches result
    api_key = config.microdata_api_key()
    if api_key:
        if _wb_key_validated is None:
            _wb_key_validated = _validate_wb_api_key(api_key)
        if _wb_key_validated:
            return AccessTier.WB_API

    return AccessTier.NONE


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

    Returns (download_url, filename) or None.
    """
    import urllib.request
    import json

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
                # Construct from resource metadata
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
        The .dta/.csv filename to extract (basename, case-insensitive match).
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
    """Download a zip and extract ALL data files. Returns list of paths."""
    import urllib.request

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = tmp.name

    extracted = []
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
# Main entry point
# ---------------------------------------------------------------------------

def get_data_file(path: str | Path,
                  populate_cache: bool = False) -> Path | None:
    """Obtain a raw data file, trying local → S3/DVC → World Bank.

    Parameters
    ----------
    path : str or Path
        Relative path like ``Uganda/2013-14/Data/GSEC1.dta``.
        Interpreted relative to the countries directory.
    populate_cache : bool
        If True and the file is downloaded from the WB, also extract
        all files from the zip (not just the one requested).
        Useful for a maintainer pre-populating the S3 cache.

    Returns
    -------
    Path or None
        Absolute path to the file on local disk, or None if unavailable.
    """
    path = Path(path)
    abs_path = _COUNTRIES_DIR / path
    target_filename = path.name

    # Parse country/wave from the path
    parts = path.parts
    if len(parts) < 3:
        logger.error("Path too short, expected Country/Wave/Data/file: %s", path)
        return None
    country = parts[0]
    wave = parts[1]

    # 1. Local file already exists
    if abs_path.exists():
        logger.debug("Local hit: %s", abs_path)
        return abs_path

    tier = permission_to_read(path)
    logger.debug("Access tier: %s", tier.name)

    # 2. Try S3/DVC (requires S3_READ or higher)
    if tier.value >= AccessTier.S3_READ.value:
        try:
            from dvc.api import DVCFileSystem
            fs = DVCFileSystem(os.fspath(_COUNTRIES_DIR))
            dvc_path = str(path)
            if fs.exists(dvc_path):
                abs_path.parent.mkdir(parents=True, exist_ok=True)
                fs.get_file(dvc_path, str(abs_path))
                logger.info("Fetched from S3 cache: %s", path)
                return abs_path
        except Exception as e:
            logger.debug("DVC/S3 fetch failed: %s", e)

    # 3. Try World Bank download (requires WB_API or higher)
    api_key = config.microdata_api_key()
    if not api_key:
        logger.warning("No MICRODATA_API_KEY set; cannot fetch from World Bank.")
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
        # Extract everything from the zip
        extracted = _extract_all_from_zip(download_url, zip_filename,
                                          data_dir, api_key)
        if abs_path.exists():
            return abs_path
        # Check if extracted under a different case
        for f in extracted:
            if f.name.lower() == target_filename.lower():
                return f
        return None
    else:
        # Extract just the one file we need
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        if _download_and_extract(download_url, zip_filename,
                                  target_filename, abs_path, api_key):
            return abs_path
        return None

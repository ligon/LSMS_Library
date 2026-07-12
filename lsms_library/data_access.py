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
interactively.  The decrypted plaintext is written to the user-writable
path returned by :func:`lsms_library.config.s3_creds_path` (defaulting
to ``~/.config/lsms_library/s3_creds``), not into the package tree —
this is what makes the library safe to install into a read-only
site-packages directory.

Environment variables
---------------------
MICRODATA_API_KEY
    World Bank Microdata Library API key.  Proves the user has accepted
    the WB terms of use.
LSMS_S3_WRITE_KEY
    If set, grants write access to S3 remotes.
LSMS_S3_CREDS
    Override path for the decrypted S3 reader credentials.  Defaults
    to ``<config_dir>/s3_creds`` (see :func:`lsms_library.config.s3_creds_path`).
LSMS_SKIP_AUTH
    If ``"1"``/``"true"``/``"yes"``, skip the interactive GPG passphrase
    prompt on import.
"""

from __future__ import annotations

import base64
import configparser
import json
import logging
import os
import random
import re
import subprocess
import tempfile
import time
import urllib.error
import zipfile
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlparse

from . import config
from .paths import countries_root
from .provenance import (
    SOURCE_WORLDBANK,
    WaveProvenance,
    read_provenance,
    write_provenance,
)

logger = logging.getLogger(__name__)

# GH #436: countries config-tree root.  Import-time snapshot of
# ``countries_root()`` -- honors ``LSMS_COUNTRIES_ROOT`` when the env var is set
# *before* import (the worktree model); it does NOT track a later override +
# ``countries_root.cache_clear()`` within the same process.
_COUNTRIES_DIR = countries_root()

# ---------------------------------------------------------------------------
# Country -> WB catalog registry
# ---------------------------------------------------------------------------
#
# An ISO alpha-3 code alone is not enough to identify "our" surveys, because
# several of our country directories are *different survey series from the
# same nation*:
#
#   GhanaLSS / GhanaSPS  -- both ISO GHA (Living Standards Survey vs.
#                           Socioeconomic Panel Survey)
#   Tanzania / Tanzania_Kegera
#                        -- both ISO TZA (National Panel Survey vs. the
#                           Kagera Health and Development Survey)
#
# Without a discriminator each of those directories sees the other's catalog
# entries as *its own* missing waves.  ``idno_pattern`` is a regex matched
# against the catalog entry's ``idno`` (e.g. ``GHA_1987_GLSS_v02_M``), which
# encodes the survey series and is the WB's own stable study identifier.
#
# ``discoverable=False`` marks a country whose data does not come from the WB
# catalog at all.  It is recorded explicitly -- rather than simply omitted --
# so that "we deliberately do not discover this" is distinguishable from "we
# forgot to add a country code".
#
# ``repositories`` (GH #597) lists the WB *collections* to search for a country,
# defaulting to ``("lsms",)``.  The World Bank publishes whole household-survey
# series outside the ``lsms`` collection -- Armenia's Integrated Living
# Conditions Survey sits in ``central``, South Africa's General Household Survey
# in ``datafirst`` -- and a search hard-coded to ``lsms`` cannot see them at all.
# They were not "not yet fetched"; they were unfindable.
#
# The fix is deliberately *targeted*.  Dropping the collection filter entirely
# inflates a country's result set 30-400x with material we do not want (Findex,
# Afrobarometer, DHS, enterprise surveys, and -- in ``datafirst`` -- 320 rows of
# South African election studies, school registers and media surveys).  That
# trades a false-negative problem for a false-positive one, and the second is
# worse: it buries the real gaps in noise.  A missing-wave list nobody trusts is
# worse than no list.
#
# So widening a country to a second repository is paired with an
# ``idno_pattern`` that pins the *survey series*.  The two levers compose:
# ``repositories`` says where to look, ``idno_pattern`` says what counts.  Both
# are config, not heuristics -- auditable beats clever.
#
# Widening without pinning the series would also resurface studies we already
# hold under a *different* catalog id in another repository: ``central`` id 3016
# (``MWI_2010_IHS-III_v01_M_v01_A_ML``) is the same Malawi IHS3 as ``lsms`` id
# 1003 (``MWI_2010_IHS-III_v01_M``), which we hold as ``Malawi/2010-11/``, and
# ``datafirst`` id 902 (``ZAF_1993_PSLSD``) is the same 1993 South African survey
# as ``lsms`` id 297 (``ZAF_1993_IHS``), which we hold as ``South Africa/1993/``.
# Naive widening reports both as missing waves.  They are not.


class CountryCatalog:
    """How a country directory maps onto the WB Microdata Library catalog."""

    __slots__ = ("code", "idno_pattern", "discoverable", "reason",
                 "repositories")

    def __init__(self, code: str | None, idno_pattern: str | None = None,
                 discoverable: bool = True, reason: str | None = None,
                 repositories: Sequence[str] | None = None):
        self.code = code
        self.idno_pattern = idno_pattern
        self.discoverable = discoverable
        self.reason = reason
        # Default: the LSMS collection alone, which is where all but a handful
        # of our series live.
        self.repositories: tuple[str, ...] = tuple(repositories or ("lsms",))

    def matches(self, entry: dict) -> bool:
        """True when a catalog *entry* belongs to this country directory."""
        if not self.idno_pattern:
            return True
        return bool(re.search(self.idno_pattern, str(entry.get("idno", ""))))


_COUNTRY_CATALOG: dict[str, CountryCatalog] = {
    "Afghanistan": CountryCatalog("AFG"),
    "Albania": CountryCatalog("ALB"),
    # ARM: the Integrated Living Conditions Survey (ILCS, 2001-2018) is an
    # annual living-standards series published under ``central``, NOT ``lsms``
    # -- 18 waves that a lsms-only search could not see (GH #597).  ``lsms``
    # carries only the 1996 Household Budget Survey, which we hold as ``1996/``.
    # The pattern admits both series and nothing else: ``central`` also returns
    # Armenian Labour Force Surveys, a migration survey and a time-use survey,
    # plus global Findex / Global Consumption Database rows tagged to every
    # country.
    "Armenia": CountryCatalog("ARM", idno_pattern=r"_(HBS|ILCS)_",
                              repositories=("lsms", "central")),
    "Azerbaijan": CountryCatalog("AZE"),
    "Benin": CountryCatalog("BEN"),
    "Bosnia-Herzegovina": CountryCatalog("BIH"),
    "Brazil": CountryCatalog("BRA"),
    "Bulgaria": CountryCatalog("BGR"),
    "Burkina_Faso": CountryCatalog("BFA"),
    "Cambodia": CountryCatalog("KHM"),
    "China": CountryCatalog("CHN"),
    "CotedIvoire": CountryCatalog("CIV"),
    "Ethiopia": CountryCatalog("ETH"),
    # GHA is shared: GLSS (Living Standards Survey) vs GSPS (Socioeconomic
    # Panel Survey).  Without the idno filter each sees the other's waves.
    "GhanaLSS": CountryCatalog("GHA", idno_pattern=r"_GLSS"),
    "GhanaSPS": CountryCatalog("GHA", idno_pattern=r"_GSPS"),
    "Guatemala": CountryCatalog("GTM"),
    "Guinea-Bissau": CountryCatalog("GNB"),
    "Guyana": CountryCatalog("GUY"),
    "India": CountryCatalog("IND"),
    "Iraq": CountryCatalog("IRQ"),
    "Kazakhstan": CountryCatalog("KAZ"),
    "Kosovo": CountryCatalog("XKX"),
    "Kyrgyz Republic": CountryCatalog("KGZ"),
    "Liberia": CountryCatalog("LBR"),
    "Malawi": CountryCatalog("MWI"),
    "Mali": CountryCatalog("MLI"),
    "Nepal": CountryCatalog("NPL"),
    "Nicaragua": CountryCatalog("NIC"),
    "Niger": CountryCatalog("NER"),
    "Nigeria": CountryCatalog("NGA"),
    "Pakistan": CountryCatalog("PAK"),
    "Panama": CountryCatalog("PAN"),
    "Peru": CountryCatalog("PER"),
    "Rwanda": CountryCatalog("RWA"),
    "Senegal": CountryCatalog("SEN"),
    "Serbia": CountryCatalog("SRB"),
    # Serbia and Montenegro was a distinct ISO entity (SCG); its two LSMS
    # rounds are catalog ids 80 and 81, matching our 2002/ and 2003/ dirs.
    "Serbia and Montenegro": CountryCatalog("SCG"),
    # ZAF: the General Household Survey (GHS, 2002-2025) is published under
    # ``datafirst`` (UCT's DataFirst archive), not ``lsms`` -- 21 waves invisible
    # to a lsms-only search (GH #597).  ``lsms`` carries only the 1993 Integrated
    # Household Survey, which we hold as ``1993/``.
    #
    # The series pin matters more here than anywhere else: ``datafirst`` returns
    # 320 ZAF rows -- quarterly labour force surveys, censuses, victim-of-crime
    # and domestic-tourism surveys, election studies, school registers, media
    # surveys.  Widening the repository without pinning the series would report
    # ~357 "missing waves" for South Africa, which is not a denominator anyone
    # could use.
    "South Africa": CountryCatalog("ZAF", idno_pattern=r"_(IHS|GHS)_",
                                   repositories=("lsms", "datafirst")),
    "Tajikistan": CountryCatalog("TJK"),
    # TZA is shared: the National Panel Survey vs the Kagera Health and
    # Development Survey (a separate longitudinal study).
    "Tanzania": CountryCatalog("TZA", idno_pattern=r"_NPS"),
    "Tanzania_Kegera": CountryCatalog("TZA", idno_pattern=r"_KHDS"),
    "Timor-Leste": CountryCatalog("TLS"),
    "Togo": CountryCatalog("TGO"),
    "Uganda": CountryCatalog("UGA"),

    # --- Not World Bank datasets -------------------------------------------
    "EthiopiaRHS": CountryCatalog(
        None, discoverable=False,
        reason="Ethiopian Rural Household Survey is an IFPRI/Addis Ababa "
               "University study distributed via Harvard Dataverse "
               "(doi:10.7910/DVN/T8G8IV), not the World Bank Microdata "
               "Library.  There is nothing to discover in the WB catalog."),
    "KenyaLPS": CountryCatalog(
        None, discoverable=False,
        reason="Kenya Life Panel Survey is not distributed via the World "
               "Bank Microdata Library (a KEN/lsms catalog query returns no "
               "entries)."),
}

# Backwards-compatible view: the plain {country: ISO code} mapping that this
# module exposed before the registry above.  Countries that are not WB-sourced
# are absent, exactly as they were before.
_COUNTRY_CODES: dict[str, str] = {
    name: spec.code for name, spec in _COUNTRY_CATALOG.items()
    if spec.code is not None
}


def _dvc_cmd() -> str:
    """Return path to the ``dvc`` executable in the current venv.

    Falls back to bare ``"dvc"`` if no venv-local binary is found.
    """
    import sys
    venv_dvc = Path(sys.executable).parent / "dvc"
    if venv_dvc.exists():
        return str(venv_dvc)
    return "dvc"


# DVC serializes every repo operation on ``.dvc/tmp/lock``.  Concurrent
# WRITERS therefore collide: multiple RAs running ``populate_and_push`` at
# once, or a ``make -j`` materialize, fail with "Unable to acquire lock"
# instead of queueing.  (The READ path does not hit this at all -- it warms
# the cache via the direct-S3 bypass in ``local_tools._ensure_dvc_pulled``,
# which never calls ``Repo.fetch`` and never takes the lock.  Concurrent
# reads should always go through ``get_dataframe()`` / that bypass, NEVER a
# raw ``dvc pull`` CLI, which would re-introduce the contention.)  This is
# the cheap, in-process mitigation short of a full fetch/write queue (see
# SkunkWorks/dvc_writer_distribution.org): retry the write CLI on lock
# contention with exponential backoff + jitter so colliding writers serialize
# gracefully rather than failing.
_DVC_LOCK_MARKERS = (
    "unable to acquire lock",
    "lock is busy",
    "failed to acquire lock",
    "lockerror",
)


def _run_dvc_with_lock_retry(cmd, *, cwd, timeout, retries: int = 5,
                             base_delay: float = 4.0) -> subprocess.CompletedProcess:
    """Run a ``dvc`` CLI command, retrying only on DVC lock contention.

    Returns the final :class:`subprocess.CompletedProcess`.  A non-zero exit
    whose stderr does not look like lock contention is returned immediately
    (no retry) so genuine errors surface fast.  ``subprocess.TimeoutExpired``
    propagates to the caller's existing handler.

    The retry targets the global ``.dvc/tmp/lock`` collision between
    concurrent writers; backoff is exponential with uniform jitter to avoid a
    thundering herd of writers waking together.
    """
    sub = cmd[1] if len(cmd) > 1 else "dvc"
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                            timeout=timeout)
    for attempt in range(1, retries + 1):
        if result.returncode == 0:
            return result
        stderr_lc = (result.stderr or "").lower()
        if not any(m in stderr_lc for m in _DVC_LOCK_MARKERS):
            return result  # genuine failure, not lock contention
        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)
        logger.warning("dvc %s: lock contention (attempt %d/%d); retrying in %.1fs",
                       sub, attempt, retries, delay)
        time.sleep(delay)
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                                timeout=timeout)
    return result


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
    - A ``s3_write_creds`` file at :func:`config.s3_write_creds_path`
      (``~/.config/lsms_library/s3_write_creds``), or the legacy
      in-tree ``<dvc_dir>/s3_write_creds`` fallback.

    Note: ``~/.aws/credentials`` is *not* checked for write access
    because there is no way to distinguish reader vs writer keys from
    the file alone (that depends on the IAM policy attached to the key).
    To grant write access, set ``LSMS_S3_WRITE_KEY`` explicitly.
    """
    url = remote_cfg.get("url", "")

    if url.startswith("s3://"):
        cred_path = remote_cfg.get("credentialpath")
        has_read = False

        # Prefer the user-writable location (new in v0.7.0).
        user_creds = config.s3_creds_path()
        if user_creds.exists() and user_creds.stat().st_size > 0:
            has_read = True
        # Fall back to the legacy in-tree location for users who
        # already have countries/.dvc/s3_creds from an editable install.
        elif cred_path:
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
        # Prefer the user-writable location (mirrors s3_creds);
        # fall back to the legacy in-tree .dvc/s3_write_creds for
        # editable installs that predate the user-config layout.
        user_write = config.s3_write_creds_path()
        if user_write.exists() and user_write.stat().st_size > 0:
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
    except (urllib.error.URLError, OSError, TimeoutError,
            json.JSONDecodeError):
        # network / HTTP / decode failure — treat as un-validated; programmer
        # bugs (TypeError, AttributeError) propagate.
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
    except (OSError, RuntimeError, ValueError) as exc:
        # python-gnupg wraps the gpg binary; runtime/IO/value errors are the
        # plausible failure modes.  Programmer bugs surface unchanged.
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
        except (OSError, subprocess.SubprocessError) as exc:
            # subprocess failure (binary missing, timeout, etc.); other
            # exceptions are programmer bugs and should propagate.
            logger.debug("gpg subprocess error: %s", exc)

    return None


def _sync_legacy_dvc_creds(dvc_dir: Path | None = None) -> bool:
    """Mirror user-config ``s3_creds`` into the in-tree ``.dvc/s3_creds``.

    Legacy wave scripts in ``lsms_library/countries/{country}/{wave}/_/*.py``
    call ``dvc.api.open(fn, mode='rb')`` directly.  These calls do not
    pass a ``credentialpath`` override, so DVC resolves the relative
    ``credentialpath = s3_creds`` from ``.dvc/config`` against the DVC
    directory itself --- i.e. ``lsms_library/countries/.dvc/s3_creds``.

    The v0.7.0 user-config migration moved credential storage to
    :func:`config.s3_creds_path` (``~/.config/lsms_library/s3_creds``)
    so the package can install into read-only site-packages, and
    :class:`DVCFileSystem` constructions in ``local_tools`` override
    ``credentialpath`` to point there.  But ``dvc.api.open()`` does
    not benefit from that override --- on a fresh clone, the in-tree
    ``s3_creds`` is missing (gitignored) and legacy scripts hit
    ``NoCredentialsError``.

    This helper bridges the gap by copying the user-config credentials
    into the in-tree ``.dvc/s3_creds`` location whenever the in-tree
    file is missing or stale.  Idempotent and safe to call repeatedly.

    Returns ``True`` if the in-tree mirror is in sync (either already
    matched or was just written), ``False`` otherwise (no source creds,
    write failed, or read-only DVC dir).
    """
    if dvc_dir is None:
        dvc_dir = _COUNTRIES_DIR / ".dvc"

    user_creds = config.s3_creds_path()
    if not (user_creds.exists() and user_creds.stat().st_size > 0):
        return False

    if not dvc_dir.exists():
        return False

    legacy_creds = dvc_dir / "s3_creds"
    try:
        user_text = user_creds.read_text()
    except OSError as exc:
        logger.debug("Could not read user-config s3_creds: %s", exc)
        return False

    # Skip the write if the legacy file already matches.
    if legacy_creds.exists():
        try:
            if legacy_creds.read_text() == user_text:
                return True
        except OSError:
            pass  # fall through to overwrite

    try:
        legacy_creds.write_text(user_text)
    except OSError as exc:
        logger.debug("Could not mirror s3_creds to %s: %s", legacy_creds, exc)
        return False

    logger.debug("Mirrored s3_creds to legacy in-tree path %s", legacy_creds)
    return True


def _auto_unlock_s3(dvc_dir: Path | None = None) -> bool:
    """Decrypt ``s3_reader_creds.gpg`` using the obfuscated passphrase.

    Called automatically when a valid WB API key is present.  The API
    key validation is the real access gate; the obfuscated passphrase
    just keeps the value from being trivially discoverable via grep.

    Always attempts to mirror the resulting creds into the legacy
    in-tree ``.dvc/s3_creds`` path (see :func:`_sync_legacy_dvc_creds`)
    so that legacy ``dvc.api.open()`` call sites still work.

    Returns ``True`` if ``s3_creds`` was written (or already exists).
    """
    if dvc_dir is None:
        dvc_dir = _COUNTRIES_DIR / ".dvc"

    creds_file = config.s3_creds_path()
    creds_file.parent.mkdir(parents=True, exist_ok=True)
    if creds_file.exists() and creds_file.stat().st_size > 0:
        # User-config creds already in place; still ensure the legacy
        # in-tree mirror is populated for `dvc.api.open()` callers.
        _sync_legacy_dvc_creds(dvc_dir)
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

    # Mirror into the legacy in-tree path so `dvc.api.open()` works.
    _sync_legacy_dvc_creds(dvc_dir)

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
    except (OSError, UnicodeDecodeError):
        # File missing or undecodable — treat as no-source-link.
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
    except (urllib.error.URLError, OSError, TimeoutError):
        # network / HTTP / timeout — caller treats None as "scrape failed".
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
    except (urllib.error.URLError, OSError, TimeoutError,
            json.JSONDecodeError) as e:
        # network / HTTP / timeout / JSON decode — caller handles None.
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
    except (urllib.error.URLError, OSError, TimeoutError,
            zipfile.BadZipFile) as e:
        # network / HTTP / disk / corrupt-zip; programmer bugs propagate.
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
    except (urllib.error.URLError, OSError, TimeoutError,
            zipfile.BadZipFile) as e:
        # network / HTTP / disk / corrupt-zip; programmer bugs propagate.
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

def _check_write_access(remote: str | None = None) -> bool:
    """Verify write access to at least one DVC remote.

    Returns ``True`` if credentials are available, ``False`` otherwise
    (with an error logged).
    """
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
    return True


def _default_remote(dvc_dir: Path) -> str | None:
    """Return the ``core.remote`` configured in ``.dvc/config``, or None."""
    cfg = configparser.ConfigParser()
    cfg.read(dvc_dir / "config")
    if cfg.has_option("core", "remote"):
        return cfg.get("core", "remote")
    return None


def _resolve_write_credentialpath(dvc_dir: Path) -> tuple[Path | None, bool]:
    """Resolve a credentials file usable for *writing* to S3.

    Mirrors the write-credential precedence documented on
    :func:`_check_remote_access`:

    1. ``s3_write_creds`` at :func:`config.s3_write_creds_path`
       (``~/.config/lsms_library/s3_write_creds``), then the legacy
       in-tree ``<dvc_dir>/s3_write_creds``;
    2. env vars — ``LSMS_S3_WRITE_KEY_ID`` / ``LSMS_S3_WRITE_KEY`` (the
       latter is the secret), or standard ``AWS_ACCESS_KEY_ID`` /
       ``AWS_SECRET_ACCESS_KEY`` — synthesized into a temp INI file.

    Returns ``(path, is_temp)``.  ``is_temp`` is True when a temporary
    file was written from env vars and the caller must delete it.
    Returns ``(None, False)`` when no write credentials are available.

    A boto/INI credentials file is needed (rather than relying on env
    vars at push time) because the DVC S3 remote pins ``credentialpath``
    to the *reader* creds, and an explicit credentialpath wins over the
    boto env-var chain — so a push silently authenticates as the reader
    unless we point credentialpath at the writer file.
    """
    user_write = config.s3_write_creds_path()
    if user_write.exists() and user_write.stat().st_size > 0:
        return user_write, False
    legacy = dvc_dir / "s3_write_creds"
    if legacy.exists() and legacy.stat().st_size > 0:
        return legacy, False

    key_id = (os.environ.get("LSMS_S3_WRITE_KEY_ID")
              or os.environ.get("AWS_ACCESS_KEY_ID"))
    secret = (os.environ.get("LSMS_S3_WRITE_KEY")
              or os.environ.get("AWS_SECRET_ACCESS_KEY"))
    if key_id and secret:
        tmp = tempfile.NamedTemporaryFile(
            "w", suffix=".ini", prefix="lsms_s3_write_", delete=False)
        tmp.write(f"[default]\naws_access_key_id = {key_id}\n"
                  f"aws_secret_access_key = {secret}\n")
        tmp.close()
        return Path(tmp.name), True

    return None, False


@contextmanager
def _s3_writer_credentialpath(remote: str | None, dvc_dir: Path):
    """Temporarily point an S3 remote's ``credentialpath`` at the writer creds.

    The DVC remote config pins ``credentialpath`` to the read-only
    ``s3_creds``; ``dvc push`` therefore authenticates as the reader and
    fails with ``AccessDenied`` even when writer credentials exist.  This
    context manager resolves the writer creds (see
    :func:`_resolve_write_credentialpath`) and applies them via a *local*
    config override (``.dvc/config.local``, git-ignored) for the duration
    of the block, snapshotting and fully restoring the prior state so the
    committed ``.dvc/config`` is never touched.

    No-op (yields normally) when the target remote is not an S3 remote or
    no writer credentials are available — in which case the push proceeds
    with whatever the existing config provides.
    """
    target = remote or _default_remote(dvc_dir)
    remotes = _parse_dvc_remotes(dvc_dir)
    cfg = remotes.get(target or "", {})
    is_s3 = cfg.get("url", "").startswith("s3://")

    write_path: Path | None = None
    is_temp = False
    if is_s3 and target:
        write_path, is_temp = _resolve_write_credentialpath(dvc_dir)

    if not write_path:
        yield
        return

    config_local = dvc_dir / "config.local"
    had_local = config_local.exists()
    prior = config_local.read_text(encoding="utf-8") if had_local else None
    try:
        _run_dvc_with_lock_retry(
            [_dvc_cmd(), "remote", "modify", "--local",
             target, "credentialpath", str(write_path)],
            cwd=str(dvc_dir.parent), timeout=60,
        )
        logger.info("Using S3 writer credentials for push to %r", target)
        yield
    finally:
        # Restore .dvc/config.local to its exact prior state.
        if prior is not None:
            config_local.write_text(prior, encoding="utf-8")
        elif config_local.exists():
            config_local.unlink()
        if is_temp:
            try:
                os.unlink(write_path)
            except OSError:
                pass


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

    if not _check_write_access(remote):
        return False

    try:
        if dvc_add:
            result = _run_dvc_with_lock_retry(
                [_dvc_cmd(), "add", str(abs_path)],
                cwd=str(_COUNTRIES_DIR), timeout=600,
            )
            if result.returncode != 0:
                logger.error("dvc add failed: %s", result.stderr.strip())
                return False
            logger.info("dvc add: %s", abs_path)

        push_cmd = [_dvc_cmd(), "push", str(abs_path) + ".dvc"]
        if remote:
            push_cmd.extend(["-r", remote])
        with _s3_writer_credentialpath(remote, _COUNTRIES_DIR / ".dvc"):
            result = _run_dvc_with_lock_retry(
                push_cmd,
                cwd=str(_COUNTRIES_DIR), timeout=600,
            )
        if result.returncode != 0:
            logger.error("dvc push failed: %s", result.stderr.strip())
            return False

        logger.info("Pushed to cache: %s", path)
        return True
    except (OSError, subprocess.SubprocessError) as exc:
        # subprocess invocation / timeout / dvc binary missing; programmer
        # bugs (TypeError, etc.) propagate.
        logger.error("push_to_cache error: %s", exc)
        return False


def push_to_cache_batch(paths: list[str | Path],
                        remote: str | None = None,
                        dvc_add: bool = True) -> list[Path]:
    """Push multiple local data files to a DVC remote in batch.

    Unlike :func:`push_to_cache`, this runs a single ``dvc add`` and a
    single ``dvc push`` for all files, which is dramatically faster.
    Follows the procedure in CONTRIBUTING.org steps 8--9:
    ``dvc add *.dta`` then ``dvc push``.

    Parameters
    ----------
    paths : list of str or Path
        Relative paths like ``Uganda/2013-14/Data/GSEC1.dta``.
        Interpreted relative to the countries directory.
    remote : str, optional
        DVC remote name to push to.  Defaults to the ``core.remote``
        configured in ``.dvc/config``.
    dvc_add : bool
        If True (default), run ``dvc add`` before pushing.  Set to
        False if the files are already DVC-tracked.

    Returns
    -------
    list[Path]
        Absolute paths of files that were successfully added and pushed.
    """
    if not paths:
        return []

    if not _check_write_access(remote):
        return []

    # Resolve and validate all paths
    abs_paths: list[Path] = []
    for p in paths:
        ap = _COUNTRIES_DIR / Path(p)
        if not ap.exists():
            logger.warning("Skipping non-existent file: %s", ap)
            continue
        abs_paths.append(ap)

    if not abs_paths:
        logger.error("No valid files to push.")
        return []

    try:
        # --- Batched dvc add -----------------------------------------------
        if dvc_add:
            add_cmd = [_dvc_cmd(), "add"] + [str(p) for p in abs_paths]
            logger.info("dvc add: %d files ...", len(abs_paths))
            result = _run_dvc_with_lock_retry(
                add_cmd,
                cwd=str(_COUNTRIES_DIR),
                timeout=600 + 30 * len(abs_paths),
            )
            if result.returncode != 0:
                logger.error("dvc add (batch) failed: %s",
                             result.stderr.strip())
                return []
            logger.info("dvc add: %d files done.", len(abs_paths))

        # --- Batched dvc push ----------------------------------------------
        dvc_files = [str(p) + ".dvc" for p in abs_paths]
        push_cmd = [_dvc_cmd(), "push"] + dvc_files
        if remote:
            push_cmd.extend(["-r", remote])
        logger.info("dvc push: %d files ...", len(abs_paths))
        with _s3_writer_credentialpath(remote, _COUNTRIES_DIR / ".dvc"):
            result = _run_dvc_with_lock_retry(
                push_cmd,
                cwd=str(_COUNTRIES_DIR),
                timeout=600 + 60 * len(abs_paths),
            )
        if result.returncode != 0:
            logger.error("dvc push (batch) failed: %s",
                         result.stderr.strip())
            return []
        logger.info("dvc push: %d files done.", len(abs_paths))

        return abs_paths
    except (OSError, subprocess.SubprocessError) as exc:
        # subprocess invocation / timeout / dvc binary missing; programmer
        # bugs (TypeError, etc.) propagate.
        logger.error("push_to_cache_batch error: %s", exc)
        return []


def unpushed_blobs(remote: str | None = None,
                   targets: list[str | Path] | None = None) -> list[str]:
    """Return DVC-tracked paths whose blobs are missing from the remote.

    Wraps ``dvc status --cloud`` (read-only; needs remote *read* access).
    Catches the "``dvc add``ed locally but never ``dvc push``ed" trap,
    where a ``.dvc`` pointer is committed but the blob never reached the
    shared cache — invisible until a build needs the file and fails with
    "No storage files available".

    Parameters
    ----------
    remote : str, optional
        Remote to compare against (defaults to ``core.remote``).
    targets : list of str or Path, optional
        Specific paths (relative to the countries dir, e.g.
        ``Uganda/2013-14/Data/foo.dta``) to check; defaults to the whole
        repository.

    Returns
    -------
    list[str]
        Sorted paths that are out of sync with the remote (need a push).
        Empty when everything tracked is already on the remote.
    """
    cmd = [_dvc_cmd(), "status", "--cloud", "--json"]
    if remote:
        cmd += ["-r", remote]
    if targets:
        cmd += [str(t) for t in targets]
    result = _run_dvc_with_lock_retry(
        cmd, cwd=str(_COUNTRIES_DIR), timeout=600,
    )
    out = (result.stdout or "").strip()
    if not out:
        return []
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        logger.warning("Could not parse `dvc status --cloud` output: %s",
                       (result.stderr or out)[:200])
        return []
    if isinstance(data, dict):
        return sorted(data.keys())
    return []


def populate_and_push(country: str, wave: str,
                      remote: str | None = None) -> list[Path]:
    """Download all data files for a wave from WB and push to DVC cache.

    Convenience wrapper that follows the procedure in CONTRIBUTING.org:
    downloads the full Stata zip via the WB NADA API, extracts all data
    files into the ``Data/`` directory, runs a single batched
    ``dvc add`` on all files, then a single batched ``dvc push``.

    Requires both ``wb_api`` read access and write access to a DVC
    remote.

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

    data_suffixes = {".dta", ".csv", ".sav"}
    data_files = sorted(
        f for f in data_dir.iterdir()
        if f.suffix.lower() in data_suffixes
    )
    if not data_files:
        logger.warning("No data files found in %s", data_dir)
        return []

    rel_paths = [f.relative_to(_COUNTRIES_DIR) for f in data_files]
    logger.info("Downloaded %d data files for %s/%s; adding to DVC ...",
                len(rel_paths), country, wave)

    return push_to_cache_batch(rel_paths, remote=remote, dvc_add=True)


# ---------------------------------------------------------------------------
# Wave discovery and setup
# ---------------------------------------------------------------------------

def _wb_catalog_search(country_code: str,
                       collection: str | None = "lsms",
                       ) -> list[dict]:
    """Query the WB Microdata Library catalog for a country.

    Returns a list of dicts with keys: ``id``, ``idno``, ``title``,
    ``year_start``, ``year_end``, ``url``, ``doi``, ``repository``.

    ``collection=None`` searches every collection, which is what provenance
    resolution needs: a wave we hold may sit outside the ``lsms`` collection.

    The ``doi`` field is what lets us resolve the many ``SOURCE.org`` files
    that record a ``https://doi.org/10.48529/…`` link instead of a
    ``/catalog/{id}`` URL -- the DOI does not encode the numeric id, but the
    catalog row carries both.
    """
    import json
    import urllib.request

    api_key = config.microdata_api_key()
    if not api_key:
        logger.error("No MICRODATA_API_KEY; cannot search WB catalog.")
        return []

    base = "https://microdata.worldbank.org/index.php/api/catalog/search"
    results: list[dict] = []
    page = 1
    page_size = 100

    while True:
        url = f"{base}?ps={page_size}&page={page}&country={country_code}"
        if collection:
            url += f"&collection={collection}"
        req = urllib.request.Request(url)
        req.add_header("X-API-KEY", api_key)
        req.add_header("Accept", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except (urllib.error.URLError, OSError, TimeoutError,
                json.JSONDecodeError) as exc:
            # network / HTTP / decode failure; programmer bugs propagate.
            logger.error("WB catalog search failed: %s", exc)
            return results

        result = data.get("result", {})
        rows = result.get("rows", []) or []
        for row in rows:
            sid = str(row.get("id", ""))
            results.append({
                "id": sid,
                "idno": row.get("idno", ""),
                "title": row.get("title", ""),
                "year_start": row.get("year_start", ""),
                "year_end": row.get("year_end", ""),
                "doi": row.get("doi", "") or "",
                "repository": row.get("repositoryid", "") or "",
                "url": (f"https://microdata.worldbank.org"
                        f"/index.php/catalog/{sid}"),
            })

        try:
            found = int(result.get("found", 0))
        except (TypeError, ValueError):
            found = len(results)
        if len(rows) < page_size or len(results) >= found:
            break
        page += 1

    return results


def _wb_catalog_search_many(country_code: str,
                            collections: Sequence[str],
                            ) -> list[dict]:
    """Search several WB collections and union the results.

    Deduplicated on the catalog ``id``: a study can be listed in more than one
    collection, and the same row must not be counted twice.  Order is preserved
    (first collection wins), so the ``lsms`` view of a study -- which is the one
    whose id our ``SOURCE.org`` files record -- takes precedence.

    Note this dedups on the catalog **id**, which does *not* catch a study that
    the WB has catalogued twice under two *different* ids in two repositories
    (Malawi IHS3 is both ``lsms`` 1003 and ``central`` 3016).  Nothing in the
    catalog metadata links those, so the defence against them is the per-country
    ``idno_pattern`` -- see the note above :class:`CountryCatalog`.
    """
    seen: set[str] = set()
    out: list[dict] = []
    for coll in collections:
        for row in _wb_catalog_search(country_code, coll):
            if row["id"] in seen:
                continue
            seen.add(row["id"])
            out.append(row)
    return out


def _local_waves(country: str) -> list[str]:
    """Return the list of wave directories that already exist locally."""
    country_dir = _COUNTRIES_DIR / country
    if not country_dir.is_dir():
        return []
    return sorted(
        d.name for d in country_dir.iterdir()
        if d.is_dir() and re.match(r"\d{4}", d.name)
    )


def _catalog_to_wave_label(entry: dict) -> str:
    """Convert a WB catalog entry to our wave label convention.

    E.g. year_start=2021, year_end=2022 -> ``"2021-22"``.
    """
    ys = str(entry.get("year_start", ""))
    ye = str(entry.get("year_end", ""))
    if ys and ye and ys != ye:
        return f"{ys}-{ye[-2:]}"
    return ys


def wave_provenance(country: str, wave: str) -> WaveProvenance:
    """Return the recorded provenance of one local wave directory.

    A wave with no ``SOURCE.org`` yields a record whose ``source`` is
    ``"unknown"`` -- never a guess.  See :mod:`lsms_library.provenance`.
    """
    return read_provenance(_COUNTRIES_DIR, country, wave)


def local_catalog_ids(country: str) -> dict[str, list[str]]:
    """Map each WB catalog id we hold locally -> the wave dirs holding it.

    One catalog entry can legitimately back **several** wave directories: WB
    id 1001 (``UGA_2005-2009_UNPS``) covers both ``Uganda/2005-06/`` and
    ``Uganda/2009-10/``.  Hence a list, not a scalar.
    """
    held: dict[str, list[str]] = {}
    for wave in _local_waves(country):
        prov = read_provenance(_COUNTRIES_DIR, country, wave)
        if prov.is_worldbank and prov.catalog_id:
            held.setdefault(prov.catalog_id, []).append(wave)
    return {k: sorted(v) for k, v in held.items()}


def discover_waves(country: str,
                   collection: str | None = None,
                   ) -> list[dict]:
    """Find WB catalog entries for *country*, flagging which we already hold.

    Matching is done on the **WB catalog id** recorded in each wave's
    ``Documentation/SOURCE.org`` (see :mod:`lsms_library.provenance`), not on
    a wave label reconstructed from the entry's year range.  Label matching
    was wrong in both directions:

    * *False positive* -- ``Nigeria/2018-19/`` holds GHS-Panel Wave 4 (id
      3557), but the Living Standards Survey (id 3827) also spans 2018-2019,
      so it too rendered as the label ``"2018-19"`` and was reported as
      already held.  It is not.
    * *False negative* -- WB id 1001 (``UGA_2005-2009_UNPS``) renders as
      ``"2005-10"``, which matches no directory, so it looked missing even
      though we hold it, split across ``2005-06/`` and ``2009-10/``.

    Each returned dict carries the WB catalog fields plus:

    ``wave``
        Our directory-name convention, derived from the entry's year range.
        Retained for display and for the label fallback below.
    ``local``
        ``bool`` -- ``True`` only when a local wave directory *records* this
        catalog id.  Unchanged in type and truthiness from previous releases,
        so existing callers keep working.
    ``local_status``
        ``"yes"`` / ``"no"`` / ``"unknown"`` -- the honest tri-state.
        ``"unknown"`` means a directory whose label matches this entry exists
        but has no recorded WB catalog id, so we cannot say whether it holds
        this study or a different one that happens to share a year range.
        These rows have ``local=False``: an unverified claim is treated as
        not-held, because wrongly believing we hold a survey is the failure
        mode that hides missing data.
    ``local_waves``
        The wave directories backing this entry (empty unless ``local``).

    Which WB *collections* are searched is per-country config: the
    ``repositories`` field of :class:`CountryCatalog`, defaulting to
    ``("lsms",)``.  Armenia adds ``central`` and South Africa adds ``datafirst``,
    where their living-standards series are actually published (GH #597).  The
    results are unioned and deduplicated on the catalog id.

    Parameters
    ----------
    country : str
        Country directory name, e.g. ``"Ethiopia"``.
    collection : str, optional
        Search this single collection instead of the country's configured
        ``repositories``.  Escape hatch for exploration; leave it ``None``
        (the default) to get the configured behaviour.

    Returns
    -------
    list[dict]
        Catalog entries sorted by year, annotated with local status.  Empty
        for countries whose data does not come from the WB catalog at all
        (e.g. ``EthiopiaRHS``); those log an explanatory message.
    """
    spec = _COUNTRY_CATALOG.get(country)
    if spec is None:
        logger.error("No WB catalog mapping for %r. Add it to "
                     "_COUNTRY_CATALOG in data_access.py.", country)
        return []
    if not spec.discoverable or not spec.code:
        logger.info("%s is not discoverable via the WB catalog: %s",
                    country, spec.reason or "not a World Bank dataset.")
        return []

    collections = (collection,) if collection else spec.repositories
    entries = [e for e in _wb_catalog_search_many(spec.code, collections)
               if spec.matches(e)]

    held = local_catalog_ids(country)
    # Wave dirs whose WB catalog id we do NOT know: either no SOURCE.org, or
    # one that records a non-WB source.  These are the only dirs for which we
    # fall back to the old label heuristic -- and we mark the result unknown.
    unresolved = {
        w for w in _local_waves(country)
        if not read_provenance(_COUNTRIES_DIR, country, w).is_worldbank
    }

    for e in entries:
        e["wave"] = _catalog_to_wave_label(e)
        if e["id"] in held:
            e["local"] = True
            e["local_status"] = "yes"
            e["local_waves"] = held[e["id"]]
        elif e["wave"] in unresolved:
            # A directory with this label exists, but nothing records what it
            # actually holds.  Do not claim either way.
            e["local"] = False
            e["local_status"] = "unknown"
            e["local_waves"] = [e["wave"]]
        else:
            e["local"] = False
            e["local_status"] = "no"
            e["local_waves"] = []

    return sorted(entries, key=lambda e: str(e.get("year_start", "")))


def _get_console(verbose: bool):
    """Return a ``rich.console.Console`` if *verbose*, else ``None``."""
    if not verbose:
        return None
    try:
        from rich.console import Console
        return Console()
    except ImportError:
        return None


def add_wave(country: str, catalog_id: str,
             wave: str | None = None,
             confirm: bool = True,
             push: bool = True,
             remote: str | None = None,
             verbose: bool = True) -> list[Path]:
    """Download a new survey wave from the WB and register it locally.

    Creates the directory structure (``Data/``, ``Documentation/``,
    ``_/``), writes ``SOURCE.org``, downloads the Stata zip, extracts
    all data files, and optionally pushes to the DVC remote.

    Parameters
    ----------
    country : str
        Country directory name, e.g. ``"Ethiopia"``.
    catalog_id : str
        WB Microdata Library catalog ID (the numeric id or the full
        URL).
    wave : str, optional
        Wave label for the directory name, e.g. ``"2021-22"``.  If
        omitted, derived from the catalog entry's year range.
    confirm : bool
        If True (default), print a summary and ask for user
        confirmation before downloading.
    push : bool
        If True (default), run ``dvc add`` + ``dvc push`` after
        extracting the data files.
    remote : str, optional
        DVC remote to push to.
    verbose : bool
        If True (default), show progress spinners and a summary table.
        Requires ``rich`` (degrades to plain logging if unavailable).

    Returns
    -------
    list[Path]
        Paths of data files that were downloaded (and pushed, if
        *push* is True).
    """
    import time

    con = _get_console(verbose)

    def status(msg):
        """Context manager: rich spinner when verbose, no-op otherwise."""
        if con is not None:
            return con.status(msg, spinner="dots")
        import contextlib
        return contextlib.nullcontext()

    def log(msg, style=""):
        if con is not None:
            con.print(f"  {msg}", style=style)
        else:
            logger.info(msg)

    # --- Resolve catalog ID -------------------------------------------------
    m = re.search(r'(\d+)', str(catalog_id))
    if not m:
        logger.error("Cannot parse catalog ID from %r", catalog_id)
        return []
    cat_id = m.group(1)
    catalog_url = (f"https://microdata.worldbank.org"
                   f"/index.php/catalog/{cat_id}")

    with status(f"Resolving catalog {cat_id} ..."):
        idno = _get_catalog_idno(catalog_url)
    if not idno:
        logger.error("Could not resolve IDNO for catalog %s", cat_id)
        return []
    log(f"IDNO: [bold]{idno}[/bold]")

    # --- Look up the catalog entry (wave label + provenance metadata) --------
    entry: dict | None = None
    spec = _COUNTRY_CATALOG.get(country)
    if spec is not None and spec.code:
        with status("Looking up catalog entry ..."):
            for e in _wb_catalog_search(spec.code, collection=None):
                if str(e["id"]) == cat_id:
                    entry = e
                    break

    if wave is None:
        if entry is not None:
            wave = _catalog_to_wave_label(entry)
        if not wave:
            logger.error("Cannot derive wave label; pass wave= explicitly.")
            return []

    wave_dir = _COUNTRIES_DIR / country / wave

    # --- Confirmation -------------------------------------------------------
    if confirm:
        exists = wave_dir.exists()
        status_note = " (directory already exists)" if exists else ""
        if con is not None:
            from rich.panel import Panel
            from rich.text import Text
            lines = Text()
            lines.append(f"  Country:    {country}\n")
            lines.append(f"  Wave:       {wave}{status_note}\n")
            lines.append(f"  Catalog:    {catalog_url}\n")
            lines.append(f"  IDNO:       {idno}\n")
            lines.append(f"  Push to S3: {'yes' if push else 'no'}")
            con.print(Panel(lines, title="Add Wave", border_style="blue"))
        else:
            print(f"\n  Country:    {country}")
            print(f"  Wave:       {wave}{status_note}")
            print(f"  Catalog:    {catalog_url}")
            print(f"  IDNO:       {idno}")
            print(f"  Push to S3: {'yes' if push else 'no'}")
        resp = input("\n  Proceed? [y/N] ").strip().lower()
        if resp not in ("y", "yes"):
            log("Aborted.", style="yellow")
            return []

    t0 = time.time()

    # --- Create directory structure -----------------------------------------
    (wave_dir / "Data").mkdir(parents=True, exist_ok=True)
    (wave_dir / "Documentation").mkdir(parents=True, exist_ok=True)
    (wave_dir / "_").mkdir(parents=True, exist_ok=True)

    # Record provenance: the WB catalog *id* this directory came from, so
    # discover_waves() can match on identity instead of guessing from a
    # year-derived label.  Always (re)written -- add_wave knows the id for a
    # fact, and an existing bare-URL SOURCE.org carries strictly less
    # information than the record we can write here.
    years = None
    if entry is not None:
        ys, ye = entry.get("year_start"), entry.get("year_end")
        years = f"{ys}-{ye}" if ys and ye else (str(ys) if ys else None)

    write_provenance(_COUNTRIES_DIR, WaveProvenance(
        country=country,
        wave=wave,
        source=SOURCE_WORLDBANK,
        catalog_id=cat_id,
        idno=idno,
        title=(entry or {}).get("title") or None,
        years=years,
        repository=(entry or {}).get("repository") or None,
        url=catalog_url,
        method="add-wave",
    ))
    log(f"Created {country}/{wave}/ directory structure")

    # --- Download -----------------------------------------------------------
    with status(f"Downloading Stata zip from World Bank ..."):
        dummy = f"{country}/{wave}/Data/_probe_.dta"
        get_data_file(dummy, populate_cache=True)

    data_dir = wave_dir / "Data"
    data_suffixes = {".dta", ".csv", ".sav"}
    data_files = sorted(
        f for f in data_dir.iterdir()
        if f.suffix.lower() in data_suffixes
    ) if data_dir.exists() else []

    if not data_files:
        logger.warning("No data files found in %s", data_dir)
        return []

    total_bytes = sum(f.stat().st_size for f in data_files)
    log(f"Extracted {len(data_files)} files "
        f"({total_bytes / 1e6:.1f} MB)")

    # --- DVC add + push -----------------------------------------------------
    if push:
        rel_paths = [f.relative_to(_COUNTRIES_DIR) for f in data_files]

        with status(f"dvc add ({len(data_files)} files) ..."):
            pushed = push_to_cache_batch(
                rel_paths, remote=remote, dvc_add=True)

        elapsed = time.time() - t0

        if pushed:
            log(f"Pushed {len(pushed)} files to S3 "
                f"in {elapsed:.0f}s", style="bold green")
        else:
            log("Push failed -- see log for details", style="bold red")
        result = pushed
    else:
        elapsed = time.time() - t0
        log(f"Downloaded {len(data_files)} files "
            f"(push=False) in {elapsed:.0f}s")
        result = data_files

    # --- Summary table ------------------------------------------------------
    if con is not None and result:
        from rich.table import Table
        tbl = Table(title=f"{country} {wave}", show_lines=False)
        tbl.add_column("File", style="cyan")
        tbl.add_column("Size", justify="right")
        for f in result:
            size = f.stat().st_size
            if size > 1e6:
                sz = f"{size / 1e6:.1f} MB"
            else:
                sz = f"{size / 1e3:.0f} KB"
            tbl.add_row(f.name, sz)
        tbl.add_section()
        tbl.add_row(f"{len(result)} files",
                    f"{total_bytes / 1e6:.1f} MB total")
        con.print(tbl)

    return result


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
            # Reuse the module-level DVCFS singleton from local_tools
            # rather than constructing a fresh DVCFileSystem here.
            # Same root, same config; the singleton avoids paying the
            # ~0.5-2s DVC handle construction cost on every WB-API
            # fallback fetch.  See slurm_logs/DESIGN_dvc_layer1_caching.md
            # ("Hot spot 2") for the full rationale.
            from .local_tools import DVCFS as fs
            dvc_path = str(path)
            if fs.exists(dvc_path):
                abs_path.parent.mkdir(parents=True, exist_ok=True)
                fs.get_file(dvc_path, str(abs_path))
                logger.info("Fetched from DVC: %s", path)
                return abs_path
        except (OSError, ValueError, KeyError, ImportError) as e:
            # DVC handle / network / config-key issues fall back to WB
            # download.  Programmer bugs (TypeError, AttributeError) propagate.
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

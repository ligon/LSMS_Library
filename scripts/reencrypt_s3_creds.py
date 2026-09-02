#!/usr/bin/env python3
"""Regenerate ``s3_reader_creds.enc`` from ``s3_reader_creds.gpg`` (GH #741).

The ``.enc`` file shipped in the wheel is a GENERATED artifact.  This is its
generator, kept next to the other promote/backfill scripts so that credential
rotation does not become archaeology.

Why a second encrypted copy exists at all
-----------------------------------------
``python-gnupg`` is a *wrapper* around the ``gpg(1)`` binary, and that binary is
not pip-installable.  On Windows it needs a separate Gpg4win install; on macOS
it is absent by default.  So a user with a perfectly valid World Bank API key
-- which is the library's actual access gate -- could not unlock the S3 read
cache, and got ``NoCredentialsError`` from botocore three layers down with no
hint that gpg was the missing piece.  ``cryptography`` is pure Python and is
already in the dependency tree, so the ``.enc`` copy removes the binary
dependency from the API-key path entirely.

The security model is UNCHANGED, and it is worth being exact about what that
means: the Fernet key is derived from the same passphrase the GPG file always
used, and that passphrase is base64-obfuscated in source.  Cosmetic anti-grep,
never a security gate -- exactly as ``CLAUDE.md`` "Three-Tier Credential Model"
describes.  The World Bank API key check is the authoritative policy, and these
are *read-only* cache credentials for data the WB distributes.  Do not "improve"
the obfuscation here: making it look stronger than it is would be worse than
leaving it plainly cosmetic.

Usage
-----
Requires the ``gpg`` binary (to read the existing file) and ``cryptography``::

    python scripts/reencrypt_s3_creds.py            # write the .enc
    python scripts/reencrypt_s3_creds.py --check    # verify, write nothing

Neither mode prints decrypted content.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DVC_DIR = REPO / "lsms_library" / "countries" / ".dvc"
GPG_FILE = DVC_DIR / "s3_reader_creds.gpg"
ENC_FILE = DVC_DIR / "s3_reader_creds.enc"

# Same obfuscated passphrase the GPG file uses.  See module docstring.
_S3_UNLOCK_PASSPHRASE = "QnVubnkgbXVmZmlu"


def derive_fernet_key(passphrase: str) -> bytes:
    """Derive the Fernet key from the shared passphrase.

    Unsalted and deterministic ON PURPOSE.  This is not a KDF protecting a
    secret -- it is the same cosmetic obfuscation as before, in a form
    ``cryptography`` can open.  It must stay byte-for-byte identical to
    ``lsms_library.data_access._derive_fernet_key``; a test pins that.
    """
    return base64.urlsafe_b64encode(hashlib.sha256(passphrase.encode()).digest())


def gpg_plaintext(passphrase: str) -> str:
    """Decrypt the .gpg file.  Returns plaintext; never prints it."""
    if not GPG_FILE.exists():
        sys.exit(f"missing {GPG_FILE}")
    for extra in ([], ["--pinentry-mode", "loopback"]):
        proc = subprocess.run(
            ["gpg", "--batch", "--yes", "--passphrase-fd", "0", "--decrypt",
             str(GPG_FILE)] + extra,
            input=passphrase.encode(), capture_output=True, timeout=60,
        )
        if proc.returncode == 0:
            return proc.stdout.decode()
    sys.exit("gpg decryption failed -- is the gpg binary installed, and the "
             "passphrase still correct?")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true",
                    help="verify the .enc matches the .gpg; write nothing")
    args = ap.parse_args()

    from cryptography.fernet import Fernet

    passphrase = base64.b64decode(_S3_UNLOCK_PASSPHRASE).decode()
    key = derive_fernet_key(passphrase)
    plain = gpg_plaintext(passphrase)

    # Sanity: it must look like an AWS credentials INI, or we are about to ship
    # a correctly-encrypted blob of the wrong thing.
    if "aws_access_key_id" not in plain or "[" not in plain:
        sys.exit("decrypted content does not look like AWS credentials; refusing")

    if args.check:
        if not ENC_FILE.exists():
            print(f"MISSING: {ENC_FILE.relative_to(REPO)}")
            return 1
        rt = Fernet(key).decrypt(ENC_FILE.read_bytes()).decode()
        ok = rt == plain
        print(f"{'OK' if ok else 'MISMATCH'}: .enc round-trips to the same "
              f"plaintext as .gpg ({len(plain)} bytes)")
        return 0 if ok else 1

    ENC_FILE.write_bytes(Fernet(key).encrypt(plain.encode()))
    # Prove the artifact we just wrote is readable before anyone ships it.
    assert Fernet(key).decrypt(ENC_FILE.read_bytes()).decode() == plain
    print(f"wrote {ENC_FILE.relative_to(REPO)} "
          f"({ENC_FILE.stat().st_size} bytes, round-trip verified)")
    print("remember: `git add -f` it (.gitignore has **/.dvc/) and keep it in "
          "the [tool.poetry] include list, or pip users never receive it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

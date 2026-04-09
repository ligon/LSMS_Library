#!/usr/bin/env python3
import git
from pathlib import Path
import getpass
import pkgutil
import gnupg

def is_git_repo(path='.'):
    """
    Check if the specified path is a Git repository.

    Parameters:
    path (str): The path to check. Default is the current directory.

    Returns:
    bool: True if the path is a valid Git repository, False otherwise.
    """
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False

def authenticate(gpg_key_file='s3_reader_creds.gpg', max_attempts: int = 3,
                 passphrase: str | None = None):
    """
    Decrypt the specified GPG file and store the decrypted credentials securely.

    When *passphrase* is supplied the decryption is attempted once with that
    value and no interactive prompt is shown.  When *passphrase* is ``None``
    (the default) the user is prompted on the TTY up to *max_attempts* times.

    Parameters:
    gpg_key_file (str): The name of the GPG file containing the encrypted credentials.
                        Default is 's3_reader_creds.gpg'.
    max_attempts (int): Maximum interactive passphrase attempts (ignored when
                        *passphrase* is provided).
    passphrase (str | None): If given, use this passphrase instead of prompting.

    Raises:
    ValueError: If decryption fails due to an incorrect passphrase or other issues.
    """
    # Construct the path to the encrypted file relative to this function's location
    gpg_path = Path(__file__).resolve().parent / 'countries' / '.dvc'
    encrypted_file = gpg_path / gpg_key_file

    # Load the encrypted data
    encrypted_data = encrypted_file.read_bytes()

    # Initialize GPG
    gpg = gnupg.GPG()

    def _write_creds(decrypted_data, interactive: bool = True):
        """Write decrypted credentials to disk."""
        creds_file = gpg_path / 's3_creds'

        if creds_file.exists() and interactive:
            user_input = input(f"The file {creds_file} already exists. Overwrite? (yes/no): ").strip().lower()
            if user_input not in ['yes', 'y']:
                print("Operation aborted. Credentials were not written.")
                return

        with open(creds_file, 'w') as f:
            f.write(str(decrypted_data))
        if interactive:
            print("*** Decryption successful; LSMS_Library can now stream data. ***")
            print()

    # Non-interactive path: single attempt with the supplied passphrase
    if passphrase is not None:
        decrypted_data = gpg.decrypt(encrypted_data, passphrase=passphrase)
        if decrypted_data.ok:
            _write_creds(decrypted_data, interactive=False)
            return
        raise ValueError("Decryption failed: incorrect passphrase.")

    # Interactive path: prompt on TTY
    print(
        "\n*** LSMS_Library DVC authentication ***\n"
        "Contact ligon@berkeley.edu to request access credentials.\n"
        "Enter your passphrase to unlock the survey data stream.\n"
    )

    for attempt in range(1, max_attempts + 1):
        pp = getpass.getpass(prompt='Enter passphrase for decryption: ')
        decrypted_data = gpg.decrypt(encrypted_data, passphrase=pp)

        if decrypted_data.ok:
            _write_creds(decrypted_data, interactive=True)
            return

        remaining = max_attempts - attempt
        if remaining > 0:
            print("Decryption failed: incorrect passphrase. Please try again.")
        else:
            raise ValueError("Decryption failed after multiple attempts; check the passphrase and try again later.")

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

def authenticate(gpg_key_file='s3_reader_creds.gpg'):
    """
    Decrypt the specified GPG file and store the decrypted credentials securely.

    This function prompts the user for a passphrase to decrypt the provided GPG
    file.

    Parameters:
    gpg_key_file (str): The name of the GPG file containing the encrypted credentials.
                        Default is 's3_reader_creds.gpg'.

    Raises:
    ValueError: If decryption fails due to an incorrect passphrase or other issues.
    """
    # Construct the path to the encrypted file relative to this function’s location
    gpg_path = Path(__file__).resolve().parent / 'countries' / '.dvc'
    encrypted_file = gpg_path / gpg_key_file

    # Load the encrypted data
    encrypted_data = encrypted_file.read_bytes()

    # Initialize GPG
    gpg = gnupg.GPG()

    # Prompt the user for the passphrase securely
    passphrase = getpass.getpass(prompt='Enter passphrase for decryption: ')

    # Decrypt the data
    decrypted_data = gpg.decrypt(encrypted_data, passphrase=passphrase)

    if decrypted_data.ok:
        # Write the decrypted data to a temporary file securely
        creds_file = gpg_path / 's3_creds'

        if creds_file.exists():
            user_input = input(f"The file {creds_file} already exists. Overwrite? (yes/no): ").strip().lower()
            if user_input not in ['yes', 'y']:
                print("Operation aborted. Credentials were not written.")
                return

        with open(creds_file, 'w') as f:
            f.write(str(decrypted_data))
        print(f"Decryption successful; credentials written to {creds_file}")
    else:
        raise ValueError("Decryption failed: check the passphrase and try again")

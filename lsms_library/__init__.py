from pathlib import Path
import os
import warnings

from . import country
from .country import Country, Wave
from . import local_tools as tools
from .categorical_mapping import ai_agent
from . import transformations
from .dvc_permissions import authenticate

gpg_path = Path(__file__).resolve().parent / 'countries' / '.dvc'
creds_file = gpg_path / 's3_creds'

SKIP_AUTH = os.getenv("LSMS_SKIP_AUTH", "").lower() in {"1", "true", "yes"}

if not SKIP_AUTH and not creds_file.exists():
    try:
        authenticate()
    except Exception as exc:
        warnings.warn(
            f"Automatic DVC authentication failed: {exc}. "
            "Set LSMS_SKIP_AUTH=1 to suppress this in non-interactive environments."
        )

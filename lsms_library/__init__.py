from pathlib import Path
import os
import warnings

import logging
from sys import stderr
from . import country
from .country import Country, Wave
from . import local_tools as tools
from .categorical_mapping import ai_agent
from . import transformations
from .dvc_permissions import authenticate
try:
    from dvc.ui import ui as dvc_ui
    from dvc.logger import LoggerHandler, setup as dvc_log_setup
    from functools import wraps
    import sys

    if hasattr(dvc_ui, "rich_console"):
        try:
            dvc_ui.rich_console.file = stderr
        except Exception:
            pass
    if hasattr(dvc_ui, "error_console"):
        try:
            dvc_ui.error_console.file = stderr
        except Exception:
            pass
    original_init = LoggerHandler.__init__

    @wraps(original_init)
    def _patched_logger_handler_init(self, stream, *args, **kwargs):
        if stream is sys.stdout:
            stream = stderr
        original_init(self, stream, *args, **kwargs)

    LoggerHandler.__init__ = _patched_logger_handler_init  # type: ignore[assignment]

    dvc_log_setup()
    for logger_name in ("dvc", "dvc_objects", "dvc_data", ""):
        logger_obj = logging.getLogger(logger_name)
        for handler in logger_obj.handlers:
            if isinstance(handler, LoggerHandler) and getattr(handler, "stream", None) is not stderr:
                handler.stream = stderr
except Exception:
    # DVC optional; if missing, proceed without UI tweaks
    pass

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

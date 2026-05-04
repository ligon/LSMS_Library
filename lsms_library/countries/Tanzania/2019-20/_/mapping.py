"""Formatting functions for Tanzania 2019-20."""
from lsms_library.local_tools import format_id


def v(x):
    """Ensure cluster ID is a clean string (strip .0 from float)."""
    return format_id(x)

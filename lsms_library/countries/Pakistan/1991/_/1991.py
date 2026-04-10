from lsms_library.local_tools import format_id


def v(value):
    """Format cluster id."""
    return format_id(value)


def strata(value):
    """Format strata as string."""
    return format_id(value)

from lsms_library.local_tools import format_id


def i(value):
    """Format composite household id from popkrug + naselje + dom."""
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))

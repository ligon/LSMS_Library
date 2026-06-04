# Formatting functions for Albania 2008.
import lsms_library.local_tools as tools


def i(value):
    """Household id for 2008: PSU - HH-within-PSU.

    Matches the canonical household identity built by this wave's
    sample.py (``format_id(psu) + '-' + format_id(hh)``), so the
    framework's _join_v_from_sample() resolves ``v`` correctly.
    """
    return tools.format_id(value.iloc[0]) + '-' + tools.format_id(value.iloc[1])

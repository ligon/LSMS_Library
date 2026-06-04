# Formatting functions for Albania 2012.
import lsms_library.local_tools as tools


def i(value):
    """Household id for 2012: hhid = psu*100 + hh.

    The education file (Modul_2B_education.sav) carries psu and hh but not
    the globally-unique hhid that this wave's sample.py uses as ``i``
    (built from poverty.sav where hhid == psu*100 + hh).  Reconstruct it
    so the framework's _join_v_from_sample() resolves ``v`` correctly.
    """
    return tools.format_id(int(value.iloc[0]) * 100 + int(value.iloc[1]))

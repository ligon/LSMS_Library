"""Peru (ENNIV) country-level formatting helpers.

EXPERIMENTAL -- written by the #684 ground-truth red-team run to test what
the config does once it is reachable.  Not a proposed patch.
"""
from lsms_library import local_tools as tools


def i(value):
    """Composite household id.

    ENNIV 1994's data dictionary (``1994/Documentation/dicciona.pdf``, "COMMON
    items") defines the household key as three fixed-width fields present on
    every REG record: SEGMENTO (3 digits), VIVIENDA (2), HOGAR (2).
    """
    parts = [tools.format_id(value.iloc[0], zeropadding=3),
             tools.format_id(value.iloc[1], zeropadding=2),
             tools.format_id(value.iloc[2], zeropadding=2)]
    if any(p is None for p in parts):
        return None
    return ''.join(parts)

# Formatting Functions for Nigeria GHS-Panel 2012-13 (W2)
#
# Every callable defined or imported here becomes a formatting function for
# BOTH W2 quarters (2012Q3, 2013Q1); keep this module to the hook below.
import re

import pandas as pd

#: The W2 post-planting quarter, the only rows the hook below touches.
_PP_QUARTER = '2012Q3'
_NUMERIC_CODE = re.compile(r'\s*[-+]?\d+(\.\d+)?\s*')


def _unlabelled_code(x):
    """True for a raw numeric code: a number, or a string that is only one.

    With convert_categoricals=True a LABELLED answer arrives as its label
    text; a code the file's value labels do not define arrives as the bare
    number (4.0).  A label that harmonize_education does not map is still
    text, and is not touched.
    """
    if isinstance(x, bool) or x is None or x is pd.NA:
        return False
    if isinstance(x, (int, float)):
        return x == x                       # NaN is missing, not a code
    if isinstance(x, str):
        return bool(_NUMERIC_CODE.fullmatch(x))
    try:
        return bool(pd.notna(x)) and float(x) == float(x)
    except (TypeError, ValueError):
        return False


def individual_education(df):
    """W2 post-planting education: an unlabelled code is 'Unknown'.

    Ruling @ligon 2026-11-13 ("Serve 'Unknown'", red team delta D4).
    sect2_plantingw2 s2q8 holds three answers (codes 4, 19, 20, one member
    each) that its own value labels do not define; they are served as the
    canonical sentinel 'Unknown' instead of "4.0" / "19.0" / "20.0".  Only
    the 2012Q3 rows; a labelled answer harmonize_education does not map still
    passes through as recorded (GH #1292), and the post-harvest rows are
    untouched.  See Nigeria/_/CONTENTS.org (GH #1713).
    """
    col = 'Educational Attainment'
    if col not in df.columns or 't' not in df.index.names:
        return df
    pp = (df.index.get_level_values('t').astype(str) == _PP_QUARTER)
    values = df[col].astype(object)
    hit = pp & values.map(_unlabelled_code).to_numpy(dtype=bool)
    if hit.any():
        df = df.copy()
        df[col] = values.where(~hit, 'Unknown')
    return df

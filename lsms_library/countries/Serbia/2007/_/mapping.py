import pandas as pd

from lsms_library.local_tools import format_id


def _composite_id(value):
    """Join a row of zero-padded numeric id parts into `a-b-c`.

    Each part is normalized through ``int`` so the leading zeros the Stata
    files carry (``popkrug`` is ``'0001'``) don't leak into the key, and a
    missing part yields ``None`` rather than a ``ValueError`` from ``int(nan)``.
    """
    parts = []
    for k in range(len(value)):
        x = value.iloc[k]
        if pd.isna(x):
            return None
        parts.append(str(int(x)))
    return format_id('-'.join(parts))


def i(value):
    """Composite household id from opstina + popkrug + dom."""
    return _composite_id(value)


def v(value):
    """Composite settlement (census-district) id from opstina + popkrug.

    GH #323.  `popkrug` alone is NOT a cluster id: it is a serial number
    LOCAL TO A MUNICIPALITY ('0001', '0002', ...), so the same popkrug recurs
    in many opstina.  In enumeration_district.dta, 510 distinct census
    districts carry only 328 distinct popkrug -- 182 collisions.  Keying `v`
    on popkrug alone made `_normalize_dataframe_index` collapse the (t, v)
    index with groupby().first(), silently discarding 182 of the 510 clusters
    and mis-attributing 1,823 households (33%) to the wrong Region and 1,015
    to the wrong Rural class.  (opstina, popkrug) is unique: 510/510.

    Deliberately the same normalization as `i`, so `v` is a strict prefix of
    the household id: i = '{opstina}-{popkrug}-{dom}', v = '{opstina}-{popkrug}'.
    """
    return _composite_id(value)


def to_float(value):
    """Coerce a scalar to float (s30 Value loads as object dtype)."""
    return pd.to_numeric(value, errors='coerce')


def Int_t(value):
    """Build interview date from (dana, mesa, goda) = (day, month, year)."""
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    try:
        return pd.Timestamp(year=int(y), month=int(m), day=int(d))
    except (ValueError, TypeError):
        return pd.NaT

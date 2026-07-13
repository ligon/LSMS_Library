# Formatting functions for Liberia 2018-19
import pandas as pd
import numpy as np

from lsms_library.local_tools import format_id


def v(value):
    '''Canonical cluster (enumeration-area) id: `ea_unique`, as a string.

    Exists solely to defeat the "format_id is applied to idxvars but NOT to
    myvars" gotcha (CLAUDE.md).  `sample` carries `v` as a *myvar*, while
    `cluster_features` carries it as an *idxvar* and therefore gets the
    builtin `format_id` for free.  `ea_unique` is float64 in the source, so
    without this the myvar side would emit raw floats / '302003132.0' while
    the idxvar side emits '302003132' -- and `_join_v_from_sample()` would
    match nothing, silently giving every Liberia household NaN cluster
    attributes.  Both sides must produce the identical string.

    (Named `v` so `map_formatting_function` auto-applies it to the `v` myvar.
    The `cluster_features` *idxvar* path is unaffected: for a plain-string
    idxvar the framework deliberately prefers the builtin `format_id`, which
    is what this delegates to, so the two agree by construction.)
    '''
    return format_id(value)


def cluster_features(df):
    '''Reduce household-grain rows to the canonical (t, v) cluster grain.

    GH #323.  `sect1_public.dta` is one row per HOUSEHOLD (2,986), but
    `cluster_features` is one row per CLUSTER (250 enumeration areas), so a
    reduction is unavoidable.  It is performed HERE, explicitly and guarded,
    rather than being left to the silent `.first()` collapses downstream --
    `Wave.cluster_features()` (country.py, GH #161) and
    `_normalize_dataframe_index()` (GH #323) -- BOTH of which would otherwise
    pick an arbitrary row per group and discard the rest without a word.

    Those collapses are justified in-code by the assumption that cluster
    attributes are "invariant within a cluster by construction of the LSMS-ISA
    sampling design".  That assumption is exactly what this wave violated: `v`
    used to be wired to `ea_code`, an EA *serial number* unique only within
    (county, district, clan), so 32 buckets stood in for 250 real EAs and one
    bucket alone (`ea_code` 12) spanned all 14 counties.  `.first()` then
    stamped every one of its 621 households with `county='bong'`.

    So we do not merely assume the invariant -- we ASSERT it, and fail LOUDLY
    if a future wave breaks it, instead of silently shipping an arbitrary
    value.  That silent-arbitrary-value behaviour is the bug being fixed; a
    guard that only documented the assumption would be prose, not enforcement.
    '''
    attrs = [c for c in df.columns if c != 'i']
    flat = df.reset_index()
    keys = [k for k in ('t', 'v') if k in flat.columns]

    # GUARD: every declared attribute must be constant within (t, v).
    varying = {
        a: int((flat.groupby(keys, observed=True)[a].nunique(dropna=False) > 1).sum())
        for a in attrs
    }
    offenders = {a: n for a, n in varying.items() if n}
    if offenders:
        raise ValueError(
            f"Liberia cluster_features: {offenders} -- cluster attribute(s) are "
            f"NOT constant within {tuple(keys)}, so collapsing to one row per "
            f"cluster would silently discard real variation (GH #323). `v` is "
            f"probably wired to a non-identifying column again; it must be "
            f"`ea_unique` (250 real EAs), NOT `ea_code` (32 serial numbers)."
        )

    # Attributes are provably constant within the cluster, so the household
    # level carries no information here: drop it and de-duplicate.  This is a
    # lossless projection, not an aggregation -- the rows are exact duplicates.
    out = flat[keys + attrs].drop_duplicates(subset=keys)
    return out.set_index(keys)


def Age(value):
    '''
    Coerce age to numeric; non-numeric values (e.g. "don't know") become NaN.
    '''
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan


def shocks(df):
    '''Keep only experienced shocks; drop the redundant Experienced flag.

    NHFS Section 17 enumerates all 14 shock types for every household
    (S17_1 = "severely negatively affected in the past 12 months", yes/no),
    producing a full household x shock-type cross-product (~40k rows, mostly
    not-experienced placeholders).  In the canonical (t, i, Shock) table a row
    exists only for a shock the household actually experienced, so filter to
    Experienced == True and drop the column: its information is carried by the
    row's existence, it would otherwise be a constant-True column, and as a
    bool it is silently nulled on the cached-read path (GH #386) -- which is
    what made this wave's row count collapse from cache.
    '''
    df = df[df['Experienced'] == True]
    return df.drop(columns='Experienced')

"""Guyana 1992 formatting functions and per-table df_edit hooks.

Identity (GH #323).  The household key is the THREE-level (ED, SN, HH) and the
sampling cluster is the TWO-level (ED, SN); SN is the ED sample-segment serial.
See ``data_info.yml`` for the evidence.  ``i()`` and ``v()`` below hyphen-join
those parts into the canonical "ED-SN-HH" / "ED-SN" string ids.

A module-level function whose name matches a declared ``data_scheme`` table is
dispatched by the framework as that table's ``df_edit`` hook -- it receives the
grabbed-and-indexed frame before ``_normalize_dataframe_index`` sees it.  The
three hooks below exist so that every duplicate in this wave is resolved by an
EXPLICIT, declared policy.  Anything still non-unique at normalize time gets
collapsed with a silent ``groupby().first()``, which is precisely the GH #323
data loss; these hooks make sure nothing reaches that fallback.
"""
import warnings

import pandas as pd

from lsms_library.local_tools import format_id


def i(value):
    """Composite household id: ED-SN-HH (roster/education/sample/interview_date)
    or ed_dvsn-ed_smpl-smpl_hh (housing) -- the same three numbers."""
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))


def v(value):
    """Composite cluster (ED sample-segment) id: ED-SN.

    NOT plain ED: ED numbers are reused across segments, so ED alone fuses
    enumeration districts in different regions (ED 5 / SN 194 is Region 4 urban;
    ED 5 / SN 702 is Region 10 rural).  See data_info.yml.
    """
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))


def sample(df):
    """df_edit hook for `sample`: drop the phantom weight-only rows.

    WEIGHT.dta is the 616-ED national sampling frame, i.e. a LOOKUP, but the
    framework merges sub-dfs with ``how='outer'``.  The 488 frame EDs that were
    never surveyed therefore arrive as rows with NO household -- i is NaN (and
    strata/Rural null), carrying only a weight.  They are not households and must
    not be in `sample`; left in, they all collapsed into a single phantom
    household (GH #323).  Dropping them restores the 1,807 real households.

    Also drops `ed_key`, the raw-ED helper column that exists only to key the
    WEIGHT merge (the cluster id v is the composite ED-SN; the weight is keyed on
    ED alone in the source -- see data_info.yml).
    """
    n0 = len(df)
    df = df[df.index.get_level_values('i').notna()]
    n_phantom = n0 - len(df)
    if n_phantom:
        # Expected: exactly 488 frame-only EDs.  Announce, don't hide.
        warnings.warn(
            f"Guyana 1992 sample: dropped {n_phantom} weight-only row(s) from "
            f"the WEIGHT.dta national frame (no household; i is NaN). "
            f"{len(df)} real households retained.",
            RuntimeWarning,
        )
    return df.drop(columns=[c for c in ['ed_key'] if c in df.columns])


def housing(df):
    """df_edit hook for `housing`: drop the one irreconcilable duplicate.

    With the correct 3-level key exactly ONE household is duplicated in
    HHCHAR.dta: (ed_dvsn=123, ed_smpl=722, smpl_hh=6) -- rows 1093/1094, same
    newid (12372206) and same hhsize (3), but disagreeing on ~35 columns
    including totexp (6,345 vs 25,843) and the wall material bldg3 (1=Wood vs
    3=Wood & Concrete).  WEIGHTID.dta carries the same duplicate on that key, so
    it is a duplicated record in the SOURCE, not an extraction artifact.

    There is no principled way to choose between the two records, so we drop
    BOTH rather than let ``groupby().first()`` pick one arbitrarily: the
    household becomes loudly MISSING from `housing` (class-2) instead of
    silently WRONG (class-1).  It remains present in roster/education/sample.

    Written as a general rule, not a hardcoded row-pair: ANY household whose
    dwelling record is duplicated and irreconcilable is dropped with a warning.
    A future duplicate cannot slip through silently.
    """
    dup = df.index.duplicated(keep=False)
    if dup.any():
        keys = sorted({str(k) for k in df.index[dup]})
        warnings.warn(
            f"Guyana 1992 housing: dropping {int(dup.sum())} row(s) for "
            f"{len(keys)} household(s) with irreconcilable duplicate dwelling "
            f"records in HHCHAR.dta (no principled way to choose between them; "
            f"dropped rather than silently first()-collapsed -- GH #323): "
            f"{', '.join(keys)}",
            RuntimeWarning,
        )
        df = df[~dup]
    return df


def cluster_features(df):
    """df_edit hook for `cluster_features`: collapse COVERN to one row per (t, v).

    COVERN is a HOUSEHOLD-level file (1,807 rows); cluster_features is declared
    at (t, v), so it must be reduced to one row per sampling segment.  Left to
    the framework this is a silent ``groupby().first()``, which with the old
    v=ED silently invented a Region for 537 households and a Rural for 274 (an
    ED spans regions -- see data_info.yml).

    With v=(ED, SN) the segment is homogeneous in Rural (0 of 168 ambiguous).
    Region is ambiguous in only 3 of 168 segments, and in each the split is a
    lone stray household against the rest (1-vs-10, 1-vs-11, 1-vs-11, all inside
    one stratum and one sector) -- a coding error, not a real split.  The DECLARED
    reducer is therefore the MODE (majority), not `first`: `first` would let row
    order decide, and for those 3 segments would sometimes return the single
    mis-coded region.

    A genuine TIE has no majority and is NOT guessed: the value becomes pd.NA
    (loudly missing) and a warning names the segment.
    """
    ties: list[str] = []

    levels = list(df.index.names)
    grouped = df.groupby(level=levels, observed=True)

    out = {}
    for col in df.columns:
        vals = {}
        for key, s in grouped[col]:
            m = s.dropna().mode()
            if len(m) == 1:
                vals[key] = m.iloc[0]
            else:
                vals[key] = pd.NA
                if len(m) > 1:
                    ties.append(f"{col}@{key}")
        out[col] = pd.Series(vals)

    res = pd.DataFrame(out)
    res.index = grouped.size().index

    if ties:
        warnings.warn(
            f"Guyana 1992 cluster_features: {len(ties)} cluster attribute(s) had "
            f"no majority value within the (ED,SN) segment and were set to NA "
            f"rather than guessed: {', '.join(sorted(ties)[:8])}",
            RuntimeWarning,
        )
    return res

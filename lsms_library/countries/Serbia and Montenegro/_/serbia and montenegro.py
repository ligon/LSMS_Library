"""Country-level ``df_edit`` hooks for Serbia and Montenegro.

A function whose name matches a declared ``data_scheme`` table is dispatched by
``Wave.column_mapping()`` as that table's ``df_edit`` hook (see
``country.py::column_mapping``), and runs on the extracted frame *before* the
framework normalizes the index.  It is therefore the right place to enforce a
grain precondition: this module runs as executable code, so what it asserts is
*enforced*, not merely documented.
"""


def cluster_features(df):
    """Reduce the person-level extraction to one row per sampling cluster.

    GH #323.  ``cluster_features`` is a CLUSTER-level table -- declared index
    ``(t, v)``, payload ``Region`` / ``Rural`` -- but both waves source it from
    the person-level demography file (``{wave} 1 demography.dta``), the very
    same file ``household_roster`` reads.  That file has one row PER PERSON
    (2002: 19,725 rows = 19,725 unique ``(mesto, rbd, clan)``; 2003: 8,027),
    while ``mesto`` (-> ``v``) takes only 618 / 301 distinct values.  The
    cluster-constant attributes ``stratum`` (-> ``Region``) and ``tip``
    (-> ``Rural``) are simply replicated down onto every person record.

    So the extraction emitted ~19.7k rows for a table that has 618 entities.
    The surplus rows are not entities; they are redundant repetitions of their
    cluster's row.  Left alone, the framework's ``_normalize_dataframe_index``
    silently collapsed them with ``groupby().first()`` -- 19,107 + 7,726 rows
    discarded with no warning on a warm cache.  This hook performs that
    reduction HERE, explicitly and with the precondition CHECKED, so the
    declared ``(t, v)`` index is naturally unique and the framework never has
    to guess.

    ``first()`` is a safe reducer here ONLY because the payload is provably
    invariant within a cluster (verified against source: 0 / 618 and 0 / 301
    clusters carry more than one distinct ``stratum`` or ``tip``; no NaNs).
    That is a precondition, not a law of the data -- so we *check* it rather
    than rely on it.  A future wave (or a re-extraction that picked up a
    genuinely household-varying column) in which a cluster carries conflicting
    payload values will RAISE here instead of silently keeping whichever row
    happened to sort first.

    Recovers 0 rows by construction: the output is byte-for-byte what the API
    already returned.  The point is that the collapse is now explicit, checked,
    and impossible to do wrong in silence.
    """
    key = [level for level in ('t', 'v') if level in df.index.names]
    if not key:
        return df

    flat = df.reset_index()
    payload = [c for c in flat.columns if c not in key]

    # The DECLARED dedup: rows identical across key+payload are the redundant
    # person-level repetitions, and collapse to the single cluster row.
    unique_rows = flat[key + payload].drop_duplicates()

    # THE ENFORCEMENT.  If a cluster survives twice, its payload is NOT
    # cluster-invariant, so no reducer can be trusted to pick the right row.
    # Fail loudly rather than silently ship one of the conflicting values.
    conflicting = unique_rows[unique_rows.duplicated(subset=key, keep=False)]
    if len(conflicting):
        offenders = conflicting.sort_values(key)
        raise ValueError(
            f"cluster_features: payload {payload} is not invariant within "
            f"{key} -- {conflicting[key].drop_duplicates().shape[0]} cluster(s) "
            f"carry conflicting values, so collapsing to one row per cluster "
            f"would silently discard real variation (GH #323).  Either the "
            f"source is no longer cluster-constant or these columns belong on "
            f"a finer-grained table.  Offending rows:\n{offenders.to_string()}"
        )

    return unique_rows.set_index(key)

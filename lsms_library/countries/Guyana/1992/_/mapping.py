"""Guyana 1992 index formatters.

Household identity is the TRIPLE ``(ED, SN, HH)``, not ``(ED, HH)`` (GH #503).
The survey says so itself: ``COVERN.NEWID == ED*100000 + SN*100 + HH`` holds for
all 1807 cover-page rows.  ``(ED, HH)`` alone collapses 1807 real households into
1502, so ~305 households were silently merged into others.

``i()`` and ``v()`` are bound by NAME to the ``idxvars`` key of the same name
(``Wave.column_mapping`` -> ``map_formatting_function``, country.py), and are
applied row-wise when the YAML value is a *list* of source columns.  A scalar
``idxvars`` value still falls back to ``format_id``, so declaring ``v()`` here
cannot affect any table that keeps a single-column ``v``.
"""
from lsms_library.local_tools import format_id


def _join(value):
    """Hyphen-join the parts of a composite id, each normalized by format_id."""
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))


def i(value):
    """Composite household id from (ED, SN, HH) -- e.g. '1-37-1'."""
    return _join(value)


def v(value):
    """Composite cluster id from (ED, SN) -- e.g. '1-37'.

    ED alone is NOT the sampling cluster: across the 130 EDs, RGN varies within
    22 of them, SECTOR within 10 and STNO within 24 -- so the ``.first()``
    collapse in ``Wave.cluster_features`` (country.py, "invariant within a
    cluster by construction") was assigning 287 of 1807 households a Region that
    is not their own.  Under (ED, SN) there are 168 clusters, SECTOR varies
    within 0 of them and RGN within 3.  (ED, SN) is the geographic cluster.
    """
    return _join(value)


def sample(df):
    """Drop the phantom households injected by the sub-df merge.

    ``sample`` merges the cover page (COVERN.dta, 1807 enumerated households in
    130 EDs) with the weights file (WEIGHT.dta, which lists all 616 EDs in the
    sampling frame).  The framework merges sub-dfs with ``how='outer'``
    (country.py, shared by every country with a ``dfs:`` block -- not ours to
    change), so the 488 EDs that were never enumerated arrive as rows with
    ``i = NaN``.  They are not households.  Dropping them here keeps them out of
    the cached parquet and, importantly, keeps them from tripping the GH#323
    duplicate-index warning -- which must stay quiet so that it remains a usable
    detector of real conflation.
    """
    return df[df.index.get_level_values('i').notna()]

"""Wave-level formatting helpers for Uganda 2009-10.

* ``District`` — coerces float-stringified district code to int-string.
* ``Age`` / ``household_roster`` — adopt ``age_handler`` to reconstruct
  Age from h2q9a/b/c when h2q8 is missing (GH #177).  Sentinel cleanup
  and the per-row glue live in ``../../_/_age_helpers.py``.
* ``cluster_features`` — gives the 565 out-of-frame households a real
  cluster instead of collapsing them into one (GH #323).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

from lsms_library.build_transforms import fill_v_with_coord_bin

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_uganda_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

INTERVIEW_YEAR = 2009


def District(x):
    """Coerce numeric District code (float-stringified from Stata) to int-string.

    h1aq1 in GSEC1.dta is stored as a numeric code (e.g. 101.0, 102.0).
    Without explicit coercion, df_data_grabber leaves myvars as float and the
    result stringifies to '101.0'.  This function strips the .0 suffix.
    """
    if pd.isna(x):
        return pd.NA
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x)


def Age(value):
    """Pre-process the YAML ``Age:`` list per row (sentinel cleanup)."""
    return _mod.age_components(value)


def household_roster(df):
    """Reduce list-valued ``Age`` to a scalar via age_handler."""
    return _mod.run_household_roster(df, interview_year=INTERVIEW_YEAR)


def cluster_features(df):
    """Give the out-of-frame households a real cluster (GH #323).

    2009-10 is a panel-refresh wave, so ``comm`` in GSEC1 is the *2005-06* EA:
    the 565 households that moved or split off fall outside that sampling frame
    and carry a blank ``comm``.  ``uganda.v`` (= ``format_id``) normalises the
    blank to ``None``, and the (t, v) collapse in ``_normalize_dataframe_index``
    then drops every one of them -- so 565 households referenced a cluster that
    did not exist in ``cluster_features`` at all.

    ``sample`` already solved exactly this, via its ``derived: coalesce_coord_bin``
    block: fall back to a synthetic ``@lat,lon`` cluster binned from the modified
    GPS.  ``cluster_features`` could not reuse that block declaratively because
    ``derived:`` runs *after* ``set_index(final_index)``, by which point ``v`` is
    an index level rather than a column.  So we invoke the SAME transformer here,
    on the same ``lat_mod``/``lon_mod`` values, with the same defaults
    (grid_degrees=0.05, prefix='@') -- which is what makes the labels byte-identical
    to ``sample``'s.  Reusing the transformer rather than re-deriving the label is
    the point: two independent implementations of this rule would drift, and the
    two tables' ``v`` MUST agree or ``_join_v_from_sample`` hands the rest of the
    library a key that matches nothing.

    541 of the 565 have usable coordinates and get a cluster.  The remaining 24
    have neither a ``comm`` nor a GPS fix, so nothing can locate them: they keep
    ``v = <NA>`` and drop out, which is the honest outcome (class-2 missing, not a
    fabricated cluster).
    """
    flat = df.reset_index()
    flat = fill_v_with_coord_bin(flat, target='v', lat='Latitude', lon='Longitude')
    return flat.set_index(['t', 'v'])

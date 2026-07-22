"""Shared ``individual_education`` ``df_edit`` helper for Uganda (GH #171).

A handful of pre-2019 GSEC4 ``h4q7`` rows (1-2 records each in 2010-11 and
2011-12) hold raw *float* codes (1.0, 2.0, 3.0, 52.0) that never received Stata
value labels.  The ``harmonize_education`` categorical table is applied via
``Series.map(lambda x: f.get(x, x))`` with string keys, so a float ``1.0``
slips through unmapped and would surface as a non-canonical leftover.

``coerce_unmapped_to_unknown`` runs as the wave-level ``df_edit`` hook
(dispatched by ``Country`` as ``individual_education(df)``) and folds any
remaining non-canonical ``Educational Attainment`` value onto the ``Unknown``
sentinel, keeping the column fully within the canonical ordinal vocabulary.
"""
from __future__ import annotations

import pandas as pd

# Canonical ordinal vocabulary (categorical_mapping/canonical_education_labels.org).
CANONICAL_EDUCATION_LABELS = frozenset({
    "No education", "Informal", "Pre-primary",
    "Primary incomplete", "Primary complete",
    "Lower secondary", "Lower secondary complete",
    "Upper secondary", "Upper secondary complete",
    "Vocational/Technical", "Tertiary certificate/diploma",
    "Bachelor", "Postgraduate", "Doctorate", "Unknown",
})

_COL = "Educational Attainment"


def coerce_unmapped_to_unknown(df: pd.DataFrame) -> pd.DataFrame:
    """Map any leftover non-canonical ``Educational Attainment`` value -> ``Unknown``."""
    if _COL not in df.columns:
        return df
    s = df[_COL]
    leftover = s.notna() & ~s.isin(CANONICAL_EDUCATION_LABELS)
    if leftover.any():
        df = df.copy()
        df.loc[leftover, _COL] = "Unknown"
    return df

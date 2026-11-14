# Wave module for Nigeria GHS-Panel 2010-11 (W1).
#
# Loaded by Wave.formatting_functions beside mapping.py; its stem is the wave
# folder, which Wave.data_scheme excludes, so it declares no table of its own.
# A file of its own (not mapping.py or data_info.yml) so that GH #1562's assets
# fix touches nothing another Nigeria stream edits (#1713 edits the W1/W2
# individual_education blocks; the food streams edit mapping.py).
import numpy as np


def assets(df):
    """Serve ``Age`` and ``Value`` as NA columns for W1 (GH #1562 follow-up).

    Ruling 2026-11-13, "Coping follow-ups", Nigeria assets: "Pin + W1 NA
    columns": W1's assets block serves Age and Value as NA (no ``optional:``),
    so a request for one quarter answers the same cold and warm.

    Since GH #1562 assets is served once, under the post-planting quarter, so a
    request for 2011Q1 alone has an EMPTY answer, and an empty answer must carry
    the required declared columns (GH #1292's check).  W1's block wires only
    ``Quantity`` (``s5q1``, sect5_plantingw1), so cold the request raised
    "successful empty YAML result is missing required declared column(s)
    ['Age', 'Value']" while warm it returned 0 rows.  With the columns present
    and NA, both answer 0 rows.  The served 2010Q3 rows are unchanged: the
    cross-wave concat already gave them Age and Value NA.

    NA here means NOT WIRED, not "not asked": W1's per-unit roster
    ``sect5b_plantingw1`` carries ``s5q3`` and ``s5q4``, the variables W2 wires
    as Age and Value.  Wiring them is a question in the GH #1562 report.
    """
    for col in ('Age', 'Value'):
        if col not in df.columns:
            df[col] = np.nan
    return df

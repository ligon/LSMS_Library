#!/usr/bin/env python
"""Anthropometry (reported body measures) for GhanaAHIES 2024, all four
quarters (t = 2024Q1 .. 2024Q4) from the one person x quarter file.

Source: ../Data/2024 AHIES Q1-Q4_20250827.dta, Section 3B (Q3-Q5: weight,
height, mode of measurement), asked of EVERY household member every quarter
(the questionnaire's "RESPONDENTS: ALL HOUSEHOLD MEMBERS"; the field manual
says "measure the weight / height of each household member").  Read once
through get_dataframe.  (t, i, pid) index as in household_roster; the same
'Not member anymore' filter is applied (48 measured rows in 2024).

Columns (the GAP 5 shape of Uganda / Malawi / Tanzania / Nigeria; REPORTED
measures only -- no z-scores, no BMI, nothing derived):
  Weight      s3bq3, kilograms.
  Height      s3bq4, centimetres (standing height or lying length -- s3bq5
              says which; the precedents fold the two into one column and so
              does this table.  The mode is NOT served: see CONTENTS.org for
              the mode-by-age crosstab).
  Age_months  12 * s1aq4y + s1aq4m, the Malawi `cage` definition; s1aq4m
              (0-11) is filled only for ages 0-4 (19,948 rows), so for members
              5+ this is exactly 12 * completed years.

NOT-MEASURED CODES, decoded to NA (this is decoding, not screening).  The
paper questionnaire says "NOT MEASURED CODE 999" for both Q3 and Q4; the CAPI
file spells that code NEGATIVELY: height -99 (13,001 rows), weight -99.94
(2,298 rows, float32).  A further 575 weight rows carry other repeated
negative values (-23.12 x334, -21.27 x235, -25.845 x2, -19.97 x2, -30.87,
-28.395), all ages 5+ and all measured standing -- no scale reads a negative
kilogram, so these are an entry defect (most plausibly a caregiver-and-child
subtraction applied to the wrong row), nulled and counted per value below.
Everything IN the domain is served as recorded and reported, never clipped:
weight 99.0 exactly (1,875 rows, ages 0-97, the second most common value --
almost certainly a "don't know" code, but inseparable from a real 99 kg),
weight 35.35 exactly (1,642 rows, ages clustered at 14-16), heights above
220 cm (3,861 rows, median age 17, median weight 40 kg), heights under 40 cm
at ages over 5 (87).  The GSS `BMI` column is 100% null in the 2024 file, so
no cross-check of the served measures against it is possible.

One row per MEASURED individual: rows with both Weight and Height NA after
decoding are dropped with a count (they are the "Scale not present" rows plus
the both-codes rows, concentrated in 2024Q1).
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))
from ghanaahies import NOT_MEMBER_LABEL, person_keys  # noqa: E402
from lsms_library.local_tools import get_dataframe, to_parquet  # noqa: E402

PERSON_FILE = '../Data/2024 AHIES Q1-Q4_20250827.dta'
MEASURES = ['Weight', 'Height']


def _decode_measure(raw: pd.Series, name: str) -> pd.Series:
    """Numeric measure with every NEGATIVE value (the CAPI not-measured code
    and the repeated negative entry defects) decoded to NA, counted per
    distinct value so the decision is auditable in the build log."""
    x = pd.to_numeric(raw, errors='coerce')
    neg = x < 0
    if neg.any():
        counts = x[neg].round(3).value_counts().sort_index().to_dict()
        print(f'anthropometry 2024: {name}: {int(neg.sum())} negative values -> NA {counts}')
    return x.where(~neg)


def build(df: pd.DataFrame) -> pd.DataFrame:
    rel = df['s1aq2x'].astype(object).map(lambda x: x.strip() if isinstance(x, str) else x)
    departed = (rel == NOT_MEMBER_LABEL).fillna(False)

    years = pd.to_numeric(df['s1aq4y'], errors='coerce')
    months = pd.to_numeric(df['s1aq4m'], errors='coerce')
    assert months.dropna().between(0, 11).all(), 's1aq4m outside 0-11'
    age_months = 12.0 * years + months.fillna(0.0)

    out = person_keys(df)
    out['Weight'] = _decode_measure(df['s3bq3'], 'Weight')
    out['Height'] = _decode_measure(df['s3bq4'], 'Height')
    out['Age_months'] = age_months.astype(float)

    n_departed = int((departed & out[MEASURES].notna().any(axis=1)).sum())
    out = out.loc[~departed]
    unmeasured = out[MEASURES].isna().all(axis=1)
    print(f'anthropometry 2024: dropping {n_departed} measured "{NOT_MEMBER_LABEL}" rows and '
          f'{int(unmeasured.sum())} rows with neither Weight nor Height '
          f'({out.loc[unmeasured].groupby("t").size().to_dict()})')
    out = out.loc[~unmeasured]

    for c in MEASURES + ['Age_months']:
        out[c] = out[c].astype('Float64')
    out = out.set_index(['t', 'i', 'pid']).sort_index()
    assert out.index.is_unique, 'duplicate (t, i, pid) in anthropometry'

    # Report, never clip: value ranges of what is served.
    for c in MEASURES:
        q = out[c].quantile([0.01, 0.99])
        print(f'anthropometry 2024: {c} n={int(out[c].notna().sum())} min={out[c].min()} '
              f'p1={q.iloc[0]:.1f} p99={q.iloc[1]:.1f} max={out[c].max()}')
    return out


if __name__ == '__main__':
    df = get_dataframe(PERSON_FILE)
    out = build(df)
    print('anthropometry 2024:', out.groupby(level='t').size().to_dict())
    to_parquet(out, 'anthropometry.parquet')

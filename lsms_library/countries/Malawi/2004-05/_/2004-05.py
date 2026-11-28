"""Wave-specific mappings for the IHS2 cover."""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '_'))


def interview_date(df):
    from _malawi_interview_dates import parsed_interview_dates

    return parsed_interview_dates(df, '2004-05', components=True)

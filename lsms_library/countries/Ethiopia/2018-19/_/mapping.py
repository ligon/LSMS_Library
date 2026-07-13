"""Wave-level formatting functions for Ethiopia 2018-19 (W4).

``interview_date`` is picked up by name as the ``df_edit`` hook for the
``interview_date`` request (see ``Wave.column_mapping`` /
``get_formatting_functions``).
"""

import pandas as pd


def interview_date(df):
    """Coerce the W4 cover InterviewStart to a real datetime (GH #323).

    data_scheme.yml declares ``Int_t: datetime``, but the W4 source column is a
    STRING ('2019-06-19T09:15:28') and nothing coerced it, so the wave emitted
    an object column and the declaration was a fiction.

    That mattered once W5 started emitting a true datetime (its own hook, which
    strips the '##N/A##' sentinel): concatenating a string wave with a datetime
    wave produced an OBJECT column, which the country parquet stored as text --
    re-rendering W5's timestamps with a space separator ('2022-04-14 11:21:42')
    while W4 kept its 'T'.  On the next (warm) read pandas inferred ONE format
    from the leading W4 rows, every W5 value failed to parse, became NaT, and
    the defensive drop-all-NaN-rows step deleted the ENTIRE 2021-22 wave --
    silently, and only on the cached path.  Coercing here keeps the column a
    real datetime64 end to end, so it never round-trips through an ambiguous
    string.  Unparseable values (8 in W4) become NaT, as before.
    """
    df = df.copy()
    if 'int_t' in df.columns:
        df['int_t'] = pd.to_datetime(df['int_t'], errors='coerce')
    return df

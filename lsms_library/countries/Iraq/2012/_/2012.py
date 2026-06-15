"""Wave-level df_edit hooks for Iraq 2012 (loaded by Wave.formatting_functions).

A function whose name matches a data_scheme request (here `shocks`) is wired
up as that request's `df_edit` post-processor in country.Wave.grab_data: it
runs on the extracted, (t, i, Shock)-indexed frame after the column mapping.
"""


def shocks(df):
    """Collapse the 23-shock x household cross-product to experienced shocks.

    The 2012ihses20 module records all 23 shock categories for every
    household, with q2001 ("Has hh experienced..?") flagging the ones that
    actually occurred.  The non-experienced rows carry NaN impact/coping and
    are pure padding, so keep only Experienced rows and drop the temporary
    Experienced column, leaving a true shocks-and-coping roster.
    """
    df = df[df['Experienced'] == True]
    df = df.drop(columns=['Experienced'])
    return df

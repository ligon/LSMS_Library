"""Wave-level df_edit hooks for Iraq 2012 (loaded by Wave.formatting_functions).

A function whose name matches a data_scheme request (here `shocks`) is wired
up as that request's `df_edit` post-processor in country.Wave.grab_data: it
runs on the extracted, (t, i, Shock)-indexed frame after the column mapping.
"""


def months_away(x):
    """Constant MonthsAway=0 for every 2012 roster member (GH #499).

    The IHSES 2012 household roster (2012ihses01) enumerates current
    household members and carries NO residence-duration question — the
    only YES/NO person attributes are parental presence (q110/q113) and
    birthplace (q108), not months present/away.  Every listed member is a
    current resident, so MonthsAway=0 (present all 12 months).

    We deliberately emit MonthsAway (not MonthsSpent) so this wave keys the
    same residence column as 2006-07, which carries real MonthsAway (q0113
    in that wave).  ``roster_to_characteristics`` picks ONE residence
    column for the whole country-concatenated roster (monthsspent has
    precedence over monthsaway); introducing MonthsSpent here would make
    2006-07's all-NaN MonthsSpent column drop that wave instead.  Keeping
    both Iraq waves on MonthsAway lets the per-wave real/constant values
    coexist without an all-NaN column silently dropping a whole wave from
    ``household_characteristics``.
    """
    return 0


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

# Formatting Functions for CotedIvoire 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools


def pid(value):
    '''Formatting person id from (grappe, menage, individual).'''
    return (tools.format_id(value.iloc[0]) + '0'
            + tools.format_id(value.iloc[1], zeropadding=2) + '0'
            + tools.format_id(value.iloc[2], zeropadding=2))


def Age(value):
    '''
    Pass Age columns through as a list for age_handler.

    CotedIvoire s01q03b (month) is already an integer (1-12); no
    month_map conversion needed.  The list is returned unchanged so
    household_roster() can unpack [age_raw, day, month, year].
    '''
    return list(value)


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    CotedIvoire s01q03b is an integer month (1-12) — no month_map needed.
    DOB columns use true NaN (no sentinel), so no additional sentinel handling required.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # Pass None for negative sentinel values so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


def food_acquired(df):
    '''
    Reshape CotedIvoire 2018-19 food_acquired to the canonical (s) form.

    Inputs (post-data_grabber):
      - Index: (i, t, v, visit, j, u)
      - Columns: Quantity (TOTAL acquired in unit u, s07bq03a),
                 Expenditure (monetary value of purchases, s07bq08),
                 Produced (subset of Quantity from own production, s07bq04)

    Output:
      - Index: (t, v, i, j, u, s)
      - Columns: Quantity, Expenditure
      - Price is api-derived (computed downstream for s=purchased).

    Reshape rules:
      - Each input row -> up to 2 long-form rows:
        * s='purchased': Quantity = (Total - Produced) clipped at 0,
          Expenditure as observed
        * s='produced':  Quantity = Produced, Expenditure = NaN
      - Rows with no measurements after the split are dropped.
      - visit (vague) is dropped: in EHCVM 2018-19 it's a sample split,
        not a repeated measure (each household appears in exactly one
        vague).  Confirmed empirically during the Benin pilot
        (commit 27e3d963: 0 of 8012 Benin households had multiple
        visits, 0 of 670 clusters did).  EHCVM-7 share this design
        per CLAUDE.md.

    See: slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org, GH #169.
    '''
    import pandas as pd
    import numpy as np

    work = df.reset_index()
    work = work.drop(columns=['visit'])

    # Purchased = Total - Produced, clipped at zero (a few survey rows
    # have Produced slightly > Quantity due to rounding; treat those as
    # purchased=0 rather than negative).
    purchased_qty = (work['Quantity'].fillna(0)
                     - work['Produced'].fillna(0)).clip(lower=0)

    purchased = pd.DataFrame({
        't': work['t'].values,
        'v': work['v'].values,
        'i': work['i'].values,
        'j': work['j'].values,
        'u': work['u'].values,
        's': 'purchased',
        'Quantity': purchased_qty.values,
        'Expenditure': work['Expenditure'].values,
    })
    purchased = purchased[(purchased['Quantity'] > 0)
                          | (purchased['Expenditure'] > 0)]

    produced = pd.DataFrame({
        't': work['t'].values,
        'v': work['v'].values,
        'i': work['i'].values,
        'j': work['j'].values,
        'u': work['u'].values,
        's': 'produced',
        'Quantity': work['Produced'].values,
        'Expenditure': np.nan,
    })
    produced = produced[produced['Quantity'].fillna(0) > 0]

    out = pd.concat([purchased, produced], ignore_index=True)
    out = out.set_index(['t', 'v', 'i', 'j', 'u', 's'])
    return out

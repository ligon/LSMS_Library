import pandas as pd
import numpy as np
import lsms_library.local_tools as tools

COPING_LABELS = {
    1: "Utilisation de son épargne",
    2: "Aide de parents ou d'amis",
    3: "Aide du gouvernement/l'Etat",
    4: "Aide d'organisations religieuses ou d'ONG",
    5: "Marier les enfants",
    6: "Changement des habitudes de consommation",
    7: "Achat d'aliments moins chers",
    8: "Membres actifs ont pris des emplois supplémentaires",
    9: "Membres adultes inactifs/chômeurs ont pris des emplois",
    10: "Enfants de moins de 15 ans amenés à travailler",
    11: "Les enfants ont été déscolarisés",
    12: "Migration de membres du ménage",
    13: "Réduction des dépenses de santé/d'éducation",
    14: "Obtention d'un crédit",
    15: "Vente des actifs agricoles",
    16: "Vente des biens durables du ménage",
    17: "Vente de terrain/immeubles/Maisons",
    18: "Louer/mettre ses terres en gages",
    19: "Vente du stock de vivres",
    20: "Pratique plus importante des activités de pêche",
    21: "Vente de bétail",
    22: "Confiage des enfants à d'autres ménages",
    23: "Engagé dans des activités spirituelles",
    24: "Pratique de la culture de contre saison",
    25: "Autre stratégie",
    26: "Aucune stratégie",
}


def Age(value):
    '''
    Convert French month names to integers for the DOB components.

    Benin s01q03b (month) uses French month names (Janvier, Février, ...)
    identical to Senegal 2018-19.  Convert month to int so tools.age_handler()
    can compute age from DOB.

    The list [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)] is
    passed to household_roster() via tools.age_handler().
    '''
    month_map = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
                 'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9,
                 'Octobre': 10, 'Novembre': 11, 'Décembre': 12}
    result = list(value)
    result[2] = month_map.get(value.iloc[2])
    return result


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    Benin s01q03b uses French month names converted to int by the Age() formatter above.
    DOB columns use true NaN (no sentinel other than -1 for age), so no extra sentinel mask needed.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # -1 is a sentinel for missing age; pass None so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        # Null out any negative result (dob slightly after interview date)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


def shocks(df):
    # Benin 2018-19 records at most 2 coping strategies per household.
    # (A probe over the full survey shows no household with a third
    # strategy flagged; HowCoped2 would be all-NaN and is therefore
    # not declared in data_scheme.yml.)
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: []}
    for _, row in df[cope_cols].iterrows():
        found = []
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue
            if val >= 1:
                found.append(COPING_LABELS.get(num, f'Strategy {num}'))
            if len(found) == 2:
                break
        for k in range(2):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df = df.drop(columns=cope_cols)
    return df


def food_acquired(df):
    '''
    Reshape Benin 2018-19 food_acquired to the canonical (s) form.

    Inputs (post-data_grabber):
      - Index: (i, t, v, visit, j, u)
      - Columns: Quantity (TOTAL acquired in unit u), Expenditure
                 (monetary value of purchases, s07bq08), Produced
                 (subset of Quantity from own production, s07bq04)

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
        not a repeated measure; each household appears in exactly one
        vague (probe 2026-05-05: 0 of 8012 households have multiple
        visits).  Households are uniquely keyed by (t, v, i, j, u, s)
        without it.

    Column semantics confirmed by data probe (2026-05-05): of the 2,597
    rows where both Quantity and Produced are populated, none have
    Produced > Quantity, consistent with Quantity being the reported
    total acquisition and Produced the own-production subset.

    See: slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org, GH #169.
    '''
    import pandas as pd
    import numpy as np

    work = df.reset_index()

    # `visit` (vague) in EHCVM 2018-19 is a sample split, not a repeated
    # measure: each grappe (and so each household) is surveyed in exactly
    # one vague.  Empirical check (2026-05-05): 0 of 8012 households have
    # multiple visit values, 0 of 670 clusters do.  So visit is metadata
    # about which sub-sample the household belongs to, not an index axis.
    # Drop it.  Households are uniquely keyed by (t, v, i, j, u, s) without
    # it.  See slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org.
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

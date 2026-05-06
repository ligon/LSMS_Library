# Formatting Functions for Togo 2018
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

    Togo s01q03b (month) uses French month names (Janvier, Février, ...)
    identical to Senegal 2018-19 — the audit incorrectly described them as
    plain integers.  Convert month to int so tools.age_handler() can
    compute age from DOB.

    The list [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)] is
    passed to household_roster() via tools.age_handler().
    '''
    month_map = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
                 'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9,
                 'Octobre': 10, 'Novembre': 11, 'Décembre': 12}
    result = list(value)
    result[2] = month_map.get(value.iloc[2])
    return result


def shocks(df):
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: [], 2: []}
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
            if len(found) == 3:
                break
        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df['HowCoped2'] = how_coped[2]
    df = df.drop(columns=cope_cols)
    return df


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    Togo s01q03b is a numeric integer month (1-12), not French text — no month_map needed.
    DOB columns use true NaN (no 9999 sentinel), so no additional sentinel handling required.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # -1 is a sentinel for missing age; pass None so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        return tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                 interview_date=x["interview_date"], interview_year=2018)

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


def food_acquired(df):
    '''
    Reshape Togo 2018 food_acquired to the canonical (s) form.

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
      - visit (vague) is dropped: in EHCVM 2018 it's a sample split,
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

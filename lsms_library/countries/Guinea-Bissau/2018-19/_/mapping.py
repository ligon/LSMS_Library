# Formatting Functions for Guinea-Bissau 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools

def Sex(value):
    '''
    Map Portuguese sex labels from s01_me_gnb2018.dta to canonical M/F.
    Stata labels: 1=Masculino, 2=Feminino.
    '''
    if value in ('Masculino', '1'):
        return 'M'
    if value in ('Feminino', '2'):
        return 'F'
    return pd.NA


# Portuguese month names used in s01q03b Stata labels.
_MONTH_MAP_PT = {
    'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Marco': 3, 'Abril': 4,
    'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9,
    'Outubro': 10, 'Novembro': 11, 'Dezembro': 12,
}


def Age(value):
    '''
    Convert DOB list [s01q04a, s01q03a, s01q03b, s01q03c] so that month
    (index 2) is a plain integer 1-12.

    s01q03b Stata labels are Portuguese month names (Janeiro…Dezembro).
    get_dataframe() returns the label strings as categoricals.
    The sentinel 9999 is left as-is; household_roster() handles it.
    '''
    result = list(value)
    raw_month = value.iloc[2]
    if isinstance(raw_month, str):
        result[2] = _MONTH_MAP_PT.get(raw_month)
    # if already numeric (int/float), keep as-is; household_roster will
    # null it out if it equals 9999
    return result


def household_roster(df):
    '''
    Recover Age from date-of-birth components (s01q03a/b/c) when s01q04a
    is missing or 9999.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    The Age() formatter above has already converted month to int 1-12 (or None).
    The 9999 sentinel marks unknown age / DOB components.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # 9999 is the sentinel for unknown age; treat as missing
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) == 9999) else age_raw
        # day sentinel
        raw_day = x["Age"][1]
        day = None if (pd.notna(raw_day) and int(float(raw_day)) == 9999) else raw_day
        # month — Age() may have returned None for unrecognised strings
        month = x["Age"][2]
        result = tools.age_handler(age=age_val, d=day, m=month, y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


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
}


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


def food_acquired(df):
    '''
    Reshape Guinea-Bissau 2018-19 food_acquired to the canonical (s) form.

    Inputs (post-data_grabber):
      - Index: (i, t, v, visit, j, u)
      - Columns: Quantity (TOTAL acquired in unit u, s07bq03a),
                 Expenditure (monetary value of purchases, s07bq08),
                 Produced (subset of Quantity from own production,
                 s07bq04)

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
        vague.  This was confirmed empirically during the Benin pilot
        (commit 27e3d963; 0 of 8012 Benin households had multiple
        visits).  EHCVM countries share this design (see CLAUDE.md
        "EHCVM countries" gotcha — each grappe is visited in exactly
        one vague).  Households are uniquely keyed by (t, v, i, j, u,
        s) without it.

    Column semantics: Quantity (s07bq03a) is the reported total
    acquisition over the recall period; Produced (s07bq04) is the own-
    production subset.  Same wide-form structure as Benin, Mali, Niger,
    Senegal, Togo and CotedIvoire 2018-19 EHCVM waves.

    See: slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org, GH #169.
    '''
    import pandas as pd
    import numpy as np

    work = df.reset_index()

    # `visit` (vague) in EHCVM 2018-19 is a sample split, not a repeated
    # measure: each grappe (and so each household) is surveyed in exactly
    # one vague.  Confirmed via the Benin pilot probe (commit 27e3d963):
    # 0 of 8012 Benin households had multiple visit values, 0 of 670
    # clusters did.  EHCVM 2018-19 design is shared across the EHCVM-7,
    # so the same conclusion applies to Guinea-Bissau.  Drop visit;
    # households are uniquely keyed by (t, v, i, j, u, s) without it.
    # See slurm_logs/DESIGN_food_acquired_canonical_2026-05-05.org.
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

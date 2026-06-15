#!/usr/bin/env python
"""South Africa 1993 life_satisfaction (SALDRU PSLSD).

Source: S5_PQL.dta -- Section 9 'Perceived Quality of Life'.  One row per
household (8804 unique hhid; resp_cod records which member answered).

The only genuine satisfaction-RATING item in this module is `satisfie`
("1 :Level of satisfaction" -- the overall "Taking everything into account,
how satisfied are you with the way you live?" question), coded 1..5 on the
standard SALDRU 5-point satisfaction scale.  All other columns are NOT
satisfaction ratings and are deliberately excluded:
  - choice1_/2_/3_ + ranks: respondent's life-improvement priorities
  - safety_h / safety_o:     perceived safety inside / outside the home
  - crime_q, c_*:            crime victimisation
  - parents_:                "compared with parents" (relative standing)
  - new_govt:                expected effect of the new government

So this feature carries a single Domain='Overall'.  Output is LONG on
(i, Domain); the framework adds t='1993' and joins v from sample().
satisfie has no .dta value labels; the 5-point scale is decoded from the
SALDRU PSLSD 1993 questionnaire (Section 9).  Sentinels -1 (refused/no
answer) and -3 (not applicable) are nulled out.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, to_parquet

SATISFACTION = {
    1: "Very satisfied",
    2: "Satisfied",
    3: "Neither satisfied nor dissatisfied",
    4: "Dissatisfied",
    5: "Very dissatisfied",
}

df = get_dataframe('../Data/S5_PQL.dta')

s = df[['hhid', 'satisfie']].copy()
s['hhid'] = s['hhid'].astype(int).astype(str)
s['Satisfaction'] = s['satisfie'].map(SATISFACTION)  # sentinels -> NaN
s = s.dropna(subset=['Satisfaction'])

s['Domain'] = 'Overall'
out = (s.rename(columns={'hhid': 'i'})[['i', 'Domain', 'Satisfaction']]
        .set_index(['i', 'Domain']))

to_parquet(out, 'life_satisfaction.parquet')

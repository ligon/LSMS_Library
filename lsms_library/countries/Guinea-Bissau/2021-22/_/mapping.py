# Formatting functions for Guinea-Bissau 2021-22 (EHCVM-2).  See _/CONTENTS.org
# "2021-22 wired".  Copied from the Togo / Benin 2021-22 wiring (PR #1711), with
# every label-keyed map re-keyed from THIS file's own Portuguese value labels.
import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

# GH #1575's construction, extended to this wave as the 2026-11-12 ruling extends it
# to Togo / Benin / Cote d'Ivoire ("Same rule as #1575"): `strata` is the cover's
# region label and milieu label, by the shared helper, and `sample` stamps the
# registry key on every row with a stratum.  The registry's `inputs` callable is
# 2018-19's, which reads each wave's own cover.
from lsms_library.ehcvm import (
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Guinea-Bissau')


#: `i` of this wave.  4,893 of the 5,351 (grappe, menage) keys also occur in 2018-19
#: (s00q07f1 / s00q07f2 record the 2018 grappe and household, and PanelHH a panel
#: flag), and whether a recurring key is the same household is not checked here.
#: No link is served (ruling 2026-11-12: no panel IDs), so the ids must not meet.
ID_PREFIX = 'E2_'


def i(value):
    '''Household id from (grappe, menage): 2018-19's form, with this wave's prefix.'''
    return ID_PREFIX + tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def Sex(value):
    '''s01q01: Masculino / Feminino, as 2018-19's labels.'''
    return {'Masculino': 'M', 'Feminino': 'F'}.get(str(value).strip(), pd.NA) \
        if pd.notna(value) else pd.NA


#: s01q03b, Portuguese month names; `Não sabe` (5,819 rows) is no month.
_MONTH = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
          'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11,
          'Dezembro': 12}


def Age(value):
    '''[s01q04a, s01q03a, s01q03b, s01q03c], with the month label as a number.

    The name collides with the `assets` myvar `Age: s12q07`, which the framework
    binds to this by name with a scalar; a scalar passes through (GH #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    result = list(value)
    result[2] = _MONTH.get(str(value.iloc[2]).strip()) if pd.notna(value.iloc[2]) else None
    return result


#: s01q00aa, "[NOM] est-il toujours un membre de ce ménage?".  The file spells `não`
#: double-encoded with a byte lost (`nÃo`), whose lower case is `não`.
_STILL = {'sim': True, 'não': False, 'nao': False}


def household_roster(df):
    '''Members only, with Age from the stated years, else the birth date.

    The 341 people s01q00aa says have left are not served (GH #1565 is open; this
    is Benin 2021-22's and ehcvm_individu's choice).  Ages as 2018-19's hook: 9999
    is no answer, and a birth date after the interview gives NA.
    '''
    still = df['StillMember'].astype('string').str.strip().str.lower()
    unknown = sorted(set(still.dropna()) - set(_STILL))
    if unknown:
        raise ValueError(f'Guinea-Bissau 2021-22 s01q00aa: unknown answers {unknown}')
    df = df[still.map(_STILL).fillna(True).astype(bool).to_numpy()].copy()

    def _known(x):
        return None if (pd.isna(x) or int(float(x)) == 9999) else x

    def age(row):
        stated, d, m, y = row['Age']
        result = tools.age_handler(age=_known(stated), d=_known(d), m=m, y=_known(y),
                                   interview_date=row['interview_date'], interview_year=2021)
        return np.nan if (pd.notna(result) and result < 0) else result

    df['Age'] = df.apply(age, axis=1)
    return df.drop(columns=['interview_date', 'StillMember'])

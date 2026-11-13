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

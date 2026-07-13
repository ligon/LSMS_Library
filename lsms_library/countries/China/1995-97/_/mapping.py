#!/usr/bin/env python
"""Formatting functions for China 1995-97 wave.

The household ID (hid) is a 5-digit code that encodes geography:
  hid = village_code * 100 + household_number
  village_code = county(1 digit, 1-6) + town(1 digit) + village(1 digit, 1-5)
  e.g., hid=10101 -> village 101 (county 1, village 1), household 01

There are 6 counties x 5 villages = 30 villages, 787 households.

NOTE (GH #323): the TOWN digit is ALWAYS 0 -- hid carries no town information
at all (observed village codes are 101-105, 201-205, ... 601-605).  This is
why hid CANNOT be bridged to the community questionnaire: NPT0101.DTA codes a
village as a 4-digit prov-prefixed hierarchy (7111 = prov 7 / county 71 /
town 711 / village 7111) and does encode a real town (711/712/713).  The two
codings are different schemes with no arithmetic bridge, no crosswalk file,
and no way to infer one.  See the cluster_features block in data_info.yml for
the full argument -- it is dispositive, and it is the reason cluster_features
is NOT sourced from the (otherwise ideal) village-level NPT0101.

Province mapping:
  Counties 1-3 -> Province 7 (Hebei)
  Counties 4-6 -> Province 8 (Liaoning)
This is a COUNTY-level correspondence and so is unaffected by the (unrecoverable)
village-level one: NPT0101 shows exactly three counties in each province
(71/72/73 -> prov 7; 81/82/83 -> prov 8), matching the six counties in hid.
Province is constant within a county, hence within a village -- the invariant
that the cluster_features hook in China/_/china.py now ASSERTS rather than
assumes.
"""


def v(value):
    """Extract 3-digit village code (cluster/PSU) from household ID."""
    return int(value.iloc[0]) // 100


def Region(value):
    """Derive province from household ID.

    First digit of hid is the county (1-6).
    Counties 1-3 are in Province 7; Counties 4-6 are in Province 8.
    """
    county = int(value.iloc[0]) // 10000
    if county <= 3:
        return 7
    else:
        return 8


def PlotArea(x):
    """Clean S05B plot area (question 2, reported in mu).

    A handful of rows carry the Stata sentinel ~1.75e+100 (extreme
    encoding for a missing/refused value); coerce anything outside a
    plausible plot range to NA.
    """
    import pandas as pd
    s = pd.to_numeric(x, errors='coerce')
    return s.where((s > 0) & (s < 1000))


def AreaUnit(x):
    """S05B plot areas are reported in mu (the Chinese land unit)."""
    return 'mu'

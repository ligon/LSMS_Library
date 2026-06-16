#!/usr/bin/env python
"""EthiopiaRHS community / market prices -> (t, v, j, u) Price  (#438 / #275).

PA-level SURVEYED food prices from the ERHS dedicated price instruments (NOT
the household food_acquired unit values).  Three sources, one per price design;
the low ``item1234`` food-code scheme is consistent across all of them:

  price1234_rev.tab : R1-R4 (1994a/1994b/1995/1997).  WOREDA-level (q1b),
                      price-per-kg in p_r1..p_r4.  Broadcast to each PA in the
                      Woreda via the q1c->q1b crosswalk (demo123).
  rd6_kgpr_Mkt.tab  : R6 (2004).  PA-level (paid), kg_pr6 = avg of 3 markets.
  price2009.tab     : R7 (2009).  PA-level (paid), price_r7.

Grain (t, v, j, u='Kg') Price.  v = PA, formatted into the sample()/
cluster_features v keyspace.  Cluster-level feature: v IS in the index, there
is NO household i, so the framework does NOT join v from sample().

j = harmonize_community_food Preferred Label (categorical_mapping.org; DRAFT,
maintainer-curated), aligned with harmonize_food so a priced food joins
food_acquired / crop_production on j.  Items absent from that table (high
aggregate codes 100+/300+/500+, ~11% of R1-R4 price rows) are dropped with a
logged count -- REPORTED prices only, no imputation.
"""
import sys
import numpy as np
import pandas as pd

sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet, df_from_orgfile, format_id

# round column (price1234_rev) -> wave label
R_TO_T = {'p_r1': '1994a', 'p_r2': '1994b', 'p_r3': '1995', 'p_r4': '1997'}

U = 'Kg'   # ERHS price instruments are all price-per-kilogram


def _item_label_map():
    """item1234 code -> Preferred Label (DRAFT harmonize_community_food)."""
    t = df_from_orgfile('./categorical_mapping.org',
                        name='harmonize_community_food', to_numeric=False)
    t.columns = [c.strip() for c in t.columns]
    t['Code'] = pd.to_numeric(t['Code'], errors='coerce').astype('Int64')
    t = t.dropna(subset=['Code'])
    return {int(k): str(v).strip()
            for k, v in zip(t['Code'], t['Preferred Label']) if str(v).strip()}


def _woreda_to_pas():
    """q1b (Woreda code) -> [q1c (PA code), ...], from the demo123 crosswalk."""
    ros = get_dataframe('../1994a/Data/demo123.dta', convert_categoricals=False)
    xw = ros[['q1c', 'q1b']].dropna().drop_duplicates()
    out = {}
    for q1b, grp in xw.groupby('q1b'):
        out[int(q1b)] = sorted({int(x) for x in grp['q1c']})
    return out


def _finish(df, t):
    """Common tail: code->j, u, t, drop unmapped/zero, return (t,v,j,u) Price."""
    labels = _item_label_map()
    df = df.copy()
    df['j'] = df['code'].astype('Int64').map(labels)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    n0 = len(df)
    df = df[df['j'].notna() & df['Price'].notna() & (df['Price'] > 0)]
    dropped = n0 - len(df)
    if dropped:
        print(f"  {t}: dropped {dropped}/{n0} rows (unmapped item code or "
              f"non-positive price)")
    df['t'] = t
    df['u'] = U
    df['v'] = df['v'].apply(format_id)
    out = df[['t', 'v', 'j', 'u', 'Price']]
    # one reported price per (t, v, j, u): average if a Preferred Label
    # collapses >1 raw code at a cluster (rare; distinct codes -> distinct j)
    out = out.groupby(['t', 'v', 'j', 'u'], as_index=False)['Price'].mean()
    return out.set_index(['t', 'v', 'j', 'u'])


pieces = []

# --- R1-R4: price1234_rev, Woreda-level, broadcast to PAs ---------------
w2p = _woreda_to_pas()
p1 = get_dataframe('../1994a/Data/price1234_rev.tab', convert_categoricals=False)
# price1234_rev ships BOTH a 100+ 'code' column and the low 'item1234' scheme;
# harmonize_community_food keys on the low scheme, so drop the 100+ 'code'
# and use item1234 as our 'code'.
p1 = p1.drop(columns=['code']).rename(columns={'item1234': 'code'})
for rcol, t in R_TO_T.items():
    sub = p1[['q1b', 'code', rcol]].dropna(subset=['q1b', 'code']).copy()
    sub = sub.rename(columns={rcol: 'Price'})
    # broadcast each Woreda row to its PAs
    rows = []
    for _, r in sub.iterrows():
        for pa in w2p.get(int(r['q1b']), []):
            rows.append({'v': pa, 'code': r['code'], 'Price': r['Price']})
    if rows:
        pieces.append(_finish(pd.DataFrame(rows), t))

# --- R6 (2004): rd6_kgpr_Mkt, PA-level ----------------------------------
rd6 = get_dataframe('../2004/Data/rd6_kgpr_Mkt.tab', convert_categoricals=False)
rd6 = rd6.rename(columns={'item1234': 'code', 'paid': 'v', 'kg_pr6': 'Price'})
pieces.append(_finish(rd6[['v', 'code', 'Price']].dropna(subset=['v', 'code']), '2004'))

# --- R7 (2009): price2009, PA-level -------------------------------------
p9 = get_dataframe('../2009/Data/price2009.tab', convert_categoricals=False)
p9 = p9.rename(columns={'item': 'code', 'paid': 'v', 'price_r7': 'Price'})
pieces.append(_finish(p9[['v', 'code', 'Price']].dropna(subset=['v', 'code']), '2009'))

prices = pd.concat(pieces)
print(f"community_prices: {len(prices)} rows, "
      f"{prices.index.get_level_values('t').nunique()} waves, "
      f"{prices.index.get_level_values('j').nunique()} items")

to_parquet(prices, '../var/community_prices.parquet')

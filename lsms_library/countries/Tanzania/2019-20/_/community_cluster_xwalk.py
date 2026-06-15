"""community_cluster_xwalk for Tanzania NPS 2019-20 (NPS-SDD Extended Panel;
issue #113).

Crosswalk from the community price cluster (``interview__key``, the grain of
``community_prices``) to the household survey cluster (``sample().v`` ==
``clusterid``), so the community prices can serve as the #113 quantity/price
fallback.  Match is (region, ward) + interview-date disambiguation; the
district code is irreconcilable and dropped.  See
``tanzania.link_community_to_cluster`` and Tanzania/_/CONTENTS.org (#113) for
the full diagnosis and the measured match rates.

  v       = community interview__key (joins community_prices.v).
  cluster = resolved survey cluster (== clusterid == sample().v) where unique,
            else <NA>.
  region  = region code (always present; region-level fallback key).
  match   = 'cluster' | 'region'.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import link_community_to_cluster


cm = get_dataframe('../Data/CM_SEC_A.dta', convert_categoricals=False)
hh = get_dataframe('../Data/HH_SEC_A.dta', convert_categoricals=False)

df = link_community_to_cluster('2019-20', cm, hh)
assert df.index.is_unique, "community_cluster_xwalk 2019-20: (t,v) not unique"
assert len(df) > 0
to_parquet(df, 'community_cluster_xwalk.parquet')

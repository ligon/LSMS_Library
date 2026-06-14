"""community_cluster_xwalk for Tanzania NPS 2020-21 (NPS Y5 Refresh Panel;
issue #113).

Crosswalk from the community price cluster (``interview__key``, the grain of
``community_prices``) to the household survey cluster (``sample().v`` ==
``y5_cluster``).  Match is (region, ward) + interview-date disambiguation with a
NARROW window (the dense Refresh Panel re-ambiguates if widened); the district
code is irreconcilable (community = national code, cluster DD = within-region
index) and dropped.  See ``tanzania.link_community_to_cluster`` and
Tanzania/_/CONTENTS.org (#113).

  v       = community interview__key (joins community_prices.v).
  cluster = resolved survey cluster (== y5_cluster == sample().v) where unique,
            else <NA>.
  region  = region code (always present; region-level fallback key).
  match   = 'cluster' | 'region'.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import link_community_to_cluster


cm = get_dataframe('../Data/cm_sec_a.dta', convert_categoricals=False)
hh = get_dataframe('../Data/hh_sec_a.dta', convert_categoricals=False)

df = link_community_to_cluster('2020-21', cm, hh)
assert df.index.is_unique, "community_cluster_xwalk 2020-21: (t,v) not unique"
assert len(df) > 0
to_parquet(df, 'community_cluster_xwalk.parquet')

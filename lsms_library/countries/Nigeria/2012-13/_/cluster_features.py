#!/usr/bin/env python
"""cluster_features for Nigeria GHS-Panel 2012-13, at HOUSEHOLD grain.

Each round from its OWN cover page (GH #1120 D1), so cluster_features.v and
sample.v are one keyspace in both rounds; the household geovariables join
LEFT onto the round's cover (GH #1261).  Wave.cluster_features projects the
(t, i, v) frame onto (t, v) through the audited Site 2 collapse.  See
nigeria.cluster_features_for_wave and _/CONTENTS.org, "Sampling Design".
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from nigeria import cluster_features_for_wave

to_parquet(cluster_features_for_wave('2012-13'), 'cluster_features.parquet')

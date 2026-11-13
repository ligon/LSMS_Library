#!/usr/bin/env python
"""sample for Nigeria GHS-Panel 2015-16: each round from its OWN cover page.

The post-planting round (t = PP_QUARTER) reads the post-planting household
cover and the post-harvest round (t = PH_QUARTER) the post-harvest one, each
with its own weights -- GH #1120 D1 (@ligon, 2026-10-23).  Both rounds land
in this one wave parquet; the framework keeps the rows whose t is the
requested round.  See nigeria.round_covers and _/CONTENTS.org, "Sampling
Design".
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from nigeria import sample_for_wave

to_parquet(sample_for_wave('2015-16'), 'sample.parquet')

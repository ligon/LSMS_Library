#!/usr/bin/env python
import pandas as pd
import numpy as np
from lsms_library.local_tools import format_id

def cs_i(value):
    return 'cs-17-'+format_id(value[0])
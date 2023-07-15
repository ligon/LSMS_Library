#!/usr/bin/env python3
import json
from uganda import Waves
import sys
sys.path.append('../../_')
from local_tools import panel_ids, format_id

D = panel_ids(Waves)

with open('panel_ids.json','w') as f:
    json.dump(D.data,f)

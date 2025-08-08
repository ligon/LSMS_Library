#!/usr/bin/env python3
import json
from tanzania import Waves
import sys
sys.path.append('../../_')
from tanzania import panel_ids

D, updated_ids = panel_ids(Waves)

with open('panel_ids.json','w') as f:
    # Convert to JSON-safe dict
    json_ready = {','.join(k): ','.join(v) for k, v in D.data.items()}
    json.dump(json_ready,f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)

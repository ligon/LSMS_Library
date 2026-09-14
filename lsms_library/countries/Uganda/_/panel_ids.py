#!/usr/bin/env python3
import json
from pathlib import Path as _Path
from uganda import Waves
import sys
sys.path.append('../../_')
from lsms_library.local_tools import panel_ids

D, updated_ids= panel_ids(Waves)

# GH #914: write beside THIS script, never into the process's cwd.  These two
# files are committed sources; `make panel-ids` regenerates them in place for a
# maintainer to review as a diff.  Resolving off __file__ means a maintainer who
# runs the script from somewhere other than `_/` updates the tracked files
# rather than scattering copies.
_HERE = _Path(__file__).resolve().parent

with open(_HERE / 'panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in D.data.items()}
    json.dump(json_ready,f)

with open(_HERE / 'updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)

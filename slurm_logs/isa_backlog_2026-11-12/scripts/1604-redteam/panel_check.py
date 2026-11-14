"""Panel-id guard checks on the candidate clone.  usage: panel_check.py <clone> <out.json>"""
import importlib.util
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, out = sys.argv[1], Path(sys.argv[2])
assert_env(clone)
from lsms_library.local_tools import panel_ids, panel_ids_guarding  # noqa: E402

here = Path(clone) / 'lsms_library/countries/Malawi/_'
spec = importlib.util.spec_from_file_location('mw_panel_ids', here / 'panel_ids.py')
mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
frame = mod.build_frame()
R = {'frame_rows_by_t': frame.index.get_level_values('t').value_counts().sort_index().to_dict(),
     'frame_prev_null_by_t': frame['previous_i'].isna().groupby(level='t').sum().to_dict(),
     'guard_from': mod.guard_from(), 'panel_waves': mod.panel_waves()}
Dp, Up = panel_ids(frame)
Dg, Ug = panel_ids_guarding(frame, guard_from=mod.guard_from())
committed_U = json.loads((here / 'updated_ids.json').read_text())
committed_D = json.loads((here / 'panel_ids.json').read_text())
R['guarded_equals_committed_updated'] = Ug == committed_U
R['guarded_equals_committed_panel'] = {','.join(k): ','.join(v) for k, v in Dg.data.items()} == committed_D
R['recursive_equal_plain_vs_guarded'] = Dp.data == Dg.data
R['waves_identical_plain_vs_guarded'] = {w: Up[w] == Ug[w] for w in Up}
w = '2024-25'
issued = set()
for ww in sorted(Up):
    if ww < w:
        issued |= set(Up[ww].values())
moved = {k: (Up[w][k], Ug[w][k]) for k in Up[w] if Up[w][k] != Ug[w][k]}
R['moved_2024_ids'] = len(moved)
R['plain_minted_collide_issued'] = sum(1 for k, v in Up[w].items() if v in issued and v != Dp.data[(w, k)][1])
R['plain_2024_values_in_issued_not_own_lineage'] = R['plain_minted_collide_issued']
R['guarded_minted_collide_issued'] = sum(1 for k, v in Ug[w].items() if v in issued and v != Dg.data[(w, k)][1])
R['moved_examples'] = list(moved.items())[:10]
R['moved_plain_in_issued'] = sum(1 for a, b in moved.values() if a in issued)
# within-2019-20 analogue (left for a separate issue per docstring)
iss19 = set()
for ww in sorted(Up):
    if ww < '2019-20':
        iss19 |= set(Up[ww].values())
R['plain_2019_minted_collide_earlier'] = sum(1 for k, v in Up['2019-20'].items() if v in iss19 and v != Dp.data[('2019-20', k)][1])
out.write_text(json.dumps(R, indent=1, default=str))
print(json.dumps(R, indent=1, default=str)[:4000])

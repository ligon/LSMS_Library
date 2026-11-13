"""Import a country's _/panel_ids.py, build its frame + dicts, compare to Design-B captures."""
import importlib.util, json, os, sys, warnings
exec(open(os.path.join(os.path.dirname(__file__), 'assert_id.py')).read())
warnings.simplefilter('ignore')
import pandas as pd
from lsms_library.local_tools import panel_ids
from lsms_library.paths import countries_root
S = os.path.dirname(os.path.abspath(__file__))
for C in sys.argv[1:]:
    script = countries_root() / C / '_' / 'panel_ids.py'
    sys.path.insert(0, str(script.parent))
    spec = importlib.util.spec_from_file_location(f'pid_{C}', script)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    fr = mod.build_frame().reset_index()[['t', 'i', 'previous_i']]
    fb = pd.read_csv(f'{S}/out_base/{C}_frameB.csv', dtype=str, keep_default_na=False)
    fr_s = fr.astype(str)
    print(C, 'frame shapes', fr.shape, fb.shape)
    for t in fb['t'].unique():
        a = fr_s[fr_s.t == t].reset_index(drop=True); b = fb[fb.t == t].reset_index(drop=True)
        same_rows = a.equals(b)
        same_set = set(map(tuple, a.values)) == set(map(tuple, b.values))
        print(f'  {t}: rows {len(a)} vs {len(b)}; same set {same_set}; same ORDER {same_rows}')
    D, U = panel_ids(mod.build_frame())
    pj = {','.join(k): ','.join(v) for k, v in D.data.items()}
    pb = json.load(open(f'{S}/out_base/{C}_panel_ids.json'))
    ub = json.load(open(f'{S}/out_base/{C}_updated_ids.json'))
    print('  panel_ids equal', pj == pb, len(pj), len(pb), '| updated_ids equal', U == ub,
          {w: len(m) for w, m in U.items()})
    if pj != pb:
        diff = [k for k in set(pj) | set(pb) if pj.get(k) != pb.get(k)]
        print('  panel diffs', len(diff), diff[:5])
    for w in set(U) | set(ub):
        if U.get(w) != ub.get(w):
            d = [k for k in set(U.get(w, {})) | set(ub.get(w, {})) if U.get(w, {}).get(k) != ub.get(w, {}).get(k)]
            print('  updated diffs', w, len(d), [(k, U.get(w, {}).get(k), ub.get(w, {}).get(k)) for k in d[:5]])

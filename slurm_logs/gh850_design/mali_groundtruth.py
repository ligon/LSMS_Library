"""GH #850 -- grade the old and the corrected inference against an EXTERNAL
ground truth: Mali's own questionnaire unit list.

``Mali/_/CONTENTS.org:786-794`` records the contradiction in full: the library
infers Sac moyen -> 26.2 kg where the questionnaire says 50, Sac petit -> 5.3
where it says 25, Sac large -> 33.0 where it says 100, and Gramme -> 0.708
where a gramme is 0.001.  Those four are the only kg factors in the corpus with
a documented right answer, so they are the only place either inference can be
scored rather than merely compared.

Read-only.  Run with LSMS_DATA_DIR pointed at a scratch root.
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import measure as M                                          # noqa: E402
from lsms_library.transformations import _get_kg_factors      # noqa: E402

# From the questionnaire's own unit list, quoted at Mali/_/CONTENTS.org:790-792.
TRUTH = {'Sac large': 100.0, 'Sac moyen': 50.0, 'Sac petit': 25.0,
         'Gramme': 0.001}


def main():
    warnings.simplefilter('ignore')
    df = M.load('Mali')
    flat = M.prep(df)
    live = _get_kg_factors(df)
    old = M.infer_old(flat)
    b = M.infer_b(flat)
    per_ju, per_u, sup, nb = M.infer_c(flat, floor=M.DEFAULT_FLOOR)
    ju = pd.Series(per_ju, dtype=float)
    if len(ju):
        ju.index = pd.MultiIndex.from_tuples(ju.index, names=['j', 'u'])

    rows = []
    for u, truth in TRUTH.items():
        n = int((flat['u'] == u).sum())
        q = flat.loc[flat['u'] == u, 'Quantity']
        try:
            per_item = ju.xs(u, level='u')
        except (KeyError, IndexError):
            per_item = pd.Series(dtype=float)
        rows.append({
            'u': u, 'truth_kg': truth, 'n_rows': n,
            'Q_median': float(q.median()) if n else np.nan,
            'live': float(live.get(u.lower(), np.nan)),
            'old': float(old.get(u, np.nan)),
            'fix_b_only': float(b.get(u, np.nan)),
            'fix_c_pooled_u': float(per_u.get(u, np.nan)),
            'fix_c_per_j_median': float(per_item.median()) if len(per_item) else np.nan,
            'fix_c_n_items': int(len(per_item)),
        })
    out = pd.DataFrame(rows)
    for col in ('live', 'old', 'fix_b_only', 'fix_c_pooled_u', 'fix_c_per_j_median'):
        out[col + '_err'] = out[col] / out['truth_kg']
    here = os.path.dirname(os.path.abspath(__file__))
    out.to_csv(os.path.join(here, 'mali_groundtruth.csv'), index=False)
    with pd.option_context('display.width', 200, 'display.max_columns', 40):
        print(out.to_string(index=False))


if __name__ == '__main__':
    main()

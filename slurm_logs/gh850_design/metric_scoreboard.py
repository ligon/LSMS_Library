"""GH #850 -- grade both inferences on the unit labels whose kg factor is
KNOWN FROM THE LABEL ITSELF.

Two classes of label reach the price-ratio inference even though nobody needs
to infer anything about them:

(A) a bare metric spelling absent from ``KNOWN_METRIC`` (``Millilitre``,
    ``Grams``, ``Litres``, ``Gramme``, ``Milligram``);
(B) a label that NAMES its metric content in a spelling
    ``_parse_explicit_metric`` does not match -- ``lts``, ``ltrs``, ``lt`` and
    ``kgs`` all fail the regexes' ``\\b`` after ``l`` / ``kg``.

Both are ground truth, and neither is circular: the right answer comes from
the label, not from the frame the factor was inferred on.

Read-only.  Run with LSMS_DATA_DIR pointed at a scratch root.
"""
import os
import re
import sys
import warnings

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import measure as M                                            # noqa: E402
from lsms_library.transformations import (                      # noqa: E402
    KNOWN_METRIC, _get_kg_factors, _parse_explicit_metric)

COUNTRIES = ['Uganda', 'Malawi', 'Nigeria', 'Ethiopia', 'Tanzania', 'Niger',
             'GhanaLSS', 'Mali']

# (A) bare metric spellings -> kg per one unit (1 litre = 1 kg, the library's
#     own volume_as_mass default).
BARE = {'millilitre': .001, 'milliliter': .001, 'mili liter': .001,
        'millilitres': .001, 'grams': .001, 'gramme': .001, 'grammes': .001,
        'milligram': 1e-6, 'litres': 1., 'liters': 1., 'kilo': 1., 'kilos': 1.,
        'kgs': 1., 'gm': .001, 'gms': .001}

# (B) a number plus a metric token the parser's regexes miss.
UNMATCHED = re.compile(
    r'(\d+(?:\.\d+)?)\s*(lts|ltrs|ltr|lt|kgs|gms|grs)\b', re.IGNORECASE)
SCALE = {'lts': 1., 'ltrs': 1., 'ltr': 1., 'lt': 1., 'kgs': 1.,
         'gms': .001, 'grs': .001}


def truth_for(label):
    key = str(label).strip().lower()
    if key in KNOWN_METRIC:
        return None, None                      # already seeded, never inferred
    if _parse_explicit_metric(str(label)) is not None:
        return None, None                      # already parsed
    if key in BARE:
        return BARE[key], 'A: bare metric spelling'
    m = UNMATCHED.search(str(label))
    if m:
        return float(m.group(1)) * SCALE[m.group(2).lower()], 'B: unparsed content'
    return None, None


def main():
    warnings.simplefilter('ignore')
    rows = []
    for c in sys.argv[1:] or COUNTRIES:
        try:
            df = M.load(c)
        except Exception as exc:                                # noqa: BLE001
            print(f'{c}: FAILED {exc}')
            continue
        flat = M.prep(df)
        live = _get_kg_factors(df)
        old = M.infer_old(flat)
        b = M.infer_b(flat)
        per_ju, per_u, _sup, _nb = M.infer_c(flat, floor=M.DEFAULT_FLOOR)
        ju = pd.Series(per_ju, dtype=float)
        if len(ju):
            ju.index = pd.MultiIndex.from_tuples(ju.index, names=['j', 'u'])
        for u in sorted({str(x) for x in flat['u']}):
            truth, cls = truth_for(u)
            if truth is None:
                continue
            n = int((flat['u'].astype(str) == u).sum())
            if n < 25:
                continue
            try:
                pi = ju.xs(u, level='u')
            except (KeyError, IndexError):
                pi = pd.Series(dtype=float)
            rows.append({
                'country': c, 'class': cls, 'u': u, 'truth_kg': truth,
                'n_rows': n, 'n_items': int(flat.loc[flat['u'].astype(str) == u, 'j'].nunique()),
                'served_now': float(live.get(str(u).lower(), np.nan)),
                'fix_b': float(b.get(u, np.nan)),
                'fix_c_median_j': float(pi.median()) if len(pi) else np.nan,
            })
    out = pd.DataFrame(rows)
    for col in ('served_now', 'fix_b', 'fix_c_median_j'):
        out[col + '_x_truth'] = (out[col] / out['truth_kg']).round(3)
    here = os.path.dirname(os.path.abspath(__file__))
    out.to_csv(os.path.join(here, 'metric_scoreboard.csv'), index=False)
    with pd.option_context('display.width', 220, 'display.max_columns', 40):
        print(out.to_string(index=False))


if __name__ == '__main__':
    main()

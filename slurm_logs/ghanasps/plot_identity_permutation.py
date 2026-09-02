"""Is W2->W3 (i, plotid) the same physical plot?  (GhanaSPS, PR #729)

Test: for households present in both waves, compare attributes that ought to be
time-invariant for a given piece of land.  Compare the TRUE pairing against a
NULL built by permuting plotid within the household -- if the true rate is not
above the null, the numbering carries no information.

Result (2026-08-24): it does not.  True 15.2% area agreement against a 14.1%
null; +1.1 points.  `plotid` in 2013-14 / 2017-18 is a within-interview
enumeration order, not a persistent plot id.

NOT A PYTEST FILE.  Deliberately named without a `test` prefix or `_test`
suffix so pytest cannot collect it, and all I/O sits behind the __main__ guard
so importing it can never touch the network.  A module-level data load here
raises NoCredentialsError under CI, and a *collection* error aborts the entire
pytest session -- zero tests run, not one (cf. GH #680).

Run it:
    LSMS_COUNTRIES_ROOT=<repo>/lsms_library/countries \
    LSMS_DATA_DIR=<scratch> PYTHONPATH=<repo> \
    <repo>/.venv/bin/python slurm_logs/ghanasps/plot_identity_permutation.py
"""
import warnings

import numpy as np
import pandas as pd

# Hectares per native plot-size unit, recovered from 2009-10 S4AII.dta's
# producer-computed `area_ha` divided by the reported native size (within-unit
# sd ~1e-8).  See slurm_logs/ghanasps/FINDINGS_agriculture.org.
HA = {'acre': 0.404694, 'acres': 0.404694, 'pole': 0.409551,
      'poles': 0.409551, 'rope': 0.236342, 'ropes': 0.236342,
      'robes': 0.236342, 'plot': 0.102388, 'plots': 0.102388}


def _load(wave, name):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(f'../../GhanaSPS/{wave}/Data/{name}.dta')


def prep(wave):
    """Plot-level frame of putatively time-invariant attributes."""
    d = _load(wave, '04h_agsection')
    # 2017-18 puts the acre answer in `plotunit`; `plotsizeunit` omits acres
    # entirely and is null for 89.3% of that wave's plots.  See GH #732.
    unit = d['plotunit'] if 'plotunit' in d.columns else d['plotsizeunit']
    factor = unit.astype(str).str.strip().str.lower().map(HA)
    out = pd.DataFrame({
        'i': d.FPrimary.astype(str),
        'plotid': pd.to_numeric(d.plotid, errors='coerce'),
        'ha': pd.to_numeric(d.totalsize, errors='coerce') * factor,
        'soilcolor': d.soilcolor.astype(str).str.strip().str.lower(),
        'dist': pd.to_numeric(d.plotdistance, errors='coerce'),
        'region': d.plotlocation_region.astype(str).str.strip().str.lower(),
    })
    return out.dropna(subset=['plotid'])


def agree(m):
    """Agreement rate per attribute on a merged (true or permuted) pairing."""
    r = {}
    ok = m.ha_x.notna() & m.ha_y.notna()
    r['area within 10%'] = (
        (np.abs(m.ha_x - m.ha_y) <= 0.10 * np.maximum(m.ha_x, m.ha_y))[ok].mean(),
        int(ok.sum()))
    for col, lab in [('soilcolor', 'soil colour'), ('region', 'plot region')]:
        x, y = m[f'{col}_x'], m[f'{col}_y']
        ok = (~x.isin(['nan', ''])) & (~y.isin(['nan', '']))
        r[lab] = ((x == y)[ok].mean(), int(ok.sum()))
    ok = m.dist_x.notna() & m.dist_y.notna()
    r['distance within 20%'] = (
        (np.abs(m.dist_x - m.dist_y)
         <= 0.20 * np.maximum(m.dist_x, m.dist_y))[ok].mean(), int(ok.sum()))
    return r


def null_draws(a2, a3, keys, rng, draws=20):
    """Agreement under plotid permuted WITHIN household in the later wave."""
    acc = {k: [] for k in keys}
    for _ in range(draws):
        b3 = a3.copy()
        b3['plotid'] = b3.groupby('i')['plotid'].transform(
            lambda s: rng.permutation(s.values))
        m = a2.merge(b3, on=['i', 'plotid'], suffixes=('_x', '_y'))
        for k, (v, _n) in agree(m).items():
            acc[k].append(v)
    return acc


def main():
    warnings.filterwarnings('ignore')
    rng = np.random.default_rng(0)

    a2, a3 = prep('2013-14'), prep('2017-18')
    common = set(a2.i) & set(a3.i)
    a2, a3 = a2[a2.i.isin(common)], a3[a3.i.isin(common)]
    print(f'households in both waves: {len(common)}   '
          f'W2 plots {len(a2)}   W3 plots {len(a3)}')

    true = a2.merge(a3, on=['i', 'plotid'], suffixes=('_x', '_y'))
    print(f'\nTRUE pairing on (i, plotid): {len(true)} matched plots')
    tr = agree(true)
    for k, (v, n) in tr.items():
        print(f'  {k:<22} {100*v:5.1f}%   (n={n})')

    print('\nNULL (plotid permuted within household in W3), 20 draws:')
    acc = null_draws(a2, a3, tr, rng)
    for k in tr:
        mu, sd = np.mean(acc[k]), np.std(acc[k])
        t, _n = tr[k]
        print(f'  {k:<22} null {100*mu:5.1f}% +/- {100*sd:.1f}   '
              f'true {100*t:5.1f}%   lift {100*(t-mu):+5.1f} pts')

    # Households with one plot in each wave make the permutation a no-op, so
    # they inflate the null.  Re-run without them.
    n2, n3 = a2.groupby('i').size(), a3.groupby('i').size()
    single = {h for h in common if n2.get(h, 0) == 1 and n3.get(h, 0) == 1}
    print(f'\nhouseholds with exactly ONE plot in both waves: {len(single)} '
          f'({100*len(single)/len(common):.1f}% of common) -- for these the '
          f'permutation is a no-op, so the null above is inflated')

    multi = true[~true.i.isin(single)]
    print(f'\nTRUE pairing restricted to MULTI-plot households '
          f'({len(multi)} plots):')
    tm = agree(multi)
    for k, (v, n) in tm.items():
        print(f'  {k:<22} {100*v:5.1f}%   (n={n})')

    a2m, a3m = a2[~a2.i.isin(single)], a3[~a3.i.isin(single)]
    acc2 = null_draws(a2m, a3m, tr, rng)
    print('  ...against the multi-plot null:')
    for k in tr:
        mu, sd = np.mean(acc2[k]), np.std(acc2[k])
        t, _n = tm[k]
        print(f'  {k:<22} null {100*mu:5.1f}% +/- {100*sd:.1f}   '
              f'true {100*t:5.1f}%   lift {100*(t-mu):+5.1f} pts')

    print('\nNOTE: `plot region` is uninformative here -- plotlocation_region '
          'is populated for only ~66 of the matched plots, so its 0.0% is an '
          'artefact of emptiness, not evidence.')


if __name__ == '__main__':
    main()

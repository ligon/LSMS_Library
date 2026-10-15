"""The CFE beta-spread gate for proposed GhanaLSS Aggregate groups.

The rule is the one ``.claude/skills/add-feature/food-acquired/SKILL.md``
("Designing the Aggregate Label") requires of every multi-member bucket:

    max(beta) - min(beta) < 1.348 * sigma_min

over the group's members, where sigma_min is the smallest standard error
among them.  Two disciplines, both of which matter:

* beta and sigma come from ONE fit that contains every member.  CFE's beta
  is a rank-1 factor identified up to a per-fit scale, so loadings from two
  fits are not comparable.  Where every member survives the full fine
  preparation the full fit is used; otherwise the members are fitted
  TOGETHER beside the same dense reference foods (the probe design of
  ``ghana_validation.probe_loadings``), and that one probe fit supplies
  both beta and sigma.
* the fitting population is the 60% training split of
  ``ghana_validation.household_split`` by default, so the gate is read on
  the same households as PR #937's validation.  ``--population all`` fits
  the whole wave instead: CFE's ``prepare_data`` keeps a pair of goods only
  if at least ``min_obs`` households report BOTH, so a sparse pair
  (Macaroni x Spaghetti: 45 households in all of 2016-17, ~27 in the
  split) can be jointly identified only on the full wave.  More households
  shrink sigma, so the full-wave gate is the stricter of the two, not a
  relaxation.
* a member delivered in no wave together with another member -- a
  cross-wave coarse label such as ``Rice`` (GLSS1-4) beside
  ``Rice (imported)`` / ``Rice (local)`` (GLSS5-7) -- cannot be gated; it
  joins only if the within-wave constituents pass, which is the PR #937
  precedent for ``Macaroni/Spaghetti``.  A group with fewer than two
  members delivered in the gated wave is reported ``not_cooccurring``.

Standard errors are CFEDemands' resampling bootstrap
(``Regression.get_beta(compute_se=True)``, >= 30 draws of the residual
matrix within (t, m), IQR-based sigma), which draws from NumPy's global
generator; it is seeded with ``--seed`` before the fits so a rerun is
reproducible.  The gate is scale-free, so the reported betas are only put
in Salt units for readability.

Decision rule used for the 2026-09-17 selection (``selected_groups.yml``):
the 2016-17 purchased training split (seed 20260916) is primary, the full
2016-17 wave where the split cannot jointly identify the members; a group
must ALSO pass in every other wave where two or more members are delivered
(full wave); the second split seed and the total basis are sensitivity
runs and are reported, not decisive.

Only aggregate outputs are written: one row per candidate group and one
row per fitted good.  No household record leaves the process.
"""

import argparse
import json
from pathlib import Path
import sys
import time
import warnings

import numpy as np
import pandas as pd
import yaml

from .ghana_validation import evaluation_arguments, household_split, load_ghana

K = 1.348          # 50% two-sided normal interval half-width, in sigma units
N_REFERENCE = 8    # densest non-member foods fitted beside the members in a probe


def _fit(logs, d, fit_options):
    from cfe.regression import Regression

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        model = Regression(y=logs, d=d, **fit_options)
        beta = model.get_beta(compute_se=True)
    se = model.beta_se
    return beta.astype(float), se.astype(float), len(caught)


def _reference(x_train, members, anchor):
    counts = x_train.gt(0).sum().drop(list(members), errors='ignore').drop(anchor, errors='ignore')
    return sorted(counts.nlargest(N_REFERENCE).index.tolist() + [anchor])


def gate_groups(data, specification, *, seed=20260916, reserved=('Onion', 'Egg'),
                anchor='Salt', population='train', output=None):
    split = household_split(data.strata, seed)
    args = evaluation_arguments(data, split, reserved=reserved, anchor=anchor)
    if population == 'train':
        train = data.x.index[split == 'train']
    elif population == 'all':
        train = data.x.index
    else:
        raise ValueError('population must be "train" or "all"')
    x_train = args['x'].loc[train]
    logs = np.log(x_train.where(x_train > 0))
    d = data.d.loc[train]
    # CFE's SE bootstrap resamples with pandas' groupby.sample and no
    # random_state, i.e. from NumPy's global generator; seed it so a rerun
    # reproduces sigma (measured: unseeded reruns moved ratios by ~0.05).
    np.random.seed(seed)
    start = time.monotonic()
    fine_beta, fine_se, n_warn = _fit(logs, d, args['fit_options'])
    print(f'{data.profile["wave"]} {data.profile["basis"]} fine fit: {len(fine_beta)} goods, '
          f'{time.monotonic()-start:.0f}s, {n_warn} warnings', file=sys.stderr, flush=True)
    scale = float(fine_beta[anchor]) if anchor in fine_beta and fine_beta[anchor] > 0 else np.nan
    loadings = pd.DataFrame({'beta_raw': fine_beta, 'se_raw': fine_se})
    loadings['beta_anchor_units'] = loadings.beta_raw / scale
    loadings['se_anchor_units'] = loadings.se_raw / scale
    loadings['n_train_positive'] = x_train.gt(0).sum().reindex(loadings.index)
    loadings.index.name = 'j'
    loadings['fit'] = 'fine'
    rows = []
    probe_rows = []
    delivered = set(args['x'].columns)
    for section in ('groups', 'deferred_alternatives'):
        for group in specification.get(section, []):
            members = [m for m in group['members'] if m in delivered]
            absent = [m for m in group['members'] if m not in delivered]
            row = dict(id=group['id'], label=group['label'], section=section,
                       members_in_wave=json.dumps(members), members_absent=json.dumps(absent),
                       n_in_wave=len(members))
            if len(members) < 2:
                row.update(status='not_cooccurring', fit='', spread=np.nan, sigma_min=np.nan,
                           ratio=np.nan, passes=None, betas='', ses='')
                rows.append(row)
                print(f'  {group["id"]}: not co-occurring in this wave', file=sys.stderr, flush=True)
                continue
            if all(m in fine_beta.index for m in members):
                beta, se, fit = fine_beta.loc[members], fine_se.loc[members], 'fine'
                unit = scale
            else:
                reference = _reference(x_train, members, anchor)
                cols = reference + members
                t0 = time.monotonic()
                try:
                    pb, pse, pn = _fit(logs[cols], d, args['fit_options'])
                except (ValueError, AssertionError, np.linalg.LinAlgError) as exc:
                    row.update(status=f'probe_failed: {exc}', fit='probe', spread=np.nan,
                               sigma_min=np.nan, ratio=np.nan, passes=None, betas='', ses='')
                    rows.append(row)
                    print(f'  {group["id"]}: probe failed ({exc})', file=sys.stderr, flush=True)
                    continue
                unit = float(pb[anchor]) if anchor in pb and pb[anchor] > 0 else np.nan
                missing = [m for m in members if m not in pb.index]
                for j in pb.index:
                    probe_rows.append(dict(j=j, group=group['id'], beta_raw=pb[j], se_raw=pse[j],
                                           beta_anchor_units=pb[j]/unit, se_anchor_units=pse[j]/unit,
                                           n_train_positive=int(x_train[j].gt(0).sum()),
                                           fit='probe', is_member=j in members))
                print(f'  {group["id"]}: probe fit {len(pb)} goods, {time.monotonic()-t0:.0f}s',
                      file=sys.stderr, flush=True)
                if missing:
                    row.update(status='member_unidentified', fit='probe', spread=np.nan,
                               sigma_min=np.nan, ratio=np.nan, passes=None,
                               betas=json.dumps({m: float(pb[m]/unit) for m in members if m in pb.index}),
                               ses=json.dumps({m: float(pse[m]/unit) for m in members if m in pb.index}),
                               members_absent=json.dumps(absent + missing))
                    rows.append(row)
                    continue
                beta, se, fit = pb.loc[members], pse.loc[members], 'probe'
            spread = float(beta.max() - beta.min())
            sigma_min = float(se.min())
            ratio = spread / (K * sigma_min) if sigma_min > 0 else np.inf
            row.update(status='gated', fit=fit, spread=spread/unit, sigma_min=sigma_min/unit,
                       ratio=ratio, passes=bool(ratio < 1),
                       betas=json.dumps({m: float(beta[m]/unit) for m in members}),
                       ses=json.dumps({m: float(se[m]/unit) for m in members}))
            rows.append(row)
            print(f'  {group["id"]}: {fit}, spread {spread/unit:.3f}, sigma_min {sigma_min/unit:.3f}, '
                  f'ratio {ratio:.2f} -> {"PASS" if ratio < 1 else "FAIL"}', file=sys.stderr, flush=True)
    result = pd.DataFrame(rows)
    if output is not None:
        output.mkdir(parents=True, exist_ok=True)
        result.to_csv(output / 'gate.csv', index=False)
        loadings.reset_index().to_csv(output / 'loadings.csv', index=False)
        pd.DataFrame(probe_rows).to_csv(output / 'probe_loadings.csv', index=False)
        metadata = dict(**data.profile, seed=seed, reserved=list(reserved), anchor=anchor,
                        population=population, n_fit_households=int(len(train)),
                        rule=f'max(beta)-min(beta) < {K} * sigma_min, one fit per group',
                        n_reference=N_REFERENCE, fit_options=args['fit_options'],
                        se='CFEDemands Regression.get_beta(compute_se=True) resampling bootstrap',
                        specification=str(specification.get('source_revision')),
                        n_fine_goods=int(len(fine_beta)))
        (output / 'gate_specification.yml').write_text(yaml.safe_dump(metadata, sort_keys=False))
    return result, loadings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--wave', default='2016-17')
    parser.add_argument('--basis', choices=['purchased', 'total'], default='purchased')
    parser.add_argument('--seed', type=int, default=20260916)
    parser.add_argument('--reserved', nargs='*', default=['Onion', 'Egg'])
    parser.add_argument('--anchor', default='Salt')
    parser.add_argument('--population', choices=['train', 'all'], default='train')
    parser.add_argument('--specification', type=Path,
                        default=Path(__file__).parent / 'ghanalss' / 'proposed_groups.yml')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    specification = yaml.safe_load(args.specification.read_text())
    data = load_ghana(args.wave, args.basis)
    result, _ = gate_groups(data, specification, seed=args.seed, reserved=args.reserved,
                            anchor=args.anchor, population=args.population, output=args.output)
    print(result[['id', 'status', 'fit', 'spread', 'sigma_min', 'ratio', 'passes']].to_string())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

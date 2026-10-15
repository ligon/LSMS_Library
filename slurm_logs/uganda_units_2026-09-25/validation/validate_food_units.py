#!/usr/bin/env python3
"""Cold-build and compare Uganda food units in explicitly private directories.

Keep generated pickle files and raw build logs private: they contain household
records. Only the aggregate comparison JSON is suitable for a durable report.
Example invocation is recorded beside this driver in the validation report.
No fixture, golden, repository source, or existing cache is changed by this tool.
"""
import argparse
import collections
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import warnings

import numpy as np
import pandas as pd
import pandas.testing as pdt

TABLES = ('food_acquired', 'food_quantities', 'food_prices',
          'food_expenditures', 'food_kg_factors', 'fct', 'nutrition')


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + '\n')


def load_frames(directory, tag):
    return {name: pd.read_pickle(directory / f'{tag}_{name}.pkl')
            for name in TABLES}


def build(args):
    import lsms_library as ll
    from lsms_library.transformations import food_kg_factors
    import pyarrow

    root = args.checkout.resolve()
    assert Path.cwd().resolve() == root, 'Run with cwd equal to --checkout'
    assert Path(ll.__file__).resolve().is_relative_to(root), ll.__file__
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == root / 'lsms_library/countries'
    assert Path(os.environ['LSMS_DATA_DIR']).resolve() == args.data_root.resolve()
    assert not list(args.data_root.glob('Uganda/**/*.parquet')), 'Cold L2 required'
    assert not list(args.out.glob(f'{args.tag}_*.pkl')), 'Never overwrite saved frames'
    assert args.data_root.joinpath('dvc-cache').is_symlink(), 'Only L1 should be shared'
    assert not subprocess.check_output([
        'git', 'diff', args.expected_source, '--', 'lsms_library', 'tests'
    ]), 'Source or tests differ from expected source commit'
    args.out.mkdir(parents=True, exist_ok=True)
    result = {
        'source_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
        'expected_source': args.expected_source,
        'import': ll.__file__, 'python': sys.version,
        'versions': {'pandas': pd.__version__, 'numpy': np.__version__,
                     'pyarrow': pyarrow.__version__},
        'tables': {}, 'errors': {},
    }
    country = ll.Country('Uganda')
    frames = {}
    for name in TABLES:
        start = time.monotonic()
        print('START', name, flush=True)
        try:
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter('always')
                if name == 'food_kg_factors':
                    frame = food_kg_factors(frames['food_acquired'],
                                           unit_kg=country._unit_kg_factors())
                else:
                    frame = getattr(country, name)()
            assert isinstance(frame, pd.DataFrame), (name, type(frame))
            frames[name] = frame
            frame.to_pickle(args.out / f'{args.tag}_{name}.pkl')
            result['tables'][name] = {
                'shape': list(frame.shape), 'index': frame.index.names,
                'index_unique': frame.index.is_unique,
                'seconds': time.monotonic() - start,
                'warning_classes': dict(collections.Counter(type(w.message).__name__
                                                            for w in captured)),
            }
            print('DONE', name, frame.shape, flush=True)
        except Exception as exc:
            result['errors'][name] = {'class': type(exc).__name__, 'message': str(exc)}
            write_json(args.out / f'{args.tag}_portable_build.json', result)
            raise
        write_json(args.out / f'{args.tag}_portable_build.json', result)
    result['price_modes'] = {}
    for mode in ('unitvalue', 'unitprice', 'kgprice'):
        print('START price mode', mode, flush=True)
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            frame = country.food_prices(units=mode)
        frame.to_pickle(args.out / f'{args.tag}_food_prices_{mode}.pkl')
        records = {}
        for unit in ('Value', 'Unknown'):
            selected = frame.loc[frame.index.get_level_values('u') == unit]
            records[unit] = {'rows': len(selected)}
            if len(selected):
                records[unit].update(min=float(selected.Price.min()),
                                     max=float(selected.Price.max()))
            if mode == 'kgprice' or (unit == 'Value' and mode == 'unitprice'):
                assert not len(selected)
            if unit == 'Value' and mode == 'unitvalue' and len(selected):
                assert selected.Price.eq(1).all()
        result['price_modes'][mode] = {'rows': len(frame), 'nonphysical': records}
        print('DONE price mode', mode, len(frame), flush=True)
    assert not subprocess.check_output([
        'git', 'diff', args.expected_source, '--', 'lsms_library', 'tests'
    ]), 'Source or tests changed during build'
    write_json(args.out / f'{args.tag}_portable_build.json', result)
    print('SOURCE_DEPENDENT_BUILD_DONE', flush=True)


def inspect_frames(frames):
    acquired = frames['food_acquired']
    factors = frames['food_kg_factors']
    quantity = frames['food_quantities']
    fct = frames['fct']
    nutrition = frames['nutrition']
    assert acquired.index.equals(factors.index)
    flat = acquired.reset_index()
    kg = acquired.Quantity * factors.kg_per_unit
    converted = kg.notna().to_numpy()
    flat['u'] = np.where(converted, 'kg', flat.u)
    flat['Quantity'] = np.where(converted, kg, acquired.Quantity)
    keep = flat.Quantity.notna() & flat.Quantity.ne(0)
    expected_q = flat.loc[keep].groupby(['t', 'i', 'j', 'u', 's']).Quantity.sum().sort_index()
    actual_q = quantity.Quantity.droplevel('v').reorder_levels(expected_q.index.names).sort_index()
    pdt.assert_series_equal(expected_q, actual_q, check_names=False,
                            check_exact=False, rtol=1e-12, atol=1e-10)
    qkg = quantity.loc[quantity.index.get_level_values('u').str.lower() == 'kg']
    amounts = qkg.Quantity.groupby(level=['i', 't', 'j']).sum().unstack('j').fillna(0)
    common = amounts.columns.intersection(fct.columns)
    amounts = amounts.reindex(columns=common)
    density = fct.reindex(columns=common).fillna(0)
    expected_n = (amounts @ density.T).sort_index()
    actual_n = nutrition.droplevel('v').reorder_levels(['i', 't']).sort_index()
    actual_n = actual_n.reindex(columns=expected_n.columns)
    pdt.assert_frame_equal(expected_n, actual_n, check_names=False,
                           check_exact=False, rtol=1e-12, atol=1e-8)
    manual = np.zeros(expected_n.shape)
    amounts = amounts.reindex(expected_n.index)
    for food in common:
        manual += amounts[food].to_numpy()[:, None] * density[food].to_numpy()[None, :]
    np.testing.assert_allclose(manual, expected_n.to_numpy(), rtol=1e-12, atol=1e-8)
    matched = qkg.index.get_level_values('j').isin(common)
    unit_counts = quantity.groupby(level='u', dropna=False).size().to_dict()
    return {
        'checks': {'factor_times_quantity': True, 'kg_quantity_dot_fct': True,
                   'explicit_food_sum': True},
        'kg_rows': len(qkg), 'kg_total': float(qkg.Quantity.sum()),
        'quantity_rows_by_u': unit_counts,
        'factor_sources': factors.KgFactorSource.value_counts(dropna=False).to_dict(),
        'nutrition_rows': len(nutrition),
        'nutrition_max_abs_replay_delta': float((expected_n - actual_n).abs().max().max()),
        'explicit_food_sum_max_abs_delta': float(np.abs(manual - expected_n.to_numpy()).max()),
        'fct_kg_coverage': {'matched_rows': int(matched.sum()),
                          'matched_kg': float(qkg.loc[matched].Quantity.sum()),
                          'unmatched_kg': float(qkg.loc[~matched].Quantity.sum()),
                          'missing_food_labels': sorted(set(qkg.index.get_level_values('j')) - set(common))},
    }


def compare(args):
    base = load_frames(args.baseline_dir, args.baseline_tag)
    candidate = load_frames(args.candidate_dir, args.candidate_tag)
    result = {'baseline': inspect_frames(base), 'candidate': inspect_frames(candidate),
              'checks': {}, 'tolerances': {'quantity': {'rtol': 1e-12, 'atol': 1e-10},
                                         'nutrition': {'rtol': 1e-12, 'atol': 1e-8}}}
    for name in ('food_expenditures', 'fct'):
        left = base[name].sort_index().sort_index(axis=1)
        right = candidate[name].sort_index().sort_index(axis=1)
        pdt.assert_frame_equal(left, right, check_exact=True)
        result['checks'][name + '_exact'] = True
    left = base['food_acquired']; right = candidate['food_acquired']
    keys = ['t', 'i', 'j', 's']
    pdt.assert_series_equal(left.Expenditure.groupby(level=keys, dropna=False).sum(min_count=1).sort_index(),
                            right.Expenditure.groupby(level=keys, dropna=False).sum(min_count=1).sort_index(),
                            check_exact=True)
    result['checks']['all_source_expenditures_exact'] = True
    for levels in (['t', 'i'], ['t', 'i', 's'], keys):
        def coverage(frame):
            return frame.index.to_frame(index=False)[levels].drop_duplicates().sort_values(levels).reset_index(drop=True)
        pdt.assert_frame_equal(coverage(left), coverage(right), check_exact=True)
        result['checks']['coverage_' + ','.join(levels)] = True
    physical = left.loc[left.index.get_level_values('u') != '---']
    assert physical.index.isin(right.index).all()
    pdt.assert_frame_equal(physical.sort_index(), right.reindex(physical.index).sort_index(), check_exact=True)
    result['checks']['existing_physical_rows_exact'] = True
    old_prices = base['food_prices']
    old_prices = old_prices.loc[old_prices.index.get_level_values('u') != '---']
    pdt.assert_frame_equal(old_prices.sort_index(),
                           candidate['food_prices'].reindex(old_prices.index).sort_index(),
                           check_exact=True)
    result['checks']['existing_label_kg_prices_exact'] = True
    common = base['food_kg_factors'].index.intersection(candidate['food_kg_factors'].index)
    old = base['food_kg_factors'].reindex(common); new = candidate['food_kg_factors'].reindex(common)
    result['factor_column_changes_on_unchanged_keys'] = {}
    for column in old:
        equal = old[column].eq(new[column]) | (old[column].isna() & new[column].isna())
        result['factor_column_changes_on_unchanged_keys'][column] = int((~equal).fillna(False).sum())
    result['candidate_nonphysical'] = {}
    for unit in ('Value', 'Unknown'):
        mask = right.index.get_level_values('u') == unit
        factor = candidate['food_kg_factors'].loc[mask]
        assert factor.kg_per_unit.isna().all()
        default_prices = candidate['food_prices']
        assert not (default_prices.index.get_level_values('u') == unit).any()
        selected = right.loc[mask]
        if unit == 'Value':
            pdt.assert_series_equal(selected.Quantity, selected.Expenditure,
                                    check_exact=True, check_names=False)
            assert selected.Price.isna().all()
        result['candidate_nonphysical'][unit] = {
            'rows': len(selected), 'nonmissing_factors': int(factor.kg_per_unit.notna().sum()),
            'expenditure': float(selected.Expenditure.sum()),
        }
    old_n = base['nutrition'].droplevel('v').sort_index()
    new_n = candidate['nutrition'].droplevel('v').sort_index()
    lost = old_n.index.difference(new_n.index); added = new_n.index.difference(old_n.index)
    surviving = old_n.index.intersection(new_n.index)
    delta = new_n.loc[surviving] - old_n.loc[surviving]
    result['nutrition_changes'] = {
        'lost_rows': len(lost), 'added_rows': len(added),
        'lost_base_zero_vectors': int(old_n.loc[lost].eq(0).all(axis=1).sum()),
        'lost_base_nonzero_vectors': int(old_n.loc[lost].ne(0).any(axis=1).sum()),
        'surviving_changed_rows': int(delta.ne(0).any(axis=1).sum()),
        'all_household_nutrient_row_delta_sums': new_n.subtract(old_n, fill_value=0).sum().to_dict(),
        'all_household_nutrient_total_subtractions': (new_n.sum() - old_n.sum()).to_dict(),
    }
    write_json(args.report, result)
    print(json.dumps(result, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    p = commands.add_parser('build')
    p.add_argument('--checkout', type=Path, required=True)
    p.add_argument('--data-root', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--tag', required=True)
    p.add_argument('--expected-source', required=True)
    p.set_defaults(func=build)
    p = commands.add_parser('compare')
    p.add_argument('--baseline-dir', type=Path, required=True)
    p.add_argument('--candidate-dir', type=Path, required=True)
    p.add_argument('--baseline-tag', required=True)
    p.add_argument('--candidate-tag', required=True)
    p.add_argument('--report', type=Path, required=True)
    p.set_defaults(func=compare)
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()

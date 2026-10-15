"""Inventory delivered GhanaLSS food labels and an explicit draft partition.

Only aggregate reports are written. Country owns extraction and valuation;
the draft has no effect on the public labels API. See ledger sections 2--5:
.coder/ledger/ghanalss-aggregate-curation.md.
"""

import argparse
from collections import Counter
from pathlib import Path
import sys
import warnings

import numpy as np
import pandas as pd
import yaml

from .evaluation import aggregate_expenditures


NONFOOD = frozenset(['Cigarette', 'Cigarrette', 'Tobacco', 'Other Tobacco'])


def _labels(values):
    return {str(v).strip() for v in values if pd.notna(v) and str(v).strip()}


def _delivered_goods(food):
    # Documentary strings may need trimming; delivered identities must not.
    for level in ('i', 't', 'j'):
        if food.index.get_level_values(level).hasnans:
            raise ValueError(f'Delivered {level} keys cannot be missing.')
    goods = food.index.get_level_values('j').unique()
    if any(not isinstance(good, str) or not good.strip() for good in goods):
        raise ValueError('Delivered food labels must be nonempty strings.')
    return pd.Index(sorted(goods), name='j')


def draft_partition(goods, specification):
    """Default to singletons; apply only explicitly listed, disjoint groups.

    A proposed member absent from the delivered universe is reported, never
    matched by spelling or silently assigned to some similar item (ledger 4).
    """
    goods = pd.Index(sorted(goods), name='j')
    if not goods.is_unique:
        raise ValueError('The delivered item universe must be unique.')
    membership = pd.Series(goods, index=goods, name='Aggregate Label')
    details = {g: dict(status='singleton', reason='No merge proposed at this stage.',
                       evidence='', group_id='') for g in goods}
    seen, names, identifiers, missing = set(), set(), set(), []
    for group in specification['groups']:
        members = group['members']
        label = group['label']
        if not group.get('id') or group['id'] in identifiers:
            raise ValueError('Proposed groups need distinct, nonempty identifiers.')
        if not label or len(members) < 2 or len(set(members)) != len(members):
            raise ValueError('Each proposed group needs a label and distinct members.')
        if seen.intersection(members) or label in names:
            raise ValueError('Proposed groups must be disjoint with unique output labels.')
        if set(members).intersection(NONFOOD):
            raise ValueError('Nonfoods cannot be members of proposed food groups.')
        if label in goods and label not in members:
            raise ValueError(f'Group label {label!r} collides with an existing singleton.')
        if not group.get('reason') or not group.get('evidence'):
            raise ValueError('Every proposed group needs its reason and evidence.')
        seen.update(members)
        names.add(label)
        identifiers.add(group['id'])
        available = [g for g in members if g in goods]
        missing.extend((group['id'], g) for g in members if g not in goods)
        # With fewer than two delivered members there is no observed merge.
        if len(available) > 1:
            membership.loc[available] = label
            for good in available:
                details[good] = dict(status='draft_merge', reason=group['reason'],
                                     evidence='; '.join(group['evidence']), group_id=group['id'])
    for good in goods.intersection(pd.Index(list(NONFOOD))):
        details[good] = dict(status='nonfood_singleton', reason='Excluded from food support profiles.',
                             evidence='Delivered item label.', group_id='')
    for good, caution in specification.get('singleton_cautions', {}).items():
        if good in details and details[good]['status'] == 'singleton':
            details[good]['reason'] = caution
    review = pd.DataFrame.from_dict(details, orient='index').rename_axis('Preferred Label')
    review.insert(0, 'Aggregate Label', membership)
    return membership, review, missing


def label_provenance(goods, country_table, wave_tables):
    """Record exact identities and documentary crosswalks without renaming."""
    rows = []
    country_labels = _labels(country_table['Preferred Label'])
    for good in sorted(goods):
        waves, sources, candidates = [], [], set()
        for wave, table in wave_tables.items():
            selected = table.loc[table['Preferred Label'].eq(good)]
            if not selected.empty:
                waves.append(wave)
                for column in selected.columns:
                    if column.startswith('Code_') or column.startswith('Label_'):
                        values = sorted(_labels(selected[column]))
                        if values:
                            sources.append(f'{wave}/{column}: {", ".join(values)}')
            if wave in country_table:
                matched = country_table.loc[country_table[wave].eq(good), 'Preferred Label']
                candidates.update(_labels(matched))
        rows.append(dict(preferred=good, in_country_preferred=good in country_labels,
                         declared_waves='; '.join(waves), source_rows='; '.join(sources),
                         country_crosswalk_targets='; '.join(sorted(candidates))))
    return pd.DataFrame(rows).set_index('preferred').rename_axis('Preferred Label')


def _sample(sample, wave):
    if sample.index.names != ['i', 't'] or not sample.index.is_unique:
        raise ValueError('sample must have unique (i,t) rows.')
    if any(sample.index.get_level_values(level).hasnans for level in ('i', 't')):
        raise ValueError('Sample household and wave keys cannot be missing.')
    result = sample.loc[sample.index.get_level_values('t') == wave]
    if result.empty:
        raise ValueError(f'No sample denominator for {wave}.')
    return result


def _amounts(food, basis):
    # Monetary derivation is reused, including its zero convention and source
    # selection. No valuation ladder or new quantity-times-price calculation.
    from lsms_library.transformations import food_expenditures_from_acquired

    values = food_expenditures_from_acquired(food, basis=basis)['Expenditure']
    return values.groupby(level=['i', 't', 'j'], observed=True,
                          dropna=False).sum(min_count=1).unstack('j')


def _grouped(x, membership):
    # Reuse the evaluator's exhaustive level sum by supplying one artificial
    # market. This adapter does not estimate a model or infer actual markets.
    adapted = x.copy()
    adapted.index = pd.MultiIndex.from_arrays(
        [x.index.get_level_values('i'), x.index.get_level_values('t'),
         np.zeros(len(x), dtype=int)], names=['i', 't', 'm'])
    result = aggregate_expenditures(adapted, membership.reindex(x.columns))
    return result.droplevel('m').reindex(x.index)


def wave_inventory(food, sample, wave, membership):
    """Measure positive amounts on sample and delivered populations separately."""
    sample = _sample(sample, wave)
    _delivered_goods(food)
    food = food.loc[food.index.get_level_values('t') == wave]
    if food.empty:
        raise ValueError(f'No delivered food rows for {wave}.')
    values = food.Expenditure.astype(float)
    if np.isinf(values).any() or (values < 0).any():
        raise ValueError('Expenditures must be nonnegative and finite or unavailable.')
    goods = _delivered_goods(food)
    if not goods.isin(membership.index).all():
        raise ValueError('Draft membership omits a delivered item.')
    food_goods = goods[~goods.isin(NONFOOD)]
    weight = sample.weight.astype(float)
    valid_weight = np.isfinite(weight) & (weight > 0)
    if not valid_weight.any():
        raise ValueError('The sample has no positive finite weights.')
    weight = weight.where(valid_weight, 0.)
    profiles, item_rows, visit_rows, group_rows = [], [], [], []
    for basis in ('purchased', 'total'):
        all_x = _amounts(food, basis).reindex(columns=goods).fillna(0.)
        outside = all_x.loc[~all_x.index.isin(sample.index)]
        x = all_x.reindex(sample.index, fill_value=0.)
        food_x = x.loc[:, food_goods]
        observed = food_x.gt(0)
        counts = observed.sum(axis=1)
        paired = observed.astype('int64').T @ observed.astype('int64')
        pairs = paired.to_numpy()[np.triu_indices(len(food_goods), k=1)]
        profiles.append(dict(
            wave=wave, basis=basis, n_sample=len(sample), n_delivered_labels=len(goods),
            n_food_labels=len(food_goods), n_positive_food_labels=int(observed.any().sum()),
            n_positive_food_households=int((counts > 0).sum()),
            median_positive_foods=float(counts.median()), mean_positive_foods=float(counts.mean()),
            entry_density=float(observed.to_numpy().mean()),
            median_pair_count=float(np.median(pairs)) if len(pairs) else np.nan,
            fraction_pairs_ge30=float((pairs >= 30).mean()) if len(pairs) else np.nan,
            weighted_food_household_coverage=float(weight[counts > 0].sum()/weight.sum()),
            n_invalid_weight=int((~valid_weight).sum()),
            n_positive_households_outside_sample=int(outside.loc[:, food_goods].gt(0).any(axis=1).sum()),
            food_expenditure_outside_sample=float(outside.loc[:, food_goods].sum().sum())))
        for good in goods:
            positive = x[good] > 0
            item_rows.append(dict(
                wave=wave, basis=basis, preferred=good, n_sample=len(sample),
                n_positive_sample=int(positive.sum()), n_positive_all=int((all_x[good] > 0).sum()),
                sample_prevalence=float(positive.mean()),
                weighted_prevalence=float(weight[positive].sum()/weight.sum()),
                expenditure_sample=float(x[good].sum()),
                expenditure_outside_sample=float(outside[good].sum()),
                food_analysis=good not in NONFOOD))
        grouped = _grouped(x, membership)
        if not np.allclose(grouped.sum(axis=1), x.sum(axis=1), rtol=1e-12, atol=1e-8):
            raise ValueError('Draft aggregation did not conserve household expenditure.')
        groups = membership.reindex(goods)
        for group in groups.unique():
            members = groups.index[groups == group]
            if len(members) < 2:
                continue
            member_counts = x[members].gt(0).sum(axis=1)
            positive = member_counts > 0
            group_rows.append(dict(
                wave=wave, basis=basis, aggregate=group, members='; '.join(members),
                n_members=len(members), n_positive_sample=int(positive.sum()),
                largest_member_support=int(x[members].gt(0).sum().max()),
                additional_hh_vs_best_member=int(positive.sum()-x[members].gt(0).sum().max()),
                mean_members_when_positive=float(member_counts[positive].mean()),
                n_hh_multiple_members=int((member_counts > 1).sum()),
                weighted_prevalence=float(weight[positive].sum()/weight.sum())))
        # Retain visits here: the ordinary monetary derivation intentionally
        # sums them away. Positive *amounts*, not padded price rows, count.
        selected = food
        if basis == 'purchased':
            selected = food.loc[food.index.get_level_values('s') == 'purchased']
        v = selected.Expenditure.groupby(level=['i', 't', 'visit', 'j'],
                                         observed=True, dropna=False).sum(min_count=1)
        v = v.loc[(v > 0) & v.index.get_level_values('j').isin(food_goods)]
        for visit in sorted(food.index.get_level_values('visit').unique()):
            amounts = v.loc[v.index.get_level_values('visit') == visit]
            on_sample = amounts.index.droplevel(['visit', 'j']).isin(sample.index)
            amounts = amounts.loc[on_sample]
            counts_visit = amounts.groupby(level=['i', 't'], observed=True).size()
            visit_rows.append(dict(
                wave=wave, basis=basis, visit=visit,
                n_positive_food_households=len(counts_visit),
                n_positive_food_labels=amounts.index.get_level_values('j').nunique(),
                median_foods_among_positive_households=float(counts_visit.median()),
                n_positive_household_item_cells=len(amounts)))
    return tuple(pd.DataFrame(rows) for rows in (profiles, item_rows, visit_rows, group_rows))


def run_inventory(waves, specification, output):
    """Build through Country once, then write only aggregate review artifacts."""
    import lsms_library as ll
    from lsms_library.local_tools import df_from_orgfile
    from lsms_library.paths import countries_root

    country = ll.Country('GhanaLSS', verbose=False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        sample = country.sample(waves=waves)
        food = country.food_acquired(waves=waves, labels='Preferred')
    wave_tables = {wave: df_from_orgfile(countries_root() / 'GhanaLSS' / wave / '_' /
                                       'categorical_mapping.org', name='harmonize_food',
                                       encoding='utf-8') for wave in waves}
    country_table = country.categorical_mapping['harmonize_food']
    goods = set(_delivered_goods(food))
    membership, review, missing = draft_partition(goods, specification)
    review = review.join(label_provenance(goods, country_table, wave_tables))
    actual_waves = pd.DataFrame({'preferred': food.index.get_level_values('j'),
                                'wave': food.index.get_level_values('t')}).drop_duplicates()
    review['delivered_waves'] = actual_waves.groupby('preferred').wave.agg(
        lambda s: '; '.join(sorted(s)))
    profile_parts, item_parts, visit_parts, group_parts = [], [], [], []
    for wave in waves:
        profiles, items, visits, groups = wave_inventory(food, sample, wave, membership)
        profile_parts.append(profiles)
        item_parts.append(items)
        visit_parts.append(visits)
        group_parts.append(groups)
        print(f'{wave}: {profiles.iloc[0].n_sample} sample households; '
              f'{profiles.iloc[0].n_delivered_labels} delivered labels', file=sys.stderr, flush=True)
    output.mkdir(parents=True, exist_ok=True)
    review.to_csv(output / 'item_review.csv')
    for name, parts in [('profiles', profile_parts), ('item_support', item_parts),
                        ('visit_support', visit_parts), ('group_support', group_parts)]:
        pd.concat(parts, ignore_index=True).to_csv(output / f'{name}.csv', index=False)
    (output / 'draft_partition.yml').write_text(yaml.safe_dump(membership.to_dict(), sort_keys=True))
    declared = _labels(country_table['Preferred Label'])
    report = dict(
        country='GhanaLSS', waves=waves, n_delivered_labels=len(goods),
        n_country_preferred_labels=len(declared),
        source_revision=specification.get('source_revision'),
        nonfood_labels=sorted(goods.intersection(NONFOOD)),
        n_draft_groups=int(membership.nunique()),
        delivered_labels_absent_from_country=sorted(goods-declared),
        country_labels_not_delivered=sorted(declared-goods),
        proposed_members_not_delivered=[dict(group=group, member=member) for group, member in missing],
        api_warning_counts=dict(Counter(type(w.message).__name__ for w in caught
                                        if not isinstance(w.message, (ResourceWarning, DeprecationWarning)))))
    (output / 'inventory.yml').write_text(yaml.safe_dump(report, sort_keys=False))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--waves', nargs='+', default=['1987-88', '1988-89', '1991-92', '1998-99',
                                                      '2005-06', '2012-13', '2016-17'])
    parser.add_argument('--specification', type=Path,
                        default=Path(__file__).parent / 'ghanalss' / 'proposed_groups.yml')
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'ghanalss')
    args = parser.parse_args()
    specification = yaml.safe_load(args.specification.read_text())
    report = run_inventory(args.waves, specification, args.output)
    print(yaml.safe_dump(report, sort_keys=False))


if __name__ == '__main__':
    main()

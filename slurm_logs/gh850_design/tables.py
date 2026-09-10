"""Render the measurement CSVs as the Org tables pasted into DESIGN.org.

Run after ``measure.py`` / ``metric_scoreboard.py`` / ``mali_groundtruth.py``.
Pure formatting; reads nothing but this directory.
"""
import os

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ORDER = ['Uganda', 'Malawi', 'Nigeria', 'Ethiopia', 'Tanzania', 'Niger',
         'Mali', 'EthiopiaRHS', 'GhanaLSS']


def org(df, floats=3):
    d = df.copy()
    for c in d.columns:
        if pd.api.types.is_float_dtype(d[c]):
            d[c] = d[c].map(lambda x: '' if pd.isna(x) else f'{x:,.{floats}g}')
        else:
            d[c] = d[c].map(lambda x: '' if pd.isna(x) else
                            (f'{x:,}' if isinstance(x, (int,)) else str(x)))
    head = '| ' + ' | '.join(d.columns) + ' |'
    rule = '|' + '+'.join(['-' * (len(c) + 2) for c in d.columns]) + '|'
    body = '\n'.join('| ' + ' | '.join(r) + ' |' for r in d.astype(str).values)
    return '\n'.join([head, rule, body])


def by_country(df):
    df = df.copy()
    df['__o'] = df['country'].map({c: i for i, c in enumerate(ORDER)})
    return df.sort_values('__o', kind='stable').drop(columns='__o')


def main():
    p = lambda n: pd.read_csv(os.path.join(HERE, n))

    print('** (i) Quantity on the rows that feed step 2\n')
    d = by_country(p('measure_i_quantity.csv'))
    d = d[['country', 'rows_total', 'rows_step2', 'rows_inference_now',
           'rows_c_removes', 'rows_inference_after_c', 'share_inference_now',
           'share_Q_eq_1', 'Q_median', 'Q_p90', 'Q_mean']]
    d.columns = ['country', 'rows', 'step2', 'infer now', '(c) removes',
                 'infer after (c)', 'share now', 'Q==1', 'Q med', 'Q p90', 'Q mean']
    print(org(d)); print()

    print('** (ii) items sharing a unit, and the spread of kg per unit\n')
    d = p('measure_ii_units.csv')
    d = d[d.n_j_with_factor >= 5].sort_values('n_rows', ascending=False)
    d = by_country(d).groupby('country', sort=False).head(4)
    d = d[['country', 'u', 'n_rows', 'n_distinct_j', 'n_j_with_factor',
           'kg_per_unit_pooled_old', 'kg_per_unit_pooled_new', 'kg_per_j_min',
           'kg_per_j_median', 'kg_per_j_max', 'ratio_max_min']]
    d.columns = ['country', 'u', 'rows', 'distinct j', 'j w/ factor',
                 'pooled now', 'pooled fixed', 'per-j min', 'per-j med',
                 'per-j max', 'max/min']
    print(org(d)); print()

    print('** (iii) movement\n')
    d = by_country(p('measure_iii_rows.csv'))
    a = d[['country', 'n_units_total', 'n_units_inferred_now',
           'n_units_inferred_after_c', 'rows_compared', 'share_move_gt_10pct',
           'share_move_gt_2x', 'share_move_gt_10x', 'median_rel_move']]
    a.columns = ['country', 'units', 'inferred now', 'inferred after (c)',
                 'rows compared', '>10%', '>2x', '>10x', 'median new/old']
    print(org(a)); print()
    b = d[['country', 'kg_selftest_old', 'kg_selftest_b', 'kg_selftest_c_median',
           'kg_selftest_c_n_items', 'kg_selftest_c_within_10pct',
           'reimpl_keys', 'reimpl_agree_with_live']]
    b.columns = ['country', 'kg now', 'kg fix (b)', 'kg fix (c) med j',
                 'n items', 'within 10%', 'reimpl keys', 'agree']
    print(org(b)); print()
    c = d[['country', 'fq_kg_rows_old', 'fq_kg_rows_new', 'fq_kg_total_old',
           'fq_kg_total_new', 'fp_rows_old', 'fp_rows_new']].copy()
    c['pct'] = (d['fq_kg_total_new'] / d['fq_kg_total_old'] - 1) * 100
    c.columns = ['country', 'fq kg rows now', 'fq kg rows new', 'fq total kg now',
                 'fq total kg new', 'fp rows now', 'fp rows new', 'total kg %']
    print(org(c, floats=4)); print()

    print('** floor sweep\n')
    f = by_country(p('measure_floor.csv'))
    piv = f.pivot_table(index=['country', 'step2_floor'],
                        columns='baseline_floor',
                        values='share_of_inference_rows').reset_index()
    piv.columns = [str(x) for x in piv.columns]
    print(org(piv)); print()

    print('** top-10 expenditure items, latest wave\n')
    d = by_country(p('measure_iii_prices.csv'))
    d = d[['country', 't', 'j', 'expenditure_share', 'baseline_n',
           'median_price_kg_old', 'median_price_kg_new', 'pct_change']]
    d.columns = ['country', 't', 'j', 'exp share', 'baseline N', 'price/kg now',
                 'price/kg new', '% change']
    print(org(d, floats=4)); print()

    for name, title in (('metric_scoreboard.csv',
                         '** ground truth A: labels that state their own kilograms'),
                        ('mali_groundtruth.csv',
                         '** ground truth B: Mali\'s questionnaire unit list')):
        path = os.path.join(HERE, name)
        if os.path.exists(path):
            print(title, '\n')
            print(org(pd.read_csv(path))); print()


if __name__ == '__main__':
    main()

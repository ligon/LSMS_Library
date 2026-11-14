"""Combine the arm JSONs into attribution tables (aggregates only).

usage: compare.py OUT_PREFIX ARMDIR [ARMDIR ...]   (each ARMDIR holds <Country>.json)
Arms are named by their directory basename: base, c04 (+#1604, sold kg, #587),
c27 (+#1509), int (+EHCVM-2 = head), ehcvm (EHCVM-2 alone, its 3 countries).
"""
import json
import os
import sys

prefix = sys.argv[1]
arms = {}
ALIAS = {'basesen': 'base', 'int5sen': 'int'}   # Senegal-only arms of the same SHAs
for d in sys.argv[2:]:
    name = os.path.basename(d.rstrip('/'))
    name = ALIAS.get(name, name)
    arms.setdefault(name, {})
    for f in sorted(os.listdir(d)):
        if f.endswith('.json') and not f.startswith('_'):
            with open(os.path.join(d, f)) as fh:
                r = json.load(fh)
            arms[name][r['country']] = r

ORDER = ['base', 'c04', 'c27', 'int', 'six', 'ehcvm']
countries = sorted({c for a in arms.values() for c in a})


def g(arm, c, *path):
    r = arms.get(arm, {}).get(c)
    for p in path:
        if r is None:
            return None
        r = r.get(p) if isinstance(r, dict) else None
    return r


def pct(a, b):
    if a in (None, 0) or b is None:
        return None
    return 100.0 * (b - a) / a


def fmt(x, nd=0):
    if x is None:
        return 'n/a'
    if isinstance(x, float):
        return f'{x:,.{nd}f}'
    return f'{x:,}'


out = {'arms': {a: sorted(v) for a, v in arms.items()}, 'countries': {}}
lines = ['| country | metric | ' + ' | '.join(a for a in ORDER if a in arms) + ' | head (six, else int) vs base |',
         '|---|---|' + '---|' * (len([a for a in ORDER if a in arms]) + 1)]
for c in countries:
    rec = {}
    for label, path in (('food_acquired rows', ('food_acquired', 'rows')),
                        ('food_prices rows', ('food_prices', 'rows')),
                        ('kg total (u=kg)', ('food_quantities_kgs', 'kg_total')),
                        ('food_quantities rows', ('food_quantities_kgs', 'rows')),
                        ('sample rows', ('sample', 'rows')),
                        ('food_acquired Expenditure sum', ('food_acquired', 'expenditure_sum'))):
        vals = {a: g(a, c, *path) for a in ORDER if a in arms}
        rec[label] = vals
        d = pct(vals.get('base'), vals.get('six', vals.get('int')))
        lines.append(f'| {c} | {label} | ' + ' | '.join(fmt(vals[a], 0) for a in vals)
                     + f" | {('%+.2f%%' % d) if d is not None else 'n/a'} |")
    # unique-index and error flags
    rec['index_unique'] = {a: (g(a, c, 'food_acquired', 'index_unique'), g(a, c, 'sample', 'index_unique'))
                           for a in arms}
    rec['errors'] = {a: {k: v['error'] for k, v in (g(a, c, 'calls') or {}).items() if v.get('error')}
                     for a in arms}
    rec['fatal'] = {a: bool(g(a, c, 'fatal')) for a in arms}
    rec['kg_by_t'] = {a: g(a, c, 'food_quantities_kgs', 'kg_by_t') for a in arms}
    rec['fa_rows_by_t'] = {a: g(a, c, 'food_acquired', 'rows_by_t') for a in arms}
    rec['fp_rows_by_t'] = {a: g(a, c, 'food_prices', 'rows_by_t') for a in arms}
    rec['kg_sources'] = {a: (g(a, c, 'food_quantities_kgs', 'attrs') or {}).get('kg_factor_sources')
                         for a in arms}
    rec['fa_rows_by_s'] = {a: g(a, c, 'food_acquired', 'rows_by_s') for a in arms}
    rec['nutrition'] = {a: g(a, c, 'nutrition') for a in arms}
    # attribution of kg
    kg = rec['kg total (u=kg)']
    att = {}
    if 'c04' in kg and 'base' in kg and kg['c04'] is not None and kg['base'] is not None:
        att['c04-base (#1604, sold kg, #587)'] = kg['c04'] - kg['base']
    if 'c27' in kg and 'c04' in kg and kg['c27'] is not None and kg['c04'] is not None:
        att['c27-c04 (#1509 on that tree)'] = kg['c27'] - kg['c04']
    if 'int' in kg and 'c27' in kg and kg['int'] is not None and kg['c27'] is not None:
        att['int-c27 (EHCVM-2 + docstring on that tree)'] = kg['int'] - kg['c27']
    if kg.get('six') is not None and kg.get('int') is not None:
        att['six-int (served-wave corrections on that tree)'] = kg['six'] - kg['int']
    if kg.get('six') is not None and kg.get('base') is not None:
        att['six-base (whole batch)'] = kg['six'] - kg['base']
    if 'ehcvm' in kg and kg.get('ehcvm') is not None and kg.get('base') is not None:
        att['ehcvm-base (EHCVM-2 alone)'] = kg['ehcvm'] - kg['base']
        if kg.get('int') is not None and kg.get('c27') is not None:
            att['interaction (int-c27) - (ehcvm-base)'] = (kg['int'] - kg['c27']) - (kg['ehcvm'] - kg['base'])
    rec['kg_attribution'] = att
    out['countries'][c] = rec

feat = {}
for d in sys.argv[2:]:
    p = os.path.join(d, '_Feature.json')
    if os.path.exists(p):
        with open(p) as fh:
            feat[os.path.basename(d.rstrip('/'))] = json.load(fh)
out['feature'] = feat
with open(prefix + '.json', 'w') as fh:
    json.dump(out, fh, indent=1, sort_keys=True, default=str)
with open(prefix + '.md', 'w') as fh:
    fh.write('\n'.join(lines) + '\n\n')
    fh.write('| country | kg attribution | value |\n|---|---|---|\n')
    for c in countries:
        for k, v in out['countries'][c]['kg_attribution'].items():
            fh.write(f'| {c} | {k} | {v:+,.1f} |\n')
print('\n'.join(lines))

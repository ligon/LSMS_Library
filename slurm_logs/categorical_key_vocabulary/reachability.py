#!/usr/bin/env python
"""Which of the four positional key-column sites in country.py is actually LIVE?

Run: cd <repo> && PYTHONPATH=$PWD .venv/bin/python <this> [survey.json]
"""
import json, sys, yaml
from pathlib import Path

class Tolerant(yaml.SafeLoader):
    """data_scheme.yml carries `!make` tags that SafeLoader refuses."""
Tolerant.add_multi_constructor(
    '!', lambda l, s, n: l.construct_scalar(n) if isinstance(n, yaml.ScalarNode) else None)

SKIP = {'index', 'materialize', 'backend', 'aggregation'}
here = Path(sys.argv[1] if len(sys.argv) > 1
            else Path(__file__).parent / 'survey.json')
T = [t for t in json.load(open(here)) if t['table'] != '<PARSE ERROR>']
root = Path('lsms_library/countries')

# NOTE: the tables live under a `Data Scheme:` key, and Country.data_scheme is a
# LIST of names, not a dict.  Both cost a wrong answer while writing this.
cols, methods = set(), set()
for ds in sorted(root.glob('*/_/data_scheme.yml')):
    scheme = (yaml.load(ds.read_text(errors='replace'), Loader=Tolerant) or {}).get('Data Scheme') or {}
    if not isinstance(scheme, dict):
        continue
    methods |= {str(k) for k in scheme}
    for entry in scheme.values():
        if isinstance(entry, dict):
            cols |= {str(k) for k in entry if str(k) not in SKIP}
assert {'Roof', 'Floor', 'Sex', 'Rural'} <= cols, "declared-column extraction is broken"
methods |= {'food_expenditures', 'food_prices', 'food_quantities', 'household_characteristics'}
cols |= {'t', 'i', 'j', 'u', 'v', 'm', 's'}

names = {t['table'] for t in T}
CONV = {'Code', 'Original Label', 'Alternate Spelling'}
low = {c.lower() for c in cols}

auto = sorted(n for n in names if n.lower() in low)
hz = {n for n in names if n.startswith('harmonize_')}
hook = sorted(h for h in hz if h[len('harmonize_'):] in methods)

print(f"declared columns {len(cols)}   declared tables {len(methods)}   mapping tables {len(T)}\n")
print(f"SITE country.py:2880  auto-apply by NAME  -> {len(auto)} of {len(names)} table names")
print(f"   {auto}")
print(f"SITE country.py:3222  harmonize_<method>  -> {len(hook)} of {len(hz)} harmonize_* tables")
print(f"   {hook}")
print(f"SITE country.py:355   global/country merge -> the 3 _ADDITIVE_CATEGORICAL_TABLES")
print(f"SITE country.py:2385  cluster_features market normalisation")

risky = [t for t in T if t['table'] in auto + hook and t['key'] and t['key'] not in CONV]
print(f"\nLIVE tables whose positional key is NOT a conventional key column: {len(risky)}")
for t in risky:
    print(f"   {t['scope']:20s} {t['table']:20s} key={t['key']!r}")
print("\n=> 0 means the positional pick is correct everywhere it currently fires:")
print("   `#+key:` is INSURANCE for adding Status/Notes, not a fix for a live defect.")

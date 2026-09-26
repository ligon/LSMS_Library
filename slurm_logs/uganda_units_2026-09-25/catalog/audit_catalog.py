from pathlib import Path
import ast
import argparse
import subprocess
import json
import pandas as pd
import lsms_library
from lsms_library.local_tools import df_from_orgfile, get_dataframe

parser = argparse.ArgumentParser(description="Audit Uganda unit catalog without writing household microdata.")
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--baseline', default='4c2223aa119381ebcd8aea05fc2f9d26c3026028')
args = parser.parse_args()
ROOT = Path.cwd().resolve()
OUT = args.output.resolve()
OUT.mkdir(parents=True, exist_ok=True)
assert Path(lsms_library.__file__).resolve().is_relative_to(ROOT), lsms_library.__file__
C = ROOT / 'lsms_library/countries/Uganda'
baseline = subprocess.run(['git', 'show', args.baseline + ':lsms_library/countries/Uganda/_/categorical_mapping.org'], check=True, text=True, capture_output=True).stdout
(OUT / 'baseline-catalog.org').write_text(baseline)
u = df_from_orgfile(OUT / 'baseline-catalog.org', name='u')
waves = [c for c in u.columns if c not in ('Code', 'Preferred Label')]
blank = u[u['Preferred Label'].isna() | u['Preferred Label'].eq('')].copy()
blank['wave_labels'] = blank[waves].apply(lambda r: json.dumps({k: str(v).strip() for k, v in r.items() if pd.notna(v) and str(v).strip()}), axis=1)
blank['distinct_labels'] = blank[waves].apply(lambda r: json.dumps(sorted({str(v).strip() for v in r if pd.notna(v) and str(v).strip()})), axis=1)
blank.to_csv(OUT / 'blank_catalog.csv', index=False)
blank_codes = set(blank.Code.astype(int))
records, unit_labels, item_labels, summaries, meta_conflicts = [], {}, {}, [], []
for script in sorted(C.glob('*/_/food_acquired.py')):
    wave = script.parent.parent.name
    if wave not in waves:
        continue
    names = {}
    for node in ast.parse(script.read_text()).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name == 'fn':
                names[name] = ast.literal_eval(node.value)
            if name == 'myvars':
                names[name] = ast.literal_eval(node.value) if isinstance(node.value, ast.Dict) else {kw.arg: ast.literal_eval(kw.value) for kw in node.value.keywords}
    fn = (script.parent / names['fn']).resolve()
    mv = names['myvars']
    print(f'READING {wave} {fn}', flush=True)
    raw = get_dataframe(fn, convert_categoricals=False)
    cats = get_dataframe(fn, convert_categoricals=True, categories_only=True)
    labels = cats.get(mv['units'], {})
    foods = cats.get(mv['item'], {})
    unit_labels[wave] = {str(k): str(v) for k, v in labels.items()}
    item_labels[wave] = {str(k): str(v) for k, v in foods.items()}
    # Aggregate observations, never persist household identifiers.
    sel = raw[raw[mv['units']].isin(blank_codes)]
    for (unit, item), rows in sel.groupby([mv['units'], mv['item']], dropna=False):
        row = {'wave': wave, 'code': int(unit), 'item': int(item) if pd.notna(item) else None,
               'item_label': foods.get(item, ''), 'raw_label': labels.get(unit, ''), 'rows': len(rows)}
        for source in ('home', 'away', 'own', 'inkind'):
            q = pd.to_numeric(rows[mv['quantity_' + source]], errors='coerce')
            x = pd.to_numeric(rows[mv['value_' + source]], errors='coerce')
            row[source + '_q_positive'] = int(q.gt(0).sum())
            row[source + '_x_positive'] = int(x.gt(0).sum())
            row[source + '_q_nonmissing'] = int(q.notna().sum())
            row[source + '_x_nonmissing'] = int(x.notna().sum())
        records.append(row)
    for row in blank.to_dict('records'):
        code = int(row['Code'])
        org = row.get(wave)
        actual = labels.get(code)
        org_text = str(org).strip() if pd.notna(org) else ''
        actual_text = str(actual).strip() if actual is not None else ''
        if org_text or actual_text:
            if org_text != actual_text:
                meta_conflicts.append({'wave': wave, 'code': code, 'org': str(org), 'dta': actual})
    summary = {'wave': wave, 'rows': len(raw), 'blank_code_rows': len(sel), 'blank_codes_used': sorted(sel[mv['units']].dropna().astype(int).unique().tolist()), 'labels_count': len(labels), 'data_source': str(fn.relative_to(ROOT))}
    summaries.append(summary)
    print(json.dumps(summary), flush=True)
    # Checkpoint each wave.
    pd.DataFrame(records).to_csv(OUT / 'raw_blank_usage.csv', index=False)
    (OUT / 'source_unit_labels.json').write_text(json.dumps(unit_labels, indent=2, sort_keys=True))
    (OUT / 'source_item_labels.json').write_text(json.dumps(item_labels, indent=2, sort_keys=True))
    (OUT / 'source_summaries.json').write_text(json.dumps(summaries, indent=2))
    (OUT / 'source_provenance_conflicts.json').write_text(json.dumps(meta_conflicts, indent=2))
# Verify the edited catalog without changing it or any fixture.
current = df_from_orgfile(C / '_/categorical_mapping.org', name='u')
assert len(u) == len(current) == 289
assert u.Code.equals(current.Code)
assert u[waves].equals(current[waves]), 'Source-wave labels changed'
changed = u['Preferred Label'].fillna('').ne(current['Preferred Label'].fillna(''))
assert int(changed.sum()) == 143
assert set(u.loc[changed, 'Code'].astype(int)) == blank_codes
assert u.loc[~changed, 'Preferred Label'].equals(current.loc[~changed, 'Preferred Label'])
proposal = pd.read_csv(Path(__file__).with_name('proposed_mapping.csv'))
actual = dict(zip(current.Code.astype(int), current['Preferred Label'].astype(str)))
assert all(actual[int(r.Code)] == str(r.Proposed_Label) for r in proposal.itertuples())
physical = proposal.loc[proposal.Physical].copy()
assert len(physical) == 131
assert int(physical.Canonical_Equivalent.notna().sum()) == 111
original_labels = dict(zip(u.Code.astype(int), u['Preferred Label']))
for row in physical.loc[physical.Canonical_Equivalent.notna()].itertuples():
    assert row.Proposed_Label == original_labels[int(row.Canonical_Equivalent)]
assert not physical.Proposed_Label.str.match(r'^\s*\d').any()
from lsms_library.transformations import _parse_explicit_metric
physical['source_factor'] = physical.Source_Label.map(_parse_explicit_metric)
physical['preferred_factor'] = physical.Proposed_Label.map(_parse_explicit_metric)
assert physical.source_factor.fillna(-1).equals(physical.preferred_factor.fillna(-1))
assert _parse_explicit_metric(actual[125051]) == 0.5
assert _parse_explicit_metric(actual[125052]) == 0.25
assert _parse_explicit_metric(actual[123060]) == 2.0
physical.to_csv(OUT / 'metric_check.csv', index=False)
assert not meta_conflicts, meta_conflicts
summary = {'baseline': args.baseline, 'catalog_codes': len(u), 'changed_preferred_labels': int(changed.sum()),
           'physical_promotions': len(physical), 'existing_equivalents': 111, 'new_named_forms': 20,
           'source_factor_matches': len(physical), 'source_label_conflicts': len(meta_conflicts),
           'blank_code_raw_rows': sum(x['blank_code_rows'] for x in summaries),
           'physical_code_raw_rows': int(proposal.loc[proposal.Physical, 'Raw_Rows'].sum()),
           'source_wave_columns_unchanged': True, 'nonblank_preferred_labels_unchanged': True}
(OUT / 'audit_summary.json').write_text(json.dumps(summary, indent=2))
print(json.dumps(summary), flush=True)
print('COMPLETE', flush=True)

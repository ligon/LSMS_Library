"""Senegal #1119 join measurement on saved frames.  Usage: python sen_join.py <dir> [<dir2>]
For each dir: per-wave counts of the maize/cowpea seed labels, and the (t, i, crop)
household/crop existence join of 2018-19 seed rows against crop_production, as
served and under the relabel (simulated on the first dir)."""
import sys, json
import pandas as pd
RELABEL = {'Maïs en grain': 'Maïs', 'Niébé/Haricots secs': 'Niébé'}
out = {}
for d in sys.argv[1:]:
    pi = pd.read_parquet(f'{d}/Senegal__plot_inputs.parquet').reset_index()
    cp = pd.read_parquet(f'{d}/Senegal__crop_production.parquet').reset_index()
    rec = out[d] = {}
    seeds = pi[pi['input'] == 'Seed']
    labs = list(RELABEL) + list(RELABEL.values())
    rec['seed_rows_by_wave_label'] = {f'{t}|{c}': int(n) for (t, c), n in
                                      seeds[seeds['crop'].isin(labs)].groupby(['t', 'crop']).size().items()}
    cpk = set(zip(cp['t'], cp['i'], cp['crop']))
    for name, frame in [('as_served', seeds), ('relabelled', seeds.assign(crop=seeds['crop'].replace(RELABEL)))]:
        s18 = frame[frame['t'] == '2018-19']
        for lab in ['Maïs', 'Niébé', 'Maïs en grain', 'Niébé/Haricots secs', 'Arachide', 'Mil']:
            sub = s18[s18['crop'] == lab]
            if len(sub):
                hit = sum((t, i, c) in cpk for t, i, c in zip(sub['t'], sub['i'], sub['crop']))
                rec[f'{name}|{lab}'] = {'seed_rows': len(sub), 'with_crop_record': int(hit)}
    rec['crop_production_labels'] = {k: int(v) for k, v in cp['crop'].value_counts().items() if k in labs}
print(json.dumps(out, indent=1, ensure_ascii=False))

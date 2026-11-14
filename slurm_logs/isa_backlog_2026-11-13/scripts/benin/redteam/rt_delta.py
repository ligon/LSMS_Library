"""Delta checks for 2aa1e4287.  usage: rt_delta.py <mode> ...

xw <cand2_clone> <cand1_clone> <outdir>
    Derive the crosswalk WITH the exception from the raw files (my own rule:
    PanelHH = Panel, key in 2018-19, and NOT every pre-loaded s01 row s01q00aa != 1),
    compare with the committed JSON at cand2 and with cand1's JSON minus the excepted.
    Also the raw poids ratio mean(all)/mean(linked).
checks <outdir> <base_pickles> <cand1_pickles> <cand2_pickles> <xw2.json>
    panel_weight and weight, the unit columns cand1 -> cand2, derivation_inputs of the
    exception (in-process: sys.path before/after), Mover.
"""
import json
import pickle
import sys
import warnings
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

warnings.simplefilter('ignore')
mode = sys.argv[1]


def key(g, m):
    return f'{int(g)}{int(m):03d}'


if mode == 'xw':
    c2, c1, out = Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4])
    out.mkdir(parents=True, exist_ok=True)
    from lsms_library.local_tools import get_dataframe
    B = c2 / 'lsms_library/countries/Benin'
    c21 = get_dataframe(B / '2021-22/Data/s00_me_ben2021.dta', convert_categoricals=True)
    c18 = get_dataframe(B / '2018-19/Data/s00_me_ben2018.dta')
    s21 = get_dataframe(B / '2021-22/Data/s01_me_ben2021.dta', convert_categoricals=False)
    keys21 = [key(g, m) for g, m in zip(c21.grappe, c21.menage)]
    old = {key(g, m) for g, m in zip(c18.grappe, c18.menage)}
    panel = c21.PanelHH.astype(str).str.strip().to_numpy()
    s21['key'] = [key(g, m) for g, m in zip(s21.grappe, s21.menage)]
    pre = s21[s21.s01q00aa.notna()]
    R = {'s01q00aa_codes': sorted(map(float, set(pre.s01q00aa)))}
    mine = set(pre.groupby('key')['s01q00aa'].apply(lambda s: bool((s != 1).all())).loc[lambda x: x].index)
    theirs = set(pre.groupby('key')['s01q00aa'].apply(lambda s: bool((s == 2).all())).loc[lambda x: x].index)
    R['roster_condition_alone'] = len(mine)
    R['mine_equals_all_eq_2'] = mine == theirs
    would = {k for k, p in zip(keys21, panel) if p == 'Panel' and k in old}
    exc = would & mine
    R['would_link'] = len(would)
    R['excepted'] = len(exc)
    R['roster_condition_outside_would_link'] = len(mine - would)
    xw2 = {'E2_' + k: k for k in sorted(would - exc)}
    R['linked'] = len(xw2)
    committed2 = json.load(open(B / '_/updated_ids.json'))
    committed1 = json.load(open(c1 / 'lsms_library/countries/Benin/_/updated_ids.json'))
    pj2 = json.load(open(B / '_/panel_ids.json'))
    R['committed2_equals_mine'] = committed2 == {'2018-19': {}, '2021-22': xw2}
    R['committed1_minus_excepted_equals_committed2'] = (
        {k: v for k, v in committed1['2021-22'].items() if v not in exc} == committed2['2021-22'])
    R['panel_json_consistent'] = pj2 == {f'2021-22,{k}': f'2018-19,{v}' for k, v in committed2['2021-22'].items()}
    ks, vs = set(xw2), set(xw2.values())
    R['chained'] = len(ks & vs)
    R['occupied'] = len(vs & {'E2_' + k for k in keys21})
    R['dup_targets'] = len(xw2) - len(vs)
    R['unlinked_total'] = len(keys21) - len(xw2)
    w = get_dataframe(B / '2021-22/Data/ehcvm_ponderations_ben2021.dta')
    wk = [key(g, m) for g, m in zip(w.grappe, w.menage)]
    p = pd.Series(w.poids.to_numpy(), index=wk).astype(float)
    R['poids_ratio_mean_all_over_mean_linked'] = float(p.mean() / p.loc[sorted(vs)].mean())
    R['excepted_ids'] = sorted('E2_' + k for k in exc)
    json.dump({'2018-19': {}, '2021-22': xw2}, open(out / 'xw2.json', 'w'))
    json.dump(R, open(out / 'xw2_facts.json', 'w'), indent=1)
    print(json.dumps({k: v for k, v in R.items() if k != 'excepted_ids'}, indent=1))

elif mode == 'checks':
    out, bdir, c1dir, c2dir, xwp = (Path(x) for x in sys.argv[2:7])
    import lsms_library as ll
    XW = json.load(open(xwp))['2021-22']
    facts = json.load(open(out / 'xw2_facts.json'))
    ld = lambda d, t: pickle.load(open(d / f'{t}.pkl', 'rb'))  # noqa: E731
    R = {}
    b, c2 = ld(bdir, 'sample'), ld(c2dir, 'sample')
    b21 = b[b.index.get_level_values('t') == '2021-22'].reset_index()
    c21 = c2[c2.index.get_level_values('t') == '2021-22'].reset_index()
    b21['i2'] = b21['i'].map(lambda x: XW.get(x, x))
    m = b21.merge(c21, left_on='i2', right_on='i', suffixes=('_b', '_c'))
    linked = m['i_c'].isin(set(XW.values()))
    R['rows21'] = [len(b21), len(c21), len(m)]
    R['pw_nonNA_linked'] = int(m.loc[linked, 'panel_weight_c'].notna().sum())
    R['pw_NA_unlinked'] = int(m.loc[~linked, 'panel_weight_c'].isna().sum())
    R['n_linked'], R['n_unlinked'] = int(linked.sum()), int((~linked).sum())
    ratio = (m.loc[linked, 'panel_weight_c'].astype(float) / m.loc[linked, 'panel_weight_b'].astype(float))
    R['pw_ratio_cand_over_base_linked'] = [float(ratio.min()), float(ratio.max())]
    R['pw_linked_mean'] = float(m.loc[linked, 'panel_weight_c'].astype(float).mean())
    R['weight_equal'] = bool(np.allclose(m.weight_b.astype(float), m.weight_c.astype(float), rtol=0, atol=1e-12))
    R['raw_ratio_expected'] = facts['poids_ratio_mean_all_over_mean_linked']
    b18 = b[b.index.get_level_values('t') == '2018-19'].sort_index()
    c18 = c2[c2.index.get_level_values('t') == '2018-19'].sort_index()
    try:
        pd.testing.assert_frame_equal(b18, c18)
        R['sample_2018_equal'] = True
    except AssertionError as e:
        R['sample_2018_equal'] = str(e)[:300]
    others = [c for c in c21.columns if c not in ('i', 't', 'panel_weight', 'v')]
    R['sample21_other_cols_equal'] = {c: bool(((m[c + '_b'] == m[c + '_c']) | (m[c + '_b'].isna() & m[c + '_c'].isna())).all())
                                      for c in others if c + '_b' in m}
    # unit columns cand1 -> cand2
    p1, p2 = ld(c1dir, 'plot_inputs'), ld(c2dir, 'plot_inputs')
    f1 = p1[p1.index.get_level_values('t') == '2018-19'].reset_index()
    f2 = p2[p2.index.get_level_values('t') == '2018-19'].reset_index()
    for col in ('Unit_purchased', 'Unit_received_as_gift'):
        pairs = Counter((str(a), str(b)) for a, b in zip(f1[col], f2[col]) if str(a) != str(b))
        R[f'{col}_changes'] = {f'{a}->{b}': n for (a, b), n in pairs.items()}
    other_cols = [c for c in f1.columns if c not in ('Unit_purchased', 'Unit_received_as_gift', 'i', 'v')]
    R['plot_inputs18_other_cols_equal'] = all(
        ((f1[c] == f2[c]) | (f1[c].isna() & f2[c].isna())).all() for c in other_cols)
    f21a = p1[p1.index.get_level_values('t') == '2021-22']
    f21b = p2[p2.index.get_level_values('t') == '2021-22']
    R['plot_inputs21_rows'] = [len(f21a), len(f21b)]
    # the exception's inputs, in process
    before = list(sys.path)
    c = ll.Country('Benin')
    inp = c.derivation_inputs('Benin::panel_ids::all-preloaded-left-not-linked', wave='2021-22')
    ids = set(map(str, inp.index.get_level_values('i')))
    R['inputs_rows'] = len(inp)
    R['inputs_households'] = len(ids)
    R['inputs_equal_my_63'] = ids == set(facts['excepted_ids'])
    R['inputs_cols'] = list(map(str, inp.columns))[:12]
    R['sys_path_added'] = [p for p in sys.path if p not in before]
    R['sys_path0_after'] = sys.path[0]
    R['bare_modules_loaded'] = sorted(m for m in ('benin', 'panel_ids', 'plot_features', 'mapping') if m in sys.modules)
    recs = c.derivations()
    r = recs['Benin::panel_ids::all-preloaded-left-not-linked']
    R['record'] = {'table': r.table, 'rows': r.rows, 'waves': r.waves}
    # does any served frame carry/list it?
    s = c.sample()
    R['sample_attrs_derivations_keys'] = sorted((s.attrs.get('derivations') or {}).get('Benin', {}).keys()) \
        if isinstance(s.attrs.get('derivations'), dict) else str(type(s.attrs.get('derivations')))
    # Mover
    hc = ld(c2dir, 'household_characteristics')
    R['mover_hc'] = int(np.asarray(hc.index.get_level_values('v').astype(str) == 'Mover', dtype=bool).sum())
    json.dump(R, open(out / 'delta_checks.json', 'w'), indent=1, default=str)
    print(json.dumps(R, indent=1, default=str))

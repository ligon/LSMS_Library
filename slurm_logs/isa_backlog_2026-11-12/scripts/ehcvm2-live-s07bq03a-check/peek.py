"""Quick console summary of one country's aggregate JSON (used during the run)."""
import json, sys
r=json.load(open(sys.argv[1]))
for w,v in r['waves'].items():
    print('=====',w, v.get('error'), v.get('seconds'))
    if 'error' in v: print(v['traceback']); continue
    d=v['discovery']; print('rows',d['files']['s07b']['rows'],'s07bq06',d['s07bq06_counts']); print('ut',d['u3_type_counts']); print('other top',list(d['u3_other_top'].items())[:8]); print('merge',d.get('merge_region_rate_pos'),d.get('merge_eqadu1_rate_pos'),d.get('region_source'),'f32',d.get('q3_pos_float32_exact_share'),d['dtypes'].get('s07bq03a'))
    g=v['check1_grid']; print('grid', {k:(x['all']['n'],x['all']['on_grid'],x['Kg/Litre']['on_grid'],x['Unite']['on_grid'],x['other']['on_grid']) for k,x in g.items()})
    c=v['check2_factor']; print('jamais',c['jamais_resid_pos'],c['jamais_q3_pos'],'pooled',c['pooled_all'],'KL',c['pooled_kglitre'])
    print('q3/q7',c['q3_over_q7_purchase_only']['all'],c['q3_over_q7_purchase_only']['kglitre'])
    for reg,x in c['by_region'].items(): print('  ',reg,x['own_never_all'].get('n'),x['own_never_all'].get('mode'),x['own_never_all'].get('share_at_mode'),'| KL',x['own_never_kglitre'].get('n'),x['own_never_kglitre'].get('mode'),x['own_never_kglitre'].get('share_at_mode'),'| KLgrid',x['q3_kglitre_on_grid'])
    c3=v['check3_classes']; print('classes',{k:(x['n'],x['share']) for k,x in c3['classes'].items()}, 'agree',c3['decimal_rule_agreement_with_tolerance_rule'])
    for k,x in c3['classes'].items():
        print('  ',k,{a:b for a,b in x.items() if a not in ('n','share')})
    print('  hhsize',c3.get('by_hhsize')); print('  unit',c3['by_unit_type']); print('  top off-grid',c3.get('off_grid_top_values'))
    c4=v['check4_served']; print('c4',{k:c4[k] for k in c4 if k!='by_q3_class'}); print('   ',c4['by_q3_class'])
print(r.get('cross_wave_kglitre_median_ratio'))

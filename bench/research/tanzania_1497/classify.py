"""Closed-source full-key accounting, ledger1497§§2–5; no ID policy change."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pickle
import subprocess
import sys
from collections import Counter,defaultdict

import pandas as pd


def location_kind(locations,frame,split):
    places=set(locations)
    if not places:return 'not_found'
    if places=={frame}:return 'frame'
    if places=={split}:return 'split'
    if frame in places and split in places:return 'both'
    if frame in places:return 'frame_and_elsewhere'
    if split in places:return 'split_and_elsewhere'
    return 'elsewhere'


def link_kind(frame,split,linked):
    return ('both' if frame in linked and split in linked else
            'frame' if frame in linked else 'split' if split in linked else 'neither')


def nonblank(series):
    return series.notna() & series.astype('string').str.strip().ne('').fillna(False)


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--capture',type=Path,required=True);ap.add_argument('--output',type=Path,required=True);args=ap.parse_args()
    w=Path(os.environ['W']).resolve();assert Path.cwd()==w
    import lsms_library
    from lsms_library.local_tools import format_id
    assert Path(lsms_library.__file__).resolve().is_relative_to(w)
    subprocess.check_call(['git','diff','--exit-code','c780562b299bb100cdd006037ba424c8ad3264e9','--','lsms_library',':(exclude)lsms_library/countries/Tanzania/_/CONTENTS.org'],stdout=subprocess.DEVNULL)
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve()==w/'lsms_library/countries'
    terminal=json.loads(Path(str(args.capture)+'.terminal.json').read_text())
    assert terminal['head']=='b05f91f02a6988cd400d4dfa09e36f39f7366ca8' and terminal['job']=='41017857' and terminal['exit_code']==0 and terminal['credential_copy_absent']
    inventory=json.loads((args.capture/'inventory.json').read_text())
    assert len(inventory['reader_roles'])==len(inventory['stata_read_returns'])==12 and inventory['new_source_projections']==0
    for rel,entry in terminal['files'].items():assert hashlib.sha256((args.capture/rel).read_bytes()).hexdigest()==entry['sha256']
    def load(n,name):
        p=args.capture/'private-captures'/f'{n:02d}-{name}'
        numeric=pd.read_pickle(Path(str(p)+'.numeric.pkl'))
        with Path(str(p)+'.categories.pkl').open('rb') as f:cats=pickle.load(f)
        labelled=numeric.copy(deep=True)
        for col,mapping in cats.items():labelled[col]=labelled[col].replace(mapping)
        return numeric,labelled,cats
    c19,lc19,cc19=load(0,'HH_SEC_A.dta');r19,lr19,cr19=load(1,'HH_SEC_B.dta')
    key,lkey,ckey=load(2,'npssdd.panel.key.dta');upd,lupd,cupd=load(3,'upd4_hh_a.dta')
    c4,lc4,cc4=load(4,'hh_sec_a.dta');r4,lr4,cr4=load(5,'hh_sec_b.DTA')
    numeric_frames=[c19,r19,key,upd,c4,r4]
    native_before=[f.copy(deep=True) for f in numeric_frames]
    sys.path.insert(0,str(w/'lsms_library/countries/Tanzania/_'))
    import tanzania
    native_path=w/'lsms_library/countries/Tanzania/_/tanzania.py'
    assert Path(tanzania.__file__).resolve()==native_path
    assert native_path.read_bytes()==subprocess.check_output(['git','show','c780562b299bb100cdd006037ba424c8ad3264e9:lsms_library/countries/Tanzania/_/tanzania.py'])
    mapping_path=w/'lsms_library/countries/Tanzania/2019-20/_/mapping.py'
    spec=importlib.util.spec_from_file_location('tanzania1497_current_mapping',mapping_path)
    mapping=importlib.util.module_from_spec(spec);spec.loader.exec_module(mapping)
    from lsms_library import local_tools,data_access
    def refuse(*a,**k):raise AssertionError('New source/API/parquet IO forbidden in closed classifier')
    local_tools.get_dataframe=data_access.get_data_file=refuse
    tanzania.get_dataframe=mapping.get_dataframe=refuse
    lsms_library.Country.__init__=lsms_library.Feature.__init__=refuse
    pd.read_parquet=pd.DataFrame.to_parquet=refuse
    args.output.mkdir(mode=0o700,parents=True,exist_ok=False)
    current_present=nonblank(key.sdd_hhid)&key.sdd_indid.notna()
    previous_present=nonblank(key.y4_hhid)&key.indidy4.notna()
    keyspec=[('SDDcover',c19,['sdd_hhid']),('SDDroster',r19,['sdd_hhid','sdd_indid']),
             ('SDDpanelkey_all',key,['UPI']),
             ('SDDpanelkey_current',key[current_present],['sdd_hhid','sdd_indid']),
             ('SDDpanelkey_NPS4',key[previous_present],['y4_hhid','indidy4']),
             ('UPDcover',upd,['UPHI','round']),('EXTcover',c4,['y4_hhid']),('EXTroster',r4,['y4_hhid','indidy4'])]
    result={'capture_head':terminal['head'],'capture_job':terminal['job'],'all_capture_files_reauthenticated':len(terminal['files']),
        'source_keys':[{'source':name,'rows':len(df),'key':keys,'duplicate_key_rows':int(df.duplicated(keys,keep=False).sum()),
                        'missing_key_rows':int(df[keys].isna().any(axis=1).sum())} for name,df,keys in keyspec]}
    assert not c19.sdd_hhid.duplicated().any() and not r19.duplicated(['sdd_hhid','sdd_indid']).any()
    result['panelkey_presence']={'all_rows':len(key),'current_complete':int(current_present.sum()),
        'current_incomplete':int((~current_present).sum()),'previous_complete':int(previous_present.sum()),
        'previous_incomplete':int((~previous_present).sum()),
        'current_household_person_partial':int((nonblank(key.sdd_hhid)!=key.sdd_indid.notna()).sum()),
        'previous_household_person_partial':int((nonblank(key.y4_hhid)!=key.indidy4.notna()).sum())}
    pc=key[current_present].copy();assert not pc.duplicated(['sdd_hhid','sdd_indid']).any()
    assert not r4.duplicated(['y4_hhid','indidy4']).any()
    # Reuse the exact native sample gate, separately from household continuation.
    sample=pd.DataFrame({'t':'2019-20','i':c19.sdd_hhid.map(format_id),'v':c19.clusterid.map(format_id),
                         '_frame':c19.sdd_cluster,'_y4_hhid':c19.y4_hhid.map(format_id)}).set_index(['t','i'])
    admitted=mapping.derive_split_off_parent_cluster(sample)
    in_frame=mapping._has_sdd_cluster(c19.sdd_cluster)
    target=admitted.Derivation.eq(mapping.SPLIT_OFF_PARENT_DERIVATION).fillna(False).to_numpy()
    result['cohort']={'raw_cover':len(c19),'frame':int(in_frame.sum()),'split':int((~in_frame).sum()),
        'native_parent_cluster_cases':int(target.sum()),'historical_expected':238,'matches_historical':int(target.sum())==238,
        'own_original_flag_vs_frame_disagreements':int((c19.hh_a10.eq(1)!=in_frame).sum())}
    by_parent=defaultdict(list);frame_by_parent=defaultdict(list)
    for row,frameflag in zip(c19.itertuples(),in_frame):
        by_parent[format_id(row.y4_hhid)].append(format_id(row.sdd_hhid))
        if frameflag:frame_by_parent[format_id(row.y4_hhid)].append(format_id(row.sdd_hhid))
    # Replay CURRENT code over the WHOLE original cover/roster, not the238 subset.
    replay=tanzania._map_with_head_tracking(lc19,lr19,'sdd_hhid','y4_hhid')
    assert not replay.i.duplicated().any(),'native selector emits repeated current households; no silent reduction'
    committed=json.loads((w/'lsms_library/countries/Tanzania/_/updated_ids.json').read_text())
    raw_replay=dict(zip(replay.i,replay.previous_i))
    canonical_replay={hh:committed['2014-15'].get(prev,prev) for hh,prev in raw_replay.items()}
    actual=committed['2019-20']
    result['whole_selector_replay']={'raw_links':len(raw_replay),'committed_links':len(actual),
        'missing_from_committed':len(set(canonical_replay)-set(actual)),
        'extra_in_committed':len(set(actual)-set(canonical_replay)),
        'target_differences':sum(actual.get(h)!=v for h,v in canonical_replay.items())}
    # Compare the raw UPD reconstruction too, without writing any JSON.
    upre=tanzania.map_08_15(lupd[['r_hhid','round','UPHI']],['r_hhid','round','UPHI'])
    up4=upre.xs('2014-15',level='t').previous_i.to_dict()
    result['UPD_R4_chain']={'replayed_entries':len(up4),'committed_entries':len(committed['2014-15']),
        'exact':up4==committed['2014-15'],'all_EXT_households_present_in_UPD_R4':bool(c4.y4_hhid.isin(set(upd.loc[upd['round'].eq(4),'r_hhid'])).all())}
    # Own source dictionaries establish these codes; no cross-wave borrowing.
    assert cr19['hh_b05'][1]=='HEAD' and cr4['hh_b05'][1]=='HEAD'
    assert cr19['hh_b06'][99]=='NOT PREVIOUSLY PRESENT'
    old_heads=r4[r4.hh_b05.eq(1)].copy()
    heads_by=old_heads.groupby('y4_hhid').indidy4.apply(list).to_dict()
    result['NPS4_heads']={'rows':len(old_heads),'households':old_heads.y4_hhid.nunique(),
        'head_person_is1':int(old_heads.indidy4.eq(1).sum()),'head_person_not1':int(old_heads.indidy4.ne(1).sum()),
        'households_without_one_head':sum(len(heads_by.get(h,[]))!=1 for h in c4.y4_hhid)}
    member1_role=r4[r4.indidy4.eq(1)].set_index('y4_hhid').hh_b05.to_dict()
    old_role_labels=cr4['hh_b05']
    current_heads=r19[r19.hh_b05.eq(1)].copy()
    head1=set(current_heads.loc[current_heads.hh_b06.eq(1),'sdd_hhid'])
    cover_prev=c19.set_index('sdd_hhid').y4_hhid.to_dict()
    # Independent individual-level panel key, preserving both household+person IDs.
    pk=pc[['sdd_hhid','sdd_indid','y4_hhid','indidy4']].rename(columns={'y4_hhid':'_key_y4','indidy4':'_key_person'})
    people=r19.merge(pk,on=['sdd_hhid','sdd_indid'],how='left',validate='one_to_one',indicator=True)
    previous=r19.hh_b06.ne(99)
    result['person_link_validation']={'SDDroster_rows':len(r19),'panelkey_current_rows':len(pc),
        'roster_not_in_panelkey':int(people._merge.ne('both').sum()),
        'previously_present_roster_rows':int(previous.sum()),
        'previous_person_number_disagreement':int((previous & people.hh_b06.ne(people._key_person)).sum()),
        'previous_household_roster_vs_panelkey_disagreement':int((previous & people.y4_hhid.ne(people._key_y4)).sum()),
        'previous_household_roster_vs_cover_disagreement':int((previous & people.y4_hhid.ne(people.sdd_hhid.map(cover_prev))).sum())}
    def locations(df,household,person):
        out=defaultdict(set)
        for hh,pid,current in df[[household,person,'sdd_hhid']].itertuples(index=False,name=None):
            if pd.notna(hh) and pd.notna(pid) and pid!=99:out[(format_id(hh),float(pid))].add(format_id(current))
        return out
    key_locations=locations(pc,'y4_hhid','indidy4')
    roster_locations=locations(r19,'y4_hhid','hh_b06')
    by_cover=r19.copy();by_cover['_cover_y4']=by_cover.sdd_hhid.map(cover_prev)
    cover_locations=locations(by_cover,'_cover_y4','hh_b06')
    # CURRENT-head/person1 selection may be a different previous household's person1.
    head_people=people[people.hh_b05.eq(1)]
    current_heads_by=defaultdict(list)
    for row in head_people.to_dict('records'):current_heads_by[format_id(row['sdd_hhid'])].append(row)
    cases=[]
    for raw in c19.loc[target].itertuples():
        prev=format_id(raw.y4_hhid);split=format_id(raw.sdd_hhid);frames=frame_by_parent.get(prev,[])
        assert len(frames)==1,'cohort parent cluster does not imply a unique frame household'
        frame=frames[0];family=by_parent[prev];pids=heads_by.get(prev,[])
        oldkey=(prev,float(pids[0])) if len(pids)==1 else None
        loc=key_locations.get(oldkey,set()) if oldkey else set()
        candidates=set(family)&head1
        current_method='current_head_previous_member1' if len(candidates)==1 else 'multiple_member1_current_heads' if candidates else 'first_cover_row_fallback'
        selected=set(family)&set(raw_replay)
        assert len(selected)==1,'whole source family does not have one native selected household'
        if not candidates:assert selected=={family[0]},'native fallback is not the first input cover household'
        role_code=member1_role.get(prev)
        case={'previous_household':prev,'frame_household':frame,'split_household':split,
            'family_households':family,'family_candidate_count':len(family),
            'selected_households':sorted(selected),
            'selected_relation':'frame' if frame in selected else 'split' if split in selected else 'other_sibling',
            'head_found_in_selected_household':bool(loc&selected),
            'head_found_in_other_family_household':bool(loc&(set(family)-selected)),
            'head_found_outside_cover_family':bool(loc-set(family)),
            'committed_link':link_kind(frame,split,set(actual)),
            'replayed_link':link_kind(frame,split,set(canonical_replay)),
            'native_method':current_method,'member1_current_head_candidates':sorted(candidates),
            'candidate_heads_with_matching_previous_household':sum(h['_key_y4']==prev and h['_key_person']==1 for candidate in candidates for h in current_heads_by[candidate] if h['hh_b06']==1),
            'candidate_heads_with_different_previous_household':sum(h['_key_y4']!=prev and h['_key_person']==1 for candidate in candidates for h in current_heads_by[candidate] if h['hh_b06']==1),
            'NPS4_head_count':len(pids),'NPS4_head_person':pids[0] if len(pids)==1 else None,
            'NPS4_head_person_is1':len(pids)==1 and pids[0]==1,
            'NPS4_member1_role':old_role_labels.get(role_code,'missing'),
            'NPS4_head_location_panelkey':location_kind(loc,frame,split) if oldkey else 'unknown_previous_head',
            'NPS4_head_location_roster_own_y4':location_kind(roster_locations.get(oldkey,set()),frame,split) if oldkey else 'unknown_previous_head',
            'NPS4_head_location_cover_y4':location_kind(cover_locations.get(oldkey,set()),frame,split) if oldkey else 'unknown_previous_head',
            'NPS4_head_currently_HEAD_in_frame':any(pd.notna(h['_key_person']) and h['_key_y4']==prev and h['_key_person']==pids[0] for h in current_heads_by[frame]) if oldkey else False,
            'NPS4_head_currently_HEAD_in_split':any(pd.notna(h['_key_person']) and h['_key_y4']==prev and h['_key_person']==pids[0] for h in current_heads_by[split]) if oldkey else False}
        cases.append(case)
    private=pd.DataFrame(cases)
    assert len(private)==int(target.sum()) and not private.duplicated(['previous_household','frame_household','split_household']).any()
    private.to_pickle(args.output/'cases.private.pkl');(args.output/'cases.private.pkl').chmod(0o600)
    result['case_accounting']={'all_pairs':len(private),'distinct_previous_households':private.previous_household.nunique(),
        'committed_link':dict(Counter(private.committed_link)),'replayed_link':dict(Counter(private.replayed_link)),
        'NPS4_head_location_panelkey':dict(Counter(private.NPS4_head_location_panelkey)),
        'NPS4_head_location_roster_own_y4':dict(Counter(private.NPS4_head_location_roster_own_y4)),
        'NPS4_head_location_cover_y4':dict(Counter(private.NPS4_head_location_cover_y4)),
        'native_method':dict(Counter(private.native_method)),
        'selected_relation':dict(Counter(private.selected_relation)),
        'head_found_in_selected_household':int(private.head_found_in_selected_household.sum()),
        'head_found_in_other_family_household':int(private.head_found_in_other_family_household.sum()),
        'head_found_outside_cover_family':int(private.head_found_outside_cover_family.sum()),
        'absent_head_distinct_previous_households':int(private.loc[private.NPS4_head_location_panelkey.eq('not_found'),'previous_household'].nunique()),
        'fallback_distinct_previous_households':int(private.loc[private.native_method.eq('first_cover_row_fallback'),'previous_household'].nunique()),
        'NPS4_head_person_is1':int(private.NPS4_head_person_is1.sum()),
        'cross_link_by_head_location_panelkey':pd.crosstab(private.committed_link,private.NPS4_head_location_panelkey).to_dict(),
        'cross_link_by_native_method':pd.crosstab(private.committed_link,private.native_method).to_dict(),
        'cross_link_by_member1_role':pd.crosstab(private.committed_link,private.NPS4_member1_role).to_dict(),
        'joint_link_location_method':[dict(zip(['committed_link','NPS4_head_location_panelkey','native_method'],k),pairs=int(v)) for k,v in private.groupby(['committed_link','NPS4_head_location_panelkey','native_method']).size().items()],
        'candidate_head_attribution':[{'committed_link':k[0],'matching_previous_household':int(k[1]),'different_previous_household':int(k[2]),'pairs':int(v)} for k,v in private.groupby(['committed_link','candidate_heads_with_matching_previous_household','candidate_heads_with_different_previous_household']).size().items()]}
    for before,after in zip(native_before,numeric_frames):pd.testing.assert_frame_equal(before,after,check_exact=True)
    for rel,entry in terminal['files'].items():assert hashlib.sha256((args.capture/rel).read_bytes()).hexdigest()==entry['sha256']
    result['native_payload_conservation']={'numeric_frames_exact':6,'sealed_capture_files_still_exact':len(terminal['files'])}
    result['scope']='Closed source/grain/selector diagnostics only; no new continuation rule, public API call or JSON regeneration'
    (args.output/'accounting.json').write_text(json.dumps(result,indent=2,default=int)+'\n');(args.output/'accounting.json').chmod(0o600)
    print(json.dumps(result,default=int))


if __name__=='__main__':main()

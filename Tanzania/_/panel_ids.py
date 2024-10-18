#!/usr/bin/env python3
import json
from tanzania import Waves
from lsms import from_dta
import dvc.api
import sys
sys.path.append('../../_')
from local_tools import format_id, RecursiveDict

def panel_ids(Waves):
    """Return RecursiveDict of household identifiers.
    Modified since Tanzania has a complex panel structure in 2008-2015, requiring more than 2 variables to link households.
    """
    D = RecursiveDict()
    for t,v in Waves.items():
        if len(v):
            fn = f"../{t}/Data/{v[0]}"
            columns = v[1] if isinstance(v[1], list) else [v[1], v[2]]
            try:
                df = from_dta(fn)[columns]
            except FileNotFoundError:
                with dvc.api.open(fn,mode='rb') as dta: df = from_dta(dta)[columns]

            if isinstance(v[1], list):
                df[v[1][0]]=df[v[1][0]].apply(format_id)
                D = map_08_15(df,v[1], D) 
            else:                  
                # Clean-up ids
                df[v[1]] = df[v[1]].apply(format_id)
                df[v[2]] = df[v[2]].apply(format_id)

                if len(v)==4: # Remap id1
                    df[v[2]] = df[v[2]].apply(v[3])

                D.update(df[[v[1],v[2]]].dropna().values.tolist())

    return D


def map_08_15(df, v1, D):
    r_hhid_column, round_column, uphis_column = v1
    # Group by household_id and round to a list of uphis
    grouped = df.groupby([r_hhid_column, round_column])[uphis_column].apply(list).to_dict()

    # Sort groups for orderly processing
    sorted_keys = sorted(grouped.keys(), key=lambda x: x[1])  # Sort by round number
    for key in sorted_keys:
        hh_id, round_num = key
        uphis = grouped[key]

        # Loop through each previously processed group to find intersections of uphis
        for prev_key in sorted_keys:
            if prev_key[1] >= round_num:  # Skip the current and future entries
                break
            other_hh_id, other_round_num = prev_key
            other_uphis = grouped[prev_key]

            if set(uphis).intersection(other_uphis):
                # Assign the hh_id to the identifier from the lowest intersecting round
                D[hh_id] = other_hh_id
                # End the loop early if processing the lowest possible round
                if other_round_num == 1:
                    break
    return D

D = panel_ids(Waves)

with open('panel_ids.json','w') as f:
    json.dump(D.data,f)
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
    # Grouping and initial dictionary population
    grouped = df.groupby([v1[0], v1[1]])[v1[2]].apply(list).to_dict()
    # Initialize the dictionary for tracking the lowest round for each hhid
    lowest_round = {}

    # Sort the items by round for orderly processing
    for (hhid, rnd), uphis in sorted(grouped.items(), key=lambda x: x[0][1]):
        if rnd == 1:
            lowest_round[hhid] = (hhid, rnd)
            continue
        else:
            # Track the lowest round and corresponding hhid for later usage
            if hhid not in lowest_round or rnd < lowest_round[hhid][1]:
                lowest_round[hhid] = (hhid, rnd)

            # Check intersections with all previous hhids and map accordingly
            for (other_hhid, other_rnd), other_uphis in grouped.items():
                if rnd > other_rnd and set(uphis).intersection(other_uphis):
                    # Map current hhid to the hhid from the lowest round that intersects
                    D[hhid] = lowest_round[other_hhid][0]
    return D

D = panel_ids(Waves)

with open('panel_ids.json','w') as f:
    json.dump(D.data,f)

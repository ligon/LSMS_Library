"""Summarise the base and branch unit censuses side by side (aggregate counts).

Usage: python summarize_census.py <census_base.json> <census_branch.json> <out.json>

Classification of a SERVED 'unit' on development (base):
  minted_at_read   the stored value is a missing key map_index renames
                   (pd.NA on a `string`-policy level, or 'nan' / '<NA>');
                   computed as served_unit - raw_unit.
  stored_unit      the parquet itself holds literal 'unit'.  Either minted at
                   BUILD (grab_data ran map_index before the L2-wave write) or a
                   genuine source label -- split by provenance below.
"""
import json
import sys

base, br = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
# Provenance of stored literal 'unit' (read from the config tree, see ledger):
#  Panama: units.json code 13 'unidad' -> 'unit' (1997), 2008 wave script code
#          39 -> 'unit'.  GENUINE.
#  Mali food_acquired 2014-15: Mali/_/CONTENTS.org documents it as minted by
#          map_index at build (no source unit value).  MINTED AT BUILD.
GENUINE_STORED = {"Panama|food_acquired"}
rows = []
for key in sorted(base["index_u"]):
    b = base["index_u"][key]
    r = br["index_u"].get(key)
    country, table, kind, wave = key.split("|")
    for t, cb in sorted(b["by_t"].items()):
        cr = r["by_t"][t] if r else {}
        if not (cb["served_unit"] or cb["raw_unit"] or cr.get("served_unknown", 0) != cb["served_unknown"]):
            continue
        minted_read = cb["served_unit"] - cb["raw_unit"]
        stored = cb["raw_unit"]
        genuine = stored if f"{country}|{table}" in GENUINE_STORED else 0
        rows.append({
            "country": country, "table": table, "tier": kind, "wave_dir": wave, "t": t,
            "u_policy": b["u_numpy_type_recorded"], "rows": cb["rows"],
            "served_unit_base": cb["served_unit"],
            "minted_at_read": minted_read,
            "stored_unit_minted_at_build": stored - genuine,
            "stored_unit_genuine": genuine,
            "served_unit_branch": cr.get("served_unit"),
            "served_unknown_base": cb["served_unknown"],
            "served_unknown_branch": cr.get("served_unknown"),
            "dup_served_base": cb.get("dup_served"), "dup_served_branch": cr.get("dup_served"),
            "dup_raw": cb.get("dup_raw"),
        })
json.dump(rows, open(sys.argv[3], "w"), indent=1)
hdr = ["country", "table", "tier", "t", "u_policy", "served_unit_base", "minted_at_read",
       "stored_unit_minted_at_build", "stored_unit_genuine", "served_unit_branch",
       "served_unknown_branch", "dup_served_base", "dup_served_branch"]
short = ["country", "table", "tier", "t", "pol", "unit_dev", "mint_rd", "mint_bld", "genuine",
         "unit_br", "Unk_br", "dup_dev", "dup_br"]
print(" | ".join(short))
for r in rows:
    print(" | ".join(str(r[h]) for h in hdr))

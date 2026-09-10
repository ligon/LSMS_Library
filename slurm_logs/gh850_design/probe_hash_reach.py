"""Which tables' build fingerprints reach conversion_to_kgs / _get_kg_factors?

Walks the SAME closure the cache hash walks (_build_registry._closure_parts),
and reports whether the qualname appears in the parts for each table.
Read-only; touches no cache.
"""
import sys
from lsms_library import _build_registry as br

TARGETS = ('lsms_library.transformations.conversion_to_kgs',
           'lsms_library.transformations._get_kg_factors',
           'lsms_library.transformations.food_quantities_from_acquired',
           'lsms_library.transformations.food_prices_from_acquired',
           'lsms_library.transformations.harvest_kg_factors')


def parts_for(table):
    seen = set()
    parts = []
    for _qn, (fn, tables) in sorted(br._BUILD_TRANSFORMS.items()):
        if table is not None and tables and table not in tables:
            continue
        parts += br._closure_parts(fn, seen)
    return seen, parts


def main():
    tables = sys.argv[1:] or [None, 'food_acquired', 'household_roster',
                              'crop_production', 'sample', 'cluster_features']
    for t in tables:
        seen, _ = parts_for(t)
        hit = [x for x in TARGETS if x in seen]
        print(f"{str(t):20s} reached={len(seen):4d}  targets_hit={hit}")
    print()
    print("fingerprints:")
    for t in tables:
        print(f"  {str(t):20s} {br.build_transforms_fingerprint(t)[:16]}")


if __name__ == '__main__':
    main()

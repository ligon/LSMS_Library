"""Cross-country verification of the new units= kwarg.

Runs food_prices() and food_quantities() with each accepted units=
value across every country that declares food_acquired, and reports
shape / column / index / exception per (country, table, mode).

Run with:
    .venv/bin/python slurm_logs/verify_units_kwarg.py
"""
import json
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter


COUNTRIES = [
    'Benin', 'Burkina_Faso', 'CotedIvoire', 'Ethiopia', 'GhanaLSS',
    'Guinea-Bissau', 'Malawi', 'Mali', 'Nepal', 'Niger',
    'Nigeria', 'Senegal', 'Tanzania', 'Togo', 'Uganda',
]

FOOD_PRICES_MODES = ['kgvalue', 'unitvalue', 'kgprice', 'unitprice']
FOOD_QUANTITIES_MODES = ['kgs', 'units']


def probe_one(country_name):
    """Returns dict with results for every (table, mode) combination."""
    out = {'country': country_name, 'tables': {}}
    try:
        import lsms_library as ll
        c = ll.Country(country_name)
    except Exception as e:
        out['load_error'] = repr(e)
        return out

    # food_prices ----------------------------------------------------
    out['tables']['food_prices'] = {}
    for mode in FOOD_PRICES_MODES:
        entry = {}
        t0 = perf_counter()
        try:
            df = c.food_prices(units=mode)
            entry['shape'] = list(df.shape)
            entry['cols'] = list(df.columns)
            entry['idx'] = list(df.index.names)
            entry['unique'] = bool(df.index.is_unique)
            if 'u' in df.index.names and len(df) > 0:
                entry['u_top5'] = (df.index.get_level_values('u')
                                   .value_counts()
                                   .head(5)
                                   .to_dict())
        except Exception as e:
            entry['error'] = repr(e)
            entry['traceback_tail'] = traceback.format_exc().split('\n')[-4:-1]
        entry['elapsed_s'] = round(perf_counter() - t0, 2)
        out['tables']['food_prices'][mode] = entry

    # food_quantities ------------------------------------------------
    out['tables']['food_quantities'] = {}
    for mode in FOOD_QUANTITIES_MODES:
        entry = {}
        t0 = perf_counter()
        try:
            df = c.food_quantities(units=mode)
            entry['shape'] = list(df.shape)
            entry['cols'] = list(df.columns)
            entry['idx'] = list(df.index.names)
            entry['unique'] = bool(df.index.is_unique)
            if 'u' in df.index.names and len(df) > 0:
                entry['u_top5'] = (df.index.get_level_values('u')
                                   .value_counts()
                                   .head(5)
                                   .to_dict())
        except Exception as e:
            entry['error'] = repr(e)
            entry['traceback_tail'] = traceback.format_exc().split('\n')[-4:-1]
        entry['elapsed_s'] = round(perf_counter() - t0, 2)
        out['tables']['food_quantities'][mode] = entry

    return out


def main():
    t0 = perf_counter()
    results = []
    with ProcessPoolExecutor(max_workers=15) as ex:
        futures = {ex.submit(probe_one, name): name for name in COUNTRIES}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                results.append({'country': name, 'fatal': repr(e)})
            print(f"  done: {name}", flush=True)

    elapsed = perf_counter() - t0
    print(f"\nTotal wall time: {elapsed:.1f}s")

    # Save full JSON
    out_path = Path(__file__).parent / 'verify_units_kwarg_results.json'
    out_path.write_text(json.dumps(results, indent=2, default=str))
    print(f"Full results: {out_path}")

    # Summarise
    print("\n=== Summary ===")
    print(f"{'Country':<18} {'fp.kgv':<10} {'fp.uv':<10} {'fp.kgp':<10} "
          f"{'fp.up':<10} {'fq.kgs':<10} {'fq.u':<10}")
    for r in sorted(results, key=lambda x: x['country']):
        if 'load_error' in r or 'fatal' in r:
            err = r.get('load_error') or r.get('fatal')
            print(f"{r['country']:<18} LOAD ERROR: {err[:80]}")
            continue
        cells = []
        for tbl, modes in [
            ('food_prices', FOOD_PRICES_MODES),
            ('food_quantities', FOOD_QUANTITIES_MODES),
        ]:
            for m in modes:
                e = r['tables'][tbl][m]
                if 'error' in e:
                    cells.append('ERR')
                else:
                    cells.append(str(e['shape'][0]))
        print(f"{r['country']:<18} " + " ".join(f"{c:<10}" for c in cells))

    # Errors
    print("\n=== Errors ===")
    n_errors = 0
    for r in sorted(results, key=lambda x: x['country']):
        if 'load_error' in r or 'fatal' in r:
            continue
        for tbl, modes in r['tables'].items():
            for m, e in modes.items():
                if 'error' in e:
                    n_errors += 1
                    print(f"\n  {r['country']}.{tbl}(units={m!r}):")
                    print(f"    {e['error']}")
    if n_errors == 0:
        print("  (none)")
    else:
        print(f"\n  {n_errors} errors total")
    return 0 if n_errors == 0 else 1


if __name__ == '__main__':
    sys.exit(main())

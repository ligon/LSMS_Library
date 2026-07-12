#!/usr/bin/env python
"""Parallel (process-pool) version of the pooled/per-country-wave share-female run.

Concurrent builds of *different* countries share the parquet cache safely and
read source blobs lock-free from S3 (scrum-master-hpc guidance + CLAUDE.md).
Uses spawn workers (clean import per worker) and LSMS_SKIP_AUTH (creds already
on disk) so workers don't each re-run import-time auth.
"""
import os
os.environ.setdefault('LSMS_SKIP_AUTH', '1')          # ~/.config/lsms_library/s3_creds exists
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings; warnings.filterwarnings('ignore')
import pandas as pd

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'share_female.csv')
N_WORKERS = 12


def process_country(c):
    import warnings; warnings.filterwarnings('ignore')
    import pandas as pd
    import lsms_library as ll
    try:
        C = ll.Country(c)
        r = C.household_roster()
        sex = r['Sex'].astype(str).str.strip().values
        df = pd.DataFrame({
            'i': r.index.get_level_values('i'),
            't': r.index.get_level_values('t'),
            'Sex': sex,
        })
        try:                                          # design weight at individual grain
            w = C.sample()['weight']                  # index (i, t)
            df = df.set_index(['i', 't'])
            df['w'] = w.reindex(df.index).values
            df = df.reset_index()
        except Exception:
            df['w'] = pd.NA
        rows = []
        for t, g in df.groupby('t'):
            mf = g['Sex'].isin(['M', 'F'])
            wv = g[mf].dropna(subset=['w'])
            rows.append(dict(
                country=c, t=str(t),
                n_F=int((g['Sex'] == 'F').sum()),
                n_M=int((g['Sex'] == 'M').sum()),
                n_other=int((~mf).sum()),
                w_F=float(wv.loc[wv['Sex'] == 'F', 'w'].sum()),
                w_M=float(wv.loc[wv['Sex'] == 'M', 'w'].sum()),
                w_cov=float(len(wv) / mf.sum()) if mf.sum() else 0.0,
            ))
        return (c, rows, len(r), None)
    except Exception as e:
        import traceback
        return (c, [], 0, f'{type(e).__name__}: {e}\n{traceback.format_exc()}')


def main():
    import lsms_library as ll
    countries = ll.Feature('household_roster').countries
    print(f'{len(countries)} countries; {N_WORKERS} workers', flush=True)
    allrows, done = [], 0
    ctx = mp.get_context('spawn')
    with ProcessPoolExecutor(max_workers=N_WORKERS, mp_context=ctx) as ex:
        futs = {ex.submit(process_country, c): c for c in countries}
        for fut in as_completed(futs):
            c = futs[fut]
            try:
                cc, rows, n, err = fut.result()
            except Exception as e:
                done += 1
                print(f'FAIL {c} [{done}/{len(countries)}]: pool error {e}', flush=True)
                continue
            done += 1
            if err:
                print(f'FAIL {cc} [{done}/{len(countries)}]: {err.splitlines()[0]}', flush=True)
            else:
                allrows.extend(rows)
                pd.DataFrame(allrows).to_csv(OUT, index=False)      # checkpoint
                print(f'OK   {cc} [{done}/{len(countries)}]: {n:>7,} records', flush=True)

    res = pd.DataFrame(allrows)
    res.to_csv(OUT, index=False)
    print('\n===== SUMMARY =====', flush=True)
    tf, tm = res['n_F'].sum(), res['n_M'].sum()
    print(f'countries done: {res.country.nunique()}/{len(countries)}; country-waves: {len(res)}')
    print(f'UNWEIGHTED pooled: F={tf:,} M={tm:,}  share female = {tf/(tf+tm):.4f}')
    res['unw_share'] = res['n_F'] / (res['n_F'] + res['n_M'])
    print(f'mean of per-country-wave unweighted shares = {res.unw_share.mean():.4f}')
    wok = res[res['w_F'] + res['w_M'] > 0].copy()
    wok['w_share'] = wok['w_F'] / (wok['w_F'] + wok['w_M'])
    print(f'weighted shares available for {len(wok)}/{len(res)} country-waves')
    print(f'mean of per-country-wave WEIGHTED shares = {wok.w_share.mean():.4f}')
    print('wrote', OUT, flush=True)


if __name__ == '__main__':
    main()

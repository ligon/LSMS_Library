"""Read every declared source file once through get_dataframe and record which
columns come back 100% null.  This is the MEASUREMENT for the null-read guard:
it uses the reader the guard would sit in, with the reader's own defaults.

Usage: python sweep_reads.py <manifest.json> <out.jsonl> <shard> <nshards>
"""
import json, sys, os, warnings, time
warnings.simplefilter('ignore')

import lsms_library
assert 'worktrees' in lsms_library.__file__, lsms_library.__file__

import pandas as pd
from pathlib import Path
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = Path(countries_root())


def candidates(rec):
    """Path forms to try, mirroring Wave.grab_data / the scripts' own cwd."""
    country, wave, fn, route = rec['country'], rec['wave'], rec['file'], rec['route']
    out = []
    if route == 'yaml':
        # grab_data: self.file_path / "Data" / file
        wf = rec.get('wave_folder') or wave
        p = ROOT / country / wf / 'Data' / fn
        out += [str(p)]
        try:
            out.append(str(p.relative_to(ROOT)))
        except ValueError:
            pass
    else:
        base = (ROOT / country / (rec.get('wave_folder') or wave) / '_') if wave \
               else (ROOT / country / '_')
        p = (base / fn).resolve()
        out += [str(p)]
        try:
            out.append(str(p.relative_to(ROOT)))
        except ValueError:
            pass
    return list(dict.fromkeys(out))


def main(manifest_path, out_path, shard, nshards):
    man = json.load(open(manifest_path))['manifest']
    # distinct file reads, carrying the union of requested columns
    keyed = {}
    for m in man:
        k = (m['country'], m['wave'], m['file'], m['route'])
        d = keyed.setdefault(k, {'country': m['country'], 'wave': m['wave'],
                                 'file': m['file'], 'route': m['route'],
                                 'wave_folder': m.get('wave_folder'),
                                 'tables': [], 'cols': {}})
        d['tables'].append({'table': m['table'], 'sub': m['sub']})
        for rc, uses in (m['cols'] or {}).items():
            d['cols'].setdefault(rc, [])
            for u in uses:
                u = dict(u, table=m['table'], sub=m['sub'])
                if u not in d['cols'][rc]:
                    d['cols'][rc].append(u)
    recs = sorted(keyed.values(), key=lambda r: (r['country'], str(r['wave']), r['file']))
    recs = [r for i, r in enumerate(recs) if i % nshards == shard]

    # wave_folder lookup
    from lsms_library.country import Country
    wf_cache = {}

    def wave_folder(country, wave):
        if wave is None:
            return None
        if (country, wave) not in wf_cache:
            try:
                wf_cache[(country, wave)] = Country(country)[wave].wave_folder
            except Exception:
                wf_cache[(country, wave)] = wave
        return wf_cache[(country, wave)]

    with open(out_path, 'w', buffering=1) as fh:
        for n, r in enumerate(recs):
            r['wave_folder'] = wave_folder(r['country'], r['wave'])
            t0 = time.time()
            df = None
            err = None
            used = None
            cands = candidates(r)
            # Only attempt files we actually HOLD: present on disk, or DVC-tracked
            # (sidecar next to the path).  A ref we do not hold is 'not-held',
            # never 'clean' -- and attempting it costs an ~80 s DVC/WB round trip.
            held = [c for c in cands
                    if Path(c).exists() or Path(str(c) + '.dvc').exists()]
            if not held:
                out0 = {k: r[k] for k in ('country', 'wave', 'file', 'route')}
                out0.update({'tables': r['tables'], 'secs': 0.0,
                             'status': 'not-held', 'err': 'no local file and no .dvc sidecar',
                             'tried': cands})
                fh.write(json.dumps(out0) + '\n')
                continue
            for c in held:
                try:
                    df = get_dataframe(c)
                    used = c
                    break
                except Exception as e:                       # noqa: BLE001
                    err = f'{type(e).__name__}: {e}'[:300]
            out = {k: r[k] for k in ('country', 'wave', 'file', 'route')}
            out['tables'] = r['tables']
            out['secs'] = round(time.time() - t0, 2)
            if df is None or not isinstance(df, pd.DataFrame):
                out['status'] = 'unmeasured'
                out['err'] = err
            else:
                nrow, ncol = df.shape
                allnull = []
                if nrow > 0:
                    for c in df.columns:
                        try:
                            if df[c].isna().all():
                                allnull.append(str(c))
                        except Exception:
                            pass
                out['status'] = 'ok'
                out['path'] = used
                out['nrow'] = int(nrow)
                out['ncol'] = int(ncol)
                out['allnull'] = allnull
                out['n_allnull'] = len(allnull)
                out['frac_allnull'] = round(len(allnull) / ncol, 4) if ncol else None
                # which of the requested raw columns are all-null / absent
                req = r['cols']
                out['requested'] = {}
                for rc, uses in req.items():
                    if rc not in df.columns:
                        state = 'absent'
                    elif nrow == 0:
                        state = 'zero-rows'
                    elif df[rc].isna().all():
                        state = 'all-null'
                    else:
                        state = 'ok'
                    if state != 'ok':
                        out['requested'][rc] = {'state': state, 'uses': uses}
            fh.write(json.dumps(out) + '\n')
            del df
    print(f'shard {shard}/{nshards}: {len(recs)} records done')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))

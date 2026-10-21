#!/usr/bin/env python
"""GH #805 L1 census: what ``get_dataframe``'s cp1252 re-decode does to every
HELD ``.dta``.

For every ``*.dta.dvc`` sidecar under the countries root whose blob is in the
blob store, read the blob the way ``get_dataframe``'s Stata branch does
(pandas ``StataReader``, which decodes format < 118 as latin-1) and apply the
LIVE predicate imported from ``lsms_library.local_tools``
(``_stata_release``, ``_cp1252_candidate``, ``_cp1252_plausible``,
``_cp1252_redecode_map``) -- so the census always judges with the code on
``PYTHONPATH``, never a copy of it.  The verdict is the FRAME-path rule: the
file's whole value-label table plus the distinct values of its str columns.

Per-file verdicts:

  clean          no C1 code point (U+0080-U+009F) anywhere
  gated-118      C1 present, but the head is format >= 118 (or unrecognised):
                 the reader does nothing
  no-candidate   C1 present only inside UTF-8 double encodings (intact or
                 truncated) or in strings that are not latin-1 encodable
  decode         the reader re-decodes the file's candidates as cp1252
  refuse         candidates present, file refused whole (a cp1252 hole byte,
                 or a cp1252 symbol between two letters: a DOS code page)
  error          pandas cannot read it; ``get_dataframe`` falls back to
                 pyreadstat, which the re-decode does not touch

What it does NOT see: a caller passing ``encoding=`` (that disables the
re-decode; three dead scripts do), and any blob not in the store -- the
summary lists the not-held sidecars per country.  ``decode`` files whose
decoded characters fall outside the set the r1 red team eyeballed are listed
under "REVIEW": the predicate cannot tell cp1252 from a code page that also
avoids the five hole bytes (Mac Roman, cp852, cp1250 letters outside the
shared Š Ž š ž), so a person must look.

Reads only: no DVC CLI, no network, nothing written to the blob store or to
``data_root()``.

Usage (from a checkout; the predicate comes from the package on PYTHONPATH):

  PYTHONPATH=<checkout> PYTHONSAFEPATH=1 OPENBLAS_NUM_THREADS=1 \\
    <venv>/bin/python slurm_logs/gh805_cp1252/census_l1.py \\
      --blobs <blob store root, e.g. $LSMS_DATA_DIR/dvc-cache> \\
      --out census.jsonl --summary census.md [--nproc 6] \\
      [--countries-root <tree>] [--strings c1_strings.jsonl] \\
      [--compare <an earlier census.jsonl>]
"""
from __future__ import annotations

import argparse
import collections
import datetime
import hashlib
import json
import os
import subprocess
import sys
from multiprocessing import Pool
from pathlib import Path

#: Characters the r1 red team (2026-10-19) eyeballed as correct cp1252 decodes
#: in real text; anything else a ``decode`` file produces goes under REVIEW.
REVIEWED_DECODES = frozenset('’‘“”–—…œŒšžŠŽ€˜')

_ARGS = {}


def _blob(cache: Path, md5: str):
    for p in (cache / md5[:2] / md5[2:],
              cache / 'files' / 'md5' / md5[:2] / md5[2:]):
        if p.exists():
            return p
    return None


def _sidecars(root: Path, cache: Path):
    """(rel, md5, blob-or-None) for every ``*.dta.dvc`` under *root*."""
    import yaml
    for dirpath, dirs, files in os.walk(root):
        dirs.sort()
        for f in sorted(files):
            if not f.lower().endswith('.dta.dvc'):
                continue
            p = Path(dirpath) / f
            try:
                md5 = yaml.safe_load(p.read_text())['outs'][0]['md5']
            except Exception:
                continue
            rel = str(p.relative_to(root))[:-len('.dvc')]
            yield rel, md5, _blob(cache, md5)


def _classify(s, lt):
    """'candidate' | 'double' | 'double_truncated' | 'ge256', by the live
    predicate (a string the predicate rejects that is not intact UTF-8 is a
    truncated double; older predicates never produce that class)."""
    try:
        b = s.encode('latin-1')
    except UnicodeEncodeError:
        return 'ge256'
    if lt._cp1252_candidate(s):
        return 'candidate'
    try:
        b.decode('utf-8')
        return 'double'
    except UnicodeDecodeError:
        return 'double_truncated'


def probe(job):
    rel, md5, blob = job
    import warnings
    import pandas as pd
    from lsms_library import local_tools as lt
    rec = {'rel': rel, 'country': rel.split('/', 1)[0], 'md5': md5}
    try:
        with open(blob, 'rb') as fh:
            head = fh.read(64)
        rec['release'] = lt._stata_release(head)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with pd.io.stata.StataReader(blob) as r:
                df = r.read(convert_dates=False, convert_categoricals=False)
                labels = r.value_labels()
                uses = collections.defaultdict(list)       # label set -> columns
                for col, lbl in zip(r._varlist, r._lbllist):
                    if lbl and col in df.columns:
                        uses[lbl].append(col)
    except Exception as e:  # noqa: BLE001 -- a census records, never stops
        rec['verdict'] = 'error'
        rec['error'] = f'{type(e).__name__}: {e}'[:200]
        return rec, None
    rec['nrows'] = len(df)
    rec['n_value_labels'] = sum(len(m) for m in labels.values())

    # Every distinct C1 string: (kind, var, string, cells).  A label's cells
    # are the rows whose code it labels, over every column using its label
    # set -- what get_dataframe serves once it substitutes labels.
    strings = []
    for var, m in labels.items():
        for code, lab in m.items():
            if isinstance(lab, str) and lt._C1_RE.search(lab):
                n = sum(int((df[c] == code).sum()) for c in uses.get(var, ()))
                strings.append(('label', str(var), lab, n))
    for c in df.columns:
        s = df[c]
        if not (s.dtype == object or pd.api.types.is_string_dtype(s)):
            continue
        for v, n in s.dropna().value_counts().items():
            if isinstance(v, str) and lt._C1_RE.search(v):
                strings.append(('value', str(c), v, int(n)))
    if not strings:
        rec['verdict'] = 'clean'
        return rec, None

    by_class = {'label': collections.Counter(), 'value': collections.Counter(),
                'cells': collections.Counter()}
    c1_bytes = collections.Counter()
    for kind, _, s, n in strings:
        k = _classify(s, lt)
        by_class[kind][k] += 1
        by_class['cells'][k] += n
        for ch in lt._C1_RE.findall(s):
            c1_bytes[f'{ord(ch):02x}'] += n
    rec['c1'] = {k: dict(v) for k, v in by_class.items()}
    rec['c1_bytes'] = dict(sorted(c1_bytes.items()))
    trunc = [s for _, _, s, _ in strings if _classify(s, lt) == 'double_truncated']
    if trunc:
        rec['double_truncated_examples'] = sorted(set(trunc))[:5]

    universe = {s for _, _, s, _ in strings}
    candidates = {s for s in universe if lt._cp1252_candidate(s)}
    release = rec['release']
    if release is None or release >= 118:
        rec['verdict'] = 'gated-118'
    elif not candidates:
        rec['verdict'] = 'no-candidate'
    else:
        mapping = lt._cp1252_redecode_map(universe)
        if mapping:
            rec['verdict'] = 'decode'
            decoded = collections.Counter()
            for kind, _, s, n in strings:
                if s in mapping:
                    decoded[kind + 's'] += 1
                    decoded['cells'] += n
                    decoded[kind + '_cells'] += n
            rec['decoded'] = dict(decoded)
            chars = set()
            for s, t in mapping.items():
                chars.update(t[i] for i, ch in enumerate(s) if lt._C1_RE.match(ch))
            rec['decoded_chars'] = ''.join(sorted(chars))
            rec['map_sha'] = hashlib.sha256(
                json.dumps(sorted(mapping.items())).encode()).hexdigest()[:16]
            rec['examples'] = [[s, mapping[s]] for s in sorted(mapping)[:4]]
        else:
            rec['verdict'] = 'refuse'
            why = collections.Counter()
            ex = []
            for s in sorted(candidates):
                if not lt._cp1252_plausible(s):
                    reason = ('hole' if any(ch in lt._CP1252_HOLES for ch in s)
                              else 'symbol-between-letters')
                    why[reason] += 1
                    if len(ex) < 4:
                        ex.append([reason, s])
            rec['refuse'] = dict(why)
            rec['examples'] = ex
    rec['n_candidates'] = len(candidates)
    dump = None
    if _ARGS.get('strings'):
        dump = {'rel': rel, 'release': release, 'strings': strings}
    return rec, dump


def _esc(obj):
    """JSON for the summary, with C1 code points spelled ``\\xNN`` (a raw C1
    control is invisible in a terminal and in most renderers)."""
    text = json.dumps(obj, ensure_ascii=False)
    return ''.join(f'\\x{ord(ch):02x}' if '\x80' <= ch <= '\x9f' else ch
                   for ch in text)


def _code_identity():
    from lsms_library import local_tools as lt
    pkg = Path(lt.__file__).resolve().parent.parent
    try:
        rev = subprocess.check_output(
            ['git', '-C', str(pkg), 'rev-parse', '--short', 'HEAD'],
            text=True, stderr=subprocess.DEVNULL).strip()
        dirty = subprocess.call(
            ['git', '-C', str(pkg), 'diff', '--quiet', '--', 'lsms_library/local_tools.py'],
            stderr=subprocess.DEVNULL) != 0
        rev += ' (local_tools.py modified)' if dirty else ''
    except (OSError, subprocess.CalledProcessError):
        rev = 'unknown'
    return lt.__file__, rev


def _summary(recs, unheld, args, compare):
    ident, rev = _code_identity()
    v = collections.Counter(r['verdict'] for r in recs)
    L = ['# GH #805 L1 census', '',
         f'- date: {datetime.datetime.now().isoformat(timespec="seconds")}',
         f'- predicate: `{ident}` @ `{rev}`',
         f'- blob store: `{args.blobs}`',
         f'- countries root: `{args.countries_root}`', '',
         '## Coverage', '',
         f'| `.dta` sidecars | held | not held |', '|---|---|---|',
         f'| {len(recs) + sum(unheld.values())} | {len(recs)} | {sum(unheld.values())} |', '',
         'Not held, by country (the census cannot see these): ' +
         (', '.join(f'{c} {n}' for c, n in unheld.most_common()) or 'none'), '',
         '## Verdicts (files)', '',
         '| verdict | files | C1 cells | decoded cells | decoded labels |',
         '|---|---|---|---|---|']
    for k in ('decode', 'refuse', 'no-candidate', 'gated-118', 'error', 'clean'):
        rs = [r for r in recs if r['verdict'] == k]
        cells = sum(sum(r.get('c1', {}).get('cells', {}).values()) for r in rs)
        dcells = sum(r.get('decoded', {}).get('cells', 0) for r in rs)
        dlabs = sum(r.get('decoded', {}).get('labels', 0) for r in rs)
        L.append(f'| {k} | {len(rs)} | {cells:,} | {dcells:,} | {dlabs:,} |')
    L += ['', 'Cells are rows served with the string: a str-column value counts its '
          'rows, a value label the rows coded with it (over every column using its '
          'label set) -- what `get_dataframe` serves once it substitutes labels.  '
          f'Pre-118 files holding a candidate: {v["decode"] + v["refuse"]} '
          f'(decode {v["decode"]}, refuse {v["refuse"]}).', '']

    def by_country(k):
        agg = collections.defaultdict(lambda: [0, 0, 0])
        for r in recs:
            if r['verdict'] == k:
                a = agg[r['country']]
                a[0] += 1
                a[1] += (r['decoded'].get('cells', 0) if k == 'decode'
                         else sum(r.get('c1', {}).get('cells', {}).values()))
                a[2] += r.get('decoded', {}).get('labels', 0)
        return agg

    L += ['## decode, by country', '', '| country | files | decoded cells | decoded labels |', '|---|---|---|---|']
    for c, (n, cells, labs) in sorted(by_country('decode').items()):
        L.append(f'| {c} | {n} | {cells:,} | {labs:,} |')
    L += ['', '## refuse, by country', '', '| country | files | C1 cells left as read |', '|---|---|---|']
    for c, (n, cells, _) in sorted(by_country('refuse').items()):
        L.append(f'| {c} | {n} | {cells:,} |')

    review = [r for r in recs if r['verdict'] == 'decode'
              and set(r.get('decoded_chars', '')) - REVIEWED_DECODES]
    L += ['', f'## REVIEW: decode files producing characters outside `{"".join(sorted(REVIEWED_DECODES))}` ({len(review)})', '',
          'The predicate cannot tell cp1252 from a code page that also avoids the hole bytes; '
          'look at these before trusting the decode.', '']
    for r in sorted(review, key=lambda r: r['rel']):
        extra = ''.join(sorted(set(r['decoded_chars']) - REVIEWED_DECODES))
        L.append(f'- `{r["rel"]}` (format {r["release"]}): new chars `{extra}`; '
                 f'{r["decoded"].get("cells", 0):,} cells, {r["decoded"].get("labels", 0):,} labels; '
                 f'e.g. {_esc(r["examples"][:2])}')
    trunc = [r for r in recs if r.get('c1', {}).get('label', {}).get('double_truncated')
             or r.get('c1', {}).get('value', {}).get('double_truncated')]
    L += ['', f'## Strings excluded as TRUNCATED double encodings ({len(trunc)} files)', '']
    for r in sorted(trunc, key=lambda r: r['rel']):
        L.append(f'- `{r["rel"]}` [{r["verdict"]}]: '
                 f'{_esc(r.get("double_truncated_examples", []))}')
    L += ['', '## refuse, per file', '']
    for r in sorted((r for r in recs if r['verdict'] == 'refuse'), key=lambda r: r['rel']):
        L.append(f'- `{r["rel"]}` (format {r["release"]}): {r["refuse"]}; '
                 f'e.g. {_esc(r["examples"][:2])}')
    L += ['', '## gated-118, per file', '']
    for r in sorted((r for r in recs if r['verdict'] == 'gated-118'), key=lambda r: r['rel']):
        L.append(f'- `{r["rel"]}` (format {r["release"]}): C1 cells {sum(r["c1"]["cells"].values()):,}, '
                 f'labels {sum(r["c1"]["label"].values()):,}')
    errs = [r for r in recs if r['verdict'] == 'error']
    if errs:
        L += ['', '## error', '']
        L += [f'- `{r["rel"]}`: {r["error"]}' for r in errs]
    if compare is not None:
        L += ['', f'## Compared with `{args.compare}`', '']
        before = compare
        now = {r['rel']: r for r in recs}
        changed = []
        for rel in sorted(set(before) | set(now)):
            a, b = before.get(rel), now.get(rel)
            ka = None if a is None else (a['verdict'], a.get('map_sha'), a.get('n_candidates'))
            kb = None if b is None else (b['verdict'], b.get('map_sha'), b.get('n_candidates'))
            if ka != kb:
                changed.append((rel, ka, kb))
        L.append(f'{len(changed)} file(s) differ in (verdict, redecode-map digest, candidate count):')
        L.append('')
        for rel, ka, kb in changed:
            L.append(f'- `{rel}`: {ka} -> {kb}')
    return '\n'.join(L) + '\n'


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--blobs', required=True, type=Path)
    ap.add_argument('--countries-root', type=Path, default=None)
    ap.add_argument('--out', required=True, type=Path)
    ap.add_argument('--summary', type=Path, default=None)
    ap.add_argument('--strings', type=Path, default=None,
                    help='also dump every C1 string per file (for offline re-judging)')
    ap.add_argument('--compare', type=Path, default=None)
    ap.add_argument('--tsv', type=Path, default=None,
                    help='also write one row per C1-bearing file (verdict, counts)')
    ap.add_argument('--summary-only', action='store_true',
                    help='re-summarise an existing --out without reading any blob')
    ap.add_argument('--nproc', type=int, default=6)
    args = ap.parse_args()
    if args.countries_root is None:
        from lsms_library.paths import countries_root
        args.countries_root = Path(countries_root())
    _ARGS['strings'] = bool(args.strings)

    jobs, unheld = [], collections.Counter()
    for rel, md5, blob in _sidecars(args.countries_root, args.blobs):
        if blob is None:
            unheld[rel.split('/', 1)[0]] += 1
        else:
            jobs.append((rel, md5, str(blob)))
    sys.stderr.write(f'{len(jobs)} held, {sum(unheld.values())} not held\n')

    if args.summary_only:
        recs = [json.loads(l) for l in open(args.out)]
    else:
        recs = []
        sfh = open(args.strings, 'w') if args.strings else None
        with open(args.out, 'w') as fh, Pool(args.nproc, maxtasksperchild=20) as pool:
            for rec, dump in pool.imap_unordered(probe, jobs, chunksize=1):
                recs.append(rec)
                fh.write(json.dumps(rec, ensure_ascii=False) + '\n')
                if sfh is not None and dump is not None:
                    sfh.write(json.dumps(dump, ensure_ascii=False) + '\n')
        if sfh is not None:
            sfh.close()
    if args.tsv:
        cols = ['rel', 'release', 'verdict', 'n_candidates', 'c1_cells',
                'decoded_cells', 'decoded_labels', 'refuse', 'decoded_chars']
        with open(args.tsv, 'w') as fh:
            fh.write('\t'.join(cols) + '\n')
            for r in sorted(recs, key=lambda r: r['rel']):
                if r['verdict'] in ('clean', 'error'):
                    continue
                fh.write('\t'.join(str(x) for x in (
                    r['rel'], r['release'], r['verdict'], r.get('n_candidates', 0),
                    sum(r['c1']['cells'].values()),
                    r.get('decoded', {}).get('cells', 0),
                    r.get('decoded', {}).get('labels', 0),
                    _esc(r['refuse']) if 'refuse' in r else '',
                    r.get('decoded_chars', ''))) + '\n')
    compare = None
    if args.compare:
        compare = {json.loads(l)['rel']: json.loads(l) for l in open(args.compare)}
    text = _summary(sorted(recs, key=lambda r: r['rel']), unheld, args, compare)
    if args.summary:
        args.summary.write_text(text)
    sys.stdout.write(text)


if __name__ == '__main__':
    main()

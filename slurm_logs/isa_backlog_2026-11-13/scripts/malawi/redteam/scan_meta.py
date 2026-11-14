"""Red team (malawi stream): inventory every held source file's variables, labels and
value labels for 2016-17 / 2019-20 / 2024-25 Malawi, and extract documentation text.

usage: python scan_meta.py <clone> <outdir>

Writes <outdir>/inventory.jsonl (one record per file: path, n_rows, columns with
label and value labels), <outdir>/season_hits.json (pattern hits), and
<outdir>/docs/<wave>/<name>.txt (PDF text, if a PDF library is importable).
Read-only on the source tree; reads through data_access.get_data_file into the
private data root.
"""
import json
import os
import re
import subprocess
import sys
import traceback
from pathlib import Path

clone, outdir = sys.argv[1], Path(sys.argv[2])
outdir.mkdir(parents=True, exist_ok=True)

import lsms_library  # noqa: E402
from lsms_library import paths  # noqa: E402
top = os.path.realpath(clone)
assert os.path.realpath(lsms_library.__file__).startswith(top + os.sep), lsms_library.__file__
dr = os.path.realpath(str(paths.data_root()))
assert dr.startswith('/local/job') or '/isa-sm-20261113/private/' in dr, dr
from lsms_library.data_access import get_data_file  # noqa: E402
import pyreadstat  # noqa: E402

root = paths.countries_root()
MW = root / 'Malawi'
WAVES = os.environ.get('SCAN_WAVES', '2016-17,2019-20,2024-25').split(',')

NAME_RE = re.compile(r'season|rainy|^ag_[a-z]_?0$|^ag_[a-z]_?0[a-z]?$|^ag_[a-z]_?00|^rs$|_rs$|^rs_|refer|period', re.I)
LABEL_RE = re.compile(r'season|rainy|reference|\bRS\b|\[RS\]|%ag_|20\d\d\s*/\s*(20)?\d\d|dimba|cropping year|agric(ultural)? year', re.I)
VAL_RE = re.compile(r'20\d\d\s*/\s*(20)?\d\d|rainy|season|dimba', re.I)

inv = open(outdir / 'inventory.jsonl', 'w')
hits = []
errors = {}
for w in WAVES:
    for sc in sorted((MW / w / 'Data').rglob('*.dvc')):
        src = sc.with_suffix('')
        rel = src.relative_to(root).as_posix()
        suf = src.suffix.lower()
        if suf not in ('.dta', '.sav', '.csv'):
            errors[rel] = f'skipped suffix {suf}'
            continue
        try:
            p = get_data_file(rel)
            if p is None:
                errors[rel] = 'get_data_file returned None'
                continue
            if suf == '.dta':
                _, meta = pyreadstat.read_dta(str(p), metadataonly=True)
            elif suf == '.sav':
                _, meta = pyreadstat.read_sav(str(p), metadataonly=True)
            else:
                import pandas as pd
                head = pd.read_csv(p, nrows=5)
                rec = {'path': rel, 'columns': [{'name': c} for c in head.columns]}
                inv.write(json.dumps(rec) + '\n')
                continue
            vvl = meta.variable_value_labels or {}
            cols = []
            for name, lab in zip(meta.column_names, meta.column_labels):
                vl = vvl.get(name)
                vl = {str(k): str(v) for k, v in vl.items()} if vl else None
                cols.append({'name': name, 'label': lab, 'value_labels': vl,
                             'value_label_set': (meta.variable_to_label or {}).get(name)})
                why = []
                if NAME_RE.search(name or ''):
                    why.append('name')
                if LABEL_RE.search(lab or ''):
                    why.append('label')
                if vl and any(VAL_RE.search(v) for v in vl.values()):
                    why.append('value_label')
                if why:
                    hits.append({'path': rel, 'name': name, 'label': lab, 'why': why,
                                 'value_labels': vl if 'value_label' in why else None})
            rec = {'path': rel, 'n_rows': meta.number_rows, 'file_label': meta.file_label,
                   'columns': cols}
            inv.write(json.dumps(rec, default=str) + '\n')
            inv.flush()
        except Exception as e:  # noqa: BLE001
            errors[rel] = f'{type(e).__name__}: {e}'
inv.close()
(outdir / 'season_hits.json').write_text(json.dumps({'hits': hits, 'errors': errors}, indent=1, default=str))
print('files with errors/skips', len(errors), 'hits', len(hits), flush=True)

# --- documentation text ---
pdflib = None
for mod in ('fitz', 'pypdf', 'pdfplumber', 'pdfminer.high_level', 'PyPDF2'):
    try:
        __import__(mod)
        pdflib = mod
        break
    except Exception:  # noqa: BLE001
        pass
print('pdf library', pdflib, flush=True)
docs = {}
for w in os.environ.get('DOC_WAVES', '2010-11,2016-17,2019-20,2024-25').split(','):
    for sc in sorted((MW / w / 'Documentation').rglob('*.pdf.dvc')):
        src = sc.with_suffix('')
        rel = src.relative_to(root).as_posix()
        dst = outdir / 'docs' / w / (src.name + '.txt')
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            p = get_data_file(rel)
            if p is None:
                docs[rel] = 'unavailable'
                continue
            text = None
            if pdflib == 'fitz':
                import fitz
                with fitz.open(str(p)) as doc:
                    text = '\n'.join(f'<<<PAGE {i+1}>>>\n' + pg.get_text() for i, pg in enumerate(doc))
            elif pdflib in ('pypdf', 'PyPDF2'):
                mod = __import__(pdflib)
                r = mod.PdfReader(str(p))
                text = '\n'.join(f'<<<PAGE {i+1}>>>\n' + (pg.extract_text() or '') for i, pg in enumerate(r.pages))
            elif pdflib == 'pdfplumber':
                import pdfplumber
                with pdfplumber.open(str(p)) as pdf:
                    text = '\n'.join(f'<<<PAGE {i+1}>>>\n' + (pg.extract_text() or '') for i, pg in enumerate(pdf.pages))
            elif pdflib == 'pdfminer.high_level':
                from pdfminer.high_level import extract_text
                text = extract_text(str(p))
            if text is None:
                try:
                    text = subprocess.check_output(['pdftotext', '-layout', str(p), '-'], text=True)
                except Exception as e:  # noqa: BLE001
                    docs[rel] = f'no text extractor: {e}'
                    continue
            dst.write_text(text)
            docs[rel] = f'ok {len(text)} chars'
        except Exception as e:  # noqa: BLE001
            docs[rel] = f'{type(e).__name__}: {e} {traceback.format_exc()[-300:]}'
(outdir / 'docs.json').write_text(json.dumps(docs, indent=1))
print('docs', json.dumps(docs, indent=1), flush=True)
print('SCAN DONE', flush=True)

"""Fetch GhanaSPS instruments + dump .dta variable/value labels, LOCK-FREE.

Follows slurm_logs/ghana_audit/community_prices/fetch_docs.py: warm the L1
blob with local_tools._ensure_dvc_pulled and read it by sidecar md5, never
data_access.get_data_file (GH #763: 1,599 s on an already-cached PDF).
__main__-guarded.
"""
import io, re, sys, zipfile
from pathlib import Path

import pandas as pd
import yaml

from lsms_library.paths import countries_root, data_root
from lsms_library.local_tools import _ensure_dvc_pulled

assert 'wt-gsps-labor' in str(countries_root()), countries_root()
ROOT = countries_root() / 'GhanaSPS'
OUT = Path('/global/scratch/fsa/fc_jevons/ligon/tmp/gsps_labor_docs')


def blob_path(src: Path) -> Path:
    sidecar = src.parent / (src.name + '.dvc')
    md5 = yaml.safe_load(sidecar.read_text())['outs'][0]['md5']
    _ensure_dvc_pulled(str(src))
    p = Path(data_root()) / 'dvc-cache' / md5[:2] / md5[2:]
    if not p.exists():
        raise FileNotFoundError(f'{src} not in L1 after warm ({p})')
    return p


def docx_text(p):
    with zipfile.ZipFile(p) as z:
        xml = z.read('word/document.xml').decode('utf8', 'replace')
    xml = re.sub(r'</w:p>', '\n', xml)
    xml = re.sub(r'</w:tc>', '\t', xml)
    return re.sub(r'<[^>]+>', '', xml)


DOCS = []
_DOCS_DONE = [
    '2009-10/Documentation/Household_questionnaire_Part_A.pdf',
    '2013-14/Documentation/ISSER_Yale_Part A_WAVE II.docx',
    '2017-18/Documentation/Questionnaire/HH_Questionnaire_23_10.docx',
    '2009-10/Documentation/CODE_BOOK.pdf',
]

DTAS = [
    *[f'2009-10/Data/S4AIX{k}.dta' for k in range(1, 9)],
    '2013-14/Data/04m_aglabour.dta',
    '2013-14/Data/04m_aglabourquestions.dta',
    '2017-18/Data/04m_aglabourquestions.dta',
]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for rel in DOCS:
        src = ROOT / rel
        name = re.sub(r'[^A-Za-z0-9_.-]+', '_', rel)
        try:
            local = blob_path(src)
        except Exception as e:
            print(f'FETCH FAILED {rel}: {type(e).__name__}: {e}', flush=True)
            continue
        print(f'--- {rel} -> {local} ({local.stat().st_size} B)', flush=True)
        try:
            if src.suffix.lower() == '.pdf':
                from pdfminer.high_level import extract_text
                txt = extract_text(str(local))
            else:
                txt = docx_text(local)
            (OUT / (name + '.txt')).write_text(txt, encoding='utf8')
            print(f'    wrote {len(txt)} chars', flush=True)
        except Exception as e:
            print(f'    EXTRACT FAILED: {type(e).__name__}: {e}', flush=True)

    for rel in DTAS:
        src = ROOT / rel
        name = re.sub(r'[^A-Za-z0-9_.-]+', '_', rel)
        local = blob_path(src)
        buf = io.BytesIO(local.read_bytes())
        with pd.io.stata.StataReader(buf) as rdr:
            vl = rdr.variable_labels()
            try:
                vals = rdr.value_labels()
            except Exception:
                vals = {}
            fmts = dict(zip(rdr._varlist, rdr._lbllist))
        lines = [f'# {rel}', '## variable labels']
        for k, v in vl.items():
            lines.append(f'{k}\t{fmts.get(k,"")}\t{v}')
        lines.append('## value label sets')
        for lname, mapping in vals.items():
            lines.append(f'[{lname}] ' + '; '.join(f'{k}={v}' for k, v in list(mapping.items())[:60]))
        (OUT / (name + '.labels.txt')).write_text('\n'.join(lines), encoding='utf8')
        print(f'--- {rel}: {len(vl)} var labels, {len(vals)} value-label sets', flush=True)


if __name__ == '__main__':
    main()

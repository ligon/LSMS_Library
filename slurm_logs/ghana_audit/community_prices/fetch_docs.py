"""Fetch the price-survey documentation blobs LOCK-FREE and extract text (read-only).

Uses local_tools._ensure_dvc_pulled (the direct-S3 L1 warm that get_dataframe
uses) and then reads the blob from the L1 cache by its sidecar md5 -- it never
touches DVCFS / the DVC index (get_data_file's DVCFS.exists() walks ~7k
sidecars on Lustre and hung for >10 min here).

Usage: fetch_docs.py OUTDIR [wave/relpath ...]
"""
import re
import sys
import zipfile
from pathlib import Path

import yaml

from lsms_library.paths import countries_root, data_root
from lsms_library.local_tools import _ensure_dvc_pulled

assert 'wt-glss-prices' in str(countries_root()), countries_root()
ROOT = countries_root() / 'GhanaLSS'

DOCS = [
    '1991-92/Documentation/pdf/G3QPrice.pdf',
    '1998-99/Documentation/questionnaire/GHA_1998_GLSS_Price_Questionnaire_EN.pdf',
    '2012-13/Documentation/QUESTIONNAIRES/GLSS6 Prices Questionnaire.pdf',
    '2016-17/Documentation/GLSS7_price questionnaire.xlsx',
    '2016-17/Documentation/COVER PAGE _PRICE.docx',
    '2005-06/Documentation/G5QPrice.pdf',
    '2005-06/Documentation/G5QComm.pdf',
    '2005-06/Data/community/community.zip',
    '1987-88/Data/PRICE.DCT',
    '1988-89/Data/PRICE.DCT',
    '1987-88/Documentation/technical document/GHA_1987_GLSS_Basicinfo_EN.pdf',
    '1991-92/Documentation/pdf/g3usersg.pdf',
    '1991-92/Documentation/pdf/data.pdf',
    '1998-99/Documentation/GHA_1998_GLSS_Data_User_Guide_EN.pdf',
    '2012-13/Documentation/MANUALS/GLSS6 CODEBOOK.pdf',
    '2016-17/Documentation/ddi-documentation-english-97.pdf',
    '2005-06/Documentation/ddi-documentation-english-5.pdf',
    '1991-92/Documentation/ddi-documentation-english-12.pdf',
    '1998-99/Documentation/ddi-documentation-english-14.pdf',
]


def blob_path(src: Path) -> Path:
    """L1 cache path of a DVC-tracked file, warming it lock-free if needed."""
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


def main(outdir, docs):
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    for rel in docs:
        src = ROOT / rel
        name = re.sub(r'[^A-Za-z0-9_.-]+', '_', rel)
        try:
            local = blob_path(src)
        except Exception as e:
            print(f'FETCH FAILED {rel}: {type(e).__name__}: {e}', flush=True)
            continue
        print(f'--- {rel} -> {local} ({local.stat().st_size} bytes)', flush=True)
        suf = src.suffix.lower()
        try:
            if suf == '.pdf':
                from pdfminer.high_level import extract_text
                txt = extract_text(str(local))
                (out / (name + '.txt')).write_text(txt)
                nonws = len(re.sub(r'\s', '', txt))
                print(f'    pdf text chars={len(txt)} nonws={nonws} pages~{txt.count(chr(12))+1}', flush=True)
            elif suf == '.xlsx':
                import pandas as pd
                sheets = pd.read_excel(local, sheet_name=None, header=None)
                with open(out / (name + '.txt'), 'w') as f:
                    for sn, df in sheets.items():
                        f.write(f'##### SHEET {sn} shape={df.shape}\n')
                        f.write(df.to_string() + '\n')
                print('    sheets:', {k: v.shape for k, v in sheets.items()}, flush=True)
            elif suf == '.docx':
                txt = docx_text(local)
                (out / (name + '.txt')).write_text(txt)
                print(f'    docx chars={len(txt)}', flush=True)
            elif suf == '.zip':
                with zipfile.ZipFile(local) as z:
                    for zi in z.infolist():
                        print(f'    {zi.filename}  {zi.file_size}', flush=True)
            else:
                txt = local.read_text(errors='replace')
                (out / (name + '.txt')).write_text(txt)
                print(f'    text chars={len(txt)}', flush=True)
        except Exception as e:
            print(f'    EXTRACT FAILED: {type(e).__name__}: {e}', flush=True)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:] or DOCS)

#!/usr/bin/env python
"""Render every page of an image-only CCITT-fax PDF to PNG (no poppler, no OCR).

GhanaLSS 1987-88 / 1988-89 questionnaires are 78 CCITT Group-4 page scans with
zero font objects, so pdfminer's text extraction yields nothing.  Each page is
a single ``CCITTFaxDecode`` XObject; PIL will open it if the raw stream is
wrapped in a synthetic single-strip TIFF header (the technique recorded in
``GhanaLSS/_/CONTENTS.org`` and ``slurm_logs/ghana_audit/FINDINGS_orphan_rural_table.org``).

Usage::

    python render_pages.py <pdf> <outdir> [--scale 0.5] [--sheet 2]

Writes ``<outdir>/full/page_NN.png`` (native resolution) and, if ``--sheet``
> 0, ``<outdir>/sheets/sheet_NN.png`` contact sheets of ``--sheet`` pages side
by side at ``--scale``.  Also writes ``<outdir>/pages.tsv`` with one row per
page: page number, width, height, K, BlackIs1, filter chain.

GH #695.  Read-only with respect to the repository: it writes only to <outdir>.
"""
from __future__ import annotations

import argparse
import io
import struct
import sys
import zlib
from pathlib import Path

from PIL import Image
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.pdftypes import PDFStream, resolve1
from pdfminer.psparser import LIT


def _name(x) -> str:
    x = resolve1(x)
    return getattr(x, 'name', str(x))


def _filters(stream: PDFStream) -> list[str]:
    f = resolve1(stream.attrs.get('Filter'))
    if f is None:
        return []
    if isinstance(f, list):
        return [_name(v) for v in f]
    return [_name(f)]


def _decode_parms(stream: PDFStream) -> list[dict]:
    dp = resolve1(stream.attrs.get('DecodeParms')) or {}
    if isinstance(dp, list):
        return [resolve1(d) or {} for d in dp]
    return [dp]


def ccitt_to_tiff(raw: bytes, width: int, height: int, k: int,
                  blackis1: bool, byte_align: bool) -> bytes:
    """Wrap a raw CCITT stream in a minimal single-strip little-endian TIFF."""
    compression = 4 if k < 0 else 3          # Group 4 vs Group 3
    tags: list[tuple[int, int, int, int]] = []   # (tag, type, count, value)
    SHORT, LONG = 3, 4
    tags.append((256, LONG, 1, width))       # ImageWidth
    tags.append((257, LONG, 1, height))      # ImageLength
    tags.append((258, SHORT, 1, 1))          # BitsPerSample
    tags.append((259, SHORT, 1, compression))
    # PhotometricInterpretation: 0 = WhiteIsZero (TIFF Class F convention).
    # PDF BlackIs1=false (default) means the *decoded* 0 bits are black, which
    # is the same convention the CCITT codes themselves carry; PIL's libtiff
    # decoder handles the run colours, so WhiteIsZero renders correctly.  If a
    # page comes out inverted, flip PhotometricInterpretation to 1.
    tags.append((262, SHORT, 1, 1 if blackis1 else 0))
    tags.append((266, SHORT, 1, 1))          # FillOrder MSB2LSB
    tags.append((273, LONG, 1, 0))           # StripOffsets (patched below)
    tags.append((277, SHORT, 1, 1))          # SamplesPerPixel
    tags.append((278, LONG, 1, height))      # RowsPerStrip
    tags.append((279, LONG, 1, len(raw)))    # StripByteCounts
    if compression == 3:
        t4 = 0
        if k > 0:
            t4 |= 1                          # 2D encoding
        if byte_align:
            t4 |= 4                          # fill bits before EOL
        tags.append((292, LONG, 1, t4))      # T4Options
    else:
        tags.append((293, LONG, 1, 0))       # T6Options
    tags.sort()
    n = len(tags)
    ifd_offset = 8
    ifd_size = 2 + 12 * n + 4
    data_offset = ifd_offset + ifd_size
    out = bytearray()
    out += b'II' + struct.pack('<HI', 42, ifd_offset)
    out += struct.pack('<H', n)
    for tag, typ, count, value in tags:
        if tag == 273:
            value = data_offset
        if typ == SHORT:
            out += struct.pack('<HHIHH', tag, typ, count, value, 0)
        else:
            out += struct.pack('<HHII', tag, typ, count, value)
    out += struct.pack('<I', 0)              # next IFD
    assert len(out) == data_offset, (len(out), data_offset)
    out += raw
    return bytes(out)


def page_images(pdf_path: Path):
    """Yield (page_no, PIL.Image, meta) for each page's CCITT XObject."""
    with open(pdf_path, 'rb') as fh:
        parser = PDFParser(fh)
        doc = PDFDocument(parser)
        for pno, page in enumerate(PDFPage.create_pages(doc), start=1):
            res = resolve1(page.resources) or {}
            xobjs = resolve1(res.get('XObject')) or {}
            found = False
            for xname, ref in xobjs.items():
                st = resolve1(ref)
                if not isinstance(st, PDFStream):
                    continue
                if _name(st.attrs.get('Subtype')) != 'Image':
                    continue
                filters = _filters(st)
                if 'CCITTFaxDecode' not in filters:
                    continue
                parms = _decode_parms(st)
                raw = st.get_rawdata()
                # Apply any filters that precede CCITTFaxDecode (e.g. Flate).
                for f in filters:
                    if f == 'CCITTFaxDecode':
                        break
                    if f in ('FlateDecode', 'Fl'):
                        raw = zlib.decompress(raw)
                    else:
                        raise RuntimeError(f'page {pno}: unsupported pre-filter {f}')
                ci = filters.index('CCITTFaxDecode')
                dp = parms[ci] if ci < len(parms) else {}
                width = int(resolve1(st.attrs.get('Width')))
                height = int(resolve1(st.attrs.get('Height')))
                k = int(resolve1(dp.get('K', 0)))
                blackis1 = bool(resolve1(dp.get('BlackIs1', False)))
                byte_align = bool(resolve1(dp.get('EncodedByteAlign', False)))
                cols = int(resolve1(dp.get('Columns', 1728)))
                rows = int(resolve1(dp.get('Rows', height)))
                tiff = ccitt_to_tiff(raw, cols, rows, k, blackis1, byte_align)
                img = Image.open(io.BytesIO(tiff))
                img.load()
                meta = dict(page=pno, xobject=str(xname), width=width, height=height,
                            columns=cols, rows=rows, K=k, BlackIs1=blackis1,
                            filters='+'.join(filters), nbytes=len(raw))
                found = True
                yield pno, img, meta
                break
            if not found:
                yield pno, None, dict(page=pno, xobject='', width=0, height=0,
                                      columns=0, rows=0, K=0, BlackIs1=False,
                                      filters='NONE', nbytes=0)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('pdf', type=Path)
    ap.add_argument('outdir', type=Path)
    ap.add_argument('--scale', type=float, default=0.5)
    ap.add_argument('--sheet', type=int, default=2,
                    help='pages per contact sheet (0 = none)')
    ap.add_argument('--pages', type=str, default='',
                    help='comma-separated page numbers to render (default all)')
    a = ap.parse_args(argv)

    want = {int(x) for x in a.pages.split(',') if x.strip()} if a.pages else None
    full = a.outdir / 'full'
    full.mkdir(parents=True, exist_ok=True)
    sheets = a.outdir / 'sheets'
    if a.sheet:
        sheets.mkdir(parents=True, exist_ok=True)

    rows = []
    small: list[tuple[int, Image.Image]] = []
    for pno, img, meta in page_images(a.pdf):
        rows.append(meta)
        if want is not None and pno not in want:
            continue
        if img is None:
            print(f'page {pno}: NO CCITT image found', file=sys.stderr)
            continue
        img = img.convert('L')
        img.save(full / f'page_{pno:02d}.png')
        if a.sheet:
            w, h = img.size
            s = img.resize((int(w * a.scale), int(h * a.scale)), Image.LANCZOS)
            small.append((pno, s))
        print(f'page {pno:2d}: {meta["width"]}x{meta["height"]} K={meta["K"]} '
              f'BlackIs1={meta["BlackIs1"]} filters={meta["filters"]} bytes={meta["nbytes"]}')

    if a.sheet and small:
        for i in range(0, len(small), a.sheet):
            chunk = small[i:i + a.sheet]
            W = sum(s.size[0] for _, s in chunk) + 20 * (len(chunk) - 1)
            H = max(s.size[1] for _, s in chunk)
            sheet = Image.new('L', (W, H), 128)
            x = 0
            for _, s in chunk:
                sheet.paste(s, (x, 0))
                x += s.size[0] + 20
            first, last = chunk[0][0], chunk[-1][0]
            sheet.save(sheets / f'sheet_p{first:02d}-p{last:02d}.png')

    with open(a.outdir / 'pages.tsv', 'w') as fh:
        keys = ['page', 'xobject', 'width', 'height', 'columns', 'rows', 'K',
                'BlackIs1', 'filters', 'nbytes']
        fh.write('\t'.join(keys) + '\n')
        for r in rows:
            fh.write('\t'.join(str(r[k]) for k in keys) + '\n')
    print(f'{len(rows)} pages; wrote {a.outdir}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

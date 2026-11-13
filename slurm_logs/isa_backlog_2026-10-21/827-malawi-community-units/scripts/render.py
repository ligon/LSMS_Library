import sys
import os
# A private PyMuPDF 1.24.10 + PyMuPDFb install (pip --target), NOT a project dependency.
sys.path.insert(0, os.environ['PDF_PYLIB'])
import fitz
doc = fitz.open(sys.argv[1])
pg = int(sys.argv[2]) - 1
zoom = float(sys.argv[4]) if len(sys.argv) > 4 else 2.0
pix = doc[pg].get_pixmap(matrix=fitz.Matrix(zoom, zoom))
pix.save(sys.argv[3])
print(pix.width, pix.height, fitz.__doc__)

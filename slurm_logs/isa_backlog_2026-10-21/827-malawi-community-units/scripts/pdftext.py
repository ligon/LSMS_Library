import sys
import os
# A private pypdf install (pip --target), NOT a project dependency.
sys.path.insert(0, os.environ['PDF_PYLIB'])
import pypdf
r = pypdf.PdfReader(sys.argv[1])
out = open(sys.argv[2], 'w')
for i, p in enumerate(r.pages):
    out.write(f'\n\n######## PAGE {i+1}\n')
    out.write(p.extract_text() or '')
print(len(r.pages))

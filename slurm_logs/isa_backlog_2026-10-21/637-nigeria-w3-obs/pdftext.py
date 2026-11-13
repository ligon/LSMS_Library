"""Minimal PDF text dump: inflate FlateDecode streams, pull Tj/TJ strings."""
import re, sys, zlib
data = open(sys.argv[1], 'rb').read()
out = []
for m in re.finditer(rb'stream\r?\n', data):
    s = m.end()
    e = data.find(b'endstream', s)
    raw = data[s:e]
    try:
        txt = zlib.decompress(raw)
    except Exception:
        try:
            txt = zlib.decompressobj().decompress(raw)
        except Exception:
            continue
    if b'Tj' not in txt and b'TJ' not in txt:
        continue
    parts = []
    for tm in re.finditer(rb'\[(.*?)\]\s*TJ|\((.*?)(?<!\\)\)\s*Tj|(T\*|Td|TD|ET)', txt, re.S):
        if tm.group(1) is not None:
            parts.append(b''.join(re.findall(rb'\(((?:\\.|[^\\)])*)\)', tm.group(1))))
        elif tm.group(2) is not None:
            parts.append(tm.group(2))
        else:
            parts.append(b'\n')
    out.append(b' '.join(parts))
sys.stdout.buffer.write(b'\n=====STREAM=====\n'.join(out))

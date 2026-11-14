"""Apply one red-team probe edit to a job-local throwaway clone.  Usage: rt_probe_edit.py CLONE PROBE"""
import re
import sys
from pathlib import Path

clone, probe = Path(sys.argv[1]), sys.argv[2]
reg = clone / 'lsms_library/_build_registry.py'
qa = clone / 'lsms_library/quantity_audit.py'
tr = clone / 'lsms_library/transformations.py'


def drop_line(p, needle):
    s = p.read_text()
    lines = s.splitlines(keepends=True)
    hit = [k for k, ln in enumerate(lines) if needle in ln]
    assert len(hit) == 1, (needle, hit)
    del lines[hit[0]]
    p.write_text(''.join(lines))


def replace_once(p, old, new):
    s = p.read_text()
    assert s.count(old) == 1, (old, s.count(old))
    p.write_text(s.replace(old, new))


if probe == 'pA':
    drop_line(reg, '"lsms_library.quantity_audit.audit_survey_kg_prices",')
elif probe == 'pB':
    drop_line(reg, '"lsms_library.quantity_audit.survey_kg_price_factors",')
elif probe == 'pAB':
    drop_line(reg, '"lsms_library.quantity_audit.audit_survey_kg_prices",')
    drop_line(reg, '"lsms_library.quantity_audit.survey_kg_price_factors",')
elif probe == 'pF':
    drop_line(reg, '"lsms_library.quantity_audit.check_quantities",')
elif probe == 'pC':
    replace_once(qa, '    n = len(df)\n    total = _numeric(df["Quantity_kg"])',
                 '    n = len(df)\n    _rt_probe_c = 1\n    total = _numeric(df["Quantity_kg"])')
elif probe == 'pD':
    replace_once(qa, 'SURVEY_KG_PRICE_MAX_SPREAD = 10.0\n', 'SURVEY_KG_PRICE_MAX_SPREAD = 10.5\n')
elif probe == 'pE':
    replace_once(qa, '    from .transformations import _FLUID_UNITS, KNOWN_METRIC, _parse_explicit_metric\n',
                 '    from .transformations import _FLUID_UNITS, KNOWN_METRIC, _parse_explicit_metric\n'
                 '    _rt_probe_e = 1\n')
elif probe == 'pPos':
    s = tr.read_text()
    m = re.search(r'\ndef food_kg_factors\(.*?\):\n    """.*?"""\n', s, re.S)
    assert m, 'food_kg_factors not found'
    s = s[:m.end()] + '    _rt_probe_pos = 1\n' + s[m.end():]
    tr.write_text(s)
else:
    raise SystemExit(f'unknown probe {probe}')
print('edited', probe)

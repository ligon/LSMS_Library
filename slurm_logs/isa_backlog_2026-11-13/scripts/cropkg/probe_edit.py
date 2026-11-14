"""No-op probe edits in a job-local clone, to measure what landing the rung would move.

Usage: probe_edit.py CLONE P.
  p1: a statement inside harvest_kg_factors (the function the rung edits).
  p2: a new layer name in KG_FACTOR_LAYERS (the constant the rung extends).
  p3: a new, unreferenced module-level helper in transformations.py.
  p4: POSITIVE CONTROL -- a statement inside food_kg_factors, which Ethiopia, GhanaLSS
      and Uganda fold through framework_imports_fingerprint since GH #1509, so the
      census must see it move (CLAUDE.md: 75 of 555 tables, 3 of 38 fwimp).
Never run in a worktree.
"""
import sys
from pathlib import Path

clone, probe = Path(sys.argv[1]), sys.argv[2]
assert str(clone).startswith('/local/job'), clone
path = clone / 'lsms_library' / 'transformations.py'
src = path.read_text()
if probe == 'p1':
    anchor = '    df = crop_production\n    inferred = _crop_inferred_kg_factors('
    assert src.count(anchor) == 1
    src = src.replace(anchor, '    df = crop_production\n    _price_rung_probe = len(df)\n'
                      '    inferred = _crop_inferred_kg_factors(')
elif probe == 'p2':
    anchor = ("KG_FACTOR_LAYERS = ('survey_kg', 'reported', 'shipped', 'survey_median', 'metric',\n"
              "                    'inferred', 'none')")
    assert src.count(anchor) == 1
    src = src.replace(anchor, ("KG_FACTOR_LAYERS = ('survey_kg', 'reported', 'shipped', 'survey_median', 'metric',\n"
                               "                    'inferred', 'price_implied', 'none')"))
elif probe == 'p3':
    src = src + '\n\ndef _price_implied_kg_probe(df):\n    """Probe: an unreferenced helper."""\n    return None\n'
elif probe == 'p4':
    anchor = ('    """\n    idx_names = list(df.index.names or [])\n'
              '    seeded = _seeded_kg_factors(df, volume_as_mass=volume_as_mass, unit_kg=unit_kg)')
    assert src.count(anchor) == 1, src.count(anchor)
    src = src.replace(anchor, ('    """\n    _positive_control_probe = None\n    idx_names = list(df.index.names or [])\n'
                               '    seeded = _seeded_kg_factors(df, volume_as_mass=volume_as_mass, unit_kg=unit_kg)'))
else:
    raise SystemExit(f'unknown probe {probe}')
path.write_text(src)
print('probe', probe, 'applied to', path)

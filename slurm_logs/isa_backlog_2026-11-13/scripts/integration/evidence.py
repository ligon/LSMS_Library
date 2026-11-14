"""Integration evidence (read-only, no table built): GNB label checks, Nigeria
questionnaire C4 / file-label D1, and a duplicate-key YAML sweep.

usage: evidence.py OUT.json EXTRA_SITE_DIR
"""
import json
import re
import sys
import warnings
from pathlib import Path

sys.path.insert(1, sys.argv[2])
warnings.simplefilter('ignore')
import yaml  # noqa: E402
import pypdf  # noqa: E402
import pyreadstat  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

out = {}


def meta(rel):
    p = get_data_file(rel)
    _, m = pyreadstat.read_dta(str(p), metadataonly=True)
    return m


# ---- GNB 2021-22 ---------------------------------------------------------
try:
    labels = get_dataframe(str(countries_root() / 'Guinea-Bissau/2021-22/Data/s14b_me_gnb2021.dta'),
                           categories_only=True)
    sets = {k: sorted({str(v) for v in dict(labels[k]).values()})
            for k in labels if re.fullmatch(r's14bq05__\d+', k)}
    out['gnb_s14bq05_label_sets'] = {k: sets[k] for k in sorted(sets, key=lambda s: int(s.split('__')[1]))}
    m = meta('Guinea-Bissau/2021-22/Data/s07b_me_gnb2021.dta')
    out['gnb_s07bq03a_label'] = m.column_names_to_labels.get('s07bq03a')
    m14 = meta('Guinea-Bissau/2021-22/Data/s14b_me_gnb2021.dta')
    out['gnb_s14bq05__25_label'] = m14.column_names_to_labels.get('s14bq05__25')
    out['gnb_s14bq05__25_labelset'] = m14.variable_to_label.get('s14bq05__25')
except Exception as e:
    out['gnb_error'] = f'{type(e).__name__}: {e}'

# ---- Nigeria C4: household questionnaires --------------------------------
docs = {
    'W1 PP': '2010-11/Documentation/Panel_Household_Questionnaire_FINAL.pdf',
    'W1 PH': '2010-11/Documentation/GHS_PANEL_HOUSEHOLD_POST_HARVEST_FINAL.pdf',
    'W2 PP': '2012-13/Documentation/GHS_Panel_PP_Household.pdf',
    'W2 PH': '2012-13/Documentation/W2_PH_HH_Questionnaire.pdf',
    'W3 PP': '2015-16/Documentation/PP_W3_Household_Quest.pdf',
    'W3 PH': '2015-16/Documentation/PH_W3_Household_Quest.pdf',
    'W4 PP': '2018-19/Documentation/pp_w4_household_quest.pdf',
    'W4 PH': '2018-19/Documentation/ph_w4_household_quest.pdf',
}
pat = re.compile(r'SHOCK|ASSET|COPING|COPE', re.I)
qnr = {}
for name, d in docs.items():
    try:
        p = get_data_file(f'Nigeria/{d}')
        r = pypdf.PdfReader(str(p))
        pages = [(i + 1, (pg.extract_text() or '')) for i, pg in enumerate(r.pages)]
        toc_pages = [i for i, t in pages[:8] if re.search(r'TABLE\s+OF\s+CONTENTS|CONTENTS', t, re.I)]
        toc = []
        for i, t in pages[:8]:
            if i in toc_pages:
                toc += [l.strip() for l in t.splitlines() if re.search(r'[.…]{3,}|\s\d+\s*$', l)]
        sections = [(i, l.strip()) for i, t in pages for l in t.splitlines()
                    if re.match(r'\s*SECTION\s*[0-9]+[A-Z]?\b', l, re.I)]
        hits = [(i, l.strip()[:160]) for i, t in pages for l in t.splitlines() if pat.search(l)]
        qnr[name] = {'path': d, 'pages': len(pages), 'toc_pages': toc_pages, 'toc': toc[:60],
                     'section_headings': sorted({(i, s[:100]) for i, s in sections})[:120],
                     'shock_asset_hits': hits[:60],
                     'chars': sum(len(t) for _, t in pages)}
    except Exception as e:
        qnr[name] = f'ERR {type(e).__name__}: {e}'
out['nigeria_questionnaires'] = qnr

# ---- Nigeria D1: what the shipped section files are ----------------------
d1 = {}
for rel in ['2010-11/Data/sect5_harvestw1.dta', '2010-11/Data/sect7_harvestw1.dta',
            '2012-13/Data/sect5_harvestw2.dta', '2012-13/Data/sect7_harvestw2.dta',
            '2010-11/Data/sect5_plantingw1.dta', '2010-11/Data/sect15a_harvestw1.dta']:
    try:
        m = meta(f'Nigeria/{rel}')
        d1[rel] = {'file_label': m.file_label, 'n_rows': m.number_rows,
                   'labels': dict(list(m.column_names_to_labels.items())[:30])}
    except Exception as e:
        d1[rel] = f'ERR {type(e).__name__}: {e}'
out['nigeria_d1_files'] = d1


# ---- duplicate keys in every shipped YAML -------------------------------
class DupLoader(yaml.SafeLoader):
    pass


def _construct(loader, node, deep=False):
    keys = set()
    for k, _ in node.value:
        key = loader.construct_object(k, deep=deep)
        try:
            hash(key)
        except TypeError:
            continue
        if key in keys:
            raise ValueError(f'duplicate key {key!r} at line {k.start_mark.line + 1}')
        keys.add(key)
    return yaml.SafeLoader.construct_mapping(loader, node, deep)


DupLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct)
DupLoader.add_multi_constructor("!", lambda loader, suffix, node: (
    _construct(loader, node) if isinstance(node, yaml.MappingNode) else None))
root = Path(countries_root()).parent
dups, n = {}, 0
for p in sorted(root.rglob('*.yml')) + sorted(root.rglob('*.yaml')):
    n += 1
    try:
        yaml.load(p.read_text(encoding='utf-8'), Loader=DupLoader)
    except ValueError as e:
        dups[str(p.relative_to(root))] = str(e)
    except Exception as e:  # noqa: BLE001 (custom tags etc.)
        if 'duplicate' in str(e):
            dups[str(p.relative_to(root))] = str(e)
out['yaml_files_scanned'] = n
out['yaml_duplicate_keys'] = dups

Path(sys.argv[1]).write_text(json.dumps(out, indent=1, ensure_ascii=False, default=str))
print('done', 'dups', len(dups), 'of', n)

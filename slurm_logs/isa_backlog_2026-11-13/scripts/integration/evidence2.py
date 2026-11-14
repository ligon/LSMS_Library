"""Nigeria C4/D1 follow-up: page text of the post-harvest asset and shock sections,
and the shipped section files' own labels.  usage: evidence2.py OUT.json EXTRA_SITE_DIR"""
import json
import sys
import warnings
from pathlib import Path

sys.path.insert(1, sys.argv[2])
warnings.simplefilter('ignore')
import pypdf  # noqa: E402
import pyreadstat  # noqa: E402
from lsms_library.data_access import get_data_file  # noqa: E402

out = {'pages': {}, 'files': {}}
pages = {
    '2010-11/Documentation/GHS_PANEL_HOUSEHOLD_POST_HARVEST_FINAL.pdf': [4, 46, 47],
    '2012-13/Documentation/W2_PH_HH_Questionnaire.pdf': [4, 49, 50, 76],
    '2012-13/Documentation/GHS_Panel_PP_Household.pdf': [4],
    '2015-16/Documentation/PH_W3_Household_Quest.pdf': [4],
    '2018-19/Documentation/ph_w4_household_quest.pdf': [4],
    '2010-11/Documentation/Panel_Household_Questionnaire_FINAL.pdf': [4],
    '2015-16/Documentation/PP_W3_Household_Quest.pdf': [4],
    '2018-19/Documentation/pp_w4_household_quest.pdf': [3],
}
for d, nums in pages.items():
    try:
        r = pypdf.PdfReader(str(get_data_file(f'Nigeria/{d}')))
        out['pages'][d] = {n: (r.pages[n - 1].extract_text() or '')[:3500] for n in nums}
    except Exception as e:  # noqa: BLE001
        out['pages'][d] = f'ERR {type(e).__name__}: {e}'
for rel in ['2010-11/Data/Post Harvest Wave 1/Household/sect7_harvestw1.dta',
            '2010-11/Data/Post Harvest Wave 1/Household/sect5_harvestw1.dta',
            '2012-13/Data/Post Harvest Wave 2/Household/sect7_harvestw2.dta',
            '2012-13/Data/Post Harvest Wave 2/Household/sect5_harvestw2.dta',
            '2010-11/Data/Post Planting Wave 1/Household/sect5_plantingw1.dta',
            '2012-13/Data/Post Planting Wave 2/Household/sect5a_plantingw2.dta']:
    try:
        _, m = pyreadstat.read_dta(str(get_data_file(f'Nigeria/{rel}')), metadataonly=True)
        out['files'][rel] = {'file_label': m.file_label, 'n_rows': m.number_rows,
                             'labels': m.column_names_to_labels}
    except Exception as e:  # noqa: BLE001
        out['files'][rel] = f'ERR {type(e).__name__}: {e}'
Path(sys.argv[1]).write_text(json.dumps(out, indent=1, ensure_ascii=False, default=str))
print('done')

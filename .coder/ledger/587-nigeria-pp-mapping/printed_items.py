"""Parse printed C2/C8 item lists from extracted questionnaire text (pypdf plain)."""
import re, json, sys, os
T = sys.argv[1]
DOCS = {
 'W2-PP': ('Nigeria__2012-13__Documentation__GHS_Panel_PP_Community.pdf', [5,6,7], 'w2'),
 'W3-PP-first': ('Nigeria__2015-16__Documentation__PP_W3_Community_Quest.pdf', [5,6,7,8,9,10], 'w3'),
 'W3-PP-second': ('Nigeria__2015-16__Documentation__PP_W3_Community_Quest.pdf', [11,12,13,14,15,16], 'w3'),
 'W4-PP': ('Nigeria__2018-19__Documentation__pp_w4_community_quest.pdf', [5,6,7,8,9], 'w3'),
 'W5-PP': ('Nigeria__2023-24__Documentation__ghs_panel_w5_pp_community_questionnaire.pdf', [6,7,8,9,10,11], 'w3'),
 'W3-PH-all': ('Nigeria__2015-16__Documentation__PH_W3_Community_Quest.pdf', list(range(14,30)), 'w3'),
 'W4-PH': ('Nigeria__2018-19__Documentation__ph_w4_community_quest.pdf', list(range(11,20)), 'w3'),
 'W5-PH': ('Nigeria__2023-24__Documentation__ghs_panel_w5_ph_community_questionnaire.pdf', list(range(17,24)), 'w3'),
 'W1-PP-printed': ('Nigeria__2010-11__Documentation__Community_Questionnaire_Final (1).pdf', [5,6,7,8], 'w1'),
}
BAD = re.compile(r'Questionnaire|Survey|Wave|Page|^\d|LATITUDE|LONGITUDE', re.I)
out = {}
for key, (base, pages, fmt) in DOCS.items():
    items = {}
    for p in pages:
        f = os.path.join(T, f'{base}.p{p:03d}.plain.txt')
        if not os.path.exists(f):
            continue
        for line in open(f, newline='').read().replace('\r', ' ').split('\n'):
            line = re.sub(r'[\t ]+', ' ', line)
            line = re.sub(r' {2,}', ' ', line).strip()
            if fmt == 'w2':
                for m in re.finditer(r'(?<![\d.])(\d{1,3})([A-Za-z(][^\d]*?)(?=\s+\d{1,3}[A-Za-z(]|$)', line):
                    items.setdefault(int(m.group(1)), (m.group(2).strip(), p))
            elif fmt == 'w1':
                for m in re.finditer(r'([A-Za-z(][^\d]*?)\s+(\d{1,3})(?=\s|$)', line):
                    lab = m.group(1).strip()
                    if BAD.search(lab):
                        continue
                    items.setdefault(int(m.group(2)), (lab, p))
            else:
                m = re.match(r'^(.*\S)\s+(\d{2,3})$', line)
                if m and not BAD.search(m.group(1)):
                    items.setdefault(int(m.group(2)), (m.group(1).strip(), p))
    out[key] = {str(k): v for k, v in sorted(items.items())}
json.dump(out, open(sys.argv[2], 'w'), indent=1)
for k, v in out.items():
    print(k, len(v), list(v.items())[:3], '...', list(v.items())[-3:])

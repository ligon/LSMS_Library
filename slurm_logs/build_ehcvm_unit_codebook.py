#!/usr/bin/env python
"""Build the curated EHCVM (WAEMU) s07bq03b unit codebook (GH #223 Layer 2).

Unions the `s07bq03b` Stata value labels across every EHCVM country/wave
(the coding is standardized; some countries -- Togo, Senegal -- lost their
.dta labels but the codes survive), then curates one clean canonical label
per code: reverse double-encoded mojibake, strip the "NNN." prefix, collapse
whitespace, group case/accent-insensitively, pick the dominant Title/accented
form.  Emits slurm_logs/ehcvm_unit_codebook_2026-06-07.org (codebook table +
flagged-for-review + full resolution log).

Run from the repo root with the LSMS venv (needs DVC data access):
    .venv/bin/python slurm_logs/build_ehcvm_unit_codebook.py
"""
import os, glob, re, json, collections, unicodedata, warnings
warnings.filterwarnings('ignore')
import pandas as pd
from lsms_library.local_tools import get_dataframe
from lsms_library.yaml_utils import load_yaml

EHCVM = ['Benin','CotedIvoire','Niger','Mali','Guinea-Bissau','Burkina_Faso','Senegal','Togo']
BASE = os.path.abspath('lsms_library/countries')
# Absolute so emit() works regardless of cwd (extract() chdir's into wave dirs).
OUT  = os.path.abspath('slurm_logs/ehcvm_unit_codebook_2026-06-07.org')


def extract():
    votes = collections.defaultdict(collections.Counter)
    for c in EHCVM:
        for di in glob.glob(f'{BASE}/{c}/*/_/data_info.yml'):
            try: d = load_yaml(open(di))
            except Exception: continue
            fa = (d or {}).get('food_acquired') if isinstance(d, dict) else None
            if not (isinstance(fa, dict) and 's07bq03b' in str(fa.get('idxvars', {}))):
                continue
            f = fa.get('file'); f = f[0] if isinstance(f, list) else f
            f = f if f.startswith('../') else '../Data/' + f
            try:
                os.chdir(os.path.dirname(di))
                raw = get_dataframe(f, convert_categoricals=False)
                lab = get_dataframe(f, convert_categoricals=True)
            except Exception as e:
                print(f"{c}: {e}"); os.chdir(BASE); continue
            os.chdir(BASE)
            uc = [x for x in raw.columns if x.lower() == 's07bq03b']
            if not uc: continue
            uc = uc[0]
            for cd, lb in zip(pd.to_numeric(raw[uc], errors='coerce'), lab[uc].astype(str)):
                if pd.notna(cd) and lb.lower() != 'nan' and lb != str(int(cd)):
                    votes[int(cd)][lb] += 1
    return votes


def _fix_mojibake(s):
    if any(m in s for m in ('Ã', 'Â', '�')):
        try: return s.encode('latin-1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError): return s
    return s

def _normalize(v):
    v = _fix_mojibake(v)
    v = re.sub(r'^\s*\d+\.\s*', '', v)
    return re.sub(r'\s+', ' ', v).strip()

def _invalid(n):
    return (not n) or n.lower() == 'nan' or re.fullmatch(r'\d+(\.\d+)?', n) is not None

def _fold(s):
    s = unicodedata.normalize('NFKD', s.casefold())
    return ''.join(c for c in s if not unicodedata.combining(c))

def _best(forms):
    def score(f):
        acc = any(unicodedata.combining(c) for c in unicodedata.normalize('NFKD', f))
        return (f[:1].isupper(), acc, forms[f])
    return max(forms, key=score)


def curate(votes):
    curated, unresolved, log = {}, [], []
    for code, cnt in sorted(votes.items()):
        norm = collections.Counter()
        for variant, n in cnt.items():
            nm = _normalize(variant)
            if not _invalid(nm): norm[nm] += n
        raw = sorted(cnt.items(), key=lambda kv: -kv[1])
        if not norm:
            unresolved.append(code); log.append((code, raw, None, [], 'UNRESOLVED')); continue
        groups = collections.defaultdict(collections.Counter)
        for form, n in norm.items(): groups[_fold(form)][form] += n
        ranked = sorted(groups.items(), key=lambda kv: -sum(kv[1].values()))
        canon = _best(ranked[0][1])
        win = sum(ranked[0][1].values())
        runner = sum(ranked[1][1].values()) if len(ranked) > 1 else 0
        flag = 'LOW-VOTES' if sum(norm.values()) < 20 else ('AMBIGUOUS' if runner and win < 1.5 * runner else '')
        curated[code] = canon
        if len(cnt) > 1 or list(cnt)[0] != canon:
            cand = [(_best(g), sum(g.values())) for _, g in ranked]
            log.append((code, raw, canon, cand, flag))
    return curated, unresolved, log


def emit(curated, unresolved, log):
    e = lambda s: str(s).replace('|', '/')
    o = ["#+title: EHCVM unit (=s07bq03b=) codebook --- curated",
         "#+date: <2026-06-07 Sun>\n",
         "Curated unit code->label map for the EHCVM (WAEMU) =s07bq03b= unit",
         "variable (GH #223 Layer 2).  Union of the =s07bq03b= Stata value labels",
         "across all 8 EHCVM countries (the coding is standardized; Togo/Senegal",
         "lost their =.dta= labels but the codes survive in the siblings), curated",
         "to one clean canonical per code.  Regenerate with",
         "=slurm_logs/build_ehcvm_unit_codebook.py=.\n",
         "NOT yet wired: the wiring step decodes =s07bq03b= for the label-less",
         "countries (Togo, Senegal) and documents the Benin<->Togo link in",
         "=Togo/_/CONTENTS.org=.\n",
         "* Method",
         "- Source :: union of =s07bq03b= value labels across " + ", ".join(EHCVM) + ".",
         "- Per code: reverse mojibake (=UnitÃ©= -> =Unité=), strip =NNN.= prefix,",
         "  collapse whitespace; group case/accent-insensitively; dominant group",
         "  wins by frequency, representative prefers Title-cased + accented.",
         f"- {len(curated)} resolved; {len(unresolved)} unresolved; "
         f"{len([l for l in log if l[4]])} flagged for review.\n",
         "* Curated codebook", "#+name: ehcvm_units",
         "| Code | Preferred Label |", "|------+-----------------|"]
    for code in sorted(curated): o.append(f"| {code} | {e(curated[code])} |")
    o += ["", "* Unresolved codes",
          "No label in any source wave; left undecoded: " + ", ".join(map(str, unresolved)) + ".\n",
          "* Flagged for review",
          "| Code | Flag | Chosen | Candidates (form:votes) |",
          "|------+------+--------+-------------------------|"]
    for code, raw, canon, cand, flag in log:
        if flag and canon is not None:
            o.append(f"| {code} | {flag} | {e(canon)} | " +
                     "; ".join(f"{e(c)}:{n}" for c, n in cand) + " |")
    o += ["", "* Full resolution log",
          "Every code whose variants conflicted (raw variant:votes -> canonical).\n"]
    for code, raw, canon, cand, flag in log:
        if canon is None:
            o.append(f"- =[{code}]= UNRESOLVED --- " + "; ".join(f"={e(v)}=:{n}" for v, n in raw))
        else:
            tag = f" [{flag}]" if flag else ""
            o.append(f"- =[{code}]= -> *{e(canon)}*{tag} --- " +
                     "; ".join(f"={e(v)}=:{n}" for v, n in raw[:10]))
    open(OUT, 'w').write("\n".join(o) + "\n")
    print(f"wrote {OUT}: {len(curated)} codes, {len(log)} log entries")


if __name__ == '__main__':
    v = extract()
    emit(*curate(v))

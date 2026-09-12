#!/usr/bin/env python
"""Convert the FAO/INFOODS West African Food Composition Table 2019 workbook
into ``GhanaLSS/_/fct_west_africa.org`` (GH #563).

One-off provenance script: run it again if the workbook is re-supplied.  The
generated org file is the committed artifact; this script records exactly how
it was produced.

Source workbook (not redistributable through this repo -- it is an FAO
publication; only the derived per-100 g table is committed, as an FCT is a
table of facts):

    WAFCT_2019.xlsx, retrieved 2026-09-04

Parsing traps this script handles, each measured on the shipped workbook:

1. THREE header rows, not one: row 0 English component name, row 1 French,
   row 2 the INFOODS tagname.  The tagname row is the stable machine
   identifier and the one we key on.  Tagnames carry trailing whitespace
   ('ENERC ', 'PROTCNT ') -- strip before matching.
2. ``ENERC`` appears TWICE under one tagname (kJ and kcal).  It is
   disambiguated on the row-0 English header, which carries the unit.
3. Category separator rows are interleaved with the data (14 of them, e.g.
   "Cereals and their products/..."), carrying no Code.  They are captured
   as a ``Category`` column and then dropped as rows.
4. Food ``Code`` is ``NN_NNN`` -- a STRING.  Read with ``dtype=str`` so
   pandas cannot coerce it.
5. Values take four forms: a plain number, ``[n]`` (1,526 occurrences -- the
   WAFCT bracket convention for a value taken from outside Africa), ``tr``
   (222, trace) and ``[tr]`` (4).  Brackets are stripped; ``tr`` -> 0.

Run:  python slurm_logs/ghana_audit/build_fct_west_africa.py <workbook.xlsx>
"""
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

# The datasheet we take values from.  See the module docstring of
# GhanaLSS/_/nutrition.py and the commentary emitted into the org file.
SHEET = '03 NV_sum_39 (per 100g EP)'

RETRIEVED = '2026-09-04'
EDITION = 'FAO/INFOODS Food Composition Table for Western Africa (2019), WAFCT 2019'

#: INFOODS tagname -> the ``Preferred Label`` axis of
#: Ethiopia/_/nutrient_labels.org.  Where a tagname is ambiguous (ENERC), the
#: English header substring that disambiguates it is given as well.
#:
#: The choices that are NOT mechanical, and why:
#:   ENERC    -> kcal, not kJ.  nutrient_labels.org's FCT Label is
#:               'energy kcal' and its FDC Label 'Energy' is kcal in FDC.
#:   VITA_RAE -> 'Vitamin A', not VITA (retinol equivalents).  The FDC Label
#:               is 'Vitamin A, RAE'; RAE is the modern basis.
#:   NIA      -> 'Niacin', not NIAEQ (niacin equivalents).  The FDC Label is
#:               'Niacin', which is preformed niacin.
#:   FOL      -> 'Folate' (folate, total).  FDC Label 'Folate, total'.
#:   CHOAVLDF -> 'Carbohydrate'.  NOTE a definitional gap: WAFCT publishes
#:               *available* carbohydrate by difference, while the FDC Label
#:               is 'Carbohydrate, by difference' (total).  Recorded, not
#:               silently reconciled.
TAGNAME_MAP = [
    # (tagname, english-header substring or None, Preferred Label, unit)
    ('ENERC',    '(kcal)', 'Energy',       'kcal'),
    ('PROTCNT',  None,     'Protein',      'g'),
    ('CHOAVLDF', None,     'Carbohydrate', 'g'),
    ('FIBTG',    None,     'Fiber',        'g'),
    ('CA',       None,     'Calcium',      'mg'),
    ('FE',       None,     'Iron',         'mg'),
    ('MG',       None,     'Magnesium',    'mg'),
    ('P',        None,     'Phosphorus',   'mg'),
    ('K',        None,     'Potassium',    'mg'),
    ('ZN',       None,     'Zinc',         'mg'),
    ('VITA_RAE', None,     'Vitamin A',    'mcg'),
    ('VITD',     None,     'Vitamin D',    'mcg'),
    ('VITE',     None,     'Vitamin E',    'mg'),
    ('THIA',     None,     'Thiamin',      'mg'),
    ('RIBF',     None,     'Riboflavin',   'mg'),
    ('NIA',      None,     'Niacin',       'mg'),
    ('VITB6C',   None,     'Vitamin B-6',  'mg'),
    ('FOL',      None,     'Folate',       'mcg'),
    ('VITB12',   None,     'Vitamin B-12', 'mcg'),
    ('VITC',     None,     'Vitamin C',    'mg'),
]

CODE_RE = re.compile(r'^\d\d_\d\d\d$')


def clean_value(v, counter=None):
    """WAFCT cell -> float.  '[n]' -> n, 'tr'/'[tr]' -> 0, '' -> NaN."""
    if v is None:
        return pd.NA
    s = str(v).strip()
    if s in ('', 'nan', 'None'):
        return pd.NA
    bracketed = s.startswith('[') and s.endswith(']')
    if bracketed:
        s = s[1:-1].strip()
    if s.lower() == 'tr':
        if counter is not None:
            counter['[tr]' if bracketed else 'tr'] += 1
        return 0.0
    try:
        out = float(s)
    except ValueError:
        if counter is not None:
            counter['unparsed:' + s[:20]] += 1
        return pd.NA
    if counter is not None:
        counter['[number]' if bracketed else 'number'] += 1
    return out


def build(workbook):
    raw = pd.read_excel(workbook, sheet_name=SHEET, header=None, dtype=str)

    english = [str(v).replace('\n', ' ').strip() for v in raw.iloc[0]]
    tagname = [str(v).strip() for v in raw.iloc[2]]

    # Resolve each Preferred Label to exactly one column index.
    picked = []
    for tag, hint, label, unit in TAGNAME_MAP:
        cands = [i for i, t in enumerate(tagname) if t == tag]
        if hint is not None:
            cands = [i for i in cands if hint in english[i]]
        if len(cands) != 1:
            raise SystemExit(
                f'tagname {tag!r} (hint {hint!r}) matched {len(cands)} columns; '
                'the workbook layout has changed -- refusing to guess.')
        picked.append((cands[0], tag, label, unit, english[cands[0]]))

    body = raw.iloc[3:].reset_index(drop=True)
    code = body[0].astype(str).str.strip()
    is_food = code.str.fullmatch(CODE_RE.pattern).fillna(False)
    is_sep = (~is_food) & body[0].notna()

    # Category: forward-fill the separator rows onto the food rows beneath.
    category = body[0].where(is_sep).ffill()
    # The workbook labels categories 'English/French'; keep the English half.
    category = category.astype(str).str.split('/').str[0].str.strip()

    counter = Counter()
    out = pd.DataFrame({
        'FCT Code': code,
        'FCT Label': body[1].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip(),
        'Category': category,
        'EDIBLE1': [clean_value(v) for v in body[5]],
    })
    for ci, tag, label, unit, _eng in picked:
        out[label] = [clean_value(v, counter) for v in body[ci]]

    out = out[is_food].reset_index(drop=True)
    assert out['FCT Code'].is_unique, 'duplicate FCT codes'

    # The nutrient axis must be a SUBSET of the canonical one -- no fourth
    # vocabulary (GH #563 brief).
    n_labels = df_from_orgfile(countries_root() / 'Ethiopia' / '_' / 'nutrient_labels.org')
    canon = set(n_labels['Preferred Label'].str.strip())
    ours = {label for _, _, label, _, _ in picked}
    assert ours <= canon, f'non-canonical nutrient labels: {sorted(ours - canon)}'

    return out, picked, canon - ours, counter, int(is_sep.sum())


def org_table(df):
    """Render a DataFrame as an org table (pipe-delimited, aligned)."""
    cols = list(df.columns)

    def cell(v):
        if pd.isna(v):
            return ''
        if isinstance(v, float):
            return ('%g' % v)
        return str(v).replace('|', '/')

    rows = [[cell(v) for v in rec] for rec in df.itertuples(index=False)]
    widths = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c)
              for i, c in enumerate(cols)]
    lines = ['| ' + ' | '.join(c.ljust(widths[i]) for i, c in enumerate(cols)) + ' |',
             '|-' + '-+-'.join('-' * w for w in widths) + '-|']
    lines += ['| ' + ' | '.join(r[i].ljust(widths[i]) for i in range(len(cols))) + ' |'
              for r in rows]
    return '\n'.join(lines)


def main():
    workbook = sys.argv[1] if len(sys.argv) > 1 else None
    if not workbook:
        raise SystemExit(__doc__)
    fct, picked, missing, counter, n_sep = build(workbook)

    comp = pd.DataFrame(
        [{'Preferred Label': label, 'INFOODS tagname': tag,
          'Unit (per 100 g EP)': unit, 'WAFCT column (English)': eng}
         for _ci, tag, label, unit, eng in picked])

    dest = countries_root() / 'GhanaLSS' / '_' / 'fct_west_africa.org'
    with open(dest, 'w') as f:
        f.write(HEADER.format(
            edition=EDITION, sheet=SHEET, retrieved=RETRIEVED,
            n_foods=len(fct), n_sep=n_sep,
            n_number=counter['number'], n_bracket=counter['[number]'],
            n_tr=counter['tr'], n_brtr=counter['[tr]'],
            missing=', '.join(sorted(missing)) or 'none',
            n_edible=int(fct['EDIBLE1'].notna().sum()),
        ))
        f.write('\n#+name: wafct_components\n')
        f.write(org_table(comp))
        f.write('\n\n#+name: fct_west_africa\n')
        f.write(org_table(fct))
        f.write('\n')
    print(f'wrote {dest} ({dest.stat().st_size/1024:.0f} KB): '
          f'{len(fct)} foods x {len(comp)} nutrients')
    print('value forms:', dict(counter))


HEADER = """# -*- mode: org -*-
#+title: West African Food Composition Table 2019 -- machine-readable extract

* Source

{edition}

  Vincent, A., Grande, F., Compaore, E., Amponsah Annor, G., Addy, P.A.,
  Aburime, L.C., Ahmed, D., Bih Loh, A.M., Dahdouh Cabia, S., Deflache, N.,
  Dembele, F.M., Dieudonne, B., Edwige, O.B., Ene-Obong, H.N., Fanou Fogny,
  N., Ferreira, M., Omaghomi Jemide, J., Kouebou, P., et al.  FAO, Rome.
  User Guide: http://www.fao.org/infoods/infoods/tables-and-databases/
  faoinfoods-databases/en/

  Datasheet: ={sheet}=
  Retrieved:  {retrieved}

This file is GENERATED.  Do not hand-edit: re-run
=slurm_logs/ghana_audit/build_fct_west_africa.py <workbook.xlsx>=.

* Decisions taken in building this extract

** Which datasheet: =03 NV_sum_39=, not 57, not =stat=

=02 Components= carries a =Datasheet= column naming the sheets each
component appears on.  Every one of the twenty components we take is marked
=03-06=, i.e. present on the condensed 39-component sheet.  The 57-component
sheets add only =EDIBLE2=, =SOP=, alcohol, the individual tocopherol and
carotenoid fractions and the inositol phosphates -- none of which is on the
=nutrient_labels.org= axis.  =sum= rather than =stat= because the =stat=
sheets interleave statistics and provenance columns with the values; the
=sum= sheets carry the compiled values alone.

** Basis: per 100 g EDIBLE PORTION, stored exactly as published

Values are per 100 g of edible portion, as the sheet name says.  They are
stored here **unscaled**; the consuming script multiplies by 10 to reach the
per-kg basis the corpus uses (=Ethiopia/_/fct_tools.py::fct_filter=,
"Convert serving size to Kgs instead of hectograms").

** Edible portion is NOT applied

=EDIBLE1= (the as-purchased -> as-described coefficient) is carried as a
column but is **not** applied to the nutrient values, and =nutrition.py= does
not apply it either.  =food_quantities= is the quantity **acquired**, not the
edible mass, so applying the coefficient would be the more physiologically
accurate choice -- but both existing precedents (=Uganda/_/nutrition.py=,
=Ethiopia/_/nutrition.py=) multiply acquired mass by per-EP densities, and
=Feature('nutrition')= assembles the three countries into one frame.
Departing here alone would make GhanaLSS silently incomparable with them.
The coefficient is kept in this file so a consumer can apply it deliberately.

Caveat if you do: =EDIBLE1= is populated for only {n_edible} of the {n_foods}
foods -- it is null for recipe/mixed-dish rows, where "as purchased" has no
meaning.

** Energy: kcal

=ENERC= appears twice on the sheet, in kJ and kcal, under a single INFOODS
tagname.  We take **kcal**: =nutrient_labels.org= gives =energy kcal= as the
FCT Label for =Energy=, and its FDC Label =Energy= is kcal in FoodData
Central.  The column is disambiguated on the English header, not on position.

** Sheets 07 / 08 / 09 are deliberately UNUSED

=07 Yield factors, sing_ing= (raw -> cooked yield), =08 Retention factors=
(nutrient retention on cooking) and =09 Mixed dishes= (recipes) are
cooked-food conversions.  GhanaLSS =food_acquired= records food **acquired**,
not food as eaten, so no yield or retention factor applies.  They are
recorded here as existing and intentionally not applied.

** Vitamin K is absent from the WAFCT

The table has no phylloquinone component, so the =nutrient_labels.org=
Preferred Label ={missing}= cannot be sourced.  It is **omitted** from this
extract rather than zero-filled: a zero would assert that these foods contain
no vitamin K, which is a claim this table does not make.

** Value conventions decoded on read

The workbook encodes four value forms; all are resolved here so the table is
purely numeric:

| form      | meaning                              | count | stored as |
|-----------+--------------------------------------+-------+-----------|
| =n=       | compiled value                       | {n_number} | =n=       |
| =[n]=     | value taken from outside Africa      | {n_bracket}  | =n=       |
| =tr=      | trace                                | {n_tr}   | =0=       |
| =[tr]=    | trace, from outside Africa           | {n_brtr}     | =0=       |

The bracket convention marks provenance, not a different quantity, so the
brackets are stripped and the number kept.  That provenance is *not*
preserved in this extract; consult the workbook's =04=/=06= =stat= sheets or
=12 Data sources with BiblioID= if you need it.

* Shape

{n_foods} foods (codes =NN_NNN=, strings) x 20 nutrients, plus =Category=
(from the {n_sep} category separator rows interleaved with the data) and
=EDIBLE1=.

=wafct_components= records, per nutrient, the INFOODS tagname and workbook
column it came from -- so the mapping onto the Preferred Label axis is
auditable without reopening the workbook.
"""


if __name__ == '__main__':
    main()

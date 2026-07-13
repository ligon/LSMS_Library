# Guatemala food_acquired — ROUND-2 FIX SPEC (supersedes the period handling in BRIEF_pilot.md)

Round-1 built a canonical, sane frame but the **contract/source-truth lens
refuted it** (confirmed against source). Three defects to fix. The fix is fully
specified below — implement exactly this; do not re-derive the period choice.

## Verified source structure (ENCOVI 2000, ECV13G12.DTA, purchased rows p12a03==1, N=310,701)
- `p12a04`  — purchase FREQUENCY code (nonnull+>0 for all 310,701). INVESTIGATE
  its Stata value labels: it likely encodes how often the item is bought, which
  is the clean monthly conversion. (read `meta.variable_value_labels['p12a04']`.)
- **`p12a05` = "gasto al mes" (MONTHLY expense), populated >0 for ALL 310,701**
  — the survey's complete monthly purchased value. USE THIS for purchased Expenditure.
- `p12a06a/06b/06c/06d` = the "last 15 days" detail block (only 197,505 nonnull):
  06a qty, 06b unit, 06c equivalence, 06d 15-day expense. This block is for the
  UNIT PRICE, not the period total.
- Produced block (obtained, p12a07==1, N=13,590): `p12a09a` = "cantidad obtuvo
  al mes" (MONTHLY qty), `p12a10a` = 15-day qty (5,577 >0), `p12a08` = ?
- `p12a11a..f` = obtained-source breakdown: a=produccion propia, b=regalo(gift),
  c=parte de un pago(in-kind pay), d=negocio(business), e=trueque(barter),
  f=otro. Each nonnull for all 13,590 obtained rows (likely 0/1 or amounts —
  INVESTIGATE whether they are flags or shares).

## Required canonical build (MONTHLY basis throughout)
1. **Purchased Expenditure = p12a05 (monthly)** for every purchased row. This
   fixes the 23.9% loss (round-1 used p12a06d=15-day, NaN on 113,196 rows).
2. **Purchased unit price** from the 15-day block where present:
   price_per_pound = p12a06d / (p12a06a * p12a06c * cnlib). (cnlib = factor de
   conversion a libras.)
3. **Purchased Quantity (monthly, pounds)**: prefer `p12a06a * cnlib * p12a06c`
   scaled to monthly via the `p12a04` frequency IF p12a04 is a clean
   times-per-month count; ELSE derive monthly qty = p12a05 / price_per_pound
   (unit-value method) where price exists. Where neither qty nor price is
   recoverable (the 113,196 rows with no 15-day detail), leave **Quantity = NaN
   but KEEP Expenditure = p12a05** (expenditure-only rows are valid canonical;
   they flow into food_expenditures and drop out of kg quantities). Document the
   chosen rule and how many rows are Expenditure-only.
4. **Produced Quantity = p12a09a (MONTHLY)** to match the purchased basis (NOT
   the 15-day p12a10a). Produced Expenditure = NaN (no obtained-value variable).
5. **Split s correctly** using p12a11*: own-production → `s='produced'`;
   gift (b) + in-kind pay (c) → `s='inkind'`; business (d) + barter (e) +
   other (f) → `s='other'`. (S_VALUES = purchased/produced/inkind/other.) If
   p12a11* are mutually-exclusive flags, assign each obtained row to one s; if
   they are amounts/shares, apportion the obtained quantity across s accordingly
   and document which.
6. u = 'lbs' (pounds) as round-1 (cnlib varies by item×unit so native u can't be
   recovered by the framework's u-keyed kg map); the monthly purchased/produced
   quantities are in pounds.

## New acceptance bar (in addition to the BRIEF_pilot.md bar)
- Purchased Expenditure total reconciles to **SUM(p12a05) = 7,479,460** (monthly),
  NOT to the 15-day p12a06d total. Paste the number; delta must be ~0.
- food_acquired Quantity has a SINGLE consistent period (monthly). State the
  Expenditure-only row count and that those rows carry Expenditure but NaN Quantity.
- s ∈ a subset of {purchased, produced, inkind, other}; report the row counts per s
  and how p12a11* drove the split.

Everything else (i/j fix, food_items.org harmonization, data_scheme registration,
delete legacy script, branch/push, stop-list, report format) is unchanged from
BRIEF_pilot.md.

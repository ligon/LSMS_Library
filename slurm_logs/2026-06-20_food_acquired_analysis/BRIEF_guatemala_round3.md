# Guatemala food_acquired — ROUND-3 SPEC (AUTHORITATIVE; supersedes round-1 AND round-2)

Maintainer decision (ligon, on PR #578): the canonical food_acquired must record
the **ACTUAL RECALLED ACQUISITION** — the survey's last-15-days window — with NO
assumptions about whether it is representative of the rest of the year. Do NOT use
the "usual/typical month" variables (p12a05 "gasto al mes", p12a09a "cantidad
obtuvo al mes") and do NOT use the months-acquired frequency (p12a04 "meses
compro", p12a08 "meses obtuvo") to annualize or average. Round-2's monthly basis
is WRONG for this intent; round-1's purchased side was closer but mixed periods and
lacked the s-split. This spec is the uniform 15-day actual-recall build.

## Reference period: LAST 15 DAYS (actual recall), uniform on both sides

### Purchased rows (s='purchased')
- Keep only rows with an actual 15-day purchase: p12a06a > 0 (equivalently
  p12a06d not-null — they are lockstep). A household that buys the item normally
  but NOT in the last 15 days has NO purchased acquisition event in the window;
  it correctly produces no purchased row. This is faithful, not data loss.
- Quantity (lbs) = p12a06a ("cantidad compro", last 15d) * p12a06c
  ("equivalencia") * cnlib ("factor de conversion a libras").
- Expenditure = p12a06d ("gasto ult 15 dias").
- u = 'lbs'.

### Obtained rows (s in {produced, inkind, other})
- Use the **15-day** obtained quantity p12a10a ("cantidad obtuvo ult 15 dias"),
  NOT the monthly p12a09a. Keep only rows with p12a10a > 0 (~5,577 rows).
- Quantity (lbs) = p12a10a * (obtained equivalence) * cnlib. DETERMINE the correct
  obtained unit/equivalence: check p12a09b/p12a10b and whether cnlib applies to the
  obtained unit the same way as purchased; verify against source and document.
- Expenditure = NaN (the source records NO value for obtained acquisition).
- s split from the p12a11* si/no flags: own-production (a) -> produced;
  gift (b) + in-kind pay (c) -> inkind; business (d) + barter (e) + other (f)
  -> other. Multi-flag rows -> priority produced > inkind > other; no-flag -> other.

### Unchanged from prior rounds
- i = hogar (household), j = item (food, harmonized via food_items.org '2000'
  column). FIX the legacy i/j swap.
- Country concat (Cambodia #561 pattern); data_scheme registration
  (index (t,i,j,u,s), materialize: make); DELETE the legacy
  food_prices_quantities_and_expenditures.py; fix _/Makefile.

## Reconciliation targets (cold, vs raw ECV13G12.DTA) — THESE are the bar now
- Purchased Expenditure total == SUM(p12a06d over purchased rows) = **2,952,285**
  (the actual 15-day spend). NOT p12a05.
- Purchased rows with Quantity > 0 == count(p12a06a > 0) = **197,505**.
- Obtained rows == count(p12a10a > 0) ~= **5,577**.
- Distinct i (households with any 15-day acquisition) and j must match the source
  restricted to rows with actual 15-day acquisition. Report the numbers.
- food_expenditures/prices/quantities non-empty + canonical; food_prices is
  unaffected by the period choice (Exp/Qty cancels the window).

## CONTENTS.org documentation (REQUIRED, ligon's explicit ask)
Add a clearly-discoverable section to lsms_library/countries/Guatemala/_/CONTENTS.org
documenting:
- The ENCOVI 2000 Capitulo 12 food instrument records, per (household, item):
  a buy flag (p12a03), months/yr bought (p12a04 "meses compro"), a USUAL monthly
  spend (p12a05 "gasto al mes"), and the ACTUAL last-15-days purchase
  (p12a06a quantity / p12a06d expense); mirrored on the obtained side
  (p12a07 flag, p12a08 meses obtuvo, p12a09a monthly qty, p12a10a 15-day qty,
  p12a11* source breakdown).
- The recall periods DIFFER by variable: p12a06d/p12a06a/p12a10a are ACTUAL
  last-15-days recall; p12a05/p12a09a are USUAL-month estimates.
- The canonical food_acquired uses the **actual 15-day recall** (p12a06*, p12a10a)
  and deliberately does NOT use the usual-month (p12a05/p12a09a) or
  months-acquired (p12a04/p12a08) variables, to avoid assuming the recalled window
  is representative of the rest of the year.

Everything else (verification recipe with LSMS_COUNTRIES_ROOT, stop-list, report
format) is per BRIEF_pilot.md.

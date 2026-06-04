# RECON RECIPE: assets coverage (verified) — 2026-06-04
# Reference declarer: Malawi. Canonical assets index (t,i,j); columns:
#   Quantity float, Age float, Value float, Purchased Recently str, Purchase Price float
# Item-level, NO aggregation; keep non-owned (all-NaN) rows; do NOT add the Yes/No ownership flag as a column.
# j = string item NAME (no within-country harmonize_* table needed; cross-country label harmonization out of scope).
# ALL findings VERIFIED (data loaded, columns read, sample-i overlap measured).

## PURE-YAML countries (clean, 100% i-overlap) — preferred
Iraq 2006-07: Data/2007ihses16_durable_goods.dta (LONG). i=xhhkey. j=q1601_n. Quantity=q1602. Value/PurchasePrice=q1604. acq-year q1603.
Iraq 2012:    Data/2012ihses18_durables.dta (LONG). i=questid. j=durable_code. Quantity=q1801. Value=q1805. PurchasePrice=q1803. acq-year q1802.
  Quirk: durable_serial(1-3) -> multiple rows per (hh,j) for big items; harmless item-level.
Kosovo 2000:  Data/DURGOODS.dta (LONG). i=hhid. j=s10e_01 (NOT s10e_00 truncated). Age=s10e_02. Value=s10e_4a. currency s10e_4b mixed (DM/Dinar, pass raw).
Serbia 2007:  Data/m2_durables.dta (LONG). i=[opstina,popkrug,dom] (list-form -> 100% overlap). j=s26 (NOT nazivtpd mojibake). Quantity=s27. Age=s28. Value=s30 (object->coerce float).
Liberia 2018-19: Data/Household/sect15_public.dta (LONG). i=hhid (float->format_id). j=item_name (NOT item_id). Quantity=S15_2. Value=S15_5 (resale). drop S15_6/7.

## SCRIPT-PATH countries (real blockers/judgment)
Albania 2002: Data/durables_cl.dta (LONG). j=m3c_q02. Age=m3c_q03. Value=m3c_q05. NO quantity.
  *** i BLOCKER: naive i:hh = 0% overlap. sample() builds i=format_id(psu)+'-'+format_id(hh) (see Albania/2002/_/sample.py).
      Composite psu-hh = 100% overlap. The ROSTER's i:hh is itself a latent bug — mirror sample.py, NOT the roster. -> needs _/assets.py.
  Other Albania waves: 2012 has .sav durables (Modul_13C1/C2) - separate add; 1996/2003/2004/2005/2008 none.
Guyana 1992: Data/DRBLS.dta (WIDE: item01..item44 + yriNN + valiNN). j lives in the COLUMN LABEL ("NO. OWNED - AIR CONDITIONER").
  Quantity=itemNN, Value=valiNN, acq-year=yriNN. *** Requires WIDE->LONG reshape keyed off label suffix to populate j.
  *** i BLOCKER (UNVERIFIED): sample i=[ED,HH] from COVERN.dta; DRBLS has id_nmbr + newid(=ED#*100000+ED_SMPL*100+SMPL_H) but no separate ED/HH.
      Must derive ED/HH from newid or join id_nmbr<->COVERN; CONFIRM before wiring. -> needs _/assets.py.
Iraq note: 2006-07 has no separate current-value vs purchase-price (single q1604).
GhanaSPS 2013-14 & 2017-18: Data/03aiii_durablegoodquestions.dta (LONG). i=FPrimary. j=durablegood. Quantity=quantity. Value=currentvalue.
  *** GhanaSPS has NO data_scheme.yml and NO sample table — all-script country. assets must be script-path; registering it
      needs the country's table-surfacing mechanism confirmed (structural gap — flag to maintainer).
  EXCLUDE 05aii_assetquestions.dta (enterprise assets, different unit of obs). 2009-10 wave: no durables file.

## Implementation order suggestion: YAML wins first (Iraq, Kosovo, Serbia, Liberia), then Albania, then Guyana/GhanaSPS (blockers).
## REMINDER: implement via REAL framework build (worktree-pinned venv) + build-verifier; no direct-script shortcut.

# RECON RECIPE: Tanzania plot_features (GH #167) — recon agent output 2026-06-03
# STATUS: awaiting adversarial refutation (Phase A.5) before implementation.

## Feasibility: only 2019-20 and 2020-21 buildable now (~8,000-8,800 rows total).
## 2008-15 (rounds 2008-09/2010-11/2012-13/2014-15): NO agriculture source on disk (only upd4_hh_*). DEFERRED until UPD ag file DVC-added.

## Source files (merge two per wave on (hhid, plot)):
## 2019-20 (Extended Panel): AG_SEC_02.dta (area+GPS) + AG_SEC_3A.dta (soil/irrig/tenure). 2305 rows.
## 2020-21 (Refresh Panel):  ag_sec_02.dta + ag_sec_3a.dta. 6560 rows.
## 3A = Long Rainy Season primary; 3B = Short Rainy backfill (optional v1).

## colmap
hhid:  2019-20 sdd_hhid | 2020-21 y5_hhid  (string; emit raw, let id_walk+_join_v_from_sample chain)
plot:  2019-20 = plotnum in BOTH SEC_02 & SEC_3A (sdd_hhid+plotnum, 1:1 2305/2305).
##       2020-21 = plot_id in BOTH ag_sec_02 & ag_sec_3a (y5_hhid+plot_id, 1:1 6560/6560). [REFUTER FIX: SEC_3A has NO plotnum in 2020-21]
##       NOTE: granted_right_of_occupancy must be ADDED to canonical TenureSystem spellings in data_info.yml (refuter-confirmed gap).
area_est(acres)=ag2a_04 ; area_gps(acres)=ag2a_09  (from SEC_02). Prefer GPS else est. HECTARES_PER_ACRE=0.404686. AreaUnit='acres'.
use=ag3a_03 ; soil_type=ag3a_10 ; irrigated=ag3a_18 ; acquire=ag3a_25 ; legal_cert=ag3a_28a  (from SEC_3A)
GPS coords ag2a_07__Latitude/Longitude = **CONFIDENTIAL** redaction -> DO NOT emit.

## plot_id = str(int(plotnum)). No _A/_B suffix (single plot roster; 3A/3B are seasonal uses merged in).

## Tenure (ag3a_25): 1 INHERITANCE->inherited, 2 GIFT->inherited, 3 BORROW FAMILY->other_tenure, 4 VILLAGE ALLOC->communal,
##   5 PURCHASED->owned, 6 USED FREE->other_tenure, 7 RENTED IN->rented_in, 8 SHARED-RENT->sharecropped_in,
##   9 SHARED-OWN->sharecropped_out, 10 SQUATTING/CLEARING->communal, 11 OTHER->other_tenure
##   Override from ag3a_03 use: 2 RENTED OUT/3 GIVEN OUT -> Tenure rented_out.
## TenureSystem (ag3a_28a): 1 GRANTED RIGHT OF OCCUPANCY->granted_right_of_occupancy (EXTEND canonical), 2 CUSTOMARY CERT->customary, 3 NO CERT->other_tenure_system/NaN
## SoilType (ag3a_10): 1 Sandy,2 Loam,3 Clay,4 Other  (map on integer code; 2020-21 labels lowercased)
## Irrigated (ag3a_18): 1 Yes->True, 2 No->False

## GPS: DEFER (coords redacted). Build path: materialize: make (cross-file merge), per-wave script + shared tanzania.plot_features_for_wave helper.
## Register plot_features in Tanzania/_/data_scheme.yml (index (t,i,plot_id), materialize: make) — NOT auto-derived.

## Open questions: (1) 2008-15 missing ag source - ship 2-wave now or block? (2) extend TenureSystem granted_right_of_occupancy;
##   (3) verify 2020-21 plot_id vs plotnum merge keys; (4) 3B backfill optional.

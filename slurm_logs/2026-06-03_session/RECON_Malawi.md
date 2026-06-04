# RECON RECIPE: Malawi plot_features (GH #167) — recon output 2026-06-03
# STATUS: awaiting adversarial refutation (Phase A.5).

## Buildable: 2010-11, 2013-14, 2016-17, 2019-20. DEFER 2004-05 (IHS2 has no standard plot roster).
## Join Module C (area) <-> Module D (tenure/soil/irrig) on (hhid, plot_id). materialize: make.
## 2016-17 & 2019-20 combine Cross_Sectional + Panel halves into one wave t (concat both).

## Source files:
## 2010-11: Full_Sample/Agriculture/ag_mod_c.dta + ag_mod_d.dta
## 2013-14: AG_MOD_C_13.dta + AG_MOD_D_13.dta
## 2016-17: Cross_Sectional/ag_mod_c.dta + Panel/ag_mod_c_16.dta ; Cross_Sectional/ag_mod_d.dta(BLOCKED latin-1) + Panel/ag_mod_d_16.dta
## 2019-20: Cross_Sectional/ag_mod_c.dta + Panel/ag_mod_c_19.dta ; Cross_Sectional/ag_mod_d.dta + Panel/ag_mod_d_19.dta

## colmap: hhid: 2010-11 case_id | 2013-14 y2_hhid | 2016-17 XS case_id/Panel y3_hhid | 2019-20 XS case_id/Panel y4_hhid
## plot key C: 2010-11/2013-14 ag_c00 | 2016-17/2019-20 gardenid+plotid ; plot key D: ag_d00 | gardenid+plotid
## area_est=ag_c04a ; area_unit=ag_c04b (1 ACRE,2 HECTARE,3 SQ METERS, 2019-20 +4 OTHER) ; area_gps(acres)=ag_c04c
## Tenure: 2010-11/2013-14 ag_d03 (LABELED) | 2016-17/2019-20 ag_d02 (CODES, NO LABELS - need questionnaire)
## SoilType=ag_d21 (all) ; Irrigated source=ag_d28a (all)
## GOTCHA: in 2013-14 ag_d02='respondent ID' NOT tenure; tenure var ag_d03. Different from later-wave ag_d02=tenure.

## Area: prefer GPS ag_c04c x0.404686; else estimate via unit (ACRE x0.404686, HECTARE x1, SQ M /10000). HECTARES_PER_ACRE=0.404686. AreaUnit='acres'.
## No region land factor (ihs3_conversions.csv is a FOOD table, irrelevant).

## Tenure ag_d03 (2010-11/2013-14): 1 granted-leaders->communal,2 inherited->inherited,3 bride-price->inherited,
##   4 purchased-title->owned,5 purchased-notitle->owned,6 leasehold->leased+TenureSystem leasehold,7 rent-short->rented_in,
##   8 tenant-farming->sharecropped_in,9 borrowed-free->other_tenure,10 moved-no-permission->other_tenure,11 other->other_tenure
## Tenure ag_d02 (2016-17/2019-20): codes 1-10 NO LABELS -> needs IHS4/IHS5 questionnaire (Documentation/).  [BLOCKER]
## SoilType ag_d21: 1 sandy,2 between-sandy-clay,3 clay,4 other (strip Chichewa parentheticals).
## Irrigated ag_d28a: 1 divert-stream,2 bucket,3 handpump,4 treadle,5 motor,6 gravity,7 RAINFED/NONE,8 other -> Irrigated=(code!=7 & notna).

## i = raw wave hhid, let id_walk/_join_v_from_sample chain (like household_roster). plot_id: 2010-11/2013-14 = ag_c00; 2016-17/2019-20 = f"{gardenid}_{plotid}". (hh,plot_id) unique.
## GPS: DEFER (no decimal-degree; coords in offset householdgeovariables files).
## materialize: make script + shared malawi.plot_features_for_wave helper. harmonize_* in categorical_mapping.org.
## Row estimate: 2010-11 ~19265, 2013-14 ~6490, 2016-17 ~4042 Panel (+XS blocked ~15751), 2019-20 ~23263.

## BLOCKERS: (1) 2016-17 Cross_Sectional ag_mod_d.dta latin-1 byte -> UnicodeDecodeError; needs encoding='latin-1' read path.
##   (2) 2004-05 no plot roster -> defer. (3) ag_d02 tenure codes (2016-17/2019-20) unlabeled -> need questionnaire.

## === REFUTER CORRECTIONS (2026-06-03) — supersede above where conflicting ===
## [CRITICAL] Tenure for 2016-17 & 2019-20: ag_d02 is "ID of Respondent" (NOT tenure); ag_d03 (acquire) is ABSENT from
##   ag_mod_d in both waves. DROP Tenure for 2016-17/2019-20 (emit NaN) OR re-recon to locate acquire in another module/
##   questionnaire. Tenure for 2010-11 & 2013-14 (ag_d03, labeled) is CORRECT. Do NOT map ag_d02 -> Tenure.
## [DOWNGRADED] latin-1 "blocker" is NOT a blocker: read 2016-17 XS ag_mod_d with usecols=[case_id,hhid,gardenid,plotid,
##   ag_d02,ag_d21,ag_d28a] -> reads cleanly (15724 rows). Bad byte is in an unused _oth free-text col. pyreadstat encoding= does NOT help; usecols does.
## [ID] 2016-17 sample().i is 'cs-17-'-prefixed; XS case_id is bare, Panel y3_hhid dashed. Must wire the cs_i / 'cs-17-' prefix
##   mapping exactly like the roster (dfs: block + mapping: cs_i in 2016-17 data_info.yml / mapping.py) else ~100% XS orphan.
## [CONFIRMED] C<->D join keys, area units (ag_c04b 1 Acre/2 Hectare/3 SqM/4 Other all waves; ag_c04c GPS acres 90-95% cov),
##   2004-05 defer, soil ag_d21, irrigation ag_d28a (codes 1-9; code 9 'Can Irrigation' exists). All quantitatively sound.

# RECON RECIPE: Ethiopia plot_features (GH #167) — recon agent output 2026-06-03
# STATUS: awaiting adversarial refutation (Phase A.5) before implementation.

## Granularity
Emit one row per FIELD (sect3). plot_id = format_id(parcel_id)+'_'+format_id(field_id).
Parcel-level attrs (Tenure, SoilType) live in sect2 → LEFT-JOIN onto field rows on (hhid, parcel_id).
Field-level attrs (Area, GPS, Irrigated) native in sect3. => cross-file join => materialize: make.

## Source files per wave (post-planting only; no ph parcel roster)
2011-12 W1: sect2_pp_w1.dta + sect3_pp_w1.dta
2013-14 W2: sect2_pp_w2.dta + sect3_pp_w2.dta
2015-16 W3: sect2_pp_w3.dta + sect3_pp_w3.dta
2018-19 W4: sect2_pp_w4.dta + sect3_pp_w4.dta
2021-22 W5: sect2_pp_w5.dta + sect3_pp_w5.dta

## colmap — FIELD (sect3). Two eras: W1-W3 pp_s3q*, W4-W5 s3q*
hhid:        W1 household_id | W2,W3 household_id2 | W4,W5 household_id
parcel_id:   parcel_id (all);  field_id: field_id (all)
area_est#:   W1-W3 pp_s3q02_a (+_b dec) | W4,W5 s3q02a
area_unit:   W1-W3 pp_s3q02_c | W4,W5 s3q02b
area_gps_sqm:W1-W3 pp_s3q05_a (W1 total _c) | W4,W5 s3q08
latitude:    W1 absent | W2,W3 pp_s3q06_a | W4,W5 s3q09__Latitude
longitude:   W1 absent | W2,W3 pp_s3q06_b | W4,W5 s3q09__Longitude
irrigated:   W1-W3 pp_s3q12 | W4,W5 s3q17   (1 Yes/2 No)
water_source:W1-W3 pp_s3q13 | W4,W5 s3q19

## colmap — PARCEL (sect2), join on (hhid, parcel_id)
acquire→Tenure: W1-W3 pp_s2q03 | W4,W5 s2q05
soil_type:      W1 NOT ASKED | W2,W3 pp_s2q14 | W4,W5 s2q16

## Area units vocab (codes): 1 Hectare,2 SqMeters,3 Timad,4 Boy,5 Senga,6 Kert,7 Tilm,8 Medeb,9 Rope,10 Ermija,11 Other
## Conversion: GPS sqm/10000 = ha (preferred, covers most rows). Hectare x1.
## BLOCKER: timad/kert/boy/... -> ha factors NOT in repo. Plan: Area from GPS sqm where present (AreaUnit='hectares');
##          farmer-estimate-only non-metric rows -> Area=NaN, AreaUnit=native unit (or timad~0.25ha, others NaN).

## Tenure (acquisition codes). W3-W5 set:
1 Granted by Local Leaders->communal, 2 Inherited->inherited, 3 Rent->rented_in, 4 Borrowed Free->other_tenure,
5 Moved without permission->other_tenure, 6 Sharecrop in->sharecropped_in, 7 Purchased->owned, 8 Other->other_tenure
(W1-W2 narrower: 1->communal,2->inherited,3->rented_in,4->other,5->other,6->other)

## SoilType (FAO/WRB): 1 leptosol,2 cambisol,3 vertisol,4 luvisol,5 mixed,6 other,7 other(non-cultivation use→other/NaN)

## Irrigated = (irrig_code==1). Direct Yes/No, no water-source inference needed.

## TenureSystem: NO ESS analogue (state usufruct). DECISION NEEDED: NaN vs extend canonical spellings
##   with state_usufruct/certified_usufruct (cert signal: W1 pp_s2q04, W4/W5 s2q03/s2q04a_1 doc type).

## household-id -> sample().i per wave: W1 household_id, W2 household_id2, W3 household_id2, W4 household_id, W5 household_id
## format_id on parcel_id/field_id to strip .0

## GPS verdict: emit Latitude/Longitude for W2-W5 (decimal degrees, ~3-15N/33-48E, ~100% populated). W1 NaN.
##   CAVEAT: WB public ESS GPS may be anonymized/offset — verify before treating as exact.

## Build path: materialize: make per wave (cross-file join). t = wave label, single round (roster is pp-only).
##   Pattern: Ethiopia/<wave>/_/plot_features.py + shared ethiopia.plot_features_for_wave + country _/plot_features.py concat+id_walk.
##   harmonize_* org tables -> Ethiopia/_/categorical_mapping.org (currently only Strata/Roof/Floor).

## Row estimate (field rows): W1 32025, W2 33147, W3 33305, W4 19339, W5 14878 = ~132,694 total.

## Open questions: (1) local area-unit factors; (2) TenureSystem NaN vs extend; (3) W1 missing soil+GPS; (4) GPS confidentiality.

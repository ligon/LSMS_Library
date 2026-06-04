# RECON RECIPE: EHCVM cluster plot_features (GH #167) — recon agent output 2026-06-03
# STATUS: awaiting adversarial refutation (Phase A.5) before implementation.

## Feasibility
6 of 7 countries feasible. TOGO 2018 DROPPED — no agriculture module (only *_forEthan food/roster extracts).
EHCVM waves only (2018-19 + 2021-22). Pre-EHCVM waves deferred to separate recipes.

| Country     | EHCVM waves        | s16a file                  |
|-------------|--------------------|----------------------------|
| Mali        | 2018-19, 2021-22   | s16a_me_mli{2018,2021}.dta |
| Niger       | 2018-19, 2021-22   | s16a_me_ner{2018,2021}.dta |
| Senegal     | 2018-19, 2021-22   | s16a_me_sen{2018,2021}.dta |
| Burkina_Faso| 2018-19, 2021-22   | s16a_me_bfa{2018,2021}.dta |
| Benin       | 2018-19 only       | s16a_me_ben2018.dta        |
| Guinea-Bissau| 2018-19 only (PT) | s16a_me_gnb2018.dta        |

## Shared module: s16a_me_{iso}{year}.dta — UNIFORM across all 6 (verified core columns present in all).
## Guinea-Bissau: Portuguese labels, SAME integer codes -> harmonize_* key on codes, no language branch.

## colmap (identical all waves)
i (hhid)     = [grappe, menage]   # matches sample().i natively, 0 unmatched (verified Senegal)
v            = grappe             # framework joins, do NOT bake in
field_no     = s16aq02
parcel_no    = s16aq03
area_est     = s16aq09a ; area_est_unit = s16aq09b (1=Hectare,2=m2)
area_gps     = s16aq47 (HECTARES) ; gps_measured = s16aq45 (1 Oui/2 Non)
tenure       = s16aq10  -> Tenure
acquire      = s16aq12  -> harmonize_acquire (supplementary)
doc_legal    = s16aq13  -> TenureSystem candidate
soil_type    = s16aq18  -> SoilType
water_source = s16aq17  -> Irrigated

## plot_id = f"{int(s16aq02)}_{int(s16aq03)}"  (field_no _ parcel_no). Key (grappe,menage,s16aq02,s16aq03) unique (0 dups).
## Area = GPS(s16aq47, ha) where gps_measured else estimate(s16aq09a) converted via s16aq09b (ha x1, m2 /10000). AreaUnit='hectares'.

## Tenure (s16aq10): 1 Proprietaire->owned, 2 Pret gratuit->communal|other_tenure(judgment), 3 Fermage->rented_in,
##   4 Metayage->sharecropped_in, 5 Gage->other_tenure, 6 Autre->other_tenure, 7 Co-proprietaire->owned (Niger 2021 only)
## SoilType (s16aq18): 1 Sableux/Arenoso, 2 Limoneux/Limoso, 3 Argileux/Argiloso, 4 Glacis/Talude, 5 Autre/Outro
## Irrigated (s16aq17 in {1,2,3}): 1 puits,2 canal,3 ruisseau ->Irrigated; 4 Pluviale,5 Marais ->False; 6 Autre->NaN/False
## acquire (s16aq12): 1 Achat,2 Heritage,3 Mariage,4 Don,5 Autre
## doc_legal (s16aq13, for TenureSystem if used): 1 Titre foncier,2 Permis exploiter,3 Proces-verbal,4 Bail,5 Convention vente,6 Autre,7 Aucun

## 2021-22 deltas (additive): Senegal 2021 s16aq10 code3 relabel (still Fermage); Niger 2021 s16aq10 adds code 7 Co-proprietaire->owned.

## TenureSystem: no freehold/leasehold question. DECISION: NaN, or extend spellings with s16aq13 doc-regime values.
## GPS: NO decimal-degree parcel GPS (s16aq47 is area not coords) -> defer Lat/Lon like Uganda.
## Build path: materialize: make, simple per-wave single-file. Shared helper ehcvm.plot_features_for_wave mirroring uganda's.

## Row estimate (parcel rows): Mali 7323/9924, Niger 6685/5729, Senegal 7767/6112, Burkina 12441/8467, Benin 7588, GNB 9873. ~81,900 total.

## DEFERRED pre-EHCVM (separate recipes): Niger 2011-12 (ECVMA ecvmaas1_p1), Niger 2014-15 (ECVMA2_AS1P1),
##   Mali 2014-15 (EACI EACIEXPLOI_p1), Mali 2017-18 (EACI 2017, needs investigation), Burkina 2014 (EMC, split GPS file emc2014_agri_gps).

## Open questions: Togo impossible; TenureSystem NaN-vs-extend; Tenure code2 communal-vs-other; Benin/GNB no 2021-22.

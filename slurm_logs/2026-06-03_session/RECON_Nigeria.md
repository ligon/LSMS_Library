# RECON RECIPE: Nigeria plot_features (GH #167) — recon output 2026-06-03
# STATUS: awaiting adversarial refutation (Phase A.5).

## Plot lasting-attrs are PP-ONLY (post-planting). PH files are crop-level, ignore.
## Each wave => single t = PP quarter: 2010Q3, 2012Q3, 2015Q3, 2018Q3, 2023Q3. materialize: make (per-row const t + cross-file join).

## Source files per wave (merge area onto detail on (hhid, plotid)):
## Area: sect11a1_planting{w1..w5}.  Detail(tenure/soil/irrig): W1 sect11b_plantingw1, W2-W5 sect11b1_planting{w2..w5}.
## W5 (2023-24) under 2023-24/Data/Post Planting Wave 5/Agriculture/.

## colmap (hhid=hhid all waves; v=ea; plot_id=plotid)
## area_est: W1-W3 s11aq4a | W4 s11aq4aa | W5 s11aq3_number
## area_unit: W1-W4 s11aq4b | W5 s11aq3_unit
## area_gps(sqm): W1 s11aq4d | W2-W4 s11aq4c | W5 s11mq3
## Tenure(acquire): W1 s11bq4 | W2-W5 s11b1q4
## TenureSystem: W5 only s11b1q4b (W1-W4 NaN)
## SoilType: W1 none | W2-W4 s11b1q44 | W5 s11b1q61
## Irrigated: W1 none | W2-W4 s11b1q39 | W5 s11b1q56
## GOTCHAS: W4 s11aq4a is a GPS yes/no flag NOT area (use s11aq4aa). W5 total renumber:
##   W5 s11b1q39='right to bequeath' NOT irrigation; W5 s11b1q44='main use' NOT soil. Use W5 cols above.
## W1 sect11b stops at q28 -> no soil/irrigation/tenure-system at plot level (Area+Tenure only).

## Area units (s11aq4b / W5 s11aq3_unit): 1 HEAPS,2 RIDGES,3 STANDS,4 PLOTS = NON-STANDARD (no in-repo factor);
##   5 ACRES x0.404686, 6 HECTARES x1, 7 SQ METERS /10000, 8 OTHER. W5 adds 8/9=sq-foot, 10=football field.
## Strategy: Area=GPS sqm/10000 preferred; estimate fallback ONLY for convertible units (5/6/7). Non-standard -> Area=NaN, AreaUnit=native label. (matches Uganda deferral)

## Tenure codes EVOLVE per wave -> WAVE-KEYED harmonize_acquire (wave,code) [same lesson as Uganda bug]:
##  W1/W2 4-code, W3 5-code, W4 7-code, W5 9-code. Canonical: purchase->owned, rented->rented_in, inheritance->inherited,
##  sharecropped->sharecropped_in, community/customary/free->communal, gov/exchange/gift->other_tenure.
## TenureSystem (W5 s11b1q4b): 1 customary,2 freehold,3 leasehold,4 STATE(extend),5 COMMUNITY(extend),6 COOPERATIVE(extend),96 other.
## SoilType (s11b1q44 / W5 s11b1q61, stable W2-W5): 1 sandy,2 clay,3 sandy_clay,4 forest_loam,5 loamy,6/96 other.
## Irrigated (s11b1q39 / W5 s11b1q56): 1 Yes,2 No, 11=.A sentinel->NaN.

## i = hhid (per Nigeria/_/CONTENTS.org + household_roster.py). v=ea -> framework joins, do NOT bake. id_walk handles panel chains.
## plot_id = format_id(plotid) (1..N, unique within hhid; no _A/_B). format_id hhid+plotid for merge.
## GPS: DEFER Lat/Lon (no decimal-degree coords; only GPS area sqm).
## Row estimate: W1 6086, W2 5893, W3 5824, W4 11076, W5 9232. ~38,100 total.
## Design caveats: non-standard area units (no factor); W1 soil/irrig absent; TenureSystem W5-only (extend vocab).

## === REFUTER (2026-06-03): SAFE AS-IS. All high-risk traps (W4 s11aq4aa vs GPS-flag; W5 s11b1q39=bequeath, s11b1q44=main-use)
## independently confirmed avoided. 0% plot_id dups, 0% sample orphan, PP-only confirmed (PH is crop-level item_cd). Minor:
## W3 acquire is 6-code {1-6} not 5; convertible-unit share W1 29%/W2 28%/W3 25%/W4 56%/W5 49% (non-standard dominates W1-W3). No design changes. ===

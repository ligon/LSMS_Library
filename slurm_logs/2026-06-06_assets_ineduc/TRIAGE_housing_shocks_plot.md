# Triage matrix: housing / shocks / plot_features (2026-06-06)

Lock-free parallel recon (17 per-country Explore agents, get_dataframe only).
Legend: ✅ implementable · 🟡 implementable-with-caveat · ❌ absent ·
(*) verdict INFERRED (DVC pull failed) — MUST confirm before building.

| Country | housing | shocks | plot_features |
|---|---|---|---|
| Albania | ✅ 2005/08/12 (dwellingA_cl / Modul_13A) | ✅ **2012 only** (Modul_6D) | ✅ 2005+ (agric/part1_roster_a, p1a_q*) |
| Azerbaijan | ✅ A02A/A02B (rooms/utils/tenure) | ❌ (A09A is ag income, not coping) | 🟡 A06A plots but **no Area/GPS** (tenure only) |
| Cambodia | ✅ hh_sec_4 (rich) | ✅ hh_sec_11 + hh_sec_12 (coping) | ✅ hh_sec_8 (area/tenure/use; no GPS) |
| China | ✅ S02 (*) | ✅ S07 family (*) | ✅ S05–S10 (*) | (* all inferred — no pull) |
| GhanaLSS | ❌ (no dwelling module; CONTENTS.org) | ❌ (documented absent, all waves) | ✅ Section 10 (sec10A/B/C/D, script) |
| Guatemala | ✅ ECV01H01 p01a* (*) | ❌ (ENCOVI, no shocks module) | ❌ (no parcel roster) |
| Guyana | ✅ HHCHAR (bldg/hous) | 🟡 **suspect** — agent said DRBLS.dta (=durables?); VERIFY | ❌ (enterprise-level ag, no plot roster) |
| India | ✅ SECT03AB (v03a*) | ❌ (no shocks roster) | ❌ (land holdings = dwellers, not plot chars) |
| Iraq | ✅ both (2007ihses03 / 2012ihses04) | ✅ **2012 only** (2012ihses20_shocks, 23 shocks) | ❌ (ag aggregated to HH, no plot_id) |
| Kazakhstan | ✅ KZ96HSG (roof/floor/area/tenure) | ❌ (pre-ISA) | ✅ KZ96AGR1/2 (**WIDE reshape**) |
| Kosovo | ✅ DWELLING (no Roof/Floor; rooms/tenure/elec) | ❌ (post-conflict ad-hoc) | ✅ OPLAND (area/tenure/GPS, rich) |
| Liberia | ✅ sect19 (roof/floor/walls/water/toilet) | ✅ sect17 (14 shocks + coping) | ✅ sect10 (area/tenure; GPS 98% NaN) |
| Pakistan | ✅ F02A/B/C (walls/floor/roof/water/toilet) | ❌ (PIHS 1991, no coping module) | 🟡 F09 crop/season (**crop-level, not parcel**) |
| Serbia | ✅ household.dta bo* (Water/Toilet/Rooms; no Roof/Floor) | ❌ (standard LSMS, no shocks) | ✅ household.dta ag3_* (**WIDE reshape**) |
| South Africa | ✅ S4_HSV1+S3_HSV2 (roof/floor/walls/water/toilet codes) | ❌ (early-90s, no shocks) | ❌ (crop-level/HH-level land, no plot roster) |
| Tajikistan | ❌ (no dwelling module in downloaded waves) | ❌ (no coping module) | 🟡 2007 only (r1m12a*, manual harmonization) |
| Timor-Leste | ✅ 2001 S02A + 2007-08 hhold (walls/roof/floor) | ✅ **2001 only** (S08A) | ✅ **2007-08 only** (plot.dta, area/tenure/irrig) |

## Counts
- **housing**: 15 ✅ (incl. 2 inferred China*/Guatemala*), 2 ❌ (GhanaLSS, Tajikistan)
- **shocks**: 6 ✅ (Albania-12, Cambodia, China*, Iraq-12, Liberia, Timor-2001) + 1 suspect (Guyana); 10 ❌
- **plot_features**: 11 ✅/🟡 (Albania, Cambodia, China*, GhanaLSS, Kazakhstan, Kosovo, Liberia, Serbia, Timor-2007 + caveat Azerbaijan/Pakistan/Tajikistan); 6 ❌ (Guatemala, Guyana, India, Iraq, South Africa)

## Plan
Fill program ≈ 32 cells + ≈16 ABSENT to document. Run in feature batches:
1. **housing** (~15) — cleanest/broadest. Per-country data_scheme + data_info (+ mapping.py
   for material code→name where needed). Confirm China/Guatemala first (inferred).
2. **shocks** (~6) — Iraq-2012, Cambodia, Liberia, Albania-2012, Timor-2001, China*; verify Guyana.
   Document the 10 ABSENT (pre-ISA/non-ISA surveys) + consolidated issue.
3. **plot_features** (~11) — several need WIDE→long reshape (Kazakhstan/Serbia) or multi-subsection
   scripts (GhanaLSS) or are single-wave (Tajikistan-2007, Timor-2007). Caveats: Azerbaijan (no Area),
   Pakistan (crop-level not parcel — likely document instead). Document the 6 ABSENT.
4. **Structural-N/A**: Armenia, Nepal (no microdata); EthiopiaRHS, Serbia-and-Montenegro (legacy).

Confidence: spot-checked per country via get_dataframe EXCEPT China & Guatemala (inferred; re-verify).

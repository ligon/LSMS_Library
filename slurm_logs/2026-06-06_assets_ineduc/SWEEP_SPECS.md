# Matrix-fill sweep — exact specs for remaining cells (2026-06-06)

All recon done lock-free via get_dataframe (parallel, no DVC-lock issues).
Every cell below is precisely specified; implementation is mechanical.

## DONE & committed (not yet pushed)
- Branch `feature/individual-education-fill-2026-06-06`: Serbia, Guyana, Iraq(×2 waves) — all is_this_feature_sane.ok, 0% NaN.
- Branch `feature/interview-date-fill-2026-06-06`: Liberia (sect1_public.dta, Int_t=interview_date1, clean datetime64).
- Already merged earlier (PR #333): Guatemala assets+ind_ed, China/Azerbaijan/Cambodia ind_ed.

## interview_date helpers — NEED a per-wave `_/mapping.py` with `def Int_t(value)`
Pattern (copy GhanaLSS/2016-17/_/mapping.py): myvars `Int_t: [cols]`; mapping.py
`Int_t(value)` reads value.iloc[...] per row, returns a datetime (pd.NaT on missing).
data_scheme block: `interview_date: {index: (t,i), Int_t: datetime}`. Do NOT emit v/date.

- **Albania** (m0_date int YYYYMMDD; single-col parse: pd.to_datetime(str(int(v)),format='%Y%m%d')):
  - 2002: file ../Data/metadata_cl.dta, i: hh
  - 2004: file ../Data/w3_hh_basic.dta, i: chid
  - 2005: file ../Data/identification_cl.dta, i: m0_q01   ## NOTE: 2005 ALREADY has a mapping.py — append Int_t, don't overwrite
  - (2003/2008/2012 lack a usable date — skip)
- **Azerbaijan** 1995: file ../Data/A00.dta, i: [ppid,hid], Int_t: [dayint,moint,yrint]; yrint is 2-digit (95 -> 1995). ~99.9% coverage.
- **South Africa** 1993: file ../Data/S4_HDEF.dta, i: hhid, Int_t: [year1,month1,day1]; year1 is offset from 1900 (1900+year1). 100% coverage.
- **Serbia** 2007: file ../Data/domacinstva.dta, i: [opstina,popkrug,dom], Int_t: [dana,mesa,goda]. 100% coverage.
- Guatemala: recon mapped day=thogar — DUBIOUS (likely HH size). RE-VERIFY before wiring.
- China, Kazakhstan: NO household-level date field anywhere — NOT implementable.

## individual_education remaining — IMPLEMENTABLE but multi-round/script
- **Tanzania** (HH_SEC_C education module, attainment numeric years; 0% orphans all waves):
  - 2008-15 (multi-round folder): file upd4_hh_c.dta, i: r_hhid, pid: UPI, col hc_04 — use multi-round script pattern (.claude/skills/multi-round-waves.md)
  - 2019-20: file HH_SEC_C.dta, i: sdd_hhid, pid: sdd_indid, col hh_c04
  - 2020-21: file hh_sec_c.dta, i: y5_hhid, pid: indidy5, col hh_c04
- **Nigeria** (GHS pp-ph; sect2_harvest, i: hhid, pid: indiv, col s2q3; ~3% orphans):
  - Wave 5 (2023-24) VERIFIED: Post Harvest Wave 5/Household/sect2_harvestw5.dta
  - Waves 1-4 UNVERIFIED (DVC not pulled) — confirm sect2_harvestw{1-4}.dta before wiring. pp-ph script pattern (.claude/skills/add-feature/pp-ph/).
- Pakistan: only primgr (primary grade) — misleading as attainment. DEFER (no highest-level col).

## assets remaining — mostly thin; user opted to build China + Quantity-only
- **China** (BEST: real Qty/Value/Age): S10A2.DTA (i: hid, j: s10aln, Qty s10a07, Value s10a13, Age from s10a10y/m) + S10B2.DTA (j: s10bln, Qty s10b06, Value s10b14) — CONCAT both modules into one (t,i,j) -> SCRIPT. 42%/27% coverage.
- **South Africa** 1993 (Quantity-only, clean LONG): M2_NFS4.dta, i: hhid, j: dura_c, Quantity: dura_n (no Value; 10 rows dura_n<=0 to clean). YAML.
- **Azerbaijan** 1995 (Quantity-only, needs filter): A08.dta, i: [ppid,hid], j: durid, filter owndur==1 -> Quantity=1; Age=yrdur (1995-yrdur); NO Value. SCRIPT (ownership filter).
- India (WIDE 5 unlabeled slots a-e, needs codebook), Kazakhstan (WIDE unlabeled, 13% cov), Cambodia (Value is ordinal 1-11 not currency, binary qty) — LOW QUALITY, recommend defer/document.
- GhanaLSS (script, multi-file), Tajikistan (opaque, needs codebook) — defer (assessed earlier).

## cluster_features (foundational) — NOT yet recon'd
- Iraq, Timor-Leste lack cluster_features (they have sample/v). Recon whether a cluster
  Region/Rural table is constructible. (sample 33/34 done; only Armenia gap = no data.)

# RECON RECIPE: individual_education coverage (verified) — 2026-06-04
# Reference declarer: Mali. Canonical individual_education index (t,v,i,pid); single column "Educational Attainment" (str).
# Mali YAML pattern: file + idxvars{v:grappe, i:[grappe,menage], pid:[grappe,menage,numind]} + myvars{Educational Attainment: educ_hi}.
# v is joined from sample() at API time — do NOT bake in. All YAML-path (single source file per wave).
# HARMONIZATION DEFERRED: no canonical spellings dict for Educational Attainment exists; reference declarers pass survey
#   labels through. First cut = emit survey labels verbatim (per-wave inline mapping: code->label). Cross-country coarse
#   vocab (None/Primary/Secondary/Vocational/Tertiary/Postgrad) is a deferred Phase-2 task (~98 spellings).

## SET A (verified: columns + value labels read; ids confirmed)
Ethiopia 5/5: sect2_hh_w{1..5}.dta. attainment W1-3 hh_s2q05 / W4-5 s2q06 (NOT the *currently-attending* q08/q09). i=household_id, pid=individual_id (unsuffixed pair). 30-code grade vocab; W4/W5 labels uppercased+code-prefixed.
GhanaLSS 4-5 (not 1987-88/1988-89): SEC2A.DTA / parta/sec2a.dta / PARTA/SEC2a.dta / g7sec2.dta. attainment s2aq2 (highest grade; prefer over s2aq3 qualification whose scheme changed). i=[clust,nh], pid=pid (2012-13 uppercase PID/HID). per-wave mapping differs.
Iraq 2/2: 2007ihses04_education.dta (q0406) / 2012ihses05_education.dta (q0503). i=xhhkey/hh, pid=xpers/idcode. 13/12-code diploma vocab; the 2 waves use different label spellings (harmonize between them).
Albania 3/7 (2005,2008,2012; NOT 2002 which is a facility file; 1996/2003/2004 none): educationB_cl.dta / Modul_2B_education.sav. attainment m2b_q04. person key m2b_q00 (2005; NOT m2b_q0a respondent), id (2008), idcode (2012). 2012 vocab differs from 2005/2008. .sav handled by get_dataframe.
Kosovo 1/1: EDUC.dta. attainment s04_q4b (level; NOT s04_q4a numeric grade). person key s04_q00 (NOT s04_q0a). 6-code vocab, labels TRUNCATED to 8 chars in .dta — mapping keys must use truncated forms.
Guyana 1/1 BLOCKED: EDUCN.dta, attainment HS but HS has NO value labels (bare numeric) — needs GLSS 1992 codebook to decode. DEFER until codebook sourced.

## SET B (verified). KEY: Educational Attainment is FREE-TEXT (no enforced vocab; Uganda emits "Completed P.7" etc.)
## -> NO harmonize_education table needed; just optional code->label decoding so values are readable. Declare only (t,i,pid).
South Africa 1993: ROSTER file Data/M8_HROST.dta. attainment educ_c (codes 0-19; null sentinels -1/-3/-4). i=hhid, pid=pcode, v=clustnum. 100%. NOT M5_EDUC/S1_EDUC3 (facility-level). manual code->label map.
Tajikistan 1999: SSEC3.DTA. attainment s3q2 (codes 0-9; labels stripped -> manual map). i=[pop_pt,hhid], pid=iid, v=pop_pt. 100%.
Tajikistan 2003: module3.dta. attainment m3bq5 (ASSUMED by parallel; labels stripped -> CONFIRM col + manual map). i=hhid, pid=hhlid. 100%.
Tajikistan 2007: r1m3b.dta. attainment m3bq5 (CLEAN 8-level diploma labels). i=hhid, pid=memid. 100%.
Tajikistan 2009: m3b.dta. attainment M3BQ4 (clean, uppercase). i=HHID, pid=MEMID. 100%.
  (Tajik m2* = migration NOT education; m3a = preschool; education attainment = m3b.)
Kazakhstan 1996: KZ96EDU_PUF.dta. attainment bw003_ (6 levels, labels truncated 8ch -> hand-expand). i=rn, pid=personnr. 100%.
India 1997-98: ROSTER Data/SECT01A.DTA. attainment v01a05 (string, truncated; already in household_roster). i=hhcode (NOT i:hh -- roster's i:hh is a latent bug resolving 32/2251!), pid=idcode, v=village. ~100%.
Liberia 2018-19: ROSTER Data/Household/sect2_public.dta. attainment S2_14 (CLEAN labels grades+AAD/U1-U5). i=hhid, pid=member_id. 100%. (coverage hint sect5_public was WRONG.)

## No hard blockers in set B. Lowest effort: Liberia, Tajik 2007/2009, India. Need manual code->label map: SAfrica, Tajik 1999/2003, Kazakhstan. Confirm Tajik-2003 m3bq5.

## Implementation: pure-YAML per wave (assets block in {wave}/_/data_info.yml + register in _/data_scheme.yml).
## Verify via REAL framework build (worktree-pinned venv, cold cache, neutral CWD) per REALBUILD_PROTOCOL.md.

# RECON RECIPE: interview_date coverage (verified) — 2026-06-04
# Reference declarer: Nigeria. Canonical interview_date index (t,v,i); column Int_t (datetime; str also accepted).
# Reference build idiom: countries/Uganda/<wave>/_/interview_date.py: Int_t = pd.to_datetime(df[[year,month,day]]).
# v is joined from sample() at API time, BUT every target's date file HAS the cluster col -> can populate v (existing parquets lack it).
# Most targets need a SMALL _/interview_date.py wave script (2-digit-year normalization, missing-year waves, YYYYMMDD ints) — not pure-YAML.
# ALL verified (files loaded, labels read, i-overlap vs live Country.sample()).

## IMPLEMENTABLE (8 waves / 7 countries)
Iraq 2006-07: 2007ihses00_cover_page.dta. date q0035d/m/y (visit 1; 4-digit yr). i=xhhkey, v=xcluster. 100%.
Iraq 2012:    2012ihses00_cover_page.dta. date q00_35_1d/m/y (visit 1; prefer over q00_31 interviewer date). i=questid, v=cluster. 100%.
India 1997-98: SECT00.DTA. intdate(day)/intmonth/intyear(2-digit 98->1998). i=hhcode, v=village. 100%.
Pakistan 1991: F00A.DTA. dayi1/moi1/yri1 (2-digit 91->1991; use *i1 not return-visit *r/*i2). i=hid, v=clust. 100%.
Tajikistan 1999: SSEC1.DTA. date_day/mth/yr (99->1999). i=[pop_pt,hhid], v=pop_pt. DEDUP to HH (file is per-person). 100%.
Tajikistan 2003: interview.dta. day_in/month_in — NO YEAR -> hardcode 2003. i=hhid, v=tlss_psu. 100%.
Tajikistan 2007: r1m0.dta. dateday/datemont — NO YEAR -> hardcode 2007. i=hhid. covers 4644/4860 (dropna rest). 96%.
Tajikistan 2009: m0.dta. HH4_D/M/Y. i=HHID. 100%.
Cambodia 2019-20: hh_sec_1.dta. intvw_day/month/year (STRING -> cast int). i=HHID, v=s01q01. 100%.
Guyana 1992: COVERN.dta. DDE/MDE/YDE. i=[ED,HH] (hyphen composite), v=ED. 100%. (YDE digit-width UNVERIFIED — confirm in build.)
Kosovo 2000: ID.dta. s0i_dat0 = YYYYMMDD int (e.g. 20001006) -> to_datetime(format='%Y%m%d'). i=hhid, v=psu. 100%.

## BLOCKED: China — S00.DTA and all sections have NO date column (exhaustive scan). No source interview date. Gap.

## DECLARED-BUT-UNCACHED (free rebuilds, not recon targets): GhanaLSS (Int_t datetime), Malawi (index has extra 'visit' level, Int_t str). Nepal: no data on disk.

## Implementation: small _/interview_date.py per wave (assemble/normalize date -> Int_t), register interview_date in _/data_scheme.yml (index (t,i) or (t,v,i); Int_t datetime; materialize: make). Verify via REAL framework build (REALBUILD_PROTOCOL.md).

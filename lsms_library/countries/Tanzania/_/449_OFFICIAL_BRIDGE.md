# Official W1–W3 geovariables: native keys and the UPD boundary (GH #449)

Documentation-only investigation at `c8e378920968fde414767855186e64f5e36f54bb`,
2026-11-10. The eight geovariables members are already held by #1514; nothing
here registers a table, chooses a version/grain, or creates household links.

The UPD **does document a semantic household-key bridge**. In the already
DVC-managed `2008-15/Documentation/NPS_UPD_Basic_Information_Document.pdf`, PDF
page 12, “Naming Convention for r_hhid”, the producer identifies original
`hhid` (R1), `y2_hhid` (R2), `y3_hhid` (R3), and `y4_hhid` (R4) as the variables
condensed into UPD `r_hhid`, distinguishable with `round`. This is an explicit
producer statement, beyond agreement of column names or geography. It is not
an instruction to use `UPHI`, canonical `i`, or sampling `v` as that source key.

Custody: held PDF MD5 `38a095404848139cacbb4244cef24626`, SHA256
`13832fd7d25894e3ad306db3fe7aef3530fbe0431a39e4b175f3168c58bf71c8`,
857,525 bytes. This investigation authenticated and reused the closed #587
text extraction for pages 6, 7 and 12; it did not reparse the PDF. The page-12
text SHA256 is `58e0732131968d2442a2e57a39ee6743529d554cb76f7e55838e17a8e48ad237`.
Current official WB3814 resource48979 was downloaded only for a byte comparison:
its bytes, MD5 and SHA256 exactly reproduce that held document and closed text
parent. It was not reparsed. The managed document is the existing archive of
this bridge instruction;
no duplicate document or hand-built DVC pointer is needed.

## What official metadata and code add

The [official producer v2.0 code archive](https://github.com/lsms-worldbank/LSMS-ISA-harmonised-dataset-on-agricultural-productivity-and-welfare/releases/download/v2.0/Reproduction_v2.zip)
was first inspected from the held reference copy, then reproduced byte-for-byte
from the public asset: 426,082 bytes, SHA256
`f09c704695c110551f6bdce95f487faec131ba3b938471c4d4b19832837c4660`.
Only selected Tanzania program text was inspected; no Stata program ran and
no harmonised data asset was read or downloaded. The archive corroborates
**native extraction**, not the release inputs used to construct UPD.

| Original round | Official cleaner in `Reproduction_v2/Code/Cleaning_code/` | Native household source/key | Native plot source/key |
|---|---|---|---|
| R1 | `TZA_NPS1.do`, globals 12–34; geo block 900–948 | `HH.Geovariables_Y1.dta` / `hhid` | `Plot.Geovariables_Y1_revised2.dta` / `hhid, plotnum` |
| R2 | `TZA_NPS2.do`, globals 12–38; geo block 928–978 | `HH.Geovariables_Y2.dta` / `y2_hhid` | `Plot.Geovariables_Y2.dta` / `y2_hhid, plotnum` |
| R3 | `TZA_NPS3.do`, globals 12–38; geo block 1096–1145 | `HouseholdGeovars_Y3.dta` / `y3_hhid` | `PlotGeovars_Y3.dta` / `y3_hhid, plotnum` |

These blocks retain the native household key while reading distances/terrain
and build a plot key from household plus plot number. They do not read
`upd4_hh_a.dta` or implement its `r_hhid` construction. Their generic ES3
header is not the release identity; archive path and byte seals identify them.
Their `duplicates drop`, derived soil index and household-to-plot reuse are
not adopted as library policy.

Official DDI exports, retrieved 2026-11-10 (metadata counts only; no new data
measurements):

- [WB76 DDI](https://microdata.worldbank.org/index.php/metadata/export/76/ddi):
  F27 `HH.Geovariables_Y1`, V926 `hhid`, 2,990 cases / 49 variables.
  Its version notes identify a Version-02 revision of this file and addition
  of community data in Version 03. This is consistent with the held ZIP's
  `v02_M` filename under the `v03_M` catalog, but does not authenticate which
  bytes entered UPD.
- [WB1050 DDI](https://microdata.worldbank.org/index.php/metadata/export/1050/ddi):
  F189 `HH.Geovariables_Y2`, V4101 `y2_hhid`, 3,917 / 50; F120 plot file,
  V2789 `y2_hhid` and V2791 `plotnum`, 4,724 / 4. Version 02 revised the
  household file; Version 03 added `comsec_cc`. The separately held
  `TZY2.HH.Geovariables.dta` and `TZY2.PlotGeovaribales.dta` have no file
  descriptions in this export. The code names the first pair; neither fact
  selects it over the second pair or explains the seven-household difference.
- [WB2252 DDI](https://microdata.worldbank.org/index.php/metadata/export/2252/ddi):
  F77 household file, V2075 `y3_hhid`, 4,988 / 50; F79 plot file,
  V2132 `y3_hhid` and V2133 `plotnum`, 5,453 / 6. F34's cover key is V1067
  `y3_hhid`. The catalog denotes `.NSDstat` representations, not a checksum
  assertion about held Stata members.
- [WB3814 DDI](https://microdata.worldbank.org/index.php/metadata/export/3814/ddi):
  F1 `upd4_hh_a.dta`, V2 `UPHI`, V3 `round`, V4 `r_hhid`. V4's label
  names y1/y2/y3/y4 household IDs. The BID's explicit R1 `hhid` wording is
  more specific than this label. A DDI label alone would not establish a bridge.

DDI labels distinguish source units: household `dist01/02/03` are kilometres,
`soil01` metres, and `soil02` percent in these three described household files.
`dist03` is nearest “Market” in W1 and nearest “Agricultural Market” in W2/W3;
the code's common `dist_market` name does not erase that distinction. Plot
`dist01` means household distance in the W1/W2 code, whereas W3 uses `plot01`.
These are source mappings to retain, not a curated covariate/default choice.

## Wave 2 producer-catalogue cross-check and plot coverage

The [Tanzania National Data Archive DDI](https://microdata.nbs.go.tz/index.php/metadata/export/29/ddi)
is a distinct metadata record, `TZA-NBS-NPS-2010-V01`, not a mirror of
`TZA_2010_NPS-R2_v03_M`. Its [household file card F62](https://microdata.nbs.go.tz/index.php/catalog/29/data-dictionary/F62?file_name=HH)
lists `HH` with 49 fields. The [World Bank household card F189](https://microdata.worldbank.org/catalog/1050/data-dictionary/F189)
lists `HH.Geovariables_Y2` with 3,917 cases and 50 fields. The catalogues share
48 field names, but several names do not carry the same meaning.

| Published distance description (km) | NBS `HH` | World Bank `HH.Geovariables_Y2` |
| --- | --- | --- |
| Nearest major road | `dist02` | `dist01` |
| Nearest population centre above 20,000 | `dist03` | `dist02` |
| Nearest market (NBS), nearest agricultural market (World Bank) | `dist04` | `dist03` |
| Nearest border crossing | `dist05` | `dist04` |
| Headquarters of the district of residence | `dist06` | `dist05` |
| Distance from Year 1 location | Not listed | `dist_Y1Y2` |

This comparison illustrates why a shared `dist02`–`dist05` name does not
establish a shared field. The market wording is also different. In addition,
`land01` and `land02` exchange the majority land-cover class and the
percentage of agriculture in an approximately 1 km buffer. Use the
release-specific label and unit if a member is later admitted. The national
DDI's `caseQnty` is zero for all 94 files, so it reports no usable case counts
for this comparison. The national `HH` card matches neither the documented
50-field member nor the historically measured 43-field alternate by field
count. The separate national F94 `TZY2.HH` card has 41 fields and no distance
variables. It is not a description of the alternate geovariables just because
the names begin alike. The [Wave 2 BID, PDF p. 7](https://microdata.worldbank.org/catalog/1050/download/30536#page=7)
does name the household geovariables with the base name
`TZY2.HH.Geovariables`. The appendix on PDF p. 36 uses the more generic
`HH.Geovariables`. The matching BID name documents the alternate at the
level of file naming, not its checksum, coverage or place in the revision
sequence. The current World Bank dictionary and the official v2.0 cleaner
are still the two documented authorities for the named WB1050 pair.

The national [`Plot` card F24](https://microdata.nbs.go.tz/index.php/catalog/29/data-dictionary/F24?file_name=Plot)
and the World Bank [`Plot.Geovariables_Y2` card F120](https://microdata.worldbank.org/catalog/1050/data-dictionary/F120)
name the same four fields. Both label `dist01` as plot-to-household distance
in kilometres. This supports the concept, not a file or revision match. In
the NBS [Wave 2 BID, PDF p. 36](https://microdata.worldbank.org/catalog/1050/download/30536#page=36)
and [Wave 3 BID, PDF p. 37](https://www.nbs.go.tz/nbs/takwimu/Statistical_Methods_and_Standards/TZNPS_2012-2013_BID-DEC2014.pdf#page=37),
plot geovariables cover rainy-season plots owned or cultivated by the
household and visited for GPS area measurement, not every plot on the
roster. The native key is `(y2_hhid, plotnum)` or `(y3_hhid, plotnum)`
respectively. Wave 2 documents distance to the household as the only
plot covariate. Wave 3 also documents landscape covariates. These statements
describe intended coverage, not the disposition of any missing key.

This document check does not choose the W2 pair or explain the seven-household
difference in the already recorded WB1050 member counts. Ask the producer
which archive member or checksum corresponds to the national `HH` and `Plot`
cards, whether either WB1050 alternate was superseded, and what the respective
revision changed. The UPD input manifest and key construction are a separate
dependency. No native payload or coordinate values were read for this check,
and the extractor, exposed grain and covariates remain undecided.

## Exact boundary still held

BID12 establishes key *meaning*. It does not supply the exact input-release
manifest or construction/recode program for `upd4_hh_a.dta` in
`TZA_2008-2014_NPS-UPD_v01_M`. The held BID has a 2019 cover, while the
catalog-listed BID is named `nps_upq_basic_information_document.pdf` and dated
2018 in the prior closed DDI witness. Current WB3814 DDI production is October
2020 and its study version-date field is 2019-01-01. The current related-materials
page instead names `NPS UPD Basic Information Document.pdf`; resource48979
reproduces the held 2019-cover PDF byte-for-byte. Thus the current public BID
is bound to the managed bridge instruction; the older 2018 DDI description
remains a distinct metadata record. The original raw-input release manifest
is still not supplied by that byte identity. BID6–7's
general response-code recoding warning does not negate BID12's specific
household-key statement, nor does it certify preservation for every release.

The [inspected WB3814 related-materials page](https://microdata.worldbank.org/index.php/catalog/3814/related-materials)
links the survey instrument (48978) and BID (48979), with no construction
program listed in that page. This is a bounded observation, not an assertion
that a producer program does not exist. Six metadata requests and three exact
public resources were used; discovery stopped at that cap. The W1 revised
plot file and both W2 alternate members are already held, so reacquiring raw
releases would not answer this documentary question.

**Precise next source request to NBS/LSMS, for Root to send:** the producer
program and input manifest that generated `upd4_hh_a.dta` / assigned `r_hhid`
for rounds 1–3 in UPD_v01_M, identifying original round catalog IDNOs and
member checksums (or an attributable statement that the corresponding
standalone keys were preserved). Ask additionally for the revision/supersession
relationship of `HH.Geovariables_Y2.dta` versus `TZY2.HH.Geovariables.dta` and
`Plot.Geovariables_Y2.dta` versus `TZY2.PlotGeovaribales.dta` in the held
WB1050 ZIP MD5 `a3e68f9a3e4f2bb307c378eaff49d18c`. No official filename for
that UPD construction program was found; its output, release and required
assignment block specify the missing operand without inventing a filename.
No producer contact occurred.

## Minimal conditional implementation plan

1. Preserve this already-managed BID instruction and current source seals.
   Archive any subsequently supplied producer manifest/program through the
   existing document-only managed workflow: targeted `push_to_cache_batch`,
   per-document SOURCE/custody, then fresh private read-only `get_data_file`
   retrieval. Keep primary UPD SOURCE and CATALOG_COVERS semantics intact.
   The bridge instruction is already managed and now byte-bound to the current
   official resource; no new managed document was needed here.
2. Root resolves the release binding and W2 variant before any native producer
   is registered. A source manifest must name each admitted existing sidecar
   and the original key, module, source units and logical round. Do not import
   #450's harmonised dataset, lineage IDs or version decision into #449.
3. After a separate grain/covariate decision, draft one minimal native extractor
   from an admitted existing member. Keep source fields and full source key
   available; household and plot sources remain distinct. Preserve literal
   measurement concepts, missingness and units. Exclude coordinates. No cluster
   projection, averaging, dedup selection or new panel mapping is implicit.
4. A separate bridge stage uses the producer-defined original-key-to-
   `(round, r_hhid)` relationship **before** the existing canonical walk. It
   must retain unmatched/ambiguous source outcomes for review, rather than
   drop, prefix-match, fan out through UPHI or resolve by geography. Only
   after admission does the existing committed ID walk apply exactly once;
   sampling `v` remains owned by `sample()`.
5. Freeze that source/test selection before any separately authorized finite
   private source experiment. Invented controls should cover round-qualified
   keys (especially reused R3/R4 strings), UPHI back-casting fan-out, missing
   and duplicate keys, unmatched records and unit preservation. No experiment,
   test, new link or runtime API was executed in this documentation task.

The country note's historical “household files link ... almost whole” describes
measured overlap, not release authority. Its cluster-constant framing is not
proof that current-location geocovariates are constant on origin sampling `v`.
WAITING #114, #1441 original-cover codes, #587 instrument fielding, #1497
continuation/fallback, weights and Extended/Refresh choices remain separate.

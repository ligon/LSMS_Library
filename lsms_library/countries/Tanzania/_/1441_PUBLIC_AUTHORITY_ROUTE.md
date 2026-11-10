# #1441: original NPS district-code authority found; UPD bridge remains open

Bounded public-document investigation, 2026-11-10 UTC, source
`4381d701ffb06ab447a37c66362119d1c1e23009`.
**The original R1 and R2 geographical code cards are now authenticated and
transcribed. Their export/recode relationship to UPD `ha_01_1`/`ha_02_1` is not
established.** No served map, label, key, weight or default changes.

## Scope and prior authority

The initial public-HTML-only route identified exact resources. Root then
explicitly extended the grant in the absolute `MESSAGES_TO_AGENT.txt` channel
for original household XLS WB76/11558 and PDF WB1050/30525, plus optional
R2 manual WB1050/30537 only if needed. The manual was not acquired: the
questionnaire itself supplied the back code card. This extension allowed
public-document acquisition and existing parser imports, not microdata,
LSMS/runtime imports, source providers, Make, tests, cache mutation or DVC push.
The private evidence packet preserves the steering and separate phase records.

Personally read full Tanzania `_/CONTENTS.org`, including LOGBOOK/TODO/WAITING,
AGENTS/CLAUDE, prior-art skill/STANDING, sample and country design guidance,
current country ledgers and the closed v3 report at final
`9248cf225634dc750ec2ff9cfa53188417557634`. The supplied fresh #1441 full body
and all four comments were read from v3's `issue-1441-fresh.json` (retrieved
2026-11-10 01:36:12 UTC), without a new GitHub call.

Actual relevant idiosyncrasy: `UPHI` backcasts the Round-4 household into prior
rounds, while `r_hhid` identifies that round's interviewed household. Four
logical rounds share `2008-15/`; Extended2019 and Refresh2020 are distinct.
WAITING #114 remains live. These facts are not dictionary authority.

#1488/#1498's registered R3-resident plurality and named digit exceptions
(`4115002`, `10506001`) remain approved; `19302076` remains unresolved under
that ruling. Uganda #1478/#1534 stays separate. Country `District` only
respells names; `upd_district_code` is the later-round construction.

The historical `reports/tanzania-1441/REPORT.md:39` says “Derivation: none
registered.” Current `_/derivations.yml` explicitly registers
`Tanzania::cluster_features::upd-district-name-from-round-3`. This historical
contradiction is retained, not used as current authority. CONTENTS says
“Rounds 1-3 share one numbering, the geocode's”; that measured construction
premise does not itself authenticate an original-to-UPD per-field recode.

## Held UPD documents deduplicated first

Existing `2008-15/Documentation/` sidecars, not new reads of these binaries:

| Object | MD5 | Bytes | Scope |
|---|---|---:|---|
| `nps_upq_questionnaire.pdf` | `348766362d76a96f4c39674e8c254d70` | 1547757 | Retrospective UPD questionnaire |
| `NPS_UPD_Basic_Information_Document.pdf` | `38a095404848139cacbb4244cef24626` | 857525 | Held 2019-cover BID |
| `ddi-documentation-english_microdata-3814.pdf` | `0da1ef7bfca74ae4dbb2dbb62b767046` | 37771 | Lists a differently named 2018 BID |

`SOURCE.org` binds these to `TZA_2008-2014_NPS-UPD_v01_M`, WB3814. Original
WB76/WB1050 are covered entries, not whole original releases held through that
record. Recorded geovariable subsets do not imply questionnaire custody.
Closed #587's `DISPOSITION.md`/`EVIDENCE.json` already distinguishes the listed
2018 `nps_upq_basic_information_document.pdf` from the held 2019-cover BID.
Neither was substituted for an original-round instrument.

The genuinely different codebook question put to existing closed text was
whether original names/response values could simply be attached to UPD cover
fields. BID PDF pp.6–7 describes renaming and response recoding; p.6 says values
“will not necessarily correspond to each original round-specific response code
value.” Existing UPQ PDF p.7 points roster birthplace/prior-location questions
to a back code list. Neither inspected passage supplies a cover-field
`ha_01_1`/`ha_02_1` translation. Page-text hashes match the closed extraction
manifest (`CLOSED-TEXT-PROOF.json`); no UPD PDF was reparsed or new absence
search repeated. Existing parser/visual limitations remain.

## New primary public route and document custody

The [WB76 study](https://microdata.worldbank.org/index.php/catalog/76) names
`TZA_2008_NPS-R1_v03_M`; [WB1050](https://microdata.worldbank.org/index.php/catalog/1050)
names `TZA_2010_NPS-R2_v03_M`. Their version notes do not attest unchanged
geography coding through UPD. Catalogue release IDs are not document hashes.
The exact resource titles, dates and filenames came from
[WB76 related materials](https://microdata.worldbank.org/index.php/catalog/76/related-materials)
and [WB1050 related materials](https://microdata.worldbank.org/index.php/catalog/1050/related-materials),
including HTML `data-filename` attributes.

| Exact acquired original-round resource | Catalogue identity | Actual sealed bytes / MD5 / SHA256 |
|---|---|---|
| [Household Questionnaire Year 1](https://microdata.worldbank.org/catalog/76/download/11558) | `NPS Household Qx - English (Year 1).xls`; XLS; NBS; 2008-01-01; English filename | 2419200; `0beef1bba8018606e995b138edcd505f`; `ba5a6c14dd5d82fa05b3a7c66ea81dcd460f14f8249357bfb1134a9a6fd91353` |
| [Household and Individual Questionnaire](https://microdata.worldbank.org/catalog/1050/download/30525) | `NPS_Household_Qx_English_Year_2.pdf`; PDF; NBS; English; 2010-10-01 | 1097377; `ed342639649c2cfbe8a4972dd77747d8`; `9cc779b470bf57258af4b45b118f82b740323ce3d445f36e77104bbe24878654` |

No matching tracked sidecar or exact-name normal held document was identified
before acquisition. Unknown MD5s could not select a content-addressed L1 blob;
after digest authentication, exact default/shared L1 legacy and `files/md5`
paths were also checked and absent. This was not an exhaustive cache search.
Each literal official resource received one GET, HTTP200, attachment filename
matching catalogue metadata, `application/octet-stream`, and expected BIFF/PDF
magic. Durable private copies are separate regular nlink1 files, mode0400,
under `evidence/documents/`. MD5/SHA/size were authenticated before parsing and
after all reads. `PUBLIC-DOCUMENT-ACQUISITION.json`, `held-document-check.json`
and `DOCUMENT-BYTE-PROOF.json` preserve the receipts. No acquisition failed or
was retried, and no version was substituted.

The producer's [NBS32 page](https://microdata.nbs.go.tz/index.php/catalog/32/related-materials)
links `Wave1_questionnaires_English.zip` under `TZA-NBS-NPS-2008-v1.1`, resource
date2010-10-29. [NBS29](https://microdata.nbs.go.tz/index.php/catalog/29/related-materials)
links `NPS_Y2_Questionnaires.zip` and `Interviewer_Manual_NPS_Y2__English_.pdf`
under `TZA-NBS-NPS-2010-V01`. These binaries were not fetched. Matching names
are not byte equality across NBS/WB versions. The R2 manual's indexed NBS
printed p.25 back-code-list pointer was a locator only, not a parsed manual.
Exact fielded revision beyond the public-resource/date identities is unverified.

## Bounded original instrument inspection

One workbook sheet-name structure call located `COVER` and `CODES`; only those
sheets' cells were read with existing read-only xlrd2.0.2. `COVER!A13` names
Section A-1; `A16`/`A18` name Region/District, with `G15` CODE and `O15` NAME.
`CODES!A1:N43` contains the geographical card. Exact raw strings, types,
coordinates and merged ranges survive in `R1-COVER-CELLS.json` and
`R1-CODES-CELLS.json`; trailing cell padding is retained there.

One PDF structure call identified the document extent. Read only PDF p.1
(Section A-1), p.47 (back-page locator, anthropometry, not used as geography),
and p.48 (`CODES - 48`). Existing system GI/Poppler20.11.0 and Cairo rendered
p.1/p.48; both were personally visually reviewed. Cover region has two code
boxes and district one. The code card's printed codes/names match the complete
independent visual transcription and explicit character-position extraction.
No row ordinal or external code formula supplied a district code.

Failed `get_text_for_area` column projection leaked neighboring columns;
subsequent strict positional attempts exposed empty text bins and slightly
different name/number baselines. These failures remain in
`DOCUMENT-READ-FAILURES.json`; the final bounded positional check uses the same
sealed p.48, preserves empty bins, and matches every explicitly printed pair.
The cover's text extraction contains stray text not visible in its render;
both witnesses remain, with no visual-completeness claim beyond selected pages.
No whole-PDF content census or microdata operation occurred.

## Complete documentary code/name transcription

Keys below are **printed region-code suffix + printed within-region district
code, qualified by original document**. They are not an implemented UPD map.
Every pair has a name/code cell locator for R1 and PDF48 column/entry locator
for R2 in `ORIGINAL-CODE-PAIRS.json`/`.tsv`; the full source cells, PDF text,
render and independent positional check accompany them. The table strips only
R1 display padding; it does not repair spellings or add qualifiers.

| Printed region suffix / name | R1 CODES explicit district code:label | R2 PDF48 explicit district code:label |
|---|---|---|
| 01 DODOMA | 1: KONDOA; 2: MPWAPWA; 3: KONGWA; 4: DODOMA RURAL; 5: DODOMA URBAN; 6: BAHI | 1: KONDOA; 2: MPWAPWA; 3: KONGWA; 4: DODOMA RURAL; 5: DODOMA URBAN; 6: BAHI |
| 02 ARUSHA | 1: MONDULI; 2: ARUMERU; 3: ARUSHA; 4: KARATU; 5: NGORONGORO | 1: MONDULI; 2: ARUMERU; 3: ARUSHA; 4: KARATU; 5: NGORONGORO |
| 03 KILIMANJARO | 1: ROMBO; 2: MWANGA; 3: SAME; 4: MOSHI RURAL; 5: HAI; 6: MOSHI URBAN | 1: ROMBO; 2: MWANGA; 3: SAME; 4: MOSHI RURAL; 5: HAI; 6: MOSHI URBAN |
| 04 TANGA | 1: LUSHOTO; 2: KOROGWE; 3: MUHEZA; 4: TANGA; 5: PANGANI; 6: HANDENI; 7: KILINDI; 8: MKINGA | 1: LUSHOTO; 2: KOROGWE; 3: MUHEZA; 4: TANGA URBAN; 5: PANGANI; 6: HANDENI; 7: KILINDI; 8: MKINGA |
| 05 MOROGORO | 1: KILOSA; 2: MOROGORO; 3: KILOMBERO; 4: ULANGA; 5: MOROGORO; 6: MVOMERO | 1: KILOSA; 2: MOROGORO RURAL; 3: KILOMBERO; 4: ULANGA; 5: MOROGORO URBAN; 6: MVOMERO |
| 06 PWANI | 1: BAGAMOYO; 2: KIBAHA; 3: KISARAWE; 4: MKURANGA; 5: RUFIJI; 6: MAFIA | 1: BAGAMOYO; 2: KIBAHA; 3: KISARAWE; 4: MKURANGA; 5: RUFIJI; 6: MAFIA |
| 07 DAR-ES-SALAAM | 1: KINONDONI; 2: ILALA; 3: TEMEKE | 1: KINONDONI; 2: ILALA; 3: TEMEKE |
| 08 LINDI | 1: KILWA; 2: LINDI RURAL; 3: NACHINGWEA; 4: LIWALE; 5: RUANGWA; 6: LINDI URBAN | 1: KILWA; 2: LINDI RURAL; 3: NACHINGWEA; 4: LIWALE; 5: RUANGWA; 6: LINDI URBAN |
| 09 MTWARA | 1: MTWARA RURAL; 2: NEWALA; 3: MASASI; 4: TANDAHIMBA; 5: MTWARA MIKINDANI | 1: MTWARA RURAL; 2: NEWALA; 3: MASASI; 4: TANDAHIMBA; 5: MTWARA MIKINDANI |
| 10 RUVUMA | 1: TUNDURU; 2: SONGEA RURAL; 3: MBINGA; 4: SONGEA URBAN; 5: NAMTUMBO | 1: TUNDURU; 2: SONGEA RURAL; 3: MBINGA; 4: SONGEA URBAN; 5: NAMTUMBO |
| 11 IRINGA | 1: IRINGA RURAL; 2: MUFINDI; 3: MAKETE; 4: NJOMBE; 5: LUDEWA; 6: IRINGA URBAN; 7: KILOLO | 1: IRINGA RURAL; 2: MUFINDI; 3: MAKETE; 4: NJOMBE; 5: LUDEWA; 6: IRINGA URBAN; 7: KILOLO |
| 12 MBEYA | 1: CHUNYA; 2: MBEYA RURAL; 3: KYELA; 4: RUNGWE; 5: ILEJE; 6: MBOZI; 7: MBALALI; 8: MBEYA URBAN | 1: CHUNYA; 2: MBEYA RURAL; 3: KYELA; 4: RUNGWE; 5: ILEJE; 6: MBOZI; 7: MBALALI; 8: MBEYA URBAN |
| 13 SINGIDA | 1: IRAMBA; 2: SINGIDA RURAL; 3: MANYONI; 4: SINGIDA URBAN | 1: IRAMBA; 2: SINGIDA RURAL; 3: MANYONI; 4: SINGIDA URBAN |
| 14 TABORA | 1: NZEGA; 2: IGUNGA; 3: UYUI; 4: URAMBA; 5: SIKONGE; 6: TABORA URBAN | 1: NZEGA; 2: IGUNGA; 3: UYUI; 4: URAMBA; 5: SIKONGE; 6: TABORA URBAN |
| 15 RUKWA | 1: MPANDA; 2: SUMBAWANGA; 3: NKASI; 4: SUMBAWANGA URBAN | 1: MPANDA; 2: SUMBAWANGA; 3: NKASI; 4: SUMBAWANGA URBAN |
| 16 KIGOMA | 1: KIBONDO; 2: KASULU; 3: KIGOMA RURAL; 4: KIGOMA URBAN | 1: KIBONDO; 2: KASULU; 3: KIGOMA RURAL; 4: KIGOMA URBAN |
| 17 SHINYANGA | 1: BARIADI; 2: MASWA; 3: SHINYANGA RURAL; 4: KAHAMA; 5: BUKOMBE; 6: MEATU; 7: SHINYANGA URBAN; 8: KISHAPU | 1: BARIADI; 2: MASWA; 3: SHINYANGA RURAL; 4: KAHAMA; 5: BUKOMBE; 6: MEATU; 7: SHINYANGA URBAN; 8: KISHAPU |
| 18 KAGERA | 1: KARAGWE; 2: BUKOBA RURAL; 3: MULEBA; 4: BIHARAMULO; 5: NGARA; 6: BUKOBA URBAN; 7: CHATO; 8: MISENYE | 1: KARAGWE; 2: BUKOBA RURAL; 3: MULEBA; 4: BIHARAMULO; 5: NGARA; 6: BUKOBA URBAN; 7: CHATO; 8: MISENYE |
| 19 MWANZA | 1: UKEREWE; 2: MAGU; 3: NYAMAGANA; 4: KWIMBA; 5: SENGEREMA; 6: GEITA; 7: MISUNGWI; 8: ILEMELA | 1: UKEREWE; 2: MAGU; 3: NYAMAGANA; 4: KWIMBA; 5: SENGEREMA; 6: GEITA; 7: MISUNGWI; 8: ILEMELA |
| 20 MARA | 1: TARIME; 2: SENGEREMA; 3: MUSOMA RURAL; 4: BUNDA; 5: MUSOMA URBAN | 1: TARIME; 2: SENGEREMA; 3: MUSOMA RURAL; 4: BUNDA; 5: MUSOMA URBAN |
| 21 MANYARA | 1: BABATI; 2: HANANG; 3: MBULU; 4: SIMANJIRO; 5: KITETO | 1: BABATI; 2: HANANG; 3: MBULU; 4: SIMANJIRO; 5: KITETO |
| 51 KASKAZINI UNGUJA | 1: KASKAZINI ‘A’; 2: KASKAZINI ‘B’ | 1: KASKAZINI ‘A’; 2: KASKAZINI ‘B’ |
| 52 KUSINI UNGUJA | 1: KATI; 2: KUSINI | 1: KATI; 2: KUSINI |
| 53 MJINI/MAGHARIBI UNGUJA | 1: MAGHARIBI; 2: MJINI | 1: MAGHARIBI; 2: MJINI |
| 54 KASKAZINI PEMBA | 1: WETE; 2: MICHWEWENI | 1: WETE; 2: MICHWEWENI |
| 55 KUSINI PEMBA | 1: CHAKECHAKE; 2: MKOANI | 1: CHAKECHAKE; 2: MKOANI |

Literal ambiguities and differences remain part of the authority:

- R1 `CODES!M1/M5/M9/M14/M18` headings start22/23/24/25/26 but end51/52/53/54/55;
  R2 headings start and end51–55. Preserve both raw components. The independently
  retrieved R1 public region categories also use51–55; no UPD inference follows.
- R1 `A28/B28` is TANGA/code4; R2 prints TANGA URBAN. R1 `A36/B36` and `A39/B39`
  both say MOROGORO, at codes2 and5; R2 prints RURAL and URBAN respectively.
  Bare source names are not silently expanded.
- Both cards literally print URAMBA at Tabora4, SENGEREMA at Mara2 as well as
  Mwanza5, and MICHWEWENI at Kaskazini Pemba2. No geographic or spelling repair
  is inferred. Repeated names across regions do not make joint keys identical.
- Within each original card, printed region suffix/district keys are unique and
  the displayed joint key sets agree. That is a document dictionary check, not
  a new survey count, historical-boundary ruling or UPD-preservation proof.

The earlier CONTENTS statement “no document scanned lists the codes” described
its then-held search. These newly admitted original instruments extend that
scope; the original note is not rewritten or labelled stale.

## Exact remaining bridge; patch and tests UNRUN

The cards establish original-round documentary code/name authority with the
qualifications above. They do **not** establish that UPD `ha_01_1`/`ha_02_1`
retain those original cover values. The precise missing operand is an attributable
original-cover-to-UPD translation/preservation statement or exact recode for
these fields, bound to held UPD `TZA_2008-2014_NPS-UPD_v01_M` and the original
public versions. BID6–7 gives general renaming/recoding evidence, not that
field-specific bridge. A roster birthplace/prior-location card reference also
does not prove cover-field equivalence. No later round, spelling, geocode digit,
code arithmetic or geographic continuity supplies this missing operand.

No replacement patch is justified before reviewed bridge authority. Proposed
subsequent controls, all **UNRUN**: original release/round/region/district joint
identity; reused district codes across regions; duplicate/conflicting keys;
wrong-release/blank/unsupported keys; exact source spelling/ambiguous-name
preservation; recorded-name/later-round preservation; unchanged residency,
household axes/dtypes/attrs; and explicit interaction with the registered R3
and named digit branches. No `Derivation` removal or reinterpretation is adopted.

## Concrete document-only placement proposal for Root/MAIN — UNRUN

Preserve the already sealed bytes, without another download. Proposed project
paths are `Tanzania/2008-15/Documentation/NPS-R1/NPS Household Qx - English (Year 1).xls`
and `Tanzania/2008-15/Documentation/NPS-R2/NPS_Household_Qx_English_Year_2.pdf`.
Their proposed `.dvc` sidecars use the exact MD5 and byte size above, `hash: md5`
and the exact basename. Full proposed sidecar objects and per-directory
`SOURCE.org` prose are in `DOCUMENT-PLACEMENT-PROPOSAL.json`.

The proposed source notes bind each literal resource URL, catalogue release,
filename/date, retrieval receipt, language and exact MD5/SHA/size. They explicitly
say document-only custody, distinct from UPD3814 and not a holding of original
microdata. Do not change the parent `SOURCE.org` primary holding or turn
`CATALOG_COVERS` into a complete original-release holding. Root/MAIN owns any
sanctioned placement/publication and verification. No sidecar/SOURCE edit, DVC
CLI/push or shared-cache mutation ran here.

## Searched scope and verification

Primary HTML inspected: WB76/WB1050 study, related-materials, data-dictionary,
F39/F157 file pages and the original region/district variable pages; NBS32/NBS29
related-materials. The public district category pages have blank names; R1
region has names, R2 region has no category table in the retrieved HTML. This
limited metadata negative did not imply missing original code cards, which the
subsequent binary phase found. Web-tool failures and successful exact HTML GETs,
all query text and off-scope rejected search hits remain in `SEARCHES.json`,
`SEARCHED-RESOURCES.json`, `web-*.json` and `public-*-receipts.json`.

Complete source conservation is recorded in private `SOURCE-PROOF.json`:
only this new Markdown and the unique ledger change, with full CONTENTS and all
nonowned Git mode/type/blob/path objects equal to source4381. No LSMS runtime,
tests/collection, native header/record, source acquisition, Make,
GitHub operation, production map or scientific default was exercised. The only
new binary/dependency operations were the exact public-document grant above.
No fielding/nonfielding, coverage, population or statistical count was inferred.
Independent Root review/integration and the export bridge remain pending;
#1441 is not closed.

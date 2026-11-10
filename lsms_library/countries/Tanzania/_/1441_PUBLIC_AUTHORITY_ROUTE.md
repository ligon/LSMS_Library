# #1441: original NPS district-code public authority route

Bounded metadata investigation, 2026-11-10 UTC, source
`4381d701ffb06ab447a37c66362119d1c1e23009`.
**Original-round resources are now identified; a complete original joint
region/district dictionary and its UPD recoding bridge remain unestablished.**
No map or served behavior changes. Root owns any subsequent binary restoration,
authority review and integration.

## Prior art and the question still open

Read the full Tanzania `_/CONTENTS.org`, including LOGBOOK/TODO/WAITING,
AGENTS/CLAUDE, STANDING, sample and country design guidance, current country
ledgers, and closed v3 final `9248cf225634dc750ec2ff9cfa53188417557634`.
The supplied fresh #1441 body and all four comments were read from v3's
`issue-1441-fresh.json` (retrieved 2026-11-10 01:36:12 UTC); no new GitHub call.
#1478/#1534's Uganda NA ruling and Tanzania #1488/#1498 are delivered, not
requests to repeat or reopen them.

Relevant actual idiosyncrasy: `UPHI` backcasts the Round-4 household into earlier
rounds, whereas `r_hhid` identifies that round's interviewed household. Four
logical rounds share `2008-15/`; the 2019 Extended and 2020 Refresh panels are
distinct. WAITING #114 remains live. None supplies dictionary authority.

Current `upd_district_code` names cells from round-3 resident `ha_02_2`
pluralities. Country `District` only respells names. Keep both the registered
R3 construction and the two named digit exceptions (`4115002`, `10506001`);
`19302076` stays unresolved under the approved ruling. No new geographic
continuity or code-arithmetic inference is admitted.

Two statements must remain distinguished:

- The historical `reports/tanzania-1441/REPORT.md:39` says
  “Derivation: none registered.” Current `_/derivations.yml` explicitly registers
  `Tanzania::cluster_features::upd-district-name-from-round-3`. This is a
  historical-stage contradiction already recorded by v3, not new authority.
- CONTENTS says “Rounds 1-3 share one numbering, the geocode's”. That is the
  existing measured construction's premise. The held UPD BID's general recoding
  warning does not disprove it, but prevents treating it as an authenticated
  original-to-UPD identity map.

## Held documents deduplicated before the new route

These are the existing `2008-15/Documentation/` sidecar identities, not fresh
binary reads:

| Held object | Sidecar MD5 | Declared bytes | Authority scope |
|---|---|---:|---|
| `nps_upq_questionnaire.pdf` | `348766362d76a96f4c39674e8c254d70` | 1547757 | Retrospective UPD questionnaire |
| `NPS_UPD_Basic_Information_Document.pdf` | `38a095404848139cacbb4244cef24626` | 857525 | Held BID with 2019 cover |
| `ddi-documentation-english_microdata-3814.pdf` | `0da1ef7bfca74ae4dbb2dbb62b767046` | 37771 | Catalogue document listing a differently named 2018 BID |

`SOURCE.org` binds the holding to `TZA_2008-2014_NPS-UPD_v01_M`, WB3814.
WB76/WB1050 are covered original-round entries, not held original questionnaire
releases. The recorded geovariable subsets do not change documentary custody.
Tracked Tanzania documentation has none of the original resource filenames in
the restoration proposal below; this says nothing about external caches.

Closed #587's `DISPOSITION.md`/`EVIDENCE.json` under
`country-workers-refill-20261108/reports/587-UPD-documentary-disposition-20261109/`
already distinguishes the listed 2018
`nps_upq_basic_information_document.pdf` from the held 2019-cover BID. No new
UPD absence search or PDF parse was performed here.

The **different codebook question** asked of existing closed text was whether
original variable names/response values can simply be attached to UPD cover
fields. Existing BID PDF pp.6–7 says both names and response values can change
during unification; it does not provide the specific `ha_01_1`/`ha_02_1` bridge.
Existing UPQ PDF p.7 refers roster birthplace/prior-location questions to
geographical codes at the back of the questionnaire. That is a documentary
pointer, not the code list and not authority that those roster codes equal
household cover codes. The text witnesses were authenticated against the
closed extraction manifest; no binary was reread. Text extraction retains the
earlier parser/visual-completeness limitations.

## New primary public metadata findings

The original-round [WB76 catalogue](https://microdata.worldbank.org/index.php/catalog/76)
identifies `TZA_2008_NPS-R1_v03_M`; the
[WB1050 catalogue](https://microdata.worldbank.org/index.php/catalog/1050)
identifies `TZA_2010_NPS-R2_v03_M`. The catalogue version notes concern
geovariables/community additions; they do not attest an unchanged geography
codebook through UPD recoding. Study release IDs are not document version seals.

Public original file metadata identifies
[R1 `SEC_A_T_English_Labels`](https://microdata.worldbank.org/catalog/76/data-dictionary/F39?file_name=SEC_A_T_English_Labels)
and [R2 `HH_SEC_A`](https://microdata.worldbank.org/catalog/1050/data-dictionary/F157?file_name=HH_SEC_A),
with separate `region` and `district` fields. These are catalogue metadata
pages, not native headers or source records.

| Exact public variable page | What was actually displayed |
|---|---|
| [R1 district, F39/V1270](https://microdata.worldbank.org/index.php/catalog/76/variable/F39/V1270?name=district) | Category codes `1` through `8`, with blank Category cells; no region-conditioned district names in this page |
| [R1 region, F39/V1285](https://microdata.worldbank.org/index.php/catalog/76/variable/F39/V1285?name=region) | Named region categories, transcribed below |
| [R2 region, F157/V3401](https://microdata.worldbank.org/index.php/catalog/1050/variable/F157/V3401?name=region) | Overview metadata; no category table in the retrieved HTML |
| [R2 district, F157/V3402](https://microdata.worldbank.org/index.php/catalog/1050/variable/F157/V3402?name=district) | Category codes `1` through `8`, with blank Category cells; no region-conditioned district names in this page |

Complete transcription of the **displayed R1 region category labels**; this is
partial authority for the task, not a joint district dictionary:

| Code | Label | Code | Label |
|---|---|---|---|
| 1 | Dodoma | 14 | Tabora |
| 2 | Arusha | 15 | Rukwa |
| 3 | Kilimanjaro | 16 | Kigoma |
| 4 | Tanga | 17 | Shinyanga |
| 5 | Morogoro | 18 | Kagera |
| 6 | Pwani | 19 | Mwanza |
| 7 | Dar es salaam | 20 | Mara |
| 8 | Lindi | 21 | Manyara |
| 9 | Mtwara | 51 | KASKAZINI UNGUJA |
| 10 | Ruvuma | 52 | KUSINI UNGUJA |
| 11 | Iringa | 53 | MJINI/MAGHARIBI UNGUJA |
| 12 | Mbeya | 54 | KASKAZINI PEMBA |
| 13 | Singida | 55 | KUSINI PEMBA |

The web tool initially failed on the four variable links and their canonical
variants. Exact public HTML GETs then succeeded with HTTP200 and text/html,
with redirects disabled. Those failures are preserved as retrieval failures,
not upstream absence. Only code/category columns were transcribed; hosted
case/frequency/statistics fields were not used to make any survey claim.

## Exact restoration proposal for Root — entirely UNRUN

Resource identity comes from the
[WB76 related-materials page](https://microdata.worldbank.org/index.php/catalog/76/related-materials)
and [WB1050 related-materials page](https://microdata.worldbank.org/index.php/catalog/1050/related-materials),
including their HTML `data-filename` attributes. The documents' binary hashes
and exact byte sizes are unknown; the displayed sizes below are rounded
catalogue descriptions, not verified seals.

| Priority / role | Exact document resource | Known identity | Finite target for a separately authorized restoration/read |
|---|---|---|---|
| First: R2 code card | [Household and Individual Questionnaire](https://microdata.worldbank.org/catalog/1050/download/30525) | `NPS_Household_Qx_English_Year_2.pdf`; PDF, 1.05 MB; English, 2010-10-01; NBS | Section A-1 region/district fields, plus the geographical code list at the back referenced by the manual; exact PDF/printed card pages unknown |
| First: R1 original form | [Household Questionnaire Year 1](https://microdata.worldbank.org/catalog/76/download/11558) | `NPS Household Qx - English (Year 1).xls`; XLS, 2.31 MB; NBS, 2008-01-01; English filename | Household-identification Q1 region/Q2 district and any attached geographical code-list sheets/print areas. Actual sheet names/cell ranges unknown |
| Supporting R2 binding | [Enumerator Manual](https://microdata.worldbank.org/catalog/1050/download/30537) | `Interviewer_Manual_NPS_Y2__English_.pdf`; PDF, 3.38 MB; English, 2010-10-01; NBS/WB | Printed pp.17–18, Section A-1, and p.25, roster questions26/28. Map printed to PDF pages only after restoration |
| Conditional R1 binding | [Enumerator Manual – NPS 2008-09](https://microdata.worldbank.org/catalog/76/download/11562) | `NPS_Enumerator_Manual_English_2008.pdf`; PDF, 2.59 MB; NBS, 2008-01-01; English filename | Household-identification region/district instructions and any explicit code-list reference. Exact pages unknown; no invented page locator |

The R2 manual's [publicly indexed NBS passage](https://www.nbs.go.tz/nbs/takwimu/nps/Wave2/Interviewer_Manual_NPS_Y2__English_.pdf),
printed p.25, explicitly directs district/region coding to the questionnaire's
back code list. This was a search-engine text result, **not** a fetched or
parsed PDF. It makes the R2 questionnaire a specific candidate; the list's
contents, completeness and applicability to cover fields remain unverified.

Producer metadata supplies additional locators:
[NBS32 related materials](https://microdata.nbs.go.tz/index.php/catalog/32/related-materials)
links `https://www.nbs.go.tz/nbs/takwimu/nps/Wave1_questionnaires_English.zip`,
under `TZA-NBS-NPS-2008-v1.1` (resource date 2010-10-29), and
[NBS29 related materials](https://microdata.nbs.go.tz/index.php/catalog/29/related-materials)
links `https://nbs.go.tz/nbs/takwimu/nps/Wave2/NPS_Y2_Questionnaires.zip` and
`https://nbs.go.tz/nbs/takwimu/nps/Wave2/Interviewer_Manual_NPS_Y2__English_.pdf`,
under `TZA-NBS-NPS-2010-V01`. None of these binary targets was opened. Matching
filenames do not establish byte equality; preserve these release/date
differences rather than deduplicating by title. Prefer the exact WB document
resources above to an entire zip restoration.

Root can concretize private document custody first, then local+DVC placement
through sanctioned project mechanisms in a separate scope. Do not acquire a
whole wave, rewrite `SOURCE.org` holding claims, or replace the held UPD BID
with an original-round BID. Any document placement needs its own origin,
retrieval date, exact bytes/hash, language and version seal; no proposed path
here claims a document is already held.

Proposed destinations, **not created**: private staging grouped by WB76/WB1050
and resource ID, followed, only if Root admits preservation, by
`Tanzania/2008-15/Documentation/NPS-R1/<exact filename>` and
`Tanzania/2008-15/Documentation/NPS-R2/<exact filename>` with per-document
provenance and sidecars. This keeps original-round custody separate from the
UPD documents; it does not turn `CATALOG_COVERS` into a whole-release holding.
Root owns any sanctioned `get_data_file`/`push_to_cache_batch` restoration and
its local/DVC verification. None ran here.

## Search scope and exact remaining operand

Actually inspected primary HTML paths are WB76 and WB1050 study pages,
`related-materials`, `data-dictionary`, their F39/F157 file pages, the four
variable pages above, and NBS32/NBS29 `related-materials`. Query text, search
hits, failed URL variants and HTML custody receipts are in the private evidence
packet's `SEARCHES.json`, `SEARCHED-RESOURCES.json`, `web-*.json` and
`public-*-receipts.json`. Search hits outside the original rounds, including
the R4 manual and other surveys, were rejected as authority. Search results
pointing to nonprimary reposts were not adopted. No binary, login, microdata,
API, source-provider or acquisition endpoint was requested.

Thus the negative is scoped to **these original-round public variable pages**:
they did not supply named joint district keys. It is not a finding that the
original manuals/questionnaires lack a code list or that no official map exists.
No fielding, nonfielding, coverage, population or survey count was inferred.

Smallest remaining documentary operand: the R2 back-of-questionnaire code card
and the R1 form's corresponding region-conditioned district dictionary,
authenticated to their original versions, together with explicit evidence that
the relevant original cover codes map to UPD `ha_01_1`/`ha_02_1`, either unchanged
or through an exact recode. The general UPD BID warning leaves that last bridge
open even if both original cards are recovered. A birthplace/migration list,
later-round label, district spelling, geocode digit or `region*10+code` formula
cannot silently supply it.

No replacement patch is established. After reviewed authority only, a candidate
would retain release/round/region/district joint identity, preserve exact source
labels/provenance/ambiguities, refuse nonunique keys, and leave unknown keys
unfilled. Proposed invented controls: reused district codes across regions;
duplicate/conflicting keys within a release/region; wrong-release, blank and
unsupported keys; recorded-name/later-round preservation; unchanged residency,
household axes and metadata; and interaction with the registered R3 and named
digit branches. These are proposals, **UNRUN**, not implemented tests or a new
default. Source conservation is verified separately in `SOURCE-PROOF.json`.

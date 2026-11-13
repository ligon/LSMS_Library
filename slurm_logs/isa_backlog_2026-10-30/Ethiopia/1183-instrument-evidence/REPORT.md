# Ethiopia #1183: qualified official-instrument checkpoint

Documentation only, extracted onto development `824ae20cdb1a9827f2ae1a978f816c627d64e9e7`. The independent review below supports the official-document findings, including direct visual confirmation from the original held PDFs. It does not settle which otherwise hollow seed observations should remain, repair production code, or close #1183. Existing CONTENTS bytes are preserved before the new appendix. No raw data, API or test cohort was replayed for this extraction.

Frozen worker `01e6872be2905c57d49b4e26f0841db6671493c4` used public source `0b88881e9bd7e8b8838552fb4b2d4e7cabab5268`; its Ethiopia runtime/config bytes equal the extraction base. Worker REPORT SHA256 `c811d28f32e28677e1af34fb305e3a423a0355869f168ad2369196bfa09180c0`; document-bindings SHA256 `dd094fee3d4b99526faced7fb5c1dc4f4759cb127c9729ffa0e1eaa62379e06d`. Independent review SHA256 `aa7797e4aa1d401cdde32d473ba98b118cf8fc63ed3105605a522d97d1f3ce75` (reproduced verbatim below).

| Official instrument | Original PDF pages relevant here | SHA256 |
| --- | --- | --- |
| [W1 post-planting form](https://microdata.worldbank.org/catalog/2053/download/40396) | 16–17 | `fe6de0761451a0b10aa7869cdda58dc3c17a70431f815b387f7687b2583d2b7b` |
| [W1 interviewer manual](https://microdata.worldbank.org/catalog/2053/download/40404) | 71–72 | `e1fb422a6a2fb02f5743766410bd68230322b8d375d294299b1921ae775ea344` |
| [W2 post-planting form](https://microdata.worldbank.org/catalog/2247/download/40378) | 18, 20–21 | `01d603ca622f4f6b84343d28902da7e02b2c4b0e7dc52dfc3ab8371e42a36641` |
| [W2 interviewer manual](https://microdata.worldbank.org/catalog/2247/download/40389) | 75 | `1e465f0e5e3b429a2a1639c78d858e45f67d404ea070bc5c522a55d8207c2f85` |

The worker also bound [W1 DDI](https://microdata.worldbank.org/metadata/export/2053/ddi) SHA256 `26aacc5cab7429e672724024ece9552955588978d7a6b33897e0c2f44186132c` and [W2 DDI](https://microdata.worldbank.org/metadata/export/2247/ddi) SHA256 `7ac407e5777b1d1cc525e847ca891dd233596b6145109ee3edc52b56f6249eb1`. Their selected Q3 dictionaries distinguish Yes/No/Sysmiss but do not identify why released source values are missing. This reviewer authenticated selected metadata, not a fresh full-DDI reread.

The initial PDF-layout extraction omitted rotated W2 text; its failure and subsequent plain/normalized extraction remain historical evidence. The independent original-PDF raster review resolves that rendering limitation for the eight listed pages, without changing the worker's other qualifications. It does not establish that a raw blank is refusal, don't-know, structural skip or omission.

Next investigation may classify affected W1 direct-join and W2 holder/crop cases using source predicates and existing code precedents. Routine necessary investigation needs no invented permission gate; a scientific retention rule must follow the evidence, leaving unclassified cases explicit. Historical raw/served/counterfactual counts below are not newly measured, constants for a filter, or authority to drop rows.

---

# Independent Mali review: Ethiopia #1183 official instruments

Qualified support for the document findings at final `01e6872be2905c57d49b4e26f0841db6671493c4`, based on public source `0b88881e9bd7e8b8838552fb4b2d4e7cabab5268`. No production changes or new cohort acceptance are established. Keep #1183 open; missing purchase answers cannot be equated universally with observed No, and this packet does not adjudicate observation retention.

Read country CONTENTS including the live #1183/#1184 caveats, same-country decoder/caller, task ledger and full REPORT before document review. Relevant idiosyncrasy: W1 attaches field/crop purchases directly; W2–W5 aggregate holder/crop purchases and attach only to a unique plot. Preserve that restriction. CONTENTS:1488–1492 says retention is "unsettled" and diagnostics "are not a new replay or authorization to remove records." The packet is consistent with those caveats. No LOGBOOK/WAITING entry was found in this file; the unrelated FCT TODO remains.

Authenticated all 24 ordinary manifest members (including four exact official PDFs), all eight Git source bindings, and the final five-path diff: ledger/report/selected DDI/document bindings only. No runtime library or config bytes changed. REPORT SHA256 `c811d28f32e28677e1af34fb305e3a423a0355869f168ad2369196bfa09180c0`; document-bindings SHA256 `dd094fee3d4b99526faced7fb5c1dc4f4759cb127c9729ffa0e1eaa62379e06d`. Full DDI XML was not independently reread: selected authenticated metadata supports its limited dictionary claim, not causes of raw missingness.

The original worker lacked raster rendering. This review rendered eight pages directly from the authenticated held PDFs with system Poppler/Cairo, preserving their original rotation and geometry. A document-only stdlib ctypes process ran once on core38, 8 GiB, threads1, timeout60+5 seconds, exit0 in about5 seconds, without project imports. Renderer source and image hashes are retained alongside AUTHENTICATION.json. The official W2 web endpoint was also attempted and was inaccessible through the web tool; no substitute document was used.

Visual findings:

- W1 form PDF p16 visibly prints Q3 No→Q13; W1 manual PDF p72 says No→Q14. This is a real discrepancy. Both bypass Q5, neither instructs leaving Q3 itself blank for No. The manual's Q14 self-reference is also visible and should remain a recorded inconsistency.
- W2 form PDF p20 visibly prints Q3 No→Q13, matching manual p75. Traditional seed Q1→Q2 bypasses variety detail, not Q3. The anomalous household-member text in Q1a is genuinely printed; it is not an artifact of rotation-normalized text extraction. Manual p75 instead gives the improved-variety question.
- W2 p20 has seed name/code, seed type, variety/reason/purchase columns but no parcel/field-ID columns. Manual p75 explicitly says "Parcel ID, Field ID". That contradiction does not authorize field-level fanout or changing the established unique-plot attachment policy.
- W1 manual p71 seed-type Q1 code3 means permanent crop/not planted this season; W1 form p16 prints only codes1/2. W2 form p20 and manual p75 seed-type Q1 code3 means improved recycled/saved. These meanings must not be transferred across waves. This difference concerns seed type, not financing Q9 code3, whose part-savings/part-credit meaning appears in both forms.
- W2 form p18 section4 Q11 code3 explicitly goes to Q13, bypassing applied quantity Q11b. This is not a section5 Q3 skip. W1 form p17 and W2 p21 place the explicit zero instruction on up-front payment Q10, not purchase quantity Q5. W1 p17 also visibly preserves free/leftover seed routes and total quantity; nonpurchase does not imply no seed use.

The source-level defect remains technically distinct from row retention: `_yesno_bool_col` supplies an existing missing-preserving same-country precedent; `_yesno_bool` uses nullable masks, and `pqty.mask(purch == False, 0.0)` also needs a missing-safe predicate. A seed-only repair should not alter unrelated callers. The existing all-payload-missing retention filter means fixing manufactured values can remove rows unless an explicitly supported observation rule is chosen. #1184's `sum(min_count=1)` and holder/crop attachment remain intact. Historical 4,433/90 missing raw flags, 4,426/60 affected served flags, 371/three retention diagnostics and 265/187 #1184 changes were not replayed or remeasured here.

Required wording correction to the worker's final proposal: ordinary bounded source-attribution investigation is already authorized by the backlog task. Its phrase "If the human elects further investigation, separately authorize" should not create a new operational permission gate. The unresolved matter is scientific interpretation of classified source records, not permission to inspect necessary evidence. Next work should independently attribute the affected W1 direct-join and W2 holder/crop cases using native crop/type/variety, other seed responses, section4 permanent-crop routing, label collisions and unique-plot membership, retain unclassified categories explicitly, then propose a source-backed rule. Do not infer missing Q3→No, blanket crop-label retention, or deletion from these PDFs alone.

No survey payload, private frame, cache, raw source, Country/Feature API, Make, test cohort, new acquisition or public mutation was used by this reviewer. Rendering official questionnaires is document inspection, not a survey-data actor. Root owns public checkpoint/adjudication.

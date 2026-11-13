# Burkina Faso #1148: retained interview results do not yet identify exposure

Diagnosis at `f8d236694c26fe9e709c5548f02c70f74ea72c33`, 2026-10-21.
No production denominator was selected or corrected. GhanaLSS is outside this lane.

## Result

Held EMC files contain useful cover-page result fields, including accepted
interviews with no served food rows. They also contain missing coverage,
conflicting results, two extra household keys, and results inconsistent with
the official report. These are source-adjudication prerequisites to a complete
household exposure correction. Counting food rows cannot resolve them.

The assigned release baseline has **Phase I only**: `RecallRecord.exposure_days`
and `recall.to_period` are absent. Both
`Country('Burkina_Faso').food_expenditures(period='week')` and
`food_quantities(period='week')` raise an unexpected-keyword `TypeError` before
reading data. Its generated method is `country.py:6150`.
The existing `origin/fix/851-recall-period` ref at
`cf117d101187799b9f985c0686a567adb2177937` contains the later option-A code;
read via `git show`, never checked out or imported here. #851 and #1148 were
open when read. The #851 ruling ships declared exposure with disclosure and
defers household correction to #1148; its implementation remains an integration
dependency at this baseline.

## Held sources and exact status semantics

All 50 declared 2014 sidecars (48 Stata files and two PDFs) had held L1 blobs.
Every blob's MD5 matched its sidecar before a physical private copy was read.
There was no source acquisition, DVC CLI operation, or shared cache write.
There is **no missing declared sidecar** to name. No standalone household cover
dataset is named among these sidecars; retained cover variables ride in module
extracts instead. An unrecorded original cover file cannot be given an invented
filename or hash.

`Questionnaire_EMC_2014_Passage_1.pdf`, page 1, section A, A13, gives:

| Code | Source wording / qualification |
|---|---|
| 1 | Interview accepted (`Interview acceptee`, accents removed here) |
| 2 | Accepted reluctantly (`Acceptee avec reticence`) |
| 3 | Abandonment (`Abandon`); partial, not a confirmed zero ask |
| 4 | Temporary absence (`Absence temporaire`) |
| 5 | Refused (`A refuse`) |
| 6 | Other; explanation is in observations, which are not retained here |

The p2 `res_entr2` / `A13` value dictionaries agree with these categories.
The p3 `res_entr3` dictionaries add household lost at code 6 and move other to
7. `res_entr4` has no value-label dictionary in any held p4 carrier. The
report's p4 category list corroborates lost/other categories, but does not bind
their numeric codes. Treat the p4 code 6/7 interpretation as an inference from
p3 until directly confirmed. Code 9 occurs once in p1 and is not in A13.
An accepted interview is positive evidence, but the literal question does not
certify completion of every food question.

The household key is `(zd, menage)` in every retained passage status carrier,
with no null key rows. Food visit identity must still be stamped from the
filename. The country formatter is `format_id(zd)` followed by a three-digit
`format_id(menage)` (`Burkina_Faso/_/burkina_faso.py:10`); it is one-to-one over
the 10,802 retained keys. Do not apply the EHCVM `ehcvm_i` formatter to 2014.

| Passage | Broadest retained result evidence | Households | Missing p1 keys | Keys outside p1 |
|---|---|---:|---:|---:|
| 1 | p1 conso3mois / conso7jours `res_entr1` | 10,800 | 0 | 0 |
| 2 | union of p2 `res_entr2` / `A13` carriers | 10,622 | 179 | 1 |
| 3 | union of p3 `res_entr3` carriers | 10,234 | 567 | 1 |
| 4 | later p4 conso7jours `res_entr4` | 10,800 | 0 | 0 |

All food and conso3mois extracts above use `16032015`. P2 broad nonfood
`A13` carriers (revenus, transfert_recu, transfert_verse and the distinct
`entreprise_indiv1_27022105`) each cover 10,621 households; individu covers
10,619. The union has one additional explicit accepted result in conso3mois.
P3 sante and securitealimentaire each cover 10,230 households, emploi 10,228;
their union with consumption status carriers has 10,234 keys. Module coverage
is therefore not the same as cover-page coverage.

P4's **older** `conso7jours_10032015` covers 10,101 households. The later
`16032015` file covers 10,800 and includes explicit absent/refused/lost/other
results. The dated versions are not interchangeable, as CONTENTS.org already
warns for food values.

Repeated item/person rows are not repeated interviews. Deduplicate each raw
status carrier at household grain, check every distinct non-null code, and
retain missingness. No carrier contains two different non-null status codes
for one household. Cross-carrier disagreement exists at p3: four households
have accepted versus abandon, three accepted versus temporary absence, and one
accepted versus other. All eight remain conflicts; no source is selected as a
winner. `status-summary.json` and `cross-status-summary.json` give counts and
pairwise source comparisons, never household IDs.

## Unknowns and contradictions

The following partitions **the p1 panel of 10,800**, not the 10,802-key union.
Each row sums to 10,800. Categories denote raw codes; p4's code binding has the
qualification above.

| Passage | Accepted 1/2 | Abandon 3 | Absence/refusal 4/5 | Other/undecoded 6/7/9 | Missing/absent/conflict |
|---|---:|---:|---:|---:|---:|
| 1 | 10,783 | 1 | 4 | 3 | 9 |
| 2 | 10,590 | 3 | 18 | 1 | 188 |
| 3 | 10,224 | 1 | 0 | 0 | 575 |
| 4 | 10,172 | 3 | 286 | 254 | 85 |

P2's unknown column is 179 absent carrier keys plus nine null results.
P3's is 567 absent keys plus eight conflicting results. P1 has nine null
results and p4 85. Absence from a carrier is unknown, never a nonresponse code.
The two extra keys are kept separately; their status does not create p1 panel
membership. There are 9,895 households with four unambiguous accepted results;
that is an observed-status cohort, not an exposure denominator for everyone else.

**The official report disagrees with the retained fields.** `Report.pdf`,
printed/PDF page 22, table 1-2, reports accepted-plus-reluctant totals of
10,800 / 10,619 / 10,221 / 10,168. The conflict-free retained-status union
gives 10,783 / 10,591 / 10,225 / 10,172. The report says all 10,800
households participated at p1 and abandonment began at p2; retained p1 fields
contain one abandonment, four temporary absences, two other, one unknown code,
and nine missing. Both facts are recorded; neither source was declared stale.
An aggregate report cannot supply the identities of unknown households.

Accepted statuses occur without any served food row: **57 / 79 / 61 / 74**
by passage across all retained keys, or **57 / 78 / 60 / 74** restricted to the
p1 panel. They must remain accepted independently of food records; absence of
rows does not itself prove zero purchases. Conversely positive served
expenditure coexists with temporary-absence codes for 4 p1 households and 13
p2 households, with partial/other/unknown results as well. Report status and
food values separately; do not use either to overwrite the other.

Dates cannot repair result classification. The three broad p2 revenue/transfer
carriers have complete day/month/year components for 10,615 of 10,621
households and all three components missing for six, with no conflicting
non-null component within a household. Raw year is **14**, month 4-6, day
1-31. Date interpretation would need an explicit 2014 century rule and the
existing parser. No complete p1/p3/p4 household date carrier was found among
the retained passage result sources. Agricultural A11/A13 variables are a
separate module and were not substituted for food interview results.

## Current disclosure and the proposed boundary

The historical #851 option-A estimate, **1,007 of 10,789 food-carrying
households (9.3%), -3.94%**, counts visits carrying nonzero food expenditure.
Its code explicitly calls this an upper bound because an accepted visit may
produce no food rows. It is not this status census and is not re-estimated as
an all-household rate correction here. Exploratory private arithmetic was
conditional on unresolved mappings and is excluded from committed estimates.

Existing country detail remains valid: four independent expenditure recalls,
quantity only in passage 1, `Unknown` unit sentinel, and a retained integer
visit index. The historical CONTENTS.org sentence at line 1093, "Nothing in
the API says so yet", predates the Phase-I `attrs['recall']` attachment; it is
not evidence that the current release lacks all recall metadata.

A concrete future boundary, **after source adjudication and #851 integration**:

1. A country producer under `Burkina_Faso/2014/_/` exposes interview-result
   evidence independently of food, at `(t, i, visit, carrier)` grain, retaining
   raw code, source file/variable, dates when held, and missing/conflict reasons.
   A country-level adapter uses the existing ID conversion once, preserves
   extra keys as findings, and does not infer results from purchases.
2. A shared resolver accepts this evidence and an explicit status policy, then
   supplies `recall.to_period` with a `(t, i)` exposure frame. Expenditure
   counts eligible passages 1-4; Quantity only passage 1. It must not sum a
   repeated exposure field across food rows or confuse missing item quantity
   with a missing interview.
3. The shared consumer prefers a valid household exposure, keeps option-A
   disclosure/declared fallback for waves without status, and has an explicit
   refusal/degrade contract for unknown households inside status-bearing waves.
   This last policy is unresolved; no missing status is filled here.

This is an interface proposal, not a new registered feature or implementation.
The country retains source detail; the framework owns conversion and unknown
exposure behavior. GhanaLSS and its stratum-varying windows stay separate.

Cache impact: this diagnosis touches only documentation/evidence. CONTENTS.org
is explicitly excluded from Org build inputs. A future country scheme change
and wave config/script would change all Burkina country hashes; the wave
change affects every 2014 table because config and `binp` files are hashed
whole. Budget one country re-warm, not just food. Shared read-path changes
require a fingerprint probe before claiming zero corpus invalidation. No
tracked-code mutation or stash probe was performed in this diagnosis.

## Verification, reproduction and remaining work

The remaining questions are different kinds of work; they are not a request
to approve source facts or repeat the #851 decision:

| Item | Classification | Remaining action |
|---|---|---|
| Accepted interview versus completed food ask | Missing source/semantic evidence | The literal A13 result is known. Find evidence that acceptance certifies the relevant ask; calling it a proxy instead would be a new modelling policy, not a source fact. |
| Official report versus retained extract totals | Evidence contradiction | Trace release/cleaning provenance or original cover records. No precedence has been chosen, and an aggregate table cannot fill household results. |
| Eight p3 carrier disagreements | Evidence contradiction | Preserve the conflicting codes and locate authoritative records; do not select a carrier by convenience. |
| Missing/absent statuses stay unknown; no food-row inference | Existing ruling | Apply #1148's instruction exactly. Missing original household cover coverage is a source gap, not permission to infer nonresponse. |
| How conversion responds to an unknown household within a status-bearing wave | Genuine new API policy choice | Specify refusal/degrade behavior without changing the source classification. #851 settled conversion approval and wave-level Contract B, not this household mask. |
| P4 numeric binding, particularly 6/7 | Missing direct source dictionary | The p3 dictionary and report categories corroborate an inference, but a p4 dictionary/instrument is still needed. No new source was acquired. |
| #851 conversion approval and release integration | Existing ruling / implementation dependency | Recover and integrate existing approved work; do not ask for conversion approval again. |

The seed's 58 Burkina L2 files (15,418,278 bytes) matched the private release
manifest SHA256 `e53acbe4b0f5918e244fc808ee326035ed86c04ec15475e2ff09339ccaaba5f0`
before physical copying. All private copy inodes differ. The private seed's
2014 food frame has 557,692 rows, 10,789 households and CFA 342,110,515.
No shared L2 was loaded or promoted. Import/config realpaths were asserted
against this worktree. Source calls used `get_data_file` / `get_dataframe`;
pyreadstat was metadata-only on those verified local paths. Source networking
was disabled in the census processes. Held PDF text was extracted locally
using the dispatcher's isolated pypdf tool, without uploading the files.

Existing config/record checks passed: **35 passed, 2 deselected**, serial,
numerical threads pinned to one. Exact selection and a local-only census
reproducer are in `REPRODUCE.md`. Full logs and individual records remain at
the private evidence path named there; committed JSON contains aggregates and
source hashes only. The committed reproducer was executed in a second private
directory and matched every p1-panel status mask, all 50 held sources and the
58-file private L2 verification.

Before/after verification for a future implementation must cover:

- Accepted interviews with zero purchases or zero food rows; explicit
  nonresponse, partial interview, missing code, unknown code, conflicting
  carriers, repeated carrier rows, and the two out-of-panel keys.
- Separate Expenditure and Quantity counts (four possible asks versus one),
  including Quantity null by module/item design; zero/unknown denominator
  refusal, never division by zero or silent `sum()` to zero.
- Household-specific weekly conversion on a resolved cohort, then aggregate
  and household multiplier distributions; separate unknown/conflict counts.
  Preserve `period=None` data exactly and refuse `period=` on prices.
- Country and Feature joins on `(country, t, i)` as applicable, metadata
  survival after concat, option-A fallback for unsupported waves, cold/warm
  equality, and a scoped hash probe before any authorized re-warm.

Remaining blockers are the #851 integration dependency, original cover-record
authority, report/source disagreement, p3 conflict policy, p4 dictionary binding,
and partial/unknown-household policy. No missing declared file was fetched.
If source adjudication is deferred, a separate useful next diagnosis is GH
#1059 (District spelling variants versus three genuine province disagreements);
it has source/config evidence already described in CONTENTS.org and does not
depend on a period conversion policy. No work on it was started.

Sue.

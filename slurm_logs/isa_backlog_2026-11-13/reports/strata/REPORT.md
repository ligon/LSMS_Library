# REPORT.md — stream `strata` (GH #1577, with #1134's rule)

## 0. Changes since 0d62f49df (red-team fix-up)
One commit, `79739f8162b706b72bbc54dffe739e855b154f80` (printed `SUCODER: published`):
- **F1.** The Mali 2014-15 one-wave `strata` shape depends on cache state, and does so identically on base:
  - a cold one-wave call has no `strata` and no `Derivation` column;
  - after an all-waves call, both are present and `strata` is NA on all 3,804 rows.
  - Reworded in all four places that said "no strata column at all": the `absent` comment in `STRATA_HOLDS`, the comment in the delivered test, Mali CONTENTS.org (which also points readers to `s.get('strata')`) and residual R3. R3 is now framed as a pre-existing build/read-composition defect.
- **F2.** A `derivation` key not equal to `format_key(*parse_key(key))` is refused. Before this, a padded key such as `' Mali::sample::region-milieu-stratum '` passed. Four parametrised cases test it (padded, trailing space, re-spaced slots).
- **F4.** New non-S3 static test class `TestANativeRecordIsWhatTheConfigServes`, reading config and source text only. For each native record it checks:
  - `variable` is the variable and file the wave's `data_info.yml` maps to `strata`;
  - the declaration is bare (no `mapping:` or `mappings:`);
  - the effective `strata` formatter (country `{c}.py` / `mapping.py`, then the wave's files) is absent or exactly `return format_id(x)`;
  - no `sample` hook or sub-df hook touches `strata`.
  - The check passes on all 7 native records.
  - Negative controls show it fails where it should: Burkina 2014 (its hook relabels), Malawi 2010-11 (its declaration carries a mapping), and Mali 2017-18 given a wrong variable or a wrong file.
- **F5.**
  - `test_a_pending_country_is_still_pending` now requires **nothing** tagged, so a half-tagged pending country fails.
  - The Guinea-Bissau proposal now covers both 2018-19 and 2021-22. On `feature/gnb-labels-2021-20261113`, the GNB registry entry spans both waves and 2021-22's `sample` hook stamps the key.
  - `constructed` is kept; whether it belongs in the tag at all is question Q5.
- **F7, `docs/guide/feature.md`.**
  - Documents `Country(c).sampling.get(wave)`: `sampling` is a plain dict, so `[wave]` raises `KeyError` on an uncurated wave.
  - Says an uncurated wave shows up as an `unrecorded` placeholder in `attrs`.
  - Adds the `df.get('strata')` advice from F1.
  - The GhanaLSS row now reads "1991-92 onward".
  - The table says Guinea-Bissau 2021-22 joins once it is wired.
- **F8.**
  - The `holds` refusal now names its origin: "added by GH #1577, each value with its first record, under GH #1134's rule". It no longer says "frozen from the Uganda exemplar", and a test checks this.
  - A comment in `sampling.py` says `confidence` on `served_strata` grades the served-column claim, not a design statement.
- **F9.** The Mali 2014-15 locator now quotes CONTENTS' sentence verbatim, `deflator` qualifier included.
- **F10.** The Mali 2021-22 note now says what the two counts are:
  - CONTENTS' 429 of 6,133 is "the households it found in both waves" (2026-11-04);
  - on the households the served panel links today (after the GH #1494 id namespace), the red team counts 415 of 5,011 in 36 clusters, all relabels (job 41277169).
- **F3.** Benin's and Guinea-Bissau's `sampling.yml` are untouched. The proposal files are now written as blocks for you to merge into the existing entries, with the duplicate-key warning in their header.
- **Not done:** F11 (CONTENTS append conflicts, resolved at merge) and F12 (the attrs shape change, already disclosed).

## 1. Commits and head
- Base `f138a3d61c203f892148592675a7ef72e160a322`.
- `0f6a671b993c0885dc3989f4760bf411db3c45f1`: the tag, its tests, Mali and Niger records.
- `0d62f49df61bb43190275aaa3a2520f724eed6af`: Burkina Faso, Togo, Côte d'Ivoire, Senegal, Uganda records; feature guide; Mali and Burkina CONTENTS notes.
- `79739f8162b706b72bbc54dffe739e855b154f80` (**head**): the fix-up above.

**Ruling implemented** (isa-decisions.org, 2026-11-12): "Registry + sampling.yml tag (Recommended)". Constructed strata stay in the derivations registry. A small per-wave tag in `_/sampling.yml` covers the native-code, native-label and absent cases, decided with #1134's extension. The #793 vocabulary is frozen; it widens one tag at a time, each with its first evidenced record.

## 2. Tag design
**Placement.** A top-level block `served_strata` in a wave record, beside `panel`.
- It describes the served column, not the design, so it stays out of `stratification`.
- It carries its own `source_type`, `confidence` and `locator`.
- An entry holding only `served_strata` (with or without `panel`) is a valid record without the design triple, so a first wave can carry one.

**Fields** (frozen in `_SERVED_STRATA_FIELDS`):

| field | rule |
|---|---|
| `holds` | required; controlled (`STRATA_HOLDS`) |
| `variable` | `"name (file)"`; required for native values, refused otherwise |
| `derivation` | required for `constructed`, refused otherwise; must be `<country>::sample::<name>`, this record's country, written canonically |
| `note` | optional |
| `source_type`, `confidence`, `locator` | required; `not-found` refused |

**Values**, each with its first record named in `sampling.py`:

| `holds` | meaning | first record |
|---|---|---|
| `native-code` | the wave's stratum variable, served as a recorded code the file does not label | Mali 2017-18 |
| `native-label` | the wave's stratum variable, served as its recorded label (spelling folded only by the country's `strata` table) | Niger 2011-12 |
| `constructed` | a library construction; `derivation` names the registry key | Mali 2018-19 |
| `absent` | no non-NA `strata` in the wave; the locator says what was searched | Mali 2014-15 |

**Scope.** "Native" claims only this: the wave's own files or documents name, label or describe the variable as the stratum. Two kinds of wave stay unrecorded until ruled on (§9):
- geographic variables served as `strata`;
- codes the library decodes through its own lookup.

**API.** `SamplingRecord.strata_holds`; `STRATA_HOLDS` and `NATIVE_STRATA` are exported.

## 3. Records written (19 cells, all measured)

| country | wave(s) | holds | key / variable |
|---|---|---|---|
| Mali | 2014-15 | absent | — |
| Mali | 2017-18 | native-code | `strate (EACI17_ECHANTILLON.dta)` |
| Mali | 2018-19, 2021-22 | constructed | `Mali::sample::region-milieu-stratum` |
| Niger | 2011-12 | native-label | `strate (NER_2011_ECVMA_v01_M_Stata8/ecvmamen_p1_en.dta)` |
| Niger | 2014-15 | native-label | `strate (NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2014_P1P2_Conso.dta)` |
| Niger | 2018-19, 2021-22 | constructed | `Niger::sample::region-milieu-stratum` |
| Burkina_Faso | 2018-19, 2021-22 | constructed | `Burkina_Faso::sample::region-milieu-stratum` |
| Togo | 2018, 2021-22 | constructed | `Togo::sample::region-milieu-stratum` |
| CotedIvoire | 2018-19 / 2021-22 | constructed | `…region-milieu-stratum` / `…region-milieu-stratum-upper-region` |
| Senegal (new file, no panel) | 2018-19, 2021-22 | constructed | `Senegal::sample::region-milieu-stratum` |
| Uganda | 2005-06, 2009-10, 2010-11 | native-label | `stratum (GSEC1.dta)` |

**What is served** (cold, job 41276185; `out/served-strata-dump.csv`):
- every count quoted in a locator matches the served frame;
- the constructed waves carry the pointed `Derivation` key on every row with a stratum (for example Burkina 7,010 of 7,010, Côte d'Ivoire 12,992 of 12,992);
- Uganda serves exactly its six labels.

The red team independently confirmed all 19 against the served frames, and all 7 native records against the raw files.

**Proposed for integration, not written** (`out/proposed-{Benin,Guinea-Bissau}-served_strata.yml`). Every block passes `SamplingRecord.from_config` (job 41277518).
- **Benin:** 2018-19 is a new entry; 2021-22 is a block to merge into the existing entry, beside its `panel`. The registry covers both waves at this head.
- **Guinea-Bissau:** 2018-19 is a new entry; 2021-22 is a block to merge into the gnb branch's existing entry.
  - At this head the registry covers 2018-19 only. On the gnb branch it covers both, and 2021-22's `sample` hook stamps the key (checked with `git show`).
  - The 2021-22 block is `confidence: medium` until its served frame is measured at integration.
- **Merge, never paste:** `_SamplingLoader` keeps the last duplicate key silently, so a second wave entry would delete a `panel` block (F3).
- **Then** remove both countries from `SERVED_STRATA_PENDING`, and tag every registered wave first: the pending test now fails on a half-tagged country.

## 4. Coverage table for the remaining cells (not written; nothing guessed)
"Form" means code or label, which nothing in the repo establishes unless stated.

| country / waves | what config serves as `strata` | what is missing |
|---|---|---|
| Albania 2002 / 2005 / 2008 | `stratum` / `m0_stratum` / `stratum` with `.str.title()` (wave scripts) | form; 2008's title-casing is a script fold |
| Albania 2003 | `STRATA`, "numeric 1-16" (docstring) | value labels |
| Albania 2004 | none ("No strata … available", docstring) | a search record before `absent` |
| Albania 2012 | `region` | Q1(ii) |
| Azerbaijan, China, Kazakhstan, Kosovo, Liberia | none (scheme says optional) | a search record |
| Burkina Faso 2014 | `strate`, decoded through the #1573 lookup | Q2 |
| Nigeria W5 (2023Q3, 2024Q1) | `strata` 1-6, decoded through `_W5_STRATA` | Q2 |
| Cambodia 2019-20 | `s01q02` | what it is; form |
| Côte d'Ivoire 1985-86 to 1988-89 | `REGION` | Q1(ii) |
| Ethiopia (5 waves) | region x Rural, built in wave `sample.py`, **unregistered** | R2 |
| EthiopiaRHS (8 waves) | `strata = v` (village), in `ethiopiarhs.py`, **unregistered** | R2 |
| GhanaAHIES 2022-2024 | `{region}-{Urban\|Rural}`, wave scripts, **unregistered** | R2 |
| GhanaLSS 1987-88, 1988-89 | none | a search record |
| GhanaLSS 1991-92 to 2016-17 | `region` (CONTENTS: the design stratum is `(strata, Rural)`) | Q1(ii) |
| GhanaSPS 2009-10 / 2017-18 | `loc7` / `region` | Q1; form |
| GhanaSPS 2013-14 | none ("ships no geography", data_info comment) | candidate `absent` |
| Guatemala 2000 | `region` ("very likely incomplete") | Q1(ii) |
| Guyana 1992 | `STNO` | what it is; form |
| India 1997-98 | `stratum` (state x phase) | form |
| Iraq 2006-07 / 2012 | `xstrat` / `stratum` | form |
| Malawi (all waves) | district, through the district mapping | Q1(ii) |
| Nepal (3 waves) | `stratum` | no microdata held |
| Nigeria W1-W3 | `zone` | Q1(ii) |
| Nigeria W4 | `strata` | form |
| Pakistan 1991 / South Africa 1993 / Serbia 2007 | `province` / `newprov` / `region2` | Q1(ii) |
| Serbia and Montenegro 2002, 2003; Tajikistan 2007, 2009; Timor-Leste 2001, 2007-08 | `stratum` / `strata` / `strata` | form |
| Tajikistan 1999, 2003 | none | a search record |
| Tanzania (6 waves) | `strataid` (2020-21: NaN for 545 booster households) | form |
| Uganda 2011-12 to 2015-16 / 2018-19, 2019-20 | `sregion` / `subreg` | Q1(i) |
| Panama, Peru | no `strata` column declared | out of scope |
| Benin, Guinea-Bissau | registered constructions | §3 proposals |

I did not use `.coder/sampling/fingerprint.csv` as evidence: it dates from 2026-09-07 and is stale.

## 5. Idiosyncrasies, and CONTENTS.org reading
**Not read in full.** For each edited country, the lines below were **not** read (line numbers at base). Every line matching `strat`, case-insensitive, was read in grep output in every file.

| country | not read |
|---|---|
| Mali | 411-2604, 2651-3128, 3169-3644, 3672-3906, 3957-4080 |
| Niger | 59-99, 137-239, 331-2575, 2631-2856, 2914-3985 |
| Uganda | 1-9, 51-99, 141-4069, 4096-6234 |
| Burkina_Faso | 21-99, 126-205, 271-295, 361-504, 523-2676, 2701-3053, 3101-3522 |
| Togo | 1-104, 161-843, 901-1146, 1231-1282, 1301-1352, 1396-1630 |
| CotedIvoire | 1-59, 76-669, 691-1028, 1076-1405, 1441-1506, 1564-1671 |
| Senegal | 46-164, 233-325, 421-802, 831-933, 961-1309 |

Mali, Niger, Togo and Côte d'Ivoire have no TODO/WAITING/LOGBOOK entries; Uganda's, Burkina's and Senegal's were read by heading.

**Idiosyncrasies that bear on the records:**
- **Mali**
  - Two programmes. EAC-I states "51 explicit sample strata"; the EHCVM BID states region x residence area.
  - 2017-18 `strate` is labelled `codestrate`, has no value labels, and 47 of its 51 codes are occupied (GH #1574).
  - 2014-15 has no stratum variable in its 55 files except `deflator`, which has 15 cells, not 51.
  - 2021-22 writes Menaka and Taoudenit under Gao and Tombouctou.
  - The 2014-15 one-wave column shape depends on cache state (F1, recorded in CONTENTS).
  - Kidal is excluded, and 2017-18 has a Lourd/Léger split.
- **Niger**
  - ECVMA `strate` is region x agro-ecological zone: 26 stated, 27 value labels, 25 occupied. The Arlit strata are excluded from the frame.
  - In 2014-15 `strate` is only in the consumption file, reduced per grappe; 2014-15 is a whole-wave panel with split-offs.
  - EHCVM is constructed, and "Niamey Rural" is contested in 2021-22.
- **Burkina Faso**
  - 2014 is decoded through the library's lookup (#1573). Its label is `group(region milieu)`, and the report says stratification "basée sur l'urbanisation".
  - `s00q01` codes change meaning across waves.
  - 2021-22 has two components, and three documents disagree on the count of 26.
- **Togo**
  - `Lomé commune` (2018) and `Grand Lomé` (2021-22) are on the same 98 grappes.
  - Lomé Commune is a single-milieu stratum, and its "rural" sentence is quoted uncorrected.
  - 2021-22 carries three disagreeing panel flags.
- **Côte d'Ivoire**
  - 2021-22 labels 23 regions in lower case, hence its own upper-region key.
  - Grappe 648 holds two strata.
  - The cover holds 728 households that were not interviewed, and the CILSS waves serve `REGION`.
- **Senegal**
  - No variable is a stratum; `domaine` is zone x vague.
  - The stratum statement is in the 2021-22 BID only.
  - The 2021-22 wave mixes panel households with top-ups.
- **Uganda**
  - `stratum` is served through 2010-11, then `sregion` ("10 sub region"), then `subreg`.
  - Spelling is folded by the `strata` table, and `ea` does not nest in `strata`.
  - The TODO "Investigate 2005-06 strata vs later sub-regions" is open.

No finding contradicted a CONTENTS.org.

## 6. Measurements and hash census
**Hash census**, base `f138a3d61` → head `79739f816` (job 41277518, `out/hashdiff-79739f816.json`). It was also 0 at `0d62f49df` (jobs 41275930 and 41276185), and the red team got the same independently (job 41276659).

| kind | moved / total |
|---|---|
| `build_transforms_fingerprint` | 0 / 42 |
| `framework_imports_fingerprint` | 0 / 38 |
| `Country._table_cache_hash` | 0 / 555 |
| `Wave._input_hash` | 0 / 2,321 |

Nothing moved in any country, and there were no errors.

**Read-path changes, attrs only.** The red team confirmed that no frame, warning or Site C report changes (job 41276686, 57 calls).
- Every Senegal frame now carries `attrs['sampling']`.
- Mali 2014-15, Niger 2011-12, Togo 2018 and Côte d'Ivoire 2018-19 replace `design_tag='unrecorded'` placeholders with records whose `design_tag` is None, the same shape panel-only records have. Counts of `design_tag == 'unrecorded'` drop them (F12).

## 7. Tests (job 41277518, head 79739f816)
- **Unit run:** `pytest -n 8 --dist=loadfile -p no:cacheprovider -m "not requires_s3"` over test_sampling, test_sample, test_schema_consistency, test_build_transform_hash, test_gh1494_mali_eaci_namespace, test_gh812_ehcvm_strata, test_feature_attrs_carriers and test_gh1503_record_caches_do_not_cross_tests: **794 passed, 0 failed, 0 skipped.**
- **Full S3 run, cold private root:** `tests/test_sampling.py` and `tests/test_gh1494_mali_eaci_namespace.py`, S3 tests included: **416 passed, 0 failed.** This includes the 19 delivered `served_strata` checks and `TestTheReproduction`.
- **Not run:** the other `requires_s3` tests in test_sample, test_gh812 and test_feature_attrs_carriers (714 `requires_s3` collected across the 8 files), and the full suite. These are cold-build costs, and none of them reads `served_strata`. The red team ran the whole non-S3 suite at `0d62f49df` (15,569 passed, 0 failed) and 93 S3 tests (0 failed).
- **Another stream's test:** `test_gh1494_mali_eaci_namespace.py`'s key-set assertion now includes 2014-15 and asserts that 2014-15 has no `panel`. The red team judged this minimal.

## 8. Issue status and drafted residuals
- **#1577: partial.** **#1134: open** (no `design_tag` added). **#1574: open.** **#792: not addressed.**
- **R1 — "served_strata for the remaining strata-serving waves (GH #1577 residual)", tier C.** Body: §4's table and Q1/Q2. Native cells need a value census or a file-metadata read; `absent` cells need a written search record.
- **R2 — "Strata constructions in Ethiopia, GhanaAHIES and EthiopiaRHS are not registered derivations", tier B.**
  - Wave scripts or the country module build them; there is no `derivations.yml` entry and no `Derivation` stamp, so `served_strata` cannot point at them.
  - EthiopiaRHS's uniform `weight = 1.0` is also unregistered.
  - Fixing this costs a re-warm per country.
- **R3 (reframed) — "A one-wave sample() serves a different column set cold than after an all-waves call (Mali 2014-15 `strata`, `Derivation`)", tier B.**
  - A pre-existing build/read-composition defect, the same on base. Cold: no `strata` or `Derivation`; after `sample()` for all waves: both present, NA.
  - CLAUDE.md: "a served value that depends on cache state is a defect to fix".
  - Check whether every YAML wave that does not wire a declared column behaves the same way.
- **R4 — "`_SamplingLoader` silently keeps the last of duplicate keys", tier C (red team F3).** Refuse duplicate keys in `sampling.yml`.

## 9. Questions for the user (reframed per the red team)
1. **Q1(i).** A geographic variable that the library's own design record names as the stratification, Uganda's `sregion`/`subreg` (GSEC1 label "10 sub region"). Tag it `native-label` once a document naming it as the stratum is cited, or keep it unrecorded?
2. **Q1(ii).** A geographic variable coarser than the documented stratum: GhanaLSS `region` (design `(strata, Rural)`), Malawi district, Côte d'Ivoire CILSS `REGION`, Nigeria W1-W3 `zone`, and similar. A new value, or unrecorded? The red team notes that widening `native-label` would erase the distinction the tag draws.
3. **Q2.** Codes decoded by a library lookup (Burkina 2014, Nigeria W5).
   - (a) a new value;
   - (b) register the decoding as a derivation and tag it `constructed`. The precedent is Malawi #1228 ("Register all waves"); it costs a Burkina re-warm, and Nigeria likewise;
   - (c) `native-label`, which is false by the module's definition.
4. **Q3.** All 19 records are `derived-from-data`, so `rec.documented` is False on every one, though Mali 2017-18 and Niger rest partly on a BID. Should a held document that names the variable earn `local-documentation`?
5. **Q4.** CLAUDE.md's Site C section still describes only the Uganda freeze and "one `design_tag` at a time". Add a line about `served_strata` at integration?
6. **Q5 (F5).**
   - `constructed`, which carries 12 of the 19 records, and the two-way registry↔tag test come from the brief ("a pointer to the registry"). The ruling's text says only "Constructed strata stay in the derivations registry".
   - The cost: every future strata registration must also edit `sampling.yml`, or be listed as pending. That includes the R2 cases.
   - Keep it, or derive `constructed` at read time from the registry, with no second copy?
7. **Q6 (F6).** One PR adds one field, 7 nested names and 4 values. Each value has a verified first record and the two-way freeze test. Is that an acceptable reading of "one tag at a time"?

**Process disclosure.** I ran system `python3` twice on n0035 (an empty `python3 -` heredoc and `python3 -c "pass"`). Neither imported `lsms_library` or read data; all real Python ran in burst jobs. All my burst jobs have finished.

---


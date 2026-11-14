# REPORT.md — stream `strata` (GH #1577, with #1134's rule)

## 1. Commits and head
- Branch `feature/1577-strata-tag-20261113`, base `f138a3d61c203f892148592675a7ef72e160a322`.
- `0f6a671b993c0885dc3989f4760bf411db3c45f1`: the `served_strata` tag in `sampling.py`, its tests, and the Mali and Niger records.
- `0d62f49df61bb43190275aaa3a2520f724eed6af` (**head**): records for Burkina Faso, Togo, Côte d'Ivoire, Senegal (new file) and Uganda; `docs/guide/feature.md`; dated notes in Mali and Burkina CONTENTS.org.
- Both commits printed `SUCODER: published`.

**Ruling implemented** (isa-decisions.org, 2026-11-12, "Strata meaning record, GH1577"): "Registry + sampling.yml tag (Recommended)". Constructed strata stay in the derivations registry. A small per-wave tag in `_/sampling.yml` covers the native-code, native-label and absent cases, decided with #1134's vocabulary extension: the #793 vocabulary is frozen, and it widens one tag at a time with its first evidenced record.

## 2. Tag design
**Placement.** A new top-level block, `served_strata`, in a wave's record. It sits beside `panel`, not inside `stratification`:
- It describes the column the library serves, not the design. `stratification` is a design field and would force the design triple.
- Like `panel`, it carries its own `source_type`, `confidence` and `locator`. An entry holding only `served_strata` (with or without `panel`) is a valid record, so a first wave can carry one.

**Fields** (frozen in `_SERVED_STRATA_FIELDS`):

| field | rule |
|---|---|
| `holds` | required; controlled by `STRATA_HOLDS` |
| `variable` | required for a native value, refused otherwise |
| `derivation` | required for `constructed`, refused otherwise; must be `<country>::sample::<name>` with this record's country, checked with `derivations.parse_key` |
| `note` | optional |
| `source_type`, `confidence`, `locator` | required |

**Values**, each with its first record named in `sampling.py`:

| `holds` | meaning | first record |
|---|---|---|
| `native-code` | the wave's stratum variable, served as a recorded code the file does not label | Mali 2017-18 |
| `native-label` | the wave's stratum variable, served as its recorded label (spelling folded only by the country's `strata` table) | Niger 2011-12 |
| `constructed` | built by the library; `derivation` names the registry key | Mali 2018-19 |
| `absent` | NA on every row; a frame of that wave alone has no `strata` column; the locator says what was searched | Mali 2014-15 |

**Scope.** "Native" claims only this: the wave's own files or documents name, label or describe the variable as the stratum. Two cases are deliberately not recorded and are questions in §7:
- a region, district or province served as `strata`;
- a code the library decodes through its own lookup.

**Refusals** (`_check_served_strata` and `from_config`), all tested:
- an unknown value or field;
- missing provenance;
- `source_type: not-found`;
- a native value without its `variable` or with a `derivation`;
- `constructed` without a key, or with a `variable`;
- a key of another country or table, or a malformed key;
- `absent` naming anything;
- non-string or empty values, and a non-mapping block;
- an entry that is still empty is refused as before.

**Other changes in `sampling.py`:**
- `SamplingRecord.strata_holds` returns the value, or `None` (unrecorded, never "absent").
- `STRATA_HOLDS` and `NATIVE_STRATA` are exported.
- The empty-record check moved from `_check_panel` to `from_config`. Behaviour is unchanged and existing tests pass.

**Scope of the widening.** My reading of "one tag at a time" is one new field whose four values each ship with a first evidenced record. No `design_tag` was added.

## 3. Records written (19 cells, all measured)

| country | wave | holds | key or variable |
|---|---|---|---|
| Mali | 2014-15 | absent | — |
| Mali | 2017-18 | native-code | `strate` (EACI17_ECHANTILLON.dta) |
| Mali | 2018-19, 2021-22 | constructed | `Mali::sample::region-milieu-stratum` |
| Niger | 2011-12 | native-label | `strate` (ecvmamen_p1_en.dta) |
| Niger | 2014-15 | native-label | `strate` (ECVMA2014_P1P2_Conso.dta) |
| Niger | 2018-19, 2021-22 | constructed | `Niger::sample::region-milieu-stratum` |
| Burkina_Faso | 2018-19, 2021-22 | constructed | `Burkina_Faso::sample::region-milieu-stratum` |
| Togo | 2018, 2021-22 | constructed | `Togo::sample::region-milieu-stratum` |
| CotedIvoire | 2018-19 | constructed | `CotedIvoire::sample::region-milieu-stratum` |
| CotedIvoire | 2021-22 | constructed | `CotedIvoire::sample::region-milieu-stratum-upper-region` |
| Senegal (new `sampling.yml`, no panel) | 2018-19, 2021-22 | constructed | `Senegal::sample::region-milieu-stratum` |
| Uganda | 2005-06, 2009-10, 2010-11 | native-label | `stratum` (GSEC1.dta) |

**Source type.** Every record uses `derived-from-data`, because the claim is about the served column. Its locator cites the config line, the registry entry, and the CONTENTS.org section that holds the measurement.

**What `sample(waves=[w])` serves** (cold, private root, job 41276185; `out/served-strata-dump.csv`):

| wave(s) | distinct values | rows keyed by the pointed `Derivation` |
|---|---|---|
| Mali 2014-15 | no `strata` column | — |
| Mali 2017-18 | 47 codes (1-45, 50, 51) | — |
| Mali 2018-19 / 2021-22 | 20 / 17 | all |
| Niger 2011-12 / 2014-15 | 25 / 25 | — |
| Niger 2018-19 / 2021-22 | 16 / 15 | all |
| Burkina 2018-19 / 2021-22 | 26 / 26 | all |
| Côte d'Ivoire 2018-19 / 2021-22 | 66 / 66 | all |
| Senegal 2018-19 / 2021-22 | 28 / 28 | all |
| Togo 2018 / 2021-22 | 11 / 11 | all |
| Uganda 2005-06 / 2009-10 / 2010-11 | exactly the six labels | — |

Every count quoted in a locator agrees with this dump. In Uganda 2009-10, 7 of 2,979 households have no stratum. The test `test_the_tag_agrees_with_the_served_strata` checks the same things and passed 19 of 19 (job 41275930).

**Proposed, not written** (Benin's and Guinea-Bissau's files belong to other streams):
- The snippets are in `out/proposed-Benin-served_strata.yml` and `out/proposed-Guinea-Bissau-served_strata.yml`. All three entries pass `SamplingRecord.from_config` and resolve in the registry (job 41276185).
  - Benin 2018-19 (new entry) and 2021-22 (beside its `panel` block): constructed, `Benin::sample::region-milieu-stratum`.
  - Guinea-Bissau 2018-19: constructed, `Guinea-Bissau::sample::region-milieu-stratum`.
- **Integration steps.** After pasting, delete each country from `tests/test_sampling.py::SERVED_STRATA_PENDING`. `test_a_pending_country_is_still_pending` fails until you do.
- If the Guinea-Bissau stream registers a 2021-22 strata construction, add a matching 2021-22 `constructed` block. Otherwise `test_every_registered_strata_construction_is_tagged` fails. The same test fails if any stream registers a strata construction in an untagged country.

## 4. Coverage table for the remaining cells (not written; nothing guessed)
"Form" means code or label, which nothing in the repo establishes unless stated.

| country / waves | what config serves as `strata` | what is missing |
|---|---|---|
| Albania 2002 / 2005 / 2008 | `stratum` / `m0_stratum` / `stratum` with `.str.title()` (wave scripts) | form; 2008's title-casing is a script fold, not a `strata` table |
| Albania 2003 | `STRATA`, "numeric 1-16" (script docstring) | value labels (likely native-code, unverified) |
| Albania 2004 | none ("No strata … available", docstring) | a search record before `absent` |
| Albania 2012 | `region` | region-type (Q1) |
| Azerbaijan, China, Kazakhstan, Kosovo, Liberia | none (scheme says optional) | a search record before `absent` |
| Benin 2018-19, 2021-22; Guinea-Bissau 2018-19 | registered constructions | proposed above |
| Burkina Faso 2014 | `strate`, decoded through the #1573 lookup | Q2 |
| Cambodia 2019-20 | `s01q02` | what the variable is; form |
| Côte d'Ivoire 1985-86 to 1988-89 | `REGION` | region-type (Q1) |
| Ethiopia (all five waves) | region x Rural, built in wave `sample.py`, **unregistered** | registration (residual R2) |
| EthiopiaRHS (all eight waves) | `strata = v` (village), set in `ethiopiarhs.py`, **unregistered** | registration (residual R2) |
| GhanaAHIES 2022-2024 | `{region}-{Urban\|Rural}` in wave scripts, **unregistered** | registration (residual R2) |
| GhanaLSS 1987-88, 1988-89 | none | a search record |
| GhanaLSS 1991-92 to 2016-17 | `region` (CONTENTS: GLSS7 serves the region only; the design stratum is `(strata, Rural)`) | region-type (Q1) |
| GhanaSPS 2009-10 / 2017-18 | `loc7` / `region` | Q1; form |
| GhanaSPS 2013-14 | none ("this wave ships no geography", data_info comment) | candidate `absent`; the comment is the locator, needs adopting |
| Guatemala 2000 | `region` ("very likely incomplete" note) | Q1 |
| Guyana 1992 | `STNO` | what it is; form |
| India 1997-98 | `stratum` (state x sampling phase, whitespace normalised) | form |
| Iraq 2006-07 / 2012 | `xstrat` / `stratum` | form |
| Malawi 2004-05 to 2024-25 | `strata` / `hh_a01` / `district`, through the district mapping | district-type (Q1) |
| Nepal (three waves) | `stratum` | no microdata held |
| Nigeria W1-W3 (2010Q3 to 2016Q1) | `zone` | region-type (Q1) |
| Nigeria W4 (2018Q3, 2019Q1) | `strata` | form |
| Nigeria W5 (2023Q3, 2024Q1) | `strata` 1-6, decoded through `_W5_STRATA` | Q2 |
| Pakistan 1991 / South Africa 1993 / Serbia 2007 | `province` / `newprov` / `region2` | Q1 |
| Panama, Peru | no `strata` column declared | outside the tag's scope |
| Serbia and Montenegro 2002, 2003 | `stratum` | form |
| Tajikistan 1999, 2003 | none | a search record |
| Tajikistan 2007, 2009 | `strata` | form |
| Tanzania 2008-09 to 2014-15 (via `format_id`), 2019-20, 2020-21 | `strataid`; 2020-21 is NaN for 545 booster households | form |
| Timor-Leste 2001, 2007-08 | `strata` | form |
| Uganda 2011-12 to 2015-16 / 2018-19, 2019-20 | `sregion` / `subreg` | Q1 (the library already reads these as `stratification.variable`) |

I did not use `.coder/sampling/fingerprint.csv` as evidence: it dates from 2026-09-07 and shows Mali's EHCVM waves with 0 strata.

## 5. Idiosyncrasies, per edited country
**CONTENTS.org read extent** (the rule asks for full files; this falls short):
- **Mali:** 1-410, 2605-2650, 3129-3168, 3592-3671, 3907-3956, plus all headings.
- **Niger:** 59-107, 2576-2630, 2857-2913.
- **Uganda:** 10-50, 100-140, 4070-4095.
- **Burkina:** 2677-2700, 3054-3100, 3523-3555, plus the TODO/DONE headings.
- **Togo:** 105-160, 1147-1230, 1283-1300, 1353-1395.
- **Côte d'Ivoire:** 60-75, 670-690, 1406-1440, 1507-1563.
- **Senegal:** 1-45, 803-830, 934-960.
- **Everywhere:** every `strat` grep hit was skimmed.
- Mali, Niger, Togo and Côte d'Ivoire have no TODO/WAITING/LOGBOOK entries; Uganda, Burkina and Senegal entries were read by heading.

**Mali**
- Two programmes. EAC-I (2014-15, 2017-18) states "51 explicit sample strata"; EHCVM (2018-19, 2021-22) states region x milieu.
- 2017-18 `strate` is labelled `codestrate` and has no value labels (GH #1574 open). 47 of 51 codes are occupied.
- 2014-15 has no stratum variable in its 55 files. `deflator` covers 15 cells, not 51.
- 2021-22 writes Menaka and Taoudenit under Gao and Tombouctou, so 429 households change stratum on the label alone.
- **New, recorded in CONTENTS:** a `sample(waves=['2014-15'])` frame has no `strata` column at all, so `s['strata']` raises `KeyError`. An all-wave frame has the column, NA on those rows.

**Niger**
- ECVMA `strate` is region x agro-ecological zone. The BID says 26 strata; the variable has 27 value labels and 25 occupied cells.
- In 2014-15 `strate` exists only in the consumption file and is reduced to one row per grappe.
- The two programmes are different stratifications, not harmonised.
- "Niamey Rural" is contested in 2021-22.

**Burkina Faso**
- 2014 is decoded through the library's own lookup (GH #1573), so it is not recorded.
- `s00q01` codes change meaning across waves; the construction uses labels, never codes.
- 2021-22 has two components; the cross-section's stratum is a modelling choice.
- Three documents disagree on the count of 26 strata.

**Togo**
- Region code 6 is `Lomé commune` in 2018 and `Grand Lomé` in 2021-22, on the same 98 grappes.
- Lomé Commune is a single-milieu stratum, and the 2021 BID's "rural" sentence is internally implausible.

**Côte d'Ivoire**
- 2021-22 labels 23 regions in lower case, hence its own upper-region key.
- Grappe 648 holds two strata as recorded.
- The CILSS waves serve `REGION`.

**Senegal**
- No variable is named or labelled a stratum; `domaine` is zone x vague.
- The construction rests on the 2021-22 BID; no held 2018-19 document states it.

**Uganda**
- The stratification variable changes: `stratum` (2005-06 to 2010-11), then `sregion`, then `subreg`.
- Spelling is folded by the `strata` table.
- The design PSU `ea` does not nest in `strata` in every wave.
- The TODO "Investigate 2005-06 strata vs later sub-regions" is open.

No finding contradicted a CONTENTS.org.

## 6. Measurements and hash census
Outputs are in `reports/strata/out/`; jobs ran with private empty roots.

**Hash census**, base `f138a3d61` → head `0d62f49df` (`hashdiff-0d62f49df.json`, jobs 41275930 and 41276185, both runs identical):

| kind | moved / total |
|---|---|
| `build_transforms_fingerprint` | 0 / 42 |
| `framework_imports_fingerprint` | 0 / 38 |
| `Country._table_cache_hash` | 0 / 555 |
| `Wave._input_hash` | 0 / 2,321 |

Nothing moved in any country, and there were no errors. That covers the `sampling.py` edit (which now imports `derivations`), the new `.yml`, both CONTENTS.org edits and the docs.

**Other measurements:**
- `Country(C).sampling` loads for every country; 19 tagged cells (`sampling-load-r2-0d62f49df.txt`).
- Served-strata dump: `served-strata-dump-0d62f49df.txt` and `served-strata-dump.csv`.

**Read-path changes** (attrs only, no data):
- Every Senegal frame now carries `attrs['sampling']`.
- Mali 2014-15, Niger 2011-12, Togo 2018 and Côte d'Ivoire 2018-19 used to get the `unrecorded` placeholder (`design_tag='unrecorded'`). They now get a record with `design_tag` None plus `served_strata`, the same shape panel-only records already have.
- No Site C warning changes: none of the new entries has a `panel` block.

## 7. Tests
**Unit run** (job 41276185):
```
pytest -n 8 --dist=loadfile -p no:cacheprovider -m "not requires_s3"
  tests/test_sampling.py tests/test_sample.py tests/test_schema_consistency.py
  tests/test_build_transform_hash.py tests/test_gh1494_mali_eaci_namespace.py
  tests/test_gh812_ehcvm_strata.py tests/test_feature_attrs_carriers.py
  tests/test_gh1503_record_caches_do_not_cross_tests.py
```
Result: **779 passed, 0 failed, 0 skipped.** Job 41275930 got the same result.

**Delivered run** (requires S3, cold): `-k test_the_tag_agrees_with_the_served_strata` gave **19 passed**.

**Not run:** the other 695 `requires_s3` tests in those files (714 collected in all, 779 deselected; `pytest-r3-collect-s3-0d62f49df.txt`), and the full suite. These are cold-build costs, and none of them reads `served_strata`.

**New tests in `test_sampling.py`:**
- `TestServedStrataIsItsOwnRecord`: placement, the triple not needed beside `panel`, the triple still needed with a design field, empty entries still refused.
- `TestServedStrataRefusals`: every refusal in §2.
- `TestServedStrataIsFrozen`: values and fields used both ways, first records, citations.
- `TestServedStrataAgreesWithTheRegistry`: both directions, filtered on the `strata` column rather than `::sample::`, the named pending list, and native/absent tags never contradicted by a registry entry.
- The delivered test above.

**Another stream's test edited:** `tests/test_gh1494_mali_eaci_namespace.py` asserted Mali's record keys exactly. It now includes 2014-15 and asserts that 2014-15 has no `panel` block.

## 8. Issue status and drafted residuals
- **#1577: partial.** 19 cells are recorded and 3 proposed; the rest is the table in §4.
- **#1134: stays open.** No `design_tag` was added; its rule was applied to the new field.
- **#1574: stays open.** Mali 2017-18 is recorded as `native-code` and its labels are still missing.
- **#792: not addressed** (weight availability is outside this brief).

**R1 — "served_strata for the remaining strata-serving waves (GH #1577 residual)", tier C.** Body: §4's table and Q1-Q2. Native cells need a value census or a file-metadata read to settle code versus label. `absent` cells need a written search record. Cache cost: 0 (measured).

**R2 — "Strata constructions in Ethiopia, GhanaAHIES and EthiopiaRHS are not registered derivations", tier B.** Body:
- Ethiopia builds region x Rural in each wave's `sample.py`; GhanaAHIES builds `{region}-{Urban|Rural}`; EthiopiaRHS sets `strata = v`.
- None is in a `derivations.yml`, so no `Derivation` is stamped and `served_strata` cannot point at them. Per CLAUDE.md, an unregistered derivation is a defect.
- EthiopiaRHS's uniform `weight = 1.0` is also an unregistered construction.
- Fixing this costs a re-warm of each country.

**R3 — "A one-wave sample() omits a declared-required column the wave does not wire (Mali 2014-15 `strata`)", tier C.** Body: `Country('Mali').sample(waves=['2014-15'])` has no `strata` column, but `data_scheme.yml` declares it required. It is NA in all-wave frames. Check whether this is general, and whether Site B or the frame should carry the column as NA.

## 9. Questions the rulings do not answer
1. **A region, district or province served as `strata`** (GhanaLSS, Côte d'Ivoire CILSS, Malawi, Nigeria W1-W3, Uganda 2011-12 onward, Pakistan, South Africa, Serbia, Guatemala, GhanaSPS, Albania 2012). Options:
   - (a) a new value, for example `native-region`;
   - (b) widen `native-label` to mean "a recorded variable, as recorded";
   - (c) leave these unrecorded.
2. **A native code decoded by the library's own lookup** (Burkina Faso 2014, Nigeria W5). Options:
   - (a) a new value;
   - (b) register the decoding as a derivation and tag it `constructed`;
   - (c) call it `native-label`.
3. **Source type.** Every record uses `derived-from-data`. Should a document that names the stratum variable earn `local-documentation` on this tag instead?
4. **CLAUDE.md.** Should its Site C / sampling section mention `served_strata`? I left CLAUDE.md untouched.

---


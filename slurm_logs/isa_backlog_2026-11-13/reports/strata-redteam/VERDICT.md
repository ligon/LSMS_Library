**Verdict: CHANGES.** No blocker. Candidate `0d62f49df61bb43190275aaa3a2520f724eed6af` (branch `feature/1577-strata-tag-20261113`), base `f138a3d61c203f892148592675a7ef72e160a322`.

All 19 records are true against the served frames, and every native record is also true against the raw files. Nothing I measured moves a served value, a hash or a warning. The changes I ask for are prose this PR adds that my data contradict (F1), a validator hole (F2), an integration hazard (F3), a weak truth test (F4) and the docs (F7).

Scripts, logs and outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/strata-redteam/{scripts,logs,out}/`. Every comparison was cold, base against candidate, in separate private roots.

## What I re-ran, independently

| job | what | result |
|---|---|---|
| 41276659 | hash census on both sides (`out/j1/hashdiff.json`) | `build_transforms_fingerprint` 0/42, framework imports 0/38, `Country._table_cache_hash` 0/555, `Wave._input_hash` 0/2,321, 0 errors. **The worker's claim holds.** |
| 41276659 | `Country(c).sampling` loads for every country, both sides (`out/j1/records-{base,cand}.json`) | The only differences are the added `served_strata` blocks, the 4 new first-wave entries and the new Senegal file. No pre-existing field changed. |
| 41276659 | the **whole** non-S3 suite on the candidate (`out/j1/pytest-nons3-cand.txt`) | 15,569 passed, 35 skipped, 4 xfailed, **0 failed**. |
| 41276686 | 57 served calls on both sides (`out/j2/compare.json`) | See below. |
| 41276936 | `requires_s3` tests in `test_sampling.py`, `test_gh1494`, `test_gh812`, candidate only | 93 passed, 0 failed. Includes `TestTheReproduction`, the delivered tag test and the GhanaLSS / Uganda attrs tests. |
| 41276707 / 41276883 / 41277076 / 41277169 | raw headers, sweeps, served against raw | Results below. |

**The 57 served calls (job 41276686).** They cover, for the 7 countries:
- `sample()` for all waves, per wave, and all waves under `LSMS_CLUSTER_STRICT=1`;
- `cluster_features()`;
- Senegal `household_roster` (also under the strict lever), Mali roster 2014-15, Guatemala `sample`;
- three `Feature` calls, Guatemala being a country with no `sampling.yml`.

Results:
- **53 frames are equal** (`assert_frame_equal`, exact). The 4 strict calls (Burkina, Mali, Niger, Uganda) raise the identical `ClusterIdentityError` on both sides.
- **No warning of any class differs on any call.** Site C warning count and text and `sampling_reports()` are identical. **The worker's "no Site C change" claim holds.**
- `attrs['sampling']` differs exactly as described: Senegal records are new, and Mali 2014-15, Niger 2011-12, Togo 2018 and Côte d'Ivoire 2018-19 replace the `design_tag='unrecorded'` placeholders.
- `Feature('sample')` over Senegal, Mali, Uganda, Guatemala and Niger carries Senegal's records. Guatemala contributes none, which is correct.
- A `to_parquet` round trip keeps attrs equal, and `records_from_attrs` returns `strata_holds` (18 records on the candidate, 16 on base).

**The records against the data.**
- **native-code, Mali 2017-18.** `EACI17_ECHANTILLON.strate` has variable label `codestrate`, no label set and 0 value labels. A Latin-1 sweep of all 49 files of the wave finds no other stratum variable and no value labels for `strate`. Raw and served counts are identical for all 47 codes.
- **native-label, Niger 2011-12.** `strate` is labelled "strata" with 27 value labels. Served equals the raw label on 3,968 of 3,968 households, matched by id.
- **native-label, Niger 2014-15.** `strate` in the consumption file is labelled "Strate" with 27 labels. 270 grappes, none with two strata. All 3,617 served households match their grappe's `strate`. All 3,617 welfare households are in the consumption file.
- **native-label, Uganda 2005-06, 2009-10, 2010-11.** `GSEC1.stratum` is labelled "UNPS Strata of Representativeness" with 6 labels. Served counts equal the raw counts after the `strata` fold, label by label, in all three waves. The 7 NA in 2009-10 are 3 raw NaN plus 4 frame-extension households. Uganda 2011-12 has no `stratum`; its `sregion` is labelled "10 sub region", so the worker's exclusion holds by their own definition.
- **constructed, all 12.** The pointed key is on every non-null row and on no other row (for example Burkina 7,010/7,010 and Côte d'Ivoire 12,992/12,992). No other strata key appears in any frame.
- **absent, Mali 2014-15.** Served NA on every row. The 55-file sweep finds only `deflator` ("Strata deflator") plus coping-strategy variables. `EACIPOIDS` holds only grappe, menage and `poids_menage`. `EACICONTROLE_p1` holds region, cercle, arrondissement and milieu, but no stratum.

**Counts in the notes, checked on served data.**
- Côte d'Ivoire grappe 648 holds 11 Rural and 1 Urbain.
- Togo's Lomé label sits on the same 98 grappes in both waves.
- Niger: 35 of 5,055 linked households change stratum. Senegal: 0 of 6,432. Burkina: 42 of 3,227 (CONTENTS gives 30 + 12).
- Burkina 2014 is decoded on both sides: 26 labels, 10,800 non-null.

**Four leads came back clean** and are verified, not findings:
- Senegal: neither wave's files carry `strate`. `ehcvm_nsu_sen2021.dta` holds `region` and `milieu` only, and the weights files hold no stratum.
- The Uganda `sample` hook writes no `strata`.
- Niger 2014-15 holds household by household.
- Burkina 2014's decode is live at base.

## Findings

**F1 — should-fix, introduced prose; the data contradict it.**
- **Claim.** The PR says a one-wave Mali 2014-15 `sample()` "has no `strata` column at all". It says so in:
  - Mali `CONTENTS.org` (new section, "An idiosyncrasy of the absent wave, measured");
  - the `"absent"` comment in `sampling.py` `STRATA_HOLDS`;
  - the comment in `test_the_tag_agrees_with_the_served_strata`;
  - residual R3.
- **What I measured** (job 41276936, `out/j4/mali-order-{cand,base}.txt`). The shape depends on cache state, identically on base:

  | call, in a fresh root | `strata` column |
  |---|---|
  | cold, one wave first | absent (and no `Derivation`) |
  | after `sample()` for all waves | present, 0 of 3,804 non-null, with `Derivation` |

- **Fix.** Reword all four places to describe this order dependence. Reframe R3 as a pre-existing build/read-composition defect: CLAUDE.md says "a served value that depends on cache state is a defect to fix".

**F2 — should-fix, introduced validator gap** (job 41276659, `out/j1/probes.txt`).
- A padded `derivation: ' Mali::sample::region-milieu-stratum '` is **accepted**. `parse_key` strips the slots before the country/table check, but the record stores the raw string, so it matches no registry key and no `Derivation` stamp.
- The shipped-record test would catch it; the validator does not.
- **Fix.** Refuse a key that is not equal to `format_key(*parse_key(key))`.

**F3 — should-fix, integration hazard; the loader behaviour is pre-existing.**
- `_SamplingLoader` silently keeps the last of duplicate keys. My probe confirms this for both a wave key and `served_strata`.
- The proposed Benin and GNB snippets (`reports/strata/out/proposed-*.yml`) are written as whole `'2021-22':` entries.
- If pasted as a second entry, they delete the existing `panel:` block. For Benin, `TestShippedDeclarations` (`"Benin","2021-22","stable"`) catches it. For GNB 2021-22, whose `panel` block the gnb branch creates (`69f2d2fb3`), nothing catches it.
- **Fix.** Sue merges the blocks into the existing entries by hand. Optionally, a later change makes the loader refuse duplicate keys.

**F4 — should-fix, the truth test is weak.**
- The only test that the tags are *true* is `requires_s3`.
- Its checks are loose: native-label means "no value is a bare code", which any library-built label passes (Burkina 2014's decoded labels, or Ethiopia's unregistered construction). Native-code means "digit strings".
- Nothing ties `served_strata.variable` to the wave's config.
- **Fix.** Add a non-S3 static test: each native record's `variable` names the source variable and file that the wave's `data_info.yml` maps to `strata`, and that wave applies no relabelling `strata` formatter beyond `format_id`. All 7 native records pass that check today (above).

**F5 — question for the user; scope beyond the ruling's text.**
- **The ruling** says the tag covers "native-code, native-label and absent cases" and that "constructed strata stay in the derivations registry".
- **What was built.** `constructed` (12 of the 19 records) and the two-way registry↔tag test come from Sue's brief, which asked for "a pointer to the registry".
- **What it costs.** Every future strata registration must also edit `sampling.yml`, or be listed in `SERVED_STRATA_PENDING`. That includes the R2 cases (Ethiopia, GhanaAHIES, EthiopiaRHS).
- **Concrete consequence at integration.** The gnb branch extends `Guinea-Bissau::sample::region-milieu-stratum` to 2018-19 and 2021-22, but the proposal tags 2018-19 only.
  - Paste it and remove GNB from the pending list: `test_every_registered_strata_construction_is_tagged` fails until a 2021-22 block is added.
  - Paste it and keep GNB pending: it passes silently half-tagged, because the pending test checks only `tagged != cells`. Tighten that to "nothing tagged".
- **Alternative.** Derive `constructed` at read time from the registry, with no second copy.

**F6 — note, vocabulary rule.**
- One PR adds a new top-level field, 7 nested field names and 4 values.
- Each value's first record is evidenced and verified true, and the two-way freeze test is in place. So #1134's substance holds; its letter ("one tag at a time") does not.
- The user should confirm this reading.

**F7 — should-fix, docs** (`docs/guide/feature.md`).
- `Country(c).sampling[wave].strata_holds` raises `KeyError` for an uncurated wave (`Country.sampling` is a plain dict). That covers Burkina 2014, the Côte d'Ivoire CILSS waves and every wave of a country without the file. `None` comes back only for curated waves that carry no tag. Use `.get(wave)`, or point readers to the attrs placeholders.
- "Region alone | GhanaLSS 1998-99 onward" is wrong: 1991-92 serves `region` too (`GhanaLSS/1991-92/_/data_info.yml:110`), as the worker's own §4 says.
- The table needs GNB 2021-22 once the gnb branch merges.

**F8 — note.**
- `_check_vocab`'s refusal message tells a curator that `served_strata.holds` was "frozen from the Uganda exemplar, GH #793". That is wrong for this vocabulary.
- The `CONFIDENCE_LEVELS` legend defines confidence for a *design* statement. `served_strata` reuses it with another meaning, as `panel` already does. Add one line saying so.

**F9 — note.** The Mali 2014-15 locator drops CONTENTS' qualifier "except `deflator` … ('Strata deflator')". The note keeps it; quote CONTENTS exactly in the locator too.

**F10 — note.**
- Mali 2021-22's note says "429 households in both waves change stratum on the label alone".
- On served data (job 41277169) it is 415 of 5,011 linked households, in 36 clusters, and all three pairs are relabels: Menaka→Gao Rural 195, Taoudénit→Tombouctou Rural 154, Menaka→Gao Urbain 66.
- So "label alone" holds. The 429 of 6,133 is CONTENTS' count over a different household set; say which set, or cite it as CONTENTS' figure.

**F11 — note, merge mechanics.** These end-of-file appends will conflict, and resolve by concatenation:
- Mali CONTENTS: this branch, coping `9abab1855`, cropkg `49aea7436`.
- Burkina CONTENTS: this branch, coping, burkina `d4d063fa1`.

**F12 — note.** The four former placeholder waves now read `design_tag` None instead of `'unrecorded'`, the same shape panel-only records already have. Any count of uncurated waves by `design_tag == 'unrecorded'` drops them. The worker disclosed this.

## Other brief items
- **Item 7, CONTENTS contradictions.** No passage I read contradicts a record, apart from F1 (prose this PR adds) and F10 (a count). The worker's Togo phrase "internally implausible" comes from Togo CONTENTS itself.
- **Item 8, the edit to `test_gh1494`.** It is minimal: it adds 2014-15 to the asserted key set and asserts 2014-15 has no `panel` block. The test's purpose (the programme switch is declared) is unchanged. Fine.

## Must-state items for the PR body
1. Nothing measured moves a served value, hash (0 of all four kinds) or warning; the red team confirmed this independently.
2. Mali 2014-15's one-wave column shape depends on cache state, and is the same on base (F1, corrected text).
3. `constructed` comes from the brief, beyond the ruling's literal scope, and the registry↔tag sync test obliges future registrations (F5).
4. Four values arrived in one PR, each with a verified first record (F6).
5. `Country(c).sampling[w]` raises `KeyError` on an uncurated wave.
6. Integration steps:
   - add GNB 2021-22;
   - merge the Benin and GNB blocks into existing entries, never as duplicate keys;
   - resolve the CONTENTS append conflicts.
7. The attrs changes: Senegal frames now carry `attrs['sampling']`, and the 4 former placeholder waves get records.

## The worker's questions
- **Q1, region/district served as `strata`.** The framing lumps two cases that the evidence separates.
  - *(i) A geographic variable the library's own design record names as the stratification.* Uganda 2011-12 onward: the shipped record says `stratification.variable: sregion`. Its GSEC1 label is "10 sub region", not "stratum" (I read both).
  - *(ii) A geographic variable coarser than the documented stratum.* GhanaLSS (design `(strata, Rural)`), Malawi district and the Côte d'Ivoire CILSS waves' `REGION`. This rests on CONTENTS and the worker's table; I did not read those files.
  - Option (b), widening `native-label`, would label (ii) native and erase the distinction the tag exists to draw. (i) may qualify under the current definition once a document is cited. (ii) needs either its own value or to stay unrecorded.
- **Q2, codes decoded by a library lookup.**
  - I read that Burkina 2014's decode is live and that the served labels equal the EHCVM region × milieu labels. "No value labels" comes from CONTENTS only.
  - The #1573 ruling calls it "recorded-code decoding" and keeps the lookup invertible.
  - Option (c), `native-label`, would be false by the module's own definition.
  - Option (b), register it and tag `constructed`, has a precedent in the Malawi #1228 relabel ("Register all waves"). It costs a Burkina re-warm, because the stamp lands in `2014/_/mapping.py`. Nigeria W5 is the same case (from the worker's table).
- **Q3, source type.** A fair question. All 19 records are `derived-from-data`, so `rec.documented` is False on every one, although Mali 2017-18 and Niger rest partly on a BID.
- **Q4, CLAUDE.md.** A fair question. Its Site C section still says the vocabulary is "FROZEN from the Uganda exemplar … one `design_tag` at a time" and does not mention the new vocabulary. Add a line at integration.

## Limits
- PDF text extraction failed on the burst nodes, so the Senegal note's "no held 2018-19 document states it" is **unverified**.
- My sweeps match names and labels against a regex, and are supplemented by full variable lists for the weights and cover files only.
- Uganda was matched on counts, not household by household.
- I did not read the CONTENTS files in full: only every `strat` hit, the passages cited by the 19 locators, and the TODO/WAITING/LOGBOOK headings.
- Integration effects are reasoned from the other branches' diffs, not executed.
- The S3 tests covered three files, candidate only.
- The Benin and GNB proposals were not checked against data.
- **Process disclosure:** I ran `python3` three times on n0035 by mistake: two empty heredoc scripts and one `python3 -c 1`. None imported `lsms_library` or read any data. All real Python ran in burst jobs.

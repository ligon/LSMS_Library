**Verdict: APPROVE.** Candidate `fc40960d27b800045984735a38c68cf91c9b2194`, base `f138a3d61c203f892148592675a7ef72e160a322`.

I re-ran every headline number and every "frame-equal" claim cold, base against candidate, in separate private roots, with my own scripts. All of them reproduce. I found no blocker. Two should-fix items: one is report text only; the other needs a small follow-up commit.

## Findings

**F1 (should-fix, report and question text). The worker's Q1 / R2 lists only Senegal and Côte d'Ivoire as still serving the gift question as the purchase. Niger 2018-19 and Niger 2021-22 have the same defect.**
- `Niger/2018-19/_/plot_inputs.py:44` and `Niger/2021-22/_/plot_inputs.py:48,57` read `s16bq05` / `s16bq07a`.
- Both files label `s16bq05` "Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?" and `s16bq08` "Avez-vous acheté [INTRANT]?" (`out/raw.json`, job 41277339). Niger's CONTENTS (~line 2030) already records it.
- Niger 2018-19: gift Oui 1,383, purchase Oui 4,557. Gift × purchase: Non/Non 7,602, Non/Oui 4,416, Oui/Non 1,242, Oui/Oui 141.
- Niger 2021-22: gift Oui 856, purchase Oui 3,593. Cross-tab 8,052 / 3,522 / 785 / 71, plus 9 blank.
- Mali (`s16b_header_adapter.py:179-181`) and Burkina are correct.
- So four cells remain after this batch: Senegal 2018-19, Côte d'Ivoire 2018-19, Niger 2018-19 and Niger 2021-22. Niger 2021-22 is the only 2021-22 wave still wrong.
- Two tests pin the wrong binding:
  - `tests/test_ehcvm_plot_inputs_identity.py:217-230` (both Niger waves, Senegal 2018-19, Côte d'Ivoire 2018-19);
  - `tests/test_senegal_1483_s16b.py:629-630`.
- Fix: list all four cells and both pins in Q1 / R2.

**F2 (should-fix). Two comments in files this branch already edits now state something false.**
- `Senegal/2018-19/_/plot_inputs.py:14` says the seed crop is "resolved to the harmonize_food crop label via harmonize_seed_crop".
- The `plot_inputs` comment in `Senegal/_/data_scheme.yml` says "the seed's crop on the shared harmonize_food labels".
- Both are false for maize and cowpea, which now take `crop_production`'s labels.
- Lines 9-10 of the same script still call `s16bq05` / `s16bq07a` "purchased?". That mislabel is older and recorded in CONTENTS, but it can be fixed at the same time.
- No extra hash cost: both files already move every Senegal hash.
- Constraints on the fix:
  - keep the `src['s16bq05']` and `srcn['s16bq07a']` tokens, and do not write `srcn['s16bq09a']` (pinned at `test_senegal_1483_s16b.py:629-630`);
  - re-pin the `test_gh1500` scheme digest again;
  - re-run those two tests and the hash census afterwards. This verdict covers `fc40960d2` only.

**F3 (note). Stale Togo prose.**
- The worker discloses two: the `2021-22/_/plot_inputs.py` docstring (lines 10-13) and the note at `_/categorical_mapping.org:199-201`.
- Not mentioned: `.coder/ledger/323-benin-togo.md:86` still has the reversed grappe 101 / menage 9 reading. It is not hashed, so fixing it is free.

**F4 (note). Precision in the Togo CONTENTS entry.**
- "Same labels as 2021-22" holds for the two flags and the 12-answer `s16bq09b` card. The quantity questions are worded differently: 2018 q07a "Selon vous, quantité … reçue" and q09a "Quelles sont la quantité et la valeur totale … achetées?"; 2021-22 "Quelle quantité … reçue" and "Quelle est la quantité … achetée?".
- Undisclosed: 6 purchase quantities and 1 gift quantity in 2018 have no unit (`s16bq09a` 7,128 against `s16bq09b` 7,122; `s16bq07a` 804 against `s16bq07b` 803). They are served with an NA `Unit_*`. Add this to CONTENTS and the PR body.

**F5 (note). The worker's R1 is confirmed by inspection.**
- `Togo/_/plot_features.py:19-28` and `Benin/_/plot_features.py:18-27` still loop over a hardcoded `WAVES` with `except Exception: continue`, and both now have two declared waves. Côte d'Ivoire has no country-level concatenator.
- This bears on Q2: a Togo R1 fix edits a country `_/*.py`, which is a build input of every Togo wave. It would move the 22 Togo 2021-22 wave hashes anyway, so the two stale Togo comments could ride along for free. R1 is outside the brief, so it is the user's scope call.

**F6 (note). Q3 should be reframed.**
- Senegal 2021-22 `plot_inputs` serves 26 columns that `Senegal/_/data_scheme.yml` does not declare: 19 raw fields (`grappe`, `menage`, `vague`, `s16bq01`…`s16bq09c`) and 7 `*_unlisted` adapter flags.
- They come from `_/s16b_2021.py`, whose `s16b_plot_inputs` returns its whole transport frame.
- `Feature` coercing the object flags to strings is the symptom. Base shows it identically (`compare2.json`, `feature_vs_country_base`, 9,368 rows × 7 columns).
- File it against the #1483 adapter (declare the columns or drop them), not as a `Feature` dtype bug.

**F7 (note). Merge conflicts with other streams.**
- Stream heads checked: benin `d5fdf326c`, burkina `f962ef7c9`, gnb `90bdd6742`, coping `3336ffc99`, strata `79739f816`, cropkg `b3365f8d4`, nigeria `3d34550d5`, malawi `5093b4939`, ci-1714 `629a7cb9f`, ci-1716 `38a3b0865`.
- None of them touches `test_962_unit_sentinel.py`, `test_gh323_benin_togo.py`, `test_ehcvm_plot_inputs_identity.py` or `test_senegal_1483_s16b.py`. The worker's "probably touch" is wrong as of these heads.
- Coping does edit `test_gh1500`, but in a different function (~line 352, the mapping.py AST digest). It merges cleanly, and both pins stay valid because coping does not touch Senegal's `data_scheme.yml` or `categorical_mapping.org`.
- The real textual conflicts are the end-of-file appends to the Togo, Senegal and Côte d'Ivoire `CONTENTS.org`, against coping. Keep both sides.
- Coping also edits the 2018 `mapping.py` of all three countries, so it is still one re-warm per country.

**F8 (note). Côte d'Ivoire cost.**
- The 2018-19 edit serves 0 rows but moves 21 2018-19 wave hashes.
- The 2021-22 edit moves 21 wave hashes for 4 rows, and those 4 rows are the country's whole served effect.
- This is within #1118's contract (no minted fill left anywhere), but the PR body should state it.

**F9 (note). Schema.**
- The canonical `data_info.yml` declares no `plot_inputs` acquisition columns, so nothing canonical was checked. The names and types are a per-country convention, and Togo matches Burkina, Benin, Côte d'Ivoire and Togo 2021-22.
- The four columns stay `optional: true` even though both waves now fill them. That is acceptable: it does not hide an empty wave.

## What I re-ran and confirmed (outputs under `reports/manquant-redteam/out/`)

**Raw files (job 41277339)**
- Togo 2018 variable and value labels: `s16bq05` / `s16bq08` are coded 1 = Oui, 2 = Non, with no unlabelled codes and no stray whitespace.
- Togo 2018: 3 lines blank on both flags; the cross-tab 5,877 / 7,049 / 725 / 79 matches the worker's; `s16bq07a` is filled exactly where `s16bq05` = Oui and `s16bq09a` exactly where `s16bq08` = Oui.
- Togo 2018 unit cards: `s16bq09b` has 12 answers; `s16bq03b` and `s16bq07b` have 5.
- Grappe 101 / menage 9: the 16 Charrette of tuber cuttings came from "Autre paysan ou ménage" as a gift (`s16bq05` Oui, `s16bq06` Autre ménage, `s16bq07a` 16, `s16bq08` Non). The 3 Charrette of "Autres semences" were bought at Marché/Boutique (`s16bq08` Oui, `s16bq09c` 6,000 FCFA). **The worker's correction of the #323 note is right.**
- Senegal: the `s16cq04` crop labels `Maïs` and `Niébé` are byte-identical (NFC) to the new map cells.
- Côte d'Ivoire 2021-22: 4 blank-unit lines, none of them in the 38 damaged households.

**Togo, cold base vs candidate (job 41277340)**
- 29,772 rows on both sides, and the keys are equal once base `Manquant` is read as `Unknown` (9 rows).
- 2018 cells changed, exactly as claimed: `Purchased` 7,774, `Quantity_purchased` 7,833, `Received_as_gift` 13,730, `Quantity_received_as_gift` 804, `Unit_purchased` 7,122, `Unit_received_as_gift` 803. 2021-22: 0.
- Base `Purchased` / `Quantity_purchased` equal candidate `Received_as_gift` / `Quantity_received_as_gift` on 13,730 of 13,730 rows, joined by key.
- Candidate `Purchased` is 7,128 True / 6,602 False / 0 NA.
- The `u` path and the `Unit_*` path agree on all 12 raw unit labels, and the vocabulary matches 2021-22.
- `seed_kg` and `nitrogen_kg` are frame-equal.
- `Feature('plot_inputs')` equals the Country table on the index and every cell (job 41277480).

**Senegal (job 41277341)**
- 19,284 rows on both sides; only the crop label moves: 735 / 460 rows in 2018-19, 555 / 386 in 2021-22.
- Seed rows with a harvest row for the same `(t, i, crop)`: maize 0 → 723, cowpea 0 → 448.
- The other named seeds already joined before this change (Arachide 1,597 of 1,610, Mil 1,197 of 1,210, Riz Paddy 476 of 481, …).
- The 2021-22 injectivity guards pass, so the shared map is safe for 2021-22.
- `crop_production` (8,416 rows), `plot_features` (13,824), `seed_kg` and `nitrogen_kg` are frame-equal.

**#1287 routes (job 41277358)** — I instrumented copies of `_/plot_features.py` to log when they run.
- Default backend: the concatenator never runs.
- `LSMS_BUILD_BACKEND=make`: it runs exactly once, inside the private build root. Constructing `Country('Senegal')` there takes 0.02 s, loads no panel ids, and causes no recursion.
- With the 2021-22 wave script made to fail, both Country routes already raised at base (`CalledProcessError` on the default route; the #1288 `RuntimeError` on the make route), and still do.
- Direct run with the 2021-22 wave parquet removed: base exits 0 and writes 7,767 rows of 2018-19 alone; the candidate raises `FileNotFoundError` naming the wave and writes nothing.
- Healthy builds are frame-equal on the default route, the make route and the direct run.

**Côte d'Ivoire (job 41277342)**
- 4 rows go `Manquant` → `Unknown`; base served no `Unknown` row in 2021-22.
- The grain report is identical on both sides, `seed_kg` and `nitrogen_kg` are equal, and Country equals Feature.

**Hash census (job 41277359, fresh on both SHAs)**

| kind | moved |
|---|---|
| `build_transforms_fingerprint` | 0 of 42 |
| `framework_imports_fingerprint` | 0 of 38 |
| `Country._table_cache_hash` | 66 of 555 (Côte d'Ivoire 21, Senegal 23, Togo 22) |
| `Wave._input_hash` | 110 of 2,321 (Côte d'Ivoire 21 + 21, Senegal 23 + 23, Togo 2018 22) |

- Nothing moves outside the three countries, and my base census is identical to the worker's.
- All three `data_scheme.yml` files parse equal to base, so those edits are comments only.
- I confirmed that `lsms_library.paths` is in `_EXCLUDED_MODULES` (`_build_registry.py:99`). That `Country` folds nothing because it is a class is inferred, not opened.

**Tests**
- The worker's 65 files, plus 9 more, at the head: 4,821 passed, 110 skipped, 8 xfailed, 0 failed (job 41277359). The worker had never run the full set at `fc40960d2`.
- 54 further cross-country files found by my own grep: 1,691 passed, 2,724 skipped, 1 failed (job 41277937).
- The one failure is `test_table_structure::test_at_least_one_country_has_cached_data`. It fails because my root was empty and the run did not opt into cold mode. That is environmental by inspection; I did not run it on base.

**Re-pins in shared test files:** all justified. Each follows from a change the branch makes, and the two Senegal source files re-pinned in `test_gh1500` are changed only off the food `u` path.

## Limits on what I verified
- Cold builds only; nothing on a warm cache.
- Frame-compared only `plot_inputs` (with `seed_kg`, `nitrogen_kg` and the Feature table) in the three countries, plus Senegal's `crop_production` and `plot_features`. The other tables are argued statically: the scheme files parse equal, and only the two `plot_inputs` drivers read `harmonize_seed_crop`.
- `attrs` compared by key, not by value.
- `test_table_structure` checked nothing in my run or the worker's (all 452 pairs skipped).
- My supplementary test selection dropped other countries' files by a name filter.
- The Benin, Burkina and Guinea-Bissau halves cannot be tested together until integration.
- The Senegal concatenator writes 13,879 rows and the API serves 13,824, on both sides. I did not chase the difference.
- I did not test the `LSMS_NO_CACHE` route.
- **Rule breach:** I ran `python3 -c` on n0035 about five times to parse small JSON outputs, before switching to `jq`. It imported no `lsms_library` and touched no data or cache.

## Must-state items for the PR body
1. Togo 2018 `Purchased` changes meaning:
   - True goes 804 → 7,128;
   - 725 lines served as purchases were gifts that were not bought, and 7,049 bought lines were served False;
   - `Quantity_purchased` is in `Unit_purchased`, not `u`;
   - 6 purchase quantities and 1 gift quantity have no unit;
   - the old answers survive row for row in the new gift columns.
2. Senegal's seed relabel covers both waves (2,136 rows). The join gain exists only in 2018-19, because 2021-22 has no `crop_production`.
3. Côte d'Ivoire 2021-22: 4 rows, at a site the #1118 inventory missed.
4. #1287: the refusal changes only the direct-run route; both Country routes already raised.
5. The remaining gift/purchase cells are the four in F1.
6. Cost: one cold re-warm each of Togo, Senegal and Côte d'Ivoire (66 country and 110 wave hashes), no `build_transforms_fingerprint` move, no other country.
7. The stale comments left behind (F2 unless fixed, F3).
8. #1118, #1119 and #1287 are all partial ("Addresses", not "Closes").

## The worker's three questions
1. **Gift fix for Senegal and Côte d'Ivoire:** the framing understates the scope. Four cells remain, Niger's two among them, and two test files pin the old binding (F1).
2. **The two stale Togo comments:** the cost framing is fair. All Togo table hashes move anyway, so fixing them adds 22 Togo 2021-22 wave hashes, at no added cost if a Togo R1 fix is done in the same re-warm (F5). Against that, a tracked docstring now says something false.
3. **The Senegal Feature dtype:** the observation is correct and predates the branch, but it is a symptom. The issue to file is 26 undeclared adapter columns reaching the served table (F6).

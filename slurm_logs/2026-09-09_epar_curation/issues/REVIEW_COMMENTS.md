# Review of the nine GitHub comment drafts, 2026-09-09

Reviewer pass over `COMMENT_{850,820,824,844,846,585,737,439,438}.md`. Every
`path:line` in every draft was opened -- ours and EPAR's -- and the sentence it
supports was checked against the text. All nine files were edited in place.
Nothing was posted.

Reference clones: `LSMS-Agricultural-Indicators-Code` @ `abfbdc9301242527b7543606aad1ad6eed2e6530`,
`Household-Consumption-Data` @ `0e9ccf2981e7cb8448605925a5111d2835bf1e31`
(`reference/epar/NOTES.md`).

Global checks, all nine files: ASCII only (0 non-ASCII bytes each; em-dashes,
`>=`, `x`, section signs replaced); no `NEW_nn` / `COMMENT_nn` filenames; no
"worth" / "genuinely" / praise; 13-20 lines each (range required: 10-25).

## Substantive corrections (a claim was wrong, not just a line number)

| file | wrong claim | what the source says |
|---|---|---|
| 737 | EPAR ships a TLU coefficient "in both countries it ships one for" (Malawi, Nigeria) | **All five ISA countries; 27 do-files define `tlu_coefficient`.** Cattle 0.5 in every one. Rewritten. |
| 737 | (absent) | EPAR agrees with `_TLU_FACTORS` on sheep/goats 0.1, pigs 0.2, poultry 0.01 in all five. That corroborates ours against ERHS's 0.2 for small ruminants in the thread's 2026-08-25 comment. Added -- it is the finding that bears on the open question. |
| 850 | "Concretely live in production today: `Ethiopia/_/conversion_to_kgs.json`" | **Nothing reads it.** Only reference in the tree is a comment at `ethiopia.py:282`. Replaced with the genuinely live case: `Uganda/_/conversion_to_kgs.json` joined on `u` alone at `uganda.py:404-410`, called by all eight Uganda wave scripts. |
| 850 | wave-5 omission presented as a new observation | `Ethiopia/_/CONTENTS.org:279-283` already records it. Now cited, not rediscovered. |
| 844 | `price_unit_hh` "not `gen`'d, `ren`'d, or produced by any merge I could find ... a possible link, not confirmed" | **Resolved.** It is Stata's unique abbreviation of `price_unit_hhid`, the household rung of the collapse at `:503`, merged at `:688` and excluded from the ladder loop at `:702`. Malawi `W1.do:1351-1352` and Nigeria `W4.do:866-867` do the rename explicitly. Also resolved: `hh_crop_unit_values.dta` merges at `:1967` into the PROCESSED-crop block and is used as `price_as_input` (`:1970`), not by the crop-revenue ladder. The gap statement is gone. |
| 585 | Ag repo hh price "only fills the main series where the entire ladder came back missing" (stated of all three cited countries) | True of Uganda `W5.do:546` and Ethiopia `W3.do:1265` only. **Malawi never lets it touch the main series; Nigeria falls back to `val_harvest_est` (`W4.do:873`) and Tanzania to `ag4a_28` (`W5.do:574`).** Rewritten with all five countries. |
| 585 | Nigeria's ladder "further quantity-weighted (`W4.do:696,731`)" | True of the unit-price collapse (`:696` applied at `:704`); the per-kg collapse at `:735` uses `weight_pop_rururb` despite `:731`. Qualified. |
| 824 | (absent) | Added the clause a reader needs: 2018-19 AGSEC5A's `a5aq6c` **does exist** (a second column on the condition scheme, `CONTENTS.org:1031-1035`), which is why `gen unit_code_harv=a5aq6c` does not error. |
| 820 | "already read `sect11_ph_w4.dta` / `sect11_ph_w5.dta` exclusively" | Those scripts read `sect9_ph` for harvest and `sect11_ph` for sale. Corrected; and the comment now states plainly that the issue's 2015-16 measurement stands (W3 ships `sect12_ph_w3.dta`). |
| 820 | EPAR's "folded section 12 into section 11" read as established | Now attributed to EPAR's own comment, with our `find` called corroboration and the questionnaire check named as not run. |
| 438 | "plus three countries outside it" then four named | Four: GhanaSPS, Mali, Niger, India. Corrected. |
| 438 | India listed without caveat | India has a do-file but appears in neither the README country sentence nor its data-source table. Flagged. |
| 438 | "identical set of 35 assignment lines" | One `gen` plus **34** `replace` lines. `diff` of `UgandaW4:888-922` vs `NigeriaW4:972-1006` is empty -- "identical" holds. |
| 438 | "folds items into ~80 buckets" | **Measured: 28 distinct `crop_category1` in Uganda W4, 30 in Nigeria W4 and Senegal W1; 16 `crop_category2`.** Number replaced. |
| 439 | 21 ids "zero-hit repo-wide" | Verified zero-hit across `lsms_library/`; they do occur in `slurm_logs/`. Scope narrowed. |
| 439 | `asset_cd` dropped by `keep hhid \`v'` at `:3507`, `:3534`, `:3561` | Those first two files carry no item code. The drop is `:3557`. Rewritten. |
| 439 | Tanzania `ag3a_08b_*` "live in `ag_sec_3a.dta`" | Verified: EPAR reads that spelling at `W5.do:298-303`; our 2019-20 holds `AG_SEC_3A.dta` and 2020-21 `ag_sec_3a.dta`; `2008-15/Data/` holds no `ag_sec_*` at all. Whether the columns are in *our* copies was not checked -- now stated as unchecked. |
| 439 | "contra an earlier draft that read it as carrying none" | Internal artifact; removed. |
| 439 | no pointer to the schema proposal | Added: "filed as a separate issue alongside this comment". |

## Line-number corrections (claim right, cite off)

- **850**: body `:780-878` -> `:782-878`. `:870-871`, `:873`, `:874-875`, `:791-792`, `:1048`, `:1051`, `:2863` all verified correct.
- **585**: `:2858-2866` -> `:2858-2867` (full signature); docstring `:2891` -> `:2892-2893`; implementation `:3029` -> `:3029-3033`. Added our gate `:3025-3026` with `threshold=10` at `:2864`.
- **737**: `_TLU_FACTORS` `:2601-2612` -> `:2612-2623`; the FAO/ILCA comment `:2596-2598` -> `:2608-2610`. (Both moved by the +11 shift from today's header edit near line 1599.)
- **439**: `ethiopia.py:747` -> `:748`. `data_info.yml:228-232` and `:578`, `DESIGN_erhs_plot_features_2026-05-20.org:54-57` verified correct.
- **820**: `W4.do:759` -> `:759-760` (the quote is two lines).
- **844**: block open `:644` -> `:644` with the quoted lines identified as `:645-647`; sale collapses `:479-509` -> `:499-534` (the range actually verified).
- **438**: `UgandaW4...do:200-284` -> `:202-283`.
- **824**: added the table cite `CONTENTS.org:1004-1020` and the note `:1031-1035`; `:1005-1028` -> `:997-1020`.

All transformations.py citations in all nine files were re-checked for the +11
shift. Only COMMENT_737 carried stale ones.

## Re-derivations run for this review

- **824, all 20 Uganda condition codes are valid unit codes.** `pandas.read_stata`
  (venv python) on `UG_Conv_fact_harvest_table.dta`: 2,181 rows, 89 distinct
  `unit_code`. All 20 codes from `categorical_mapping.org:982-1001`
  (`{11..14,20..24,31,32,33,35,40..45,99}`) are present. **Confirmed, 20/20.**
- **438, crosswalk size.** `diff` of the two `crop_category2` blocks: empty.
  Distinct-value counts as above.
- **737, sourcing.** Case-insensitive grep for `jahnke`, `FAO/ILCA`, `250 kg`
  across every `.do` and `readme.md` in the Ag repo: **zero hits.** Malawi W1
  readme TLU section `:93-97` records "Known Issues: None".
- **439, `dm_ids.dta` columns.** The 11-column list is **confirmed**: six from the
  reshape, `formal_land_rights` from `:398-401`, and `personid, female, age,
  hh_head` from `person_ids.dta` (`:278`).

## Citation verdicts

**Verified as written** (opened, text supports the sentence): 850 `:870-871`,
`:873`, `:874-875`, `:791-792`, `:1048`, `:1051`, `:2863`, `CONTENTS.org:145-172`;
820 `W4.do:1019-1033`, `:3687`, `W5.do:4081`, `crop_production.py` cites;
824 `Master_Conversionfactor_table.do:248`, `:260`, `:271`, `:286`, `W7.do:520-523`,
`:579`, `CONTENTS.org:1016`, `categorical_mapping.org:982-1001`;
844 `:685`, `:686`, `:709-710`, `:1944`, `:1954-1955`, `:1967`;
846 `Tanzania_W5:341`, `:348`, `:354`, `:360`, `:366`, `:372`, `EthiopiaW5:423`,
`UgandaW4:693`;
585 `W5.do:542`, `W1.do:1347-1348`, `W4.do:863-864`, `W5.do:482-483`,
`W1.do:504-505`, `W4.do:695-696`, `ETH W3.do:1253-1265`, `TZ W5.do:565-573`,
`SYNTHESIS.org:750-753` and `:732-736` (23% is Uganda-only, stated at `:754-756`);
737 `MWI W1.do:1514-1518`, `NGA W4.do:1851-1856`;
439 `UG W5.do:413-424`, `:433`, `TZ W3.do:261`, `:263`, `ETH W4.do:515`,
`:3553-3556`, `UG W5.do:3223`, `TZ W3.do:2930/3049/3241/3387`, `:3410-3416`,
Uganda/Ethiopia wave-script cites;
438 `#833` title and the Malawi/Uganda ratios.

**Corrected**: everything in the two tables above.

**Unverifiable, and now stated as such in the comment**: whether ESS4/ESS5
genuinely fold section 12 into section 11 (questionnaire not opened, 820);
whether our Tanzania 2019-20 / 2020-21 copies contain the `ag3a_08b_*` columns
(would require reading the `.dta`, 439); India's provenance in the consumption
repo (438).

## Issue numbers added after the coordinator filed the twelve new issues

Titles confirmed against `gh api` before citing.

- **439**: "a separate issue filed alongside this comment" -> **#862** (Schema
  proposal: `plot_cultivation`, a per-season plot decision-maker table).
- **850**: added a clause naming **#852** (Ethiopia Crop_CF into `harvest_kg`)
  and **#859** (Nigeria per-row `KgFactor`) as the reported-factor route, so the
  price-ratio inference is positioned as the fallback where no reported factor
  exists -- which is where the missing `j` bites.
- **824**: added **#859** parenthetically where the comment addresses Nigeria's
  `Unit_sold` wiring.
- **820**: added **#852** where the comment names the two Ethiopia
  `crop_production.py` scripts the section-12 wiring would touch.

No numbers were added to 585, 737, 438, 844 or 846 -- none of the twelve is on
those threads' subject matter.

Final state: nine files, 13-20 lines each, 0 non-ASCII bytes, no internal
filenames.

## Second pass (cross-comment consistency)

- **844 vs 585 no longer contradict on Nigeria.** 585 says W4's main ladder
  falls back to `val_harvest_est` (`W4.do:873`); 844 said "`sa3q18` feeds
  nothing in the harvest valuation". Checked: **`EPAR_UW_Nigeria_GHS_W4.do`
  contains no `sa3q18` at all** -- `val_harvest_est` is built from `sa3iq6a`
  with `sa3iiiq14` as fallback (`:833-834`). 844's claim is now scoped to W2 and
  the W4 behaviour is stated, with the `sa3iq6a`-is-`sa3q18`-renumbered question
  left explicitly unestablished. This also answers the issue body's "check the
  later waves" item.
- **585's "livestock price" ladder was unverified** and sat inside a sentence
  claiming verification. The only livestock price seen (Nigeria W2 `:1587`) is a
  single national median with no gate. Replaced with two cascades that *were*
  verified on the same `>9` gate: input price (`W4.do:1375`, `:1511`) and wage
  (`:1186`).
- **820**: "#852 touches the same two scripts" overstated -- #852 covers four
  Ethiopia waves. Reworded to "overlaps #852 ... on these two scripts".
- **439**: parcel-rights collapse `:398-401` -> `:397-401` (`gen
  formal_land_rights` is `:397`).
- **737**: replaced the unqualified "in every one of them" with the counted form.
  `grep -rl tlu_coefficient --include=*.do` = **27 files**; all 27 set cattle to
  0.5 (a regex on `= 0.5` returns 26 -- Uganda W2 `:1295` uses a tab before the
  `=`, checked by hand).
- **844**: dropped "deliberately" (asserted intent not visible in the source).

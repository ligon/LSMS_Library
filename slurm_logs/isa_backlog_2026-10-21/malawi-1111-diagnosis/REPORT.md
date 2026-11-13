# Malawi #1111 / #1118 - read-only source diagnosis

Sue (GPT-6), 2026-10-21. Frozen reviewed-organic HEAD `de9c2734b8a6bcf1326d0d4285ff9c1db1221767`; immutable baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
No tracked edits or implementation. Source facts below precede any repair choice.

## Finding

The current driver pairs enumerator-total quantities with timing decoded as units in 2010, 2016 and 2019. The alleged acquired unit `ag_d39c` is actually the reported **applied** quantity's native unit in Module D.
2013 uses the same old caller layout, but its held metadata has generic truncated labels; its exact d/e/i/j roles remain unresolved without C4 authority.
This is not evidence for multiplying existing numeric quantities by 1,000.

Live [#1111](https://github.com/ligon/LSMS_Library/issues/1111), fetched read-only with zero comments, explicitly requires C4, the questionnaire Module D Q39 unit code list, before any code change.
The held 2010 and 2019 instruments address that requirement for their instruments; 2016 has explicit primary native headers but no agriculture questionnaire sidecar; 2013's questionnaire and DDI are unheld. No all-wave implementation is approved.

## Country documentation and contradictions

`Malawi/_/CONTENTS.org` was read in full, including both live TODOs; there are no WAITING/LOGBOOK task entries. Concrete caveats are quantity/unit pairing, harvest versus sale-unit differences, the redundant 2010 Panel beside Full_Sample, different cross-sectional/panel universes, 2016 `cs-17-` household namespacing versus 2019 bare cross-sectional identifiers, and the IHS4 XS unused-string bad byte requiring its restricted reader.

The following are results to reconcile, not assumptions that documentation is stale:

- `CONTENTS.org:3052-3054`: "reads the applied-amount unit (=ag_d39e= / =ag_d39j=) with the acquired-amount code set (=ag_d39c=: 1 = gram, 2 = kilogram, ...)". Held 2010 labels identify e/j as MONTH; c/h are native applied units. Modern e is weeks and j is the second kg total.
- `malawi.py:2905-2913`: "unit value-label set (ag_d39e / ag_d39j) is the SAME bag-size coding" and "Codes 11/12 ... free-text ... residuals". On the held 2010 questionnaire p.13 those are November/December in the month code list.
- Modern wave driver docstrings say "no distinct slot-2 applied-unit column". Both halves of both waves have `ag_d39i`, labelled SECOND APPLICATION UNIT. It belongs to native quantity h, **not** the currently stored kg total j.
- The reviewed organic correction at `CONTENTS.org:3025-3042` is supported by the held instruments/headers; no new evidence contradicts it. Its earlier quoted no-organic-quantity statement remains explicitly superseded.

## Exact field binding

| Wave / loaded extract | Current slot 1 type / Quantity / u source | Current slot 2 | Primary meaning and native pair |
|---|---|---|---|
| 2010 Full_Sample (Panel held but not loaded) | a / d / e | f / i / j | d/i TOTAL KGs; e/j MONTH. Native b/c and g/h |
| 2013 flat | a / d / e | f / i / j | c/h explicitly labelled unit dictionaries; d/e/i/j generic/truncated. Exact numeric/timing roles unresolved |
| 2016 XS + Panel | a / d / e | g / j / absent | d/j ENUMERATOR TOTAL KGs; e first-application weeks. Native b/c and h/i; f second-application gate |
| 2019 XS + Panel | a / d / e | g / j / absent | Same explicit extract roles, corroborated by held Q39. Native b/c and h/i |

All letters above have prefix `ag_d39`.
The current 2016 XS whitelist omits b/c/h/i, so it cannot recover those native pairs without changing its selection.
The 2019 paper asks second-application weeks too, but no corresponding k timing carrier exists in either held modern extract; none is invented here.

Primary instrument anchors:

- `2010-11/Documentation/IHS3.Agriculture.Questionnaire.FINAL.pdf`, PDF/printed p.13, Module D Q39: separate QUANTITY, UNIT, ENUMERATOR TOTAL KGs, MONTH columns for both applications. MD5 `d6e0d71486fd6ae4d7dda5b8e1854278`.
- `2019-20/Documentation/malawi_fifth _integrated_household_survey_2019_2020_agriculture_questionnaire.pdf`, PDF/printed p.10, Module D Q39: separate native pairs, kg totals and weeks. MD5 `86cc1f419c1320eb969d11a6d24a63e0`.
- Text and column-preserving layout extraction used held bytes through `get_data_file` and local pypdf. No renderer was available; no visual-render claim is made.

The native applied unit list is 1 gram, 2 kg, 3/4/5/6/7 = 2/3/5/10/50 kg bags, 13 Other. It agrees in all seven D headers, including redundant 2010 Panel.
Organic instead uses 2 kg, 10 bucket, 11 wheelbarrow, 12 ox cart, 13 Other. G/H seed has code5 = 3.7 kg bag and 8 = 50 kg bag. F acquired-input dictionaries additionally contain volume/container units and use code5 = 5 kg bag. The unused `_MWI_PURCH_UNIT_LABELS` comment combines H/F although those codebooks differ; no change is proposed there.
All 21 held F/G/H metadata sources were independently checked; exact source/field dictionaries are in `codebooks.safe.json` and the private metadata inventory.

Q39 instructs that a mixture's main type is recorded with both types' summed quantity. That is a composition limitation for #1106, not permission to revise nominal shares here.

## Current source and normal API census

The target plot_inputs L2 was absent at both tiers. A normal `Country('Malawi').plot_inputs()` build used private physically copied held L1 and verified prerequisite L2, without assumed-fresh or read-gate bypass. Fresh-process warm output, all dtypes and attrs were exact.

| Wave | Raw valid-type application slots | Current inorganic rows | Current Gram | Current null u | All-table rows |
|---|---:|---:|---:|---:|---:|
| 2010-11 | 16,649 | 16,611 | 8,292 | 6,217 | 46,627 |
| 2013-14 | 5,224 | 5,186 | 1,804 | 91 | 16,188 |
| 2016-17 | 16,001 | 16,001 | 1,317 | 5,371 | 52,494 |
| 2019-20 | 16,544 | 16,541 | 1,889 | 5,365 | 64,875 |
| Total | 54,418 | 54,339 | 13,302 | 17,044 | 180,184 |

The historical 13,302 / 17,044 counts reproduce, but their interpretation changes with primary evidence. Total null u is now 17,649 after #1109, not the older 28,724.
Current inorganic labels also include 9,518 Kilogramme and 14,475 bag-labelled rows; for the verified waves those bag labels come from timing, not native unit identity. Native raw gram codes total 596 across the six loaded extracts, a different denominator from the 13,302 served timing-derived Gram groups.
The modern second slots contain 10,403 valid-type raw applications and recorded native units in every one; their missing current u is an unwired/mismatched carrier, not unreported source identity. Of 17,044 current null-u inorganic groups, 17,034 have at least one recorded native unit; only ten have all native units absent (seven 2010, three 2013). That still does not pair native units with the current enumerator totals.
Raw numeric parsing was strict after blank-string handling; no nonempty parse failure was suppressed in the source census.

All 11,075 organic rows independently match raw affirmative ag_d36, reported amounts/own dictionary after unchanged ID mapping. There are 11,068 positive and seven missing amounts; units: 5,518 Kilogramme, 2,155 Ox-Cart, 1,451 Bucket, 1,204 Other, 733 Wheelbarrow, eight Unknown, five Organic unit code1, one Organic unit code3.
The entire current frame equals the reviewed de9 result; all 169,109 nonorganic rows equal f8 in index/order/fields/dtypes apart from reviewed added Applied. Thus #1174's 437 residual records and existing quantities/retention remain preserved.

## Repeated applications, payload and grouping boundary

The existing assembler at `malawi.py:3250-3266` sums Quantity for identical `(t,i,plot_id,input,crop,u)` and keeps different u separate. It is already an additive plot/input/unit summary, with no application or timing axis. The current unit mistake accidentally preserves some applications as separate unit keys.

| Wave | Unit-distinct pairs coalescing under kg | Source members | Sum of current numeric amounts in those pairs |
|---|---:|---:|---:|
| 2010-11 | 51 | 102 | 4,670 |
| 2013-14 | 21 | 42 | 1,512 |
| 2016-17 | 78 | 156 | 7,207 |
| 2019-20 | 184 | 368 | 17,322 |

All 334 groups are within one source, one household/plot/input, with exactly slots1+2. There are no duplicate source/household/plot/slot identities. In 2010 the paired month values differ; in modern waves current slot2 u is absent while slot1 comes from weeks. These are repeated applications, not mistaken keys.
Every source member has positive current amount except two missing 2013 amounts; no candidate group has both source amounts missing. Current ordinary sum has already represented those missing amounts as zero where isolated, and this diagnosis does not change that #1174 policy.
All 54,339 current inorganic rows have nonnull Quantity (79 zero); every Quantity_purchased, Purchased, Improved and Applied value is null. Grouping preserves that payload mask, per-input numeric sum and all unaffected inputs. Full source-member frames remain private in `merge-groups.private.pkl`.

Complete source-use masks are separate from type-based retention: among all 54,418 valid-type slots, ag_d38 is Yes for54,353, No for49 (20/5/24/0 by wave), and missing for16 (13/3/0/0). Modern2016 additionally has21 second-slot types despite a No second-application gate; nine are among its78 candidate merge pairs. These are source disagreements and no gate precedence is selected here. All235 pairs in the intended2010+2019 subset have affirmative inorganic-use status and observed positive kg contributions; none has a use-gate conflict. Its full unaffected labelled frame, each corrected group's Quantity sum, null flags and attrs are exact in `merge-source-checks.json`. Counterfactual concatenation order is not a future normal-build row-order proof.
The current79 zeros decompose into41 all-missing source-amount groups (12/13/16/0) and38 groups with explicit zero answers (0/5/15/18); both kinds remain untouched under the proposed label repair.

## Bounded private counterfactuals, not current API behavior or approval

Each kg option below retains current numeric Quantity, uses existing factors/shares unchanged, and applies the unchanged reducer after correcting the unit key. The grouping operation itself changes derived nitrogen by zero; the N differences arise from changing false unit interpretation. Current N is 840,235.7795800001 kg under the existing transformation.

| Kg-label scope | Authority limit | Result rows | Row reduction | Remaining null u | Counterfactual N kg | N delta |
|---|---|---:|---:|---:|---:|---:|
| 2010 + 2019 | Held questionnaires cover both | 179,949 | 235 | 6,067 | 1,028,195.63206 | +187,959.85248 |
| Add 2016 | Explicit native KG headers; literal questionnaire sidecar gap | 179,871 | 313 | 696 | 940,755.38213 | +100,519.60255 |
| Add 2013 | Conditional only; C4 unresolved | 179,850 | 334 | 605 | 832,834.967 | -7,400.81258 |

The numerical inorganic Quantity sum stays 2,899,339.95 in every projection. For 2013 that is a numerical conservation statement, not a verified physical-unit declaration.

Switching instead to the **reported native pair** is a distinct measurement choice: it changes numeric amounts, units and grouping. Keeping the same reducer gives these illustrative results, with unknown/unlisted raw identities qualified and no new conversion factors:

| Native-pair scope | Rows | Row delta | Counterfactual N kg |
|---|---:|---:|---:|
| 2010 + 2019 | 179,969 | -215 | 1,140,671.3498145707 |
| Add 2016 | 179,896 | -288 | 1,084,374.1583845706 |
| Add 2013, conditional | 179,882 | -302 | 1,003,783.8538845706 |

Native amount vectors remain separated by unit in `supplement.json`; summing grams, bags and containers as one physical amount would be meaningless. Native applied unit residuals include code12 in 2010 and codes10/52 in 2013, unlisted in their applied dictionaries; acquired/food codebooks cannot supply their meaning. Enumerator kg and native-pair results need not agree. Switching to native pairs would therefore be a separate enhancement. Retaining the current drivers' enumerator totals preserves the existing measurement choice and does not need a new policy ruling.
An application axis could retain all 54,418 fertilizer source slots instead of 54,339 current groups, but requires a separately chartered schema/grain design. No such implementation is proposed.

## #1118 independent boundary and purchase channel

Outside inorganic, 605 current null-unit rows are 598 Seed, four Herbicide, three Insecticide. Independent raw-to-wave equality classifies 602 as truly absent source unit (595 Seed + seven pesticide), and three as recorded unlisted seed codes (one12, two13). The seed dictionary defines Other as9; neither recorded code may borrow a meaning from harvest, acquired-input or organic codebooks.
For the 602 true-missing rows, existing #1041 independently supports Unknown: 11 current positive quantities and 591 current zeros, including unchanged missing-to-zero reducer effects. A private unit-only projection retains all 180,184 rows, unique keys, every nonunit value/dtype and attrs. It leaves 17,047 null u (17,044 inorganic plus three unlisted seed identities). No quantity/retention change is needed.
The three observed unlisted codes need source-qualified identity preservation if separately scoped; they are not proof that the source omitted a unit. Organic's existing eight Unknown and six qualified identities remain untouched.

Fertilizer has no purchased amount channel in this table. Quantity_purchased is seed-only: raw H native amount columns (`ag_h16a/ag_h26a`, modern XS `ag_h07a/ag_h38a`) are value-gated and summed, while the paired native unit columns passed to `_seed_purchase_block` are unused. Its own unit identity cannot be borrowed from applied fertilizer kg or assumed to match G's planted seed unit.
The independent held-source audit finds 9,837 purchase measurements and 7,008 attachable single-plot household/crop groups: 13 mix purchase units, and 1,066 do not have the same observed singleton unit as planted seed. This is a separate seed-purchase measurement residual; no changes are proposed under #1111.

## Missing authority and next decision

- 2013 questionnaire: `Malawi/2013-14/Documentation/IHPS.Agriculture.Qx.FINAL.pdf.dvc`, MD5 `7240c127062552554f2e03ece6a05f0d`, 1,562,895 bytes, unheld.
- 2013 DDI: `ddi-documentation-english_microdata-2248.pdf.dvc`, MD5 `799054eed5560b132c90b7d15a6bbd4c`, unheld.
- 2016 agriculture questionnaire: no sidecar exists anywhere under that wave's held configuration. Household questionnaire/BID are not a substitute for Q39.
- 2016 XS DDI MD5 `48c52077339830389cf09d2dc5162700` and Panel DDI MD5 `1c0be6d66b9764ebcdb766db56ffad7a` are unheld. The held BID has no matching Q39/field binding.

No acquisition was attempted. C4 is an existing ruling; 2013 is missing authority, not a new policy question. Root clarified that the intended minimal correction preserves the existing enumerator-total amounts and additive reducer, scoped only to questionnaire-covered 2010+2019. The 235 coalescences must conserve every source amount contribution and all other waves/inputs/fields. The 2016 metadata-only and 2013 unheld subsets remain outside that boundary. Native-pair or application-axis alternatives are optional enhancements, not prerequisites for fixing the source-label mismatch. Organic #1109 and missing-identity #1041 rulings need no renewed human decision. No implementation begins before a separate concrete worktree brief.

If a future fix changes `malawi.py`, all22 Malawi source-table hashes move, requiring one private country rewarm; the26-table API surface also includes four runtime-derived tables. Wave plot_inputs script-only edits have a narrower declared hash footprint. No framework hash changes are justified here. This read-only diagnosis moves no tracked build input or fingerprint.

## Validation and limits

Target-cold normal Country build, fresh-process warm complete-frame/attrs equality, independent organic raw conservation and f8 nonorganic equality passed. Existing organic tests: 28 passed, 13 expected synthetic audit warnings. Actual Country cold and warm each emit two warning classes: absent older-wave Improved and null crop/u keys; full messages stay private. No other country or Feature sweep was required for this diagnosis and none is claimed.
`freeze-validation.json` checks all 14,910 reviewed-head tracked contents/modes plus 14,880 baseline entries, index modes, immutable HEAD and clean status. No in-tree parquet exists. Private physical source MD5s/manifests, scripts and counterfactual frames remain under this directory; safe aggregates are separate from private raw members/logs.

The fresh-root preparation command was also exercised:87 physical copies, target plot_inputs L2 absent. The original complete cold/warm/source pipeline is the executed validation; the reproduction commands are supplied for a new root, without claiming a second full rebuild was needed or run.

Stop: source diagnosis complete; await root's concrete worktree brief before implementation. Sue.

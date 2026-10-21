# Prior-art ledger draft - Malawi #1111 / #1118 diagnosis

Diagnosis only, 2026-10-21. Sue (GPT-6).
This private draft is deliberately untracked under the dispatcher's no-edit instruction.
Search tier: git/ripgrep/AST floor; inherits the reviewed de9 #1109 ledger, `.coder/ledger/STANDING.md`, `CLAUDE.md`, and `lsms_library/data_info.yml`.

## §1 Task

Bind the current Malawi inorganic Quantity/u source fields to held primary Module D instruments and native metadata before proposing any physical-unit repair.
Separate source misbinding, genuine missing unit identity, application aggregation, and the live C4 questionnaire gate.
Preserve reviewed #1109 organic behavior, #1174 nonorganic retention, and #1106 composition assumptions.

## §2 Existing machinery

| Machinery | Existing contract / evidence | Decision |
|---|---|---|
| Malawi `_/CONTENTS.org:3025-3071` | Organic source correction; historical applied-unit suspicion and null counts; two live TODO tasks elsewhere remain open | Read in full first; contradictions quoted in REPORT |
| `_fertilizer_block`, `malawi.py:2977` | Reads each supplied numeric amount, decodes supplied unit column; keeps valid type 1-6, no kg conversion | Source projection is the defect; do not change generic amount retention |
| Four wave `_/plot_inputs.py` callers | 2010/2013 a,d,e + f,i,j; 2016/2019 a,d,e + g,j,None | Bind independently by wave; do not infer common letter meaning |
| IHS4 `_read_usecols` | Existing restricted reader for unused bad-byte free text | Reused unchanged function through AST for diagnosis; expanded numeric/ID analysis selection only |
| `assemble_plot_inputs`, `malawi.py:3178` | `(t,i,plot_id,input,crop,u)` with `dropna=False`; ordinary Quantity sum for nonorganic, first purchase/flags; organic-only min_count overlay | Preserve existing reducer; correct u can coalesce applications under this existing contract |
| `_seed_purchase_block`, same file | Reads native H source quantities and values, accepts but does not use unit_cols, sums at household/crop, joins only single-plot seeds | Do not let a fertilizer unit repair label this separate seed purchase measurement |
| `_MWI_FERT_UNIT_LABELS` | Valid for native applied c/h or c/i dictionaries; false comment assigns it to timing e/j | Do not map timing; do not borrow acquired F or seed G/H codebooks |
| `_organic_block`, `_MWI_ORGANIC_UNIT_LABELS` | Applied=True, own 2/10/11/12/13 dictionary; qualified unlisted 1/3; true missing Unknown | Reuse unchanged; independently verified all 11,075 source rows |
| `U_UNKNOWN`, #1041 | Missing/blank identity with or without amount; no physical conversion | Independent #1118 subset only where actual source identity is absent |
| `nitrogen_kg` | Existing factors and nominal shares, explicit organic refusal | Used unchanged only for labelled private counterfactual deltas; #1106 remains separate |
| Existing tests | `tests/test_1109_malawi_input_quantity.py` 28 pass; existing fertilizer reducer controls explicitly include ordinary all-missing->0 behavior | Preserve; no new tracked tests during diagnosis |

## §3 Definitions and source authority

- C4 is an **existing gate**, quoted in live #1111: questionnaire Module D Q39 unit code list before any code change.
- 2010 held IHS3 agriculture questionnaire PDF p.13 and native labels bind b/c = reported quantity/unit, d = enumerator TOTAL KGs, e = MONTH; second g/h, i = TOTAL KGs, j = MONTH.
- 2019 held IHS5/IHPS agriculture questionnaire PDF p.10 and both native extracts bind b/c = reported quantity/unit, d = enumerator TOTAL KGs, e = first-application weeks; second h/i = quantity/unit, j = TOTAL KGs. f is a second-application gate and g its type.
- 2016 both native extracts explicitly label d/j TOTAL KGs, e weeks, c/i UNIT. This proves the extract's field identities, corroborated by 2019 routing; no held 2016 agriculture questionnaire sidecar exists, so literal C4 coverage is still separate from metadata evidence.
- 2013 c/h have explicit unit dictionaries but d/e/i/j labels repeat a generic truncated question. No held questionnaire or DDI settles those column roles. Similar letters/distributions and EPAR are insufficient authority.
- Native applied dictionary: 1 gram, 2 kg, 3/4/5/6/7 = 2/3/5/10/50 kg bag, 13 Other. Organic uses a separate dictionary; seed 5 means 3.7 kg bag. Acquired F additionally includes volume/containers; it is not D39c.
- Instrument instruction: mixtures within one application use the main fertilizer type but the sum of both amounts. That limits scientific composition interpretation; no #1106 change is authorized.

## §4 Invariants

- No production edits, git/ref mutation, GitHub writes, acquisition, DVC CLI, shared cache mutation, or raw in-tree files.
- Exact de9 checkout/config/image Python, private physical L1/L2, active socket guard; BLAS/build/make threads 1, test workers 2.
- Keep organic source counts/answers, qualified codes, all seven missing organic quantities, and all nonorganic values including the #1174 residual.
- The current API has 180,184 rows, including 54,339 inorganic rows; corrected unit identity is part of the key, so preserving both row count and unique key is not generally possible.
- A kg-label projection retains numeric quantities and current nonorganic zero/missing reducer; it is not multiplication by 1,000 and not a proposal to reinterpret missing source amounts.
- All 334 candidate all-wave group merges are same-source first/second application pairs, not source-slot identity mistakes. Coalescing additive kg amounts follows the existing reducer; exposing application/timing as a new axis is a separate design.
- Source-gate conflicts remain distinct: all235 questionnaire-covered pairs have affirmative use/positive kg;2016 has nine second-application No-gate conflicts within its78 pairs, outside the intended minimal scope. Across all valid types,49No-use/16missing-use slots are preserved by the current type-based retention.
- Fertilizer Quantity_purchased/Purchased/Improved/Applied are entirely null. Seed purchase measurements have their own native units and must remain outside this repair.
- Null crop keys, absent Improved in older waves, #1106 shares, #1174 source-use semantics, organic free text, and seed purchase unit design are separate residuals.

## §5 Reuse decisions and bounded options

| Candidate | Classification / boundary |
|---|---|
| Retain enumerator kg numeric amounts and declare Kilogramme for 2010+2019 fertilizer slots | Source-backed by held questionnaires; preserves the measurement already selected by current drivers and coalesces 235 unit-distinct application pairs under the existing reducer. Root identified this as the intended minimal bug boundary; await separate concrete worktree brief |
| Add 2016 to that projection | Native headers prove kg identity; adds 78 coalescences. Literal C4 questionnaire gap must be reconciled by dispatcher before code |
| Include 2013 | C4 authority remains missing; no implementation proposal authorized |
| Switch Quantity/u to native b/c and g/h or h/i | Distinct measurement choice, changes numeric amounts and grouping; source amount/unit vectors and counterfactuals recorded, not a minimal relabel |
| Add application/timing axis or retain both native and enumerator measures | Genuine broader API/data design; no framework/schema authorization |
| #1118 true missing noninorganic units | 602 independently classified rows can follow existing Unknown rule without a physical-unit decision; preserve all values and retention |
| Three observed unlisted seed codes | Retain raw source identity with qualifier if scoped separately, rather than borrow unrelated codebook or invent a physical unit |

## §6 Decisions remaining

1. Existing-choice preservation is settled by the current drivers and root's clarification: retain their numeric enumerator totals and existing additive reducer. This does not require a new measurement policy decision.
2. Root intends the instrument-covered 2010+2019 subset only. 2016 remains metadata-covered but outside this literal-C4 scope; 2013 remains unresolved.
3. Switching to native pairs, retaining extra measures, or adding application detail would be a separately chartered enhancement. Those optional designs do not block the minimal source-label correction.

No human approval is needed to establish source facts or repeat #1041/#1109 rulings.
All options remain unimplemented pending root's concrete fix brief.

## Phase 3 - diagnosis checks

- CONTRADICTION (§3): current e/j unit documentation and code comments do not describe the held source columns.
- OK (§4): actual target-cold normal Country build and fresh-process warm frame/attrs exact; all nonorganic fields exact to f8 apart from the reviewed added Applied column.
- OK (§3-4): all organic answers independently source-exact; 28 existing organic tests pass.
- OK (§4-5): counterfactual group/source/payload masks and N deltas exported as aggregates; raw members remain private.
- No implementation or reinvention occurred. Frozen content/mode/index/HEAD proof is in `freeze-validation.json`.

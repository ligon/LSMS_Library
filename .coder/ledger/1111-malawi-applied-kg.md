# Prior-art ledger - GH #1111 Malawi applied-fertilizer kg (partial)

Search tier: git/ripgrep/AST floor. Base `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
Inherits `.coder/ledger/STANDING.md`, `CLAUDE.md`, and `lsms_library/data_info.yml`.
Root approved this concrete boundary on 2026-10-21 after the frozen read-only diagnosis.

## §1 Task

Correct only the 2010-11 and 2019-20 plot_inputs drivers' inorganic unit identity.
Keep their already-selected enumerator-total numeric Quantity columns and the existing additive reducer.
Set the fertilizer block's typed u to existing Kilogramme before assembly, without decoding timing.
Addresses #1111; does not close the whole issue.

## §2 Existing machinery

| Symbol / input | Existing behavior / verification | Decision |
|---|---|---|
| `Malawi/2010-11/_/plot_inputs.py` | Full_Sample only; type a/f, Quantity d/i; e/j are currently decoded as units | Extend only fertilizer projection |
| `Malawi/2019-20/_/plot_inputs.py::_half` | XS bare case_id and Panel y4_hhid; type a/g, Quantity d/j; e decoded and second u absent | Extend only fertilizer projection, both halves |
| `Malawi/_/malawi.py::_fertilizer_block` | Two type-valid slots, nullable Float64 Quantity, nullable string u; no rescaling or use-gate precedence | Reuse byte-exact with unit1/2=None |
| `assemble_plot_inputs` | Full `(t,i,plot_id,input,crop,u)` key, dropna=False; Quantity sum, purchase/flags first | Reuse byte-exact; relabel before grouping |
| Kilogramme | Existing country spelling in categorical_mapping.org and canonical API output | Reuse typed constant; no new decoder/factor |
| `nitrogen_kg` | Existing physical factors and nominal shares; organic refusal | Reuse unchanged; measure expected delta, no scientific recalibration |
| Plot/nitrogen tests | `tests/test_plot_id_canonical.py`, `tests/test_867_nitrogen_content.py` | Run bounded relevant selection; add actual-driver controls |
| Source evidence | Frozen private Malawi/1111 REPORT.md, LEDGER.md, source manifests and independent slot census | Reuse facts; recheck held MD5s and original contributions against actual f8/candidate builds |

## §3 Definitions and primary evidence

- Held IHS3 agriculture questionnaire PDF/printed p.13, Module D Q39, MD5 `d6e0d71486fd6ae4d7dda5b8e1854278`: native first b/c and second g/h quantity/unit; d/i are ENUMERATOR TOTAL KGs; e/j are MONTH.
- Held IHS5/IHPS agriculture questionnaire PDF/printed p.10, Module D Q39, MD5 `86cc1f419c1320eb969d11a6d24a63e0`: native b/c and h/i; d/j are ENUMERATOR TOTAL KGs; e is first-application weeks in both extracts; f is second-application gate, g its type.
- The source-native unit is paired with the native quantity, not with an enumerator kg total. No 1,000x multiplier or native-pair substitution is authorized.
- Q39 records a mixture under its main type with both types' summed amount; composition remains #1106.
- Existing live #1111 C4 gate requires questionnaire evidence before code change. Only 2010+2019 are instrument-covered here. 2016 agriculture questionnaire sidecar is absent; 2013 questionnaire MD5 `7240c127062552554f2e03ece6a05f0d` is unheld.
- Baseline CONTENTS says "Module D records no organic quantity". Reviewed de9/#1109 independently disproved that claim using ag_d37a/b, but this f8 branch does not stack that correction and must preserve f8 organic output exactly.
- Baseline CONTENTS calls ag_d39c "acquired-amount" and e/j applied units; shared helper comment claims their unit code sets are the same. Primary instruments contradict both. Qualify these claims in CONTENTS; leave shared helper and other wave drivers untouched.
- Country idiosyncrasies: 2010 Panel source is redundant and not loaded; 2016 XS needs cs-17- prefix whereas 2019 XS emits bare IDs and relies on unchanged framework ID walking; IHS4 XS has an unused bad-byte string and an existing restricted reader.

## §4 Invariants

- Only the two wave fertilizer projections change. All other branches, quantity/type/source predicates, IDs, native-pair alternatives, schema, shared helper and reducer stay exact.
- Before-grouping kg identity coalesces 51+184 same-source first/second application pairs. Each of these 235 pairs has affirmative inorganic use and two positive observed kg answers. Preserve every amount contribution, flags/null masks and all unaffected frames/attrs/dtypes.
- Retain f8's organic behavior, all 2013/2016 values and #1174's missing/zero retention, including source No/missing-use disagreements. No new gate policy.
- Full numeric inorganic Quantity sum remains 2,899,339.95. Expected N delta is approximately +187,959.85248 kg under unchanged factors/shares; report measured floating precision.
- No framework/dependency/fixture/golden/curation change, no application axis, no missing-unit #1118 repair, no seed-purchase unit repair.
- Sanctioned held-only readers, physically private L1/L2, active socket guard plus unshare -rn; exact checkout/import/config identity, mounted image Python, numeric/build/make threads 1, at most four loadfile workers.
- Actual baseline/candidate normal target-cold and fresh-process warm proofs; healthy current-hash Uganda comparator; scanner findings individually reconciled; all 549 API table hashes and 39 framework fingerprints measured.
- Separate #1109 composition proof is root-owned after freeze/integration or explicit approval of an isolated combined country configuration. Do not edit or silently inherit reviewed de9 code.

## §5 Reuse decisions

| Quantity / behavior | Decision | Reason |
|---|---|---|
| Existing enumerator kg amount | Reuse | Already chosen by both drivers; primary questionnaires name its unit |
| Fertilizer u | Extend driver projection | Typed Kilogramme constant before unchanged assembly; timing is not a unit |
| Grouping | Reuse | Existing additive full-key contract, not a new grain policy |
| Organic / missing units / purchases | Reuse unchanged | Independent issues and explicit branch boundary |
| Cache invalidation | Reuse hashes | Actual measurement changes all26 Malawi hashes: Wave._input_hash folds every wave Python input into every table; no framework or other-country change |

## §6 Open decisions

No human decision blocks this approved partial implementation.
2016/2013 C4 authority, native pairs/application detail, #1106, #1118, #1174 and seed purchase units remain separate.
Root handles live issue/PR and separate composition after this lane freezes for independent review.

## Phase 3 verification

Implementation uses the existing source-selected totals, typed kg labels and unchanged reducer (§2-4).
Actual hash scope disproves the frozen read-only diagnosis's tentative narrower-footprint hypothesis.
The existing `Wave._input_hash`, `lsms_library/country.py:1317-1328`, says "content-hash EVERY build INPUT in the wave + country _/ dirs" and executes:

```python
for d in (wave_dir, self.country.file_path / "_"):
    if not d.is_dir():
        continue
    for p in sorted(d.glob("*")):
        if p.suffix.lower() in _BUILD_INPUT_SUFFIXES:
            parts.append(f"binp:{p.name}=" + (cached_file_hash(p) or "none"))
```

Consequently all 26 Malawi API hashes change: 22 nonderived entries and four runtime-derived entries (`food_expenditures`, `food_prices`, `food_quantities`, `household_characteristics`).
The complete 549-key f8 inventory exactly matches the independent frozen Burkina #1123 baseline, including values; all 523 other-country hashes and all 39 framework fingerprints remain exact.
Earlier 22-key receipts are not rewritten or silently reinterpreted here.
Normal candidate flow rebuilt the unchanged sample prerequisite across all five held waves, without a read-gate bypass, assumed-fresh serve or stale re-stamp.
Separate physically fresh f8 and candidate all-wave sample builds produce identical complete frames/dtypes/attrs, all six sample cache frames, and all 18 complete categorical warning records; fresh-process warm serving makes zero L2 writes.
The main target-cold candidate's six categorical events are pre-existing source-label omissions, not new source losses; their reported columns are unused by sample and remain raw on decoder failure.

Independent source expectations account for 33,193 selected application slots and 1,765,763.95 kg in these two waves.
All 235 newly coalesced pairs preserve each source contribution, the exact existing sum, and every other payload/null mask; all other frames/attrs/dtypes and f8's 11,075 organic zero rows remain exact.
Normal Country rows are 180,184 -> 179,949 (nonorganic subset 169,109 -> 168,874); inorganic numeric Quantity remains 2,899,339.95.
The unchanged nitrogen calculation gives 840,235.7795800001 -> 1,028,195.63206 kg, a binary-float delta 187,959.85247999988, within 1.17e-10 of the expected decimal delta.
Actual-driver negatives: baseline 10 fail / 2 pass, candidate 12 pass, dangerous late-label mutant 12 fail; bounded regressions 101 pass.
All 15 scanner records are individually reconciled in the report, with no new finding; phase 2 has no plot_inputs kwargs in the actual harness.
Source-safe receipts and runnable commands: `slurm_logs/isa_backlog_2026-10-21/Malawi/1111/`.
Root-owned #1109 composition remains pending; this branch has not inherited de9 organic code. Sue (GPT-6).

## Test-only review correction after 9b883974a

Independent review returned NEEDS_FIX at frozen `9b883974a81d6ee664bbca425b7d484f0f14eb7a`: the new test pinned f8's excluded organic zero/null/no-Applied defect and would reject the independently reviewed #1109 correction.
Reviewer receipt: `/local/job40255479/control/isa-backlog-20261021/review-1111-Malawi/verdict.json`; review stopped before runtime/pytest validation and released the unchanged clean snapshot.
Root then authorized deleting only those five organic selection/assertion lines while retaining every in-scope check, plus this note/report and a descendant receipt.
No production code, fixture payload, original 9b runtime/quantity proof or original private evidence is changed; independent f8 organic invariance remains historical source evidence, not a permanent test requirement.
The original report/validation receipts are bound to 9b; `test-descendant-validation.json` records the new test hash, rerun outcomes and exact all-other-blob/mode equivalence.
Separate organic composition is still pending root, with no attempt in this lane. Sue (GPT-6).

## Root-owned delivery composition

The original a935 standalone branch and its independent PASS remain unchanged.
For delivery, root starts from reviewed organic PR1175 at de9c2734b and merges a935 into a separate branch, preserving the organic helper and all other parent production blobs.
The incremental runtime diff remains exactly the two reviewed fertilizer drivers; the new test no longer pins the excluded organic defect.
The public PR will be stacked on PR1175, with separate source-pinned composition verification before publication.
The original REPORT/validation receipts describe the standalone source state; the campaign records the composed proof without rewriting those receipts. Sue (GPT-6).

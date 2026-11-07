# Prior-Art Ledger — Malawi food follow-ups 49/50

Search tier: ripgrep + git; source metadata through `get_data_file`, factor rows
through `get_dataframe`. `make api-audit` passes (15 listed skills).
Grounding revision: `8558fefe0d26e3f26e4082e52684382df94887d5`.
Author payload is read-only `83a0ec25287d321b11f2a47a7d5f77180f276c5f`.
Frozen weight research `131dcdd4540b1fd62ab58033a60836342ee38e2f` is untouched.

## §1 Task, restated

Implement the maintainer's 2026-11-07 rulings: borrow released IHS5 food factors
for Malawi 2024–25 as a registered derivation; classify code 119 as maize flour.
Recover only the unfinished food producer and its necessary sample/identity and
food-metadata dependencies onto current development. Preserve native food rows,
quantity, expenditure, units, and unrelated fields. Keep the old author's sample
mapping only as unmerged comparison evidence: this task selects no weight policy,
normalization change, date clamp, or whole-wave release. Task authority is
`.sucoder/tasks/malawi-food-followups-20261107.org` in the coordinating checkout.

## §2 Existing machinery

| Machinery | Use and tests | Decision |
|---|---|---|
| `Malawi/_/malawi.py:handling_unusual_units` | Source unit cleanup; `test_malawi_unit_cleanup.py`, `test_gh1042_missing_unit_spellings.py` | Reuse; preserve Unknown and source-unit details |
| `Malawi/_/malawi.py:food_acquired_to_canonical` | Summable per-source `Quantity_kg`, native units, shared canonical reducer; `test_quantity_kg.py`, `test_food_prices_units_kwarg.py` | Reuse, with a minimal provenance extension only if required |
| `Malawi/_/malawi.py:apply_harmonize_food` | Wave-specific label map; `test_label_selection.py` | Reuse; no factor match on a coarse label |
| `Malawi/2019-20/_/food_acquired.py` | Released factor source and household-region join | Reuse source, NOT its fuzzy `conversion_table_matching` or mean over code 23 |
| `transformations.food_kg_factors`, `_apply_kg_conversion` | Existing exact/metric/inference ladder, rung provenance | Reuse unchanged; no new estimator or clipping |
| `derivations.py`, `Country.derivation_inputs` | Registered operation, raw-named input-grain operands, ID walk, attrs summary; `test_derivations.py` | Extend country registry and scoped callable |
| `83a` 2024 food script and sample hook | One interview per refreshment household, native IDs and unit decode; `83a:tests/test_gh1275_malawi_2024.py` | Recover necessary pieces separately from food changes |
| Committed panel maps and `83a` guard | Source maps, not generated on ordinary builds; default/Make parity needs the pending guard integration | Placement review before recovery; no ID-policy invention |

## §3 Definitions and evidence

Inherit `STANDING.md` §§3–4, `CLAUDE.md` Derived Values and canonical
`lsms_library/data_info.yml`. The food and units skills require summable kilograms
without changing native amounts/units; the derivation contract requires original
variable names, reproducibility, named assumptions and surviving provenance.

Read the entire current Malawi CONTENTS (5,373 lines, blob
`5470d54c0180a32b31bd8abda2b425c093160038`), including TODO/WAITING/LOGBOOK.
Relevant idiosyncrasies: 815 refreshment interviews are released twice; retained
panel EAs describe baseline geography; item/unit vocabularies change across waves;
IHS2 lacks a factor table; IHS3+ conversion tables are region keyed; unknown units
must not gain generic OTHER factors; a new food wave changes older-wave inference.
Current shocks use food purchases and food stocks separately and must remain so.

Explicit contradictions to retain in the new country note:

- Historical CONTENTS recommends “the mean of conversion factors was taken” and
  “A fuzzy-matching approach ... is needed.” New borrowing is governed by the
  maintainer's exact supported-key ruling, not those historical recommendations.
- `83a` food docstring says using IHS5 factors “would be a modelling choice
  nobody has made.” The maintainer has now explicitly made that choice.
- The review's item 119 Cereals / Other Cereals assignment is superseded by the
  maize-flour ruling. Classification does not identify an older factor.

Source: `2019-20/Data/Cross_Sectional/ihs_foodconversion_factor_2020.dta`, sidecar
and acquired bytes MD5 `dc1bed57a31e20232d04bd1ac5bb0f01`, size 227,655 bytes.
1,768 rows; fields `region,item_name,item_code,unit_name,Otherunit,unit_code,factor,source`.
`factor` label is **2019 Factor**. Three regions (1 North, 2 Central, 3 South),
116 item codes,133 names,60 unit codes. Exact key
`(region,item_code,unit_code,Otherunit)` has 1,768 unique rows. Dropping Otherunit
leaves 1,709 groups, 30 duplicate groups, 28 with conflicting factors, all code 23.
No mean/median reduction is necessary or authorized.

`source` is factor origin, NOT food acquisition source: 1 field 1,397 rows;
2 imputed from other item-units 71; 3 calculated standard 57; 4 field/few observations
not all regions 243. Retain it in inputs and call these **released factors**.

Sanctioned metadata compares both 2024 Module G files with IHS5. Important refusals:

- Factor item 119 is **MAIZE, YELLOW FLOUR** (four Central-region rows), while
  2024 item 119 is **Grain Meal (Gramill)**. No numeric crosswalk.
- Unit 74 changes **TIN 1 KG** to **BUNDLE**. Do not borrow by that code.
- Current questionnaire p.49 defines size codes 4A/B/C,6A/B,7A/B,8A/B/C,9A/B/C,
  10A/B/C,22A/B/C,25A/B,27A/D/E,44A/B/C. Preserve suffixes. It does not define
  factor-table 27B/C or 37B; no analogy with their base codes.
- New 120 Popcorn coexists with 835 Popcorn. Do not invent a 120→835 crosswalk.
- Survey labels 112 (adds muffins) and 509 (adds quails) broaden. Exact numerical
  identity alone is insufficient for carrying the narrower old factors.

A conservative dictionary screen (case/punctuation/whitespace only) supports
1,492 factor-table rows before observed-food joins. The 276 others are documented
in the durable compatibility inventory : 111 absent item codes; 18 changed survey
labels; 133 factor-item label differences; 12 factor-unit label differences;
6 currently undocumented unit codes (4 overlap the item-label category).
Counts concern factors, not coverage or food rows. Benign spelling aliases may be
admitted only with explicit evidence; unknown/ambiguous remains missing.

Item 50 evidence: current questionnaire p.49 lists Grain Mill (Gramil) among maize
flours; both source dictionaries say Grain Meal (Gramill). P58 separately asks
Milling fees, grain (nonfood code 201, 30 days). NSO's earlier COM_CK dictionary
distinguishes maize flour (Gramil, Rab, or locally milled), item 2, from grinding
charges 3/4: https://microdata.worldbank.org/index.php/catalog/2248/variable/F187/V6616?name=com_ck00a
Preserve native item 119 and its label. No evidence identifies refined versus
mgaiwa versus madeya processing; do not silently merge it into one of those.

## §4 Invariants and assumptions

- Cross-wave physical stability is an authorized modelling choice, not observed
  2024 truth; disclose source year and replace when appropriate newer factors ship.
- Region, full size-bearing unit code, native item meaning and applicable
  Otherunit are needed. Keep raw factor origin and raw labels retrievable.
- A factor join is many-to-one; neither duplicate source keys nor missing join
  keys may manufacture rows or match unknown to unknown.
- Native rows/index/Quantity/Expenditure and unrelated output must match the
  recovered baseline exactly. Metric-label conversions already run at one choke
  point; do not change native shape or apply kilograms twice.
- Unknown/code 23 refusal follows the current GH #1074/1042 rulings. A supported
  code 23 factor needs matching informative source text, not merely numeric23.
- Preserve existing derivation keys by explicit union if a row needs multiple
  operations. Explain registered borrowing separately from the ladder's source
  marker, which presently calls nonmissing Quantity_kg `survey_kg`.
- Original `83a` sample mapping/normalization remain identical before/after food
  changes and explicitly unapproved for whole-wave release (decision 48).
- Current older-wave producers and nonfood tables remain byte-identical except
  necessary recovered dependencies; measure all older food inference changes.
- Private L2; shared L1 only. No DVC CLI, LSMS_NO_CACHE, shared rewarm, fixture,
  dependency or frozen-research edits. Every source probe asserts code/config.

## §5 Reuse and placement plan for coordinator review

1. Recover a minimal baseline in its own commit: 2024 food script; 2024 YAML
   sample and panel-ID blocks only; mapping hooks `cs_i`, `sample`, `panel_ids`;
   country CS_24 / refreshment selection helpers only; sample optional fields;
   exact committed 83a maps; food harmonization 2024 column/new rows; recall entry.
   Existing Make food wildcard already discovers the script, so no other table's
   wave lists need recovery. Leave 2024 agriculture, livestock, shocks, cluster,
   labor and remaining YAML tables unwired in this carrier.
2. Default/Make identity parity also requires the author's pending panel-ID
   guard (`local_tools` functions, narrow `Country` hook, country guard declaration
   and panel_ids.py). Recover as isolated prerequisite hunks with tests, not a
   historical whole-file overwrite. Its known broad fingerprint effect must be
   measured and kept separate from the food transformation effect.
3. Implement factor preparation/input reconstruction in a wave-local helper or
   mapping module; registry in country derivations.yml. Match only supported
   native keys, preserve raw factor records, stamp the actual borrowed rows after
   canonical collapse, without touching the global kg estimator.
4. Preserve the native119 Preferred Label, place its GD Category in existing Maize;
   avoid claiming a processing subtype in an existing narrower aggregate. Exact
   aggregate placement is part of this coordinator review, not a new user policy.
5. Synthetic join/ambiguity/metric/provenance controls, then source-backed native
   invariance, input rederivation and sample parity; all-wave food effects and
   fingerprint scope; default/Make and singleton Feature; isolated offline CI and
   fresh independent review before any publication decision.

Durable aggregate receipts:
`/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/reports/malawi-food-followups-20261107/`
contains `source-inventory.json` (public dictionaries and factor rows) and
`compatibility-inventory.json` (per-factor admission/refusal inventory).

## §6 Open questions

No new human scientific ruling is required for unsupported factors: keep missing.
Coordinator placement review approved the minimal baseline and isolated shared
panel-ID prerequisite on 2026-11-07. Item 119 retains Preferred Label
`Grain Meal (Gramill)`, GD Category `Maize`, and its own short/aggregate label
`Gramil`; no processing subtype or older factor crosswalk is inferred.
Decision 48 and whole-wave merge remain held outside this lane.

Recovery tree `fix/malawi-food-recovery-20261107` starts from ledger commit
`074c41f8442fefe46702c65e9802c6026857f902`. The original food-followups tree
and its three dirty prerequisite files remain untouched; exact patch/test backups
and SHA256 bindings are in the durable report's `recovery-backup/` directory.
The recovered `country.py`/`local_tools.py`/test bytes match those originals.
The approved narrow hook must be integrated with future #1590 retry/error work;
do not overwrite that work with a historical whole file. Fingerprint scope will
be measured; its global Country-hook cache cost is accepted within the bundled
rewarm, which is not authorized here. No shared rewarm.

### Phase 3 verification and final metadata refinement

Recovery/prerequisite commits are 7bd873b72 and 2d85ec951; borrowing and sanctioned
reader correction are 3b9f7a771 and 7c3d3461f. Bounded invented controls passed:
11 guard; 72 combined borrowing/guard/registry/Quantity_kg; 12 latest borrowing/
read-assembly. Source inventory admission reproduces exactly 1,492/276 of 1,768.
All 147 existing food-label rows keep all earlier cells; all ten new rows are
absent in old-wave columns. All previous Malawi helper function ASTs and all
earlier committed-map entries remain unchanged.

Free private job 41004923 at frozen 7c3d3461f completed: 2,269,908 wide source
rows, 331,677 native rows, 169,995 derived rows and 169,995 input rows. Exact
native frame/payload invariance and input rederivation pass. Factor origins on
derived inputs: field 168,089, sparse-field 1,792, imputed 114. No standard-unit
factor was borrowed because metric conversion was left at its existing point.
This is native/source-composed verification, not Country/Make/Feature proof.

The earlier job 41004878 failed before Module G decode on unsupported reader
keyword `usecols`; 7c3 selects columns after sanctioned reads and adds a strict
reader-API control. A cwd-bound invented-control failure and two interrupted
incorrectly gated offline attempts are retained. The corrected full offline run
at 7c3 explicitly closed the credential gate: 11,300 passed, 5,569 skipped,
four xfailed, 18 failed in 986.77 s. The 18 failures are stale wiring fixtures:
16 GH1210 cases hardcode five food wave caches; two GH970 cases lack a 2024
synthetic panel frame. Add the new wave, assert exact declared-wave pathsets
including 2024, and retain physical-state/no-subprocess assertions. This updates
the represented configuration without relaxing the protected behaviors.

Before final API verification: preserve original factor keys separately from
normalized join keys in inputs, and explicitly declare five raw source literals
in the thin food producer so source sidecars enter its wave hash despite helper
path construction. These are provenance/cache-binding refinements; no factor
admission rule, native amount/unit, or derived-food arithmetic changes.

Metadata-only guard scope on fixed config: 553/553 country-table, 2,283/2,313
wave hashes and 39/39 build fingerprints move. Holding guard code constant,
food/config changes move 26/553 country and 156/2,313 wave hashes, all Malawi,
and zero build fingerprints. No shared caches rebuilt. API sample equality,
older-wave kg effects, default/Make/public-input/Feature/warm and independent
review remain pending. Durable live status is `reports/malawi-food-followups-
20261107/STATUS.md` below the burst root; failed logs are not discarded.

### Independent contract correction, before code edits

At frozen 10c7e12ef, full offline CI passed 11,320 / 5,569 skipped / four xfailed
in 984.29 s. Source API job 41005113 produced and saved all six baseline/candidate
API frames, then failed a harness assertion that incorrectly included intended
2024 Quantity_kg changes. All actors are terminal. Closed-frame comparison proves
native index/payload exact and all five old-wave Quantity_kg unchanged; exactly
169,995 kilogram cells change, exclusively 2024. Keep that failed log private
(pandas printed household indices); no assertion was a failed science oracle.

Independent EARLY review on 7c3 confirms two contract bugs, not observed-hit
claims: a supported positive quantity plus an expenditure-only unknown-quantity
contributor can certify a partial sum, and punctuation stripping confuses No1/2
with No12 and LOAF30.0G with LOAF300G. Root explicitly approved both conservative
corrections after actor closure. Count EVERY active retained canonical contributor
in completeness; require its quantity and factor. Preserve numeric token text,
fraction/decimal/range punctuation and Unicode numeric fractions in label
compatibility. Do not infer equivalent numeric aliases or alter other helpers.

The critic found zero numeric-token mismatches among the frozen inventory's
1,492 admitted rows; final admission-mask equality must still be verified.
Port both counterexamples and test source/observed impact, preserving native
quantity/expenditure/index. The final API comparison must exclude only intended
2024 kg/provenance changes and explicitly enforce old-wave kg equality. Final
head needs its own focused/adversarial, source/API and appropriate CI gates;
10c passing frames remain prior-candidate evidence. Keep JSON-retry/guard
integration with future #1452/#1590 under the coordinator's scope.

### Current-development integration — before implementation

Root approved a new isolated branch from development
`c964c6c4f25945dc14636fd094893b16b12180b7`, after all frozen5af actors closed.
The5af source job41005203 exited0: all six fresh API frames equal closed10c
exactly, original native/index/sample/unitvalue/expenditure invariants and all
older native kilograms pass; source admission mask0changes;169995 public inputs
join/rederive and preserve raw factor fields. Actual Make/Feature parity passes.
Fresh process Country/Feature typed values/attrs equal;18cache files unchanged.
Full offline5afCI:11331passed/5569skipped/4xfailed in1016.71s, exit0.
Food audit retains350records:3pass/345warn/2fail, the latter kgprice/unitprice
where reportedPrice is unavailable; never claim an all-green audit.

Reuse exact scientific food/config/local_tools payload from5af, without copying
its country.py. In currentc964 country.py add only the opt-in import/helper and
guarded-versus-legacy call INSIDE the current DataFrame try branch. Preserve
entry recursion/completion state, exact JSON/Unicode classification, reset-to-
retry/cache cleanup, raise and optional-absence handling byte-for-byte.
Source behavior, weights and whole-wave policy are not reselected.

New branch `fix/malawi-food-integration-20261107` may be normal-fast-forward
published to the filesystem origin ONLY; no GitHub/force/deletion/old-ref update.
Require combined1590failure/retry+guard controls, source/defaultMake/Feature/warm
parity, appropriateCI and a fresh independent review on the exact integration
head. Keep prior scientific evidence source-bound, including the material
older-wave inference changes in closed-10c-effects.json, distinct from unchanged
older native Quantity_kg. Decision48/whole-wave production merge remain HELD.

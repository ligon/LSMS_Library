# Prior-Art Ledger — GH1594 Mali 2014-15 purchased-food sentinel

**Search tier:** ripgrep + git + static Python source for grounding; subsequent exact-two-held-DTA and four-produced-output grants are documented below.
**Base:** 609b7720f8dbc72b85cb846fdd1f63e84d51f977. Inherits STANDING.md, CLAUDE.md and lsms_library/data_info.yml.

## §1 Task, restated
Apply the user's 2026-11-08 ruling that exact 99,999,999 is a missing-value sentinel to Mali 2014-15 raw s13q03c in BOTH EACIALI passages before the existing food_acquired aggregation. The authority is the user's interpretation, recorded on GH1594; the old producer-document diagnosis did not establish a sentinel code. Preserve native contribution rows and keys, all quantities, real NA, other acquisition sources, genuine paired 16,325 XOF and existing aggregation/Price/recall/weights policy.

## §2 Existing machinery
| Symbol | Path | Role / tests | Decision |
|---|---|---|---|
| food_acquired | countries/Mali/2014-15/_/mapping.py:84 | Existing source-stream reshape; no direct existing wave mapper test found; tests/test_gh1513_mali_padded_food_labels.py covers other-wave labels | extend only purchased raw expenditure mask |
| myvars/file | same wave data_info.yml:128 | Both EACIALI_p1/p2 s13q03c -> ExpenditurePurchased before mapping | reuse; no source/schema change |
| _normalize_dataframe_index | country.py:9124 | Actual additive canonical aggregation; sum(min_count=1), rederived Price; tests/test_assets_additive.py and food duplicate controls | reuse actual pure normalizer in focused tests |
| _mask_value_code / _is_quantity_sentinel | countries/Mali/_/mali.py:506 | Source-specific tree-value 999999 and separate tree-quantity rules | precedent only; NOT reuse code/field/value scope |
| _unit_canonicalizer | wave mapping.py | Existing own-org unit canonicalization | unchanged |

## §3 Definitions and country grounding
Full current Mali CONTENTS personally authenticated: 3551 lines / 247025 bytes, SHA256 e10f15f214218de84c76850ee9c7f4bab17e3be00315a1d960d85ed6df6d26a7; previously read full3481-line prefix and all70 additions, including LOGBOOK/WAITING/TODO. EACI14 identifiers are separate from EACI17 and later EHCVM; 2014 rural agricultural holdings differ from urban households.

Contradiction retained: mapping says visit is a "sample split, not a repeated measure", while the closed1594 source/document diagnosis shows the same3804 units with repeated seven-day asks. GH851 recall/exposure interpretation remains separate. CONTENTS says own-production value is absent from the instrument, while own first-passage manual p73 instructs estimated own/gift value; released files lack s13q04c/05c. No imputation follows from that conflict.

Own2014 documentary derivatives: reports/mali-1594-diagnosis-20261107/{REPORT.md,documents-summary.json,instrument-hashes.json}; manuals128/118 pages and forms57/30 pages already closed. Initial grounding reopened no PDF or source payload; the later explicitly authorized two-source proof is recorded below. Exact raw amount 99999999 differs from a large aggregated cell. Existing aggregation sums available components and leaves all-NA additive groups missing (country.py:9341), preserving its Price formula.

## §4 Invariants
- Original per-stream eligibility is computed BEFORE masking; retain a positive-sentinel-only raw contribution even if Quantity is missing/zero. Do not manufacture a zero to preserve public count.
- Only ExpenditurePurchased originating from own2014 s13q03c is masked by exact equality. No tolerance, locale/nines rule, other fields or waves.
- Input frame, Quantity, unit mapping, t/v/i/j/source keys, raw component order and passage behavior remain unchanged.
- Existing normal all-NA serving policy may change a public row count; report its causal effect rather than force parity. No post-aggregation large-cell deletion.
- Existing sum/Price/recall851, weights and observation unit unchanged. Any missing-component Price concern is a separate finding.
- Further source/cache/getter loads require a new explicit root grant. The closed two-held-DTA/four-produced-output stages below do not authorize API/Make/warm, new acquisition or old-capture replays.

## §5 Reuse decision
Reuse the existing wave source-stream mapper, unit canonicalizer and canonical additive kernel. Extend only the purchased stream with exact raw expenditure masking AFTER its original eligibility mask. Tree-specific helpers cannot be called because their field/value policy differs. Add discriminating invented paired-passage tests and exact old-parent counterfactual; no old cohort replay.

## §6 Decisions / remaining gates
Sentinel interpretation is resolved by user ruling (not producer proof). Root subsequently chose a finite exact-two-held-DTA proof instead of replaying the limited retained case-A capture. No acquisition or shared-cache writes. Independent product review, configured CI/release checks and root-owned PR/closure remain pending; no public Country/Feature/Make/warm claim. GH851 exposure and weights remain unchanged. Existing aggregate-amount/aggregate-quantity Price can retain quantity whose expenditure is now missing (the invented pair16325 /3 pins that unchanged behavior). Any consumer completeness rule for partial amount coverage is a separate decision, not silently changed or scientifically endorsed here.

### Phase 3 — verification
Closed source-free and finite produced-output evidence below; public-serving and independent product acceptance remain separate gates.


- **OK (§3–4), implementation c4d51302bc6ff6e06e870c4347d7ecd9403e5303:** only wave purchased Expenditure code equality mask; original eligibility precedes mask. Exact609b-parent healthy payload/dtypes match, other mapping AST unchanged. No dtype/unit/quantity/global sentinel machinery changed.
- **17 source-free PASS**, 20.04s pytest /34.158s wrapper: both passages, all-sentinel -> missing, value-only contribution retention, genuine paired16325, healthy neighbors/NA/nullable values, other-source quantities, original payload/attrs. Three preimport guards active/0denials/data0. Three of17 are exact-git-parent counterfactuals, not another cohort. Frozen proposal fed854a3; closed manifest590a6570a2a4586364985d76b3dd35faa84b210ad83c03747210d47d48a06ef2.
- **5 new private-adapter PASS**, 26.175s wrapper, not a replay of17: numeric/string ownCode carriers, unknown/NA/punctuation/input immutability, entire current own food dictionary and exact Wave-constructor callback refusal. Three guards active/0denials/data0. Closed manifestfdf50050f216bdddf96933f651d9c74095f3cbc8626e30d283d8fc710ef03e3d. This external adapter explicitly normalizes both dictionary/source Code carriers; actual framework lookup precedes final format_id, so no generic/public-reader equality is claimed.
- **Own held source custody:** EACIALI_p1.dta physicalMD5 d9c39f6269b9e9e9269f5a138f92aae2 /34038816B; p2 feaec780c9fb029a240bb5d6beb77881 /34038840B. Read-only existing L1 blobs -> private600/nlink1 copies, originalpre/post and privateSHA/MD5 exact, no acquisition/credentials/shared write. First V3 attempt FAILED before numeric parse because the package creates empty private dvc-cache; exact import-layout check corrected only in new V4 namespace. V3 original failure e113d6f3568110965f20c0bd6f5c089e134ae6dd153f3a55d18a3167e59a7b59 retained.
- **V4 source attempt remains FAILED**, exit1/122.521s, final JSON serialization of own Stata value-card NumPy int32 keys. Two numeric attempts, both source pre/post seals equal, four typed canonical/pure-derived outputs produced. Original partial JSON/log and strict completed top-level prefix preserved; no repair or raw reread. Prefix statusPASS appears after the finally boolean gates; independent static review qualifies that implication separately, not fabricated individual guard counters. Raw dynamic per-file counters were not durably serialized: historical29+26 and new derived55 equivalents remain distinct. Failed manifestbf86efc8f2dca531d7c8b66a1c46c5be575c6d1d68f80cb3d559d48d9d3e3e0a.
- **Four-produced-output recovery PASS**, exit0/35.017s: exactly4own trusted output loads, all4SHA/UID/mode/inode pre/post equal, guardactive/0denials/0rawdecodes/0datafiles. Canonical122085 ->122085 indices/Quantity/dtypes/attrs exact; amount5652532205 ->152532260XOF.55canonicalcells change,45become missing. Difference5499999945 is exactly55 retained canonical code-equivalents; no fresh raw-counter claim. Saved pure-derived outputs match unchanged helper,90925 ->90885 rows (40existing all-NA sparsity drops). Legitimate16325XOF retained in affected cells. Operands/attrs and healthy vectors/NA exact. Closed manifestda5affc451902baee2449837555b04655495ea856662e1f034874bcd305f2810.
- **Nominal2014 diagnostic trace only:** actual unchanged attach_currency/convert with explicit interview_dates={} and own tracked factors FX493.8 /PPP2017 195.2 at2014-01-01. FX11447007.30052653 ->308894.8157148643; PPP28957644.49282787 ->781415.2663934426. No date load, annualization, recall, weighting or currency-default change. Do not present as a fresh household-date API run.

Durable report root (fully defined): `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/reports/`.
Report namespaces: `1594-mali-raw-sentinel-focused-proposal-v1-20261108/`, `1594-private-food-adapter-controls-v1-20261108/`, `1594-mali-two-held-dta-proposal-v3-20261108/` (predecodefailure), `1594-mali-two-held-dta-proposal-v4-20261108/` (serializationfailure), `1594-produced-output-recovery-proposal-v1-20261108/` (REPORT.md/AGGREGATE-RESULT.json/FINAL-MANIFEST.json). Private identifiers/operands stay outside git. These Python profiles are finite observational guards, not universal OS sandboxes.

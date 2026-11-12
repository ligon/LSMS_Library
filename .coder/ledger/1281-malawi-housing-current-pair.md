# Prior-Art Ledger — GH #1281 Malawi housing current pair

Source: c8e378920968fde414767855186e64f5e36f54bb. Search tier: git + ripgrep + AST floor (no GitNexus tool available). Measurement only; no product/test edit.

## §1 Task, restated
Establish one new current private cold/fresh-process warm Country('Malawi').housing(waves=['2010-11']) pair, with actual contributing producer/status/writer witnesses. This is not a historical warm snapshot or a frequency estimate. The task-specific grant supersedes the bootstrap source-only profile and the prior NEXT-1281 plan's missing-operand prerequisite.

## §2 Existing machinery
| symbol / declaration | anchor | reuse / tests |
|---|---|---|
| housing single-file YAML | Malawi/2010-11/_/data_info.yml:202 | case_id, hh_f08, hh_f09; reuse |
| normal sample join | country.py:3376 | calls sample() with all six registered logical waves; reuse, test_join_v_from_sample.py |
| grab_data | country.py:2031 | sanctioned YAML extraction + wave writer; reuse |
| get_dataframe / df_data_grabber | local_tools.py:2243 / 2613 | sanctioned native getter; reuse |
| build_wave / load_from_waves | country.py:6502 / 6450 | completed outcome is separate from rows and manifest; reuse |
| _read_l2_country_cache | country.py:4941 | current hash + coverage gate; reuse |
| to_parquet | local_tools.py | normal writer; reuse |
| _typed / _axis / _exact | tests/test_gh1196_parquet_index_policy.py:16 | unchanged strict comparator; reuse via AST extraction |

## §3 Definitions & conventions in force
Inherits STANDING.md §3/§4, CLAUDE.md data/cache/ID contracts and lsms_library/data_info.yml.
Country CONTENTS.org:2037 defines categorical Roof/Floor, module G in IHS2 but F in IHS3. CONTENTS.org:2078 says IHS3 case_id is canonical, no updated_ids entry; committed crosswalk sources must remain normal.
Current issue1281 distinguishes primary dfs versus single-file failures and does not establish this housing cell. Current build_wave has GH1590 completed outcomes; historical requested-manifest wording is not a current diagnosis.

## §4 Invariants & assumptions
Normal constructor/default backend/hash/coverage/ID reader gate; no fixed sample, assumed-fresh, memo seam or fabricated map. Normal _join_v_from_sample visits all six registered waves: five declare sample (seven cover files); 2024-25 is registered but declares no table (CONTENTS.org 2026-11-04; its data_info.yml has only Country/Wave). These seven cover sources are the only allowed native dependencies. The initial five-wave wording omitted that unsupported logical visit; the eight-object source cap did not change. Freeze all source roles and tighter per-file caps before data. Cumulative full native reads <=20; one cold <=900s and one warm <=180s, no body retry. Private source copies, L1/L2 and captures; no acquisition/shared cache writes. Warm must have zero raw/producer/writer calls and unchanged data inventory. Strict served comparison preserves unused axis domains, dtypes, masks, order and attrs.

## §5 Reuse decision
Reuse approved Mali finite-role source observer/stager and Uganda cold/warm runner skeleton, adapting only country, exact source roles/caps, runtime paths and selected single call. Observe normal build outcome and writer returns; no new guard framework. Reuse GH1196 comparator unchanged. No pure tests are needed for measurement-only docs.

## §6 Open questions
No permission question: the explicit finite actual grant is sufficient. Stop before payload if dependency closure cannot be positively bounded. Empty/config/unavailable categories will be reported only if reached, never injected.

### Phase 3 — verification
- OK (§2–§5): exact eight-object freeze; cold PID 2147138 exit 0 (103.200s), warm PID 2149620 exit 0 (58.659s), all process groups closed. Source/operator seals unchanged during actors.
- OK (§2/§4): housing build_wave returned declared=True, completed=True, successful_empty=False, origin=materialized, 12,271 rows; wave and country writers returned 12,271 rows. Normal sample built five declared waves and recorded an undeclared 2024-25 visit; no rows/source read from that wave.
- OK (§4/§5): 8 full native reads/8 getters, each exact object once; 6 wave + 2 country writer returns. Warm 0 native, 0 producer, 0 writer; same cache inventory; ordinary hash fresh and housing manifest [2010-11].
- OK (§4): unchanged GH1196 strict served comparison passed without normalization; 12,271 rows each, (i,t,v), Roof/Floor string, no missing v. Manifest alone was not used as contribution proof.
- Qualified mechanics: pre-actor system-Python launcher failed before data; MAIN corrected it. Post-pair publisher first compared UID 65534 inside unshare with outer UID 43292; approved UID translation corrects that bookkeeping assertion, retaining all other identity fields. No actual-body replay.
- Qualified source wording: frozen constructor-event cap 2 was stricter than the optional second metadata reader; actual initial StataReader/inherited constructors consumed exactly 2, with 1 value_labels event per object. No cap increase or expected-count repin.
- Full issue/corpus closure, historical warm-cache frequency and scientific choices remain out of scope. Evidence: worker Malawi1281-HousingWarm/4f7331dee1304a1dba00917e1f9894f4/evidence, PRE-DATA-FREEZE SHA256 7c64fb6710f97b6200c043cfb501c1a6ba148b8615f64321252a7648c72f593e.

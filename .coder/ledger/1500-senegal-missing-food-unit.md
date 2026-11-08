# Prior-Art Ledger — GH #1500 Senegal missing food unit

**Search tier:** private Git + ripgrep + stdlib AST/text. No GitNexus service,
package import, source-data read, test execution, build, cache or network call.
**Base:** `ebb65d191b6f856ff1b0f77068765409b3957929`, tree
`6b984601cc26b3de624f591d5863fc0b38626ea8`, branch
`dispatch/792d27ce70b04b9b932e37dd2458ed63`, own checkout
`/local/job41031488/mirrors/dispatch_792d27ce70b04b9b932e37dd2458ed63`.

## §1 Task, restated

Relabel only genuinely missing `u` in Senegal 2018-19 `food_acquired` to the
existing `transformations.U_UNKNOWN == 'Unknown'`. Deliver the country-local
repair and a new, **UNRUN** regression draft. The selected policy is settled by
the supplied REPAIR-CONTRACT.md; general skill recipes grant no runtime here.
Historical issue counts (21 records, quantity 63.5) are old evidence only.

## §2 Existing machinery

All paths/lines below refer to the exact base; tests named here were read, not run.

| Symbol/binding | Source anchor | Existing behavior / coverage | Decision |
|---|---|---|---|
| `food_acquired` | `Senegal/2018-19/_/mapping.py:52` | Calls `_food_acquired_with_unit_weights(df, FOOD_ACQUIRED_DERIVATION, wave='2018-19')`; `tests/test_ehcvm_unit_weights.py::TestWiring` | extend only its return boundary |
| YAML extraction | `Senegal/2018-19/_/data_info.yml:194` | Full `file`/`idxvars`/`myvars`, `u: s07bq03b`, separate consumption/last-purchase unit and size inputs, declared NSU dependencies | reuse byte-exact |
| Hook dispatch | `country.py:1500,1607,2030` | Wave `mapping.py` table-named callable becomes `df_edit`; live YAML path | reuse |
| Canonical declaration | `Senegal/_/data_scheme.yml:67` | `(t,v,i,j,u,s)`; no `materialize: make` for food | reuse byte-exact |
| `food_acquired_with_unit_weights` | `ehcvm.py:1318` | `_nsu_factor` first, canonical hook, duplicate-key kg refusal, NSU provenance stamping; `tests/test_ehcvm_unit_weights.py::TestHook` | reuse unchanged |
| `food_acquired_ehcvm`, `carry_unit_size` | `ehcvm.py:626,482` | Raw columns govern value eligibility; sized labels precede canonical split; no base means no suffix; `tests/test_ehcvm_unit_size.py::TestHook`, `TestCarryUnitSize`; `tests/test_ehcvm_7day.py::TestFunction` | reuse unchanged |
| `U_UNKNOWN`, `_U_SENTINELS` | `transformations.py:4148,4152` | Existing canonical literal and closed vocabulary; `tests/test_962_unit_sentinel.py::test_sentinel_spelling_gets_no_price_ratio_factor`, `test_sentinel_rows_keep_survey_kilograms` | import the constant only |
| `fill_missing_u` | `countries/Niger/_/niger.py:506` | Country-wide helper uses string casts and reset/set-index | do not borrow: would broaden scope/retype present codes |
| `_normalise_index_as_served`, input functions | `ehcvm.py:895,734,1377` | Derivation inputs use `(t,i,j)` input grain; original raw unit answers remain columns | leave exact; real joins UNRUN |
| Hashing | `country.py:1354,4517`; `_build_registry.py:603` | Whole wave mapping bytes affect every wave table; country hashes fold waves. Framework import closure follows callables, not the imported string constant | reuse; no measured hash census |

Source paths in the first two columns are relative to `lsms_library/` unless
they start `tests/` or `countries/`; `Senegal/` abbreviates `countries/Senegal/`.

## §3 Definitions & conventions in force

Inherit `.coder/ledger/STANDING.md` §§3–5, `CLAUDE.md` (especially Derived
Tables, Site I, grain policy, cache coupling), and `lsms_library/data_info.yml`.
The units skill's closed-sentinel rule applies; present undecodable codes and
literal strings `'nan'`, `'None'`, zero or blanks are not evidence of absence.
No labels/Aggregate curation or code equivalence is inferred from #842/#847.

**Personal full country read, before live config inspection:** all lines 1–1083
of `lsms_library/countries/Senegal/_/CONTENTS.org`, including the opening TODOs
and every later entry (no separate LOGBOOK block omitted), through its last NSU
correction. Read sequential ranges 1–210, 211–430, 431–650, 651–865, 866–1083
from the supplied exact source copy. Checkout and copy were byte-compared:
74,262 bytes, SHA256
`e95ffaeef89c9388c7f5b1c01ce532433457771d0173732dbe3185e522bf4585`, Git blob
`9e9f4e94ed905af6b75f5b90622449a5842c060d`.

**Actual idiosyncrasy:** 2018 section 7B's format-115 label decoding yields
mojibake; both `u` and `j` crosswalk keys deliberately match that output.
Quoted instructions: `*Do not* "clean up" these keys in isolation.` (548), and
`** =2018-19/_/food_acquired.py= is DEAD CODE -- do not reason from it` (552).
The live YAML/hook distinction is confirmed by current source, not by running
that legacy script. Both halves of each mojibake pairing stay untouched.

**Documented contradiction, retained with its correction:** the #876 text says
`no EHCVM wave ships a unit-conversion table` (610); the final line says
`*Corrects the #876 section above*: "no EHCVM wave ships a unit-conversion table" is wrong for 2021-22.`
The latter paragraph also keeps #876's different-unit value NA rule. Current
NSU processing is already wired; no factor, value or scalar answer changes.
The older size paragraph's 20 no-unit source rows and #1500's 21 served rows
have different historical denominators; neither is a new measurement.

## §4 Invariants & assumptions

- Preserve the shared helper call and all its arguments. Fill after size, NSU,
  reshaping, eligibility and Derivation stamping; never fill a raw answer.
- Change only missing positions in the output `u` index. Preserve every other
  index level/code/name/dtype and every payload, order, row and typed attribute.
  Operate on the helper's output, which is already distinct from the input.
- Use MultiIndex levels/codes to avoid resetting unrelated axes or stringifying
  present units. Add the sentinel category if a categorical `u` needs it.
- Detect newly coalescing full keys and raise with a source-free message;
  preserve pre-existing duplicates and do not aggregate or weaken grain guards.
- Keep all other hooks, 2021 source, shared framework, registry, crosswalks,
  dependencies and existing tests byte-exact. Product paths are only those in
  REPAIR-CONTRACT.md. Country documentation is a dated append, never a rewrite.
- Runtime/source/API/Org/parquet/DVC/cache/Make/CI checks are **UNRUN**; only
  stdlib syntax/AST/text/JSON and private Git checks are authorized now.

## §5 Reuse decision

| Need | Decision | Reason |
|---|---|---|
| Missing-unit meaning | reuse | Import `U_UNKNOWN`; no new sentinel or inference |
| Purchase/production/gift and NSU computations | reuse | Existing live shared helper owns them |
| Output label repair | extend | Only the wave-local live return boundary needs it; Niger's broad helper changes more types |
| Collision visibility | extend | Refuse two distinct old full keys becoming one; do not introduce a reducer |
| Focused evidence | new | Required new test module; invented literal oracles plus explicitly stubbed NSU/label boundaries, UNRUN |
| Cache behavior | reuse | Anticipate Senegal-only invalidation, all its country tables and 2018 wave tables; no shared schema bump |

## §6 Open questions for the human

None blocks the selected routine repair. No approval is requested. This worker
has no runtime grant; source-free draft execution, independent review, real
source parity, typed Country/Feature, cold/warm/hash checks, full CI and GitHub
acceptance remain separate coordinator gates. Read the absolute dispatch
MESSAGES_TO_AGENT.txt at checkpoints; append a genuine blocker once to the
existing absolute QUESTIONS_FROM_AGENT.txt only if one arises.

### Phase 3 — verification

Pending implementation. This first commit is the ledger checkpoint, before
production edits. Final anchored static findings will be committed separately.

# GH #1291 — Uganda other-table identifier rendering

## §1 Task

At fixed public `2ae987e7b853226e0ea7a9825d2d22144658079e`, finish the
remaining other-table rendering check after merged #1325. Nonfood's 54,027
repair and the genuine 244 target + 21 other-wave location gaps are settled;
do not repeat those cohorts or infer missing location/household mappings.

## §2 Existing machinery (ripgrep + git floor)

- `diagnostics._check_float_stringified_index`: reuse the existing flag, but
  its first-500-distinct-value sample and >50%/>5 thresholds do not establish
  zero residual. Add complete per-index-level regex counts in operator output.
- Existing `local_tools.format_id` is the established rendering repair, not
  approximate linking or a new ID canonicalizer.
- `Country`/`Wave.data_scheme` and physical folders provide actual declarations;
  shared/saved cache content requires exact source/hash and wave coverage checks.
- Original `burst/jobs/uganda/idmismatch.md` identifies the old nonfood source
  as numeric `GSEC15c.hh`, unlike most GSEC1-style `HHID` columns. Thus raw
  `astype(str)` elsewhere is a risk to investigate, not automatic bug proof.
- Native months_food_inadequate 2009/2010 casts HHID directly; assets/shocks
  retain it. Food helper already corrects a numeric household-ID index.

## §3 Definitions

Inherit STANDING.md, CLAUDE.md and data_info.yml. Uganda CONTENTS includes
LOGBOOK/TODO/WAITING caveats: native ag plot household prefixes need not equal
walked i; parish v is a library proxy, not proof of PSU redesign (#689);
2019 GPS has no public release despite the old TODO; #829 correction remains
WAITING. Preserve #1290 nine unsupported keys and #1297 parcel/plot limits.

## §4 Invariants

No stale cache or partial coverage is accepted as a complete current census.
Numeric-looking item/unit/native codes are investigated in their own source
vocabulary; a regex flag alone cannot authorize a scientific relabelling.
No broad acquisition, global cache clearing, whole-country blind build,
scientific producer/reducer/eligibility change or public write.

## §5 Reuse decision

First inventory metadata and retained evidence. Run finite private normal API
checks only for named meaningful gaps with declared cached inputs. If an actual
same-class rendering defect is proved, use format_id at its native boundary and
preserve all other source values, keys, rows, reducers, types and attrs.

## §6 Open questions

No scientific decision is introduced. The suspicious direct HHID paths were
tested below and show no current float-string IDs. Remaining source-only or
prior-acceptance coverage is explicit; missing-v alone is not this defect.

## §7 Verification

- OK (§§2–5): current normal APIs, complete per-level integer-float-string
  regex counts, all zero: assets 116,808/all8; shocks 14,457/all8; earnings
  22,447/all8; enterprise income 13,935/all8; months_food_inadequate
  14,796/all5; sample 24,362/all8. Native source/cache hash and requested wave
  manifests are verified. Sample was already current from those prerequisites.
- OK (§2): months2009/2010 raw HHID columns are strings; format_id changes
  zero. Their direct string casts do not reproduce nonfood's numeric hh defect.
- OK (§§2–5): current food_acquired all8 355,571 rows and default derived
  expenditures 247,049, quantities 355,487, prices 348,181. Full index regex
  counts all zero; derived household/wave identities are subsets of parent.
  No FCT/nutrition or provider acquisition. Exact 17-source preflight totals
  118,919,298 bytes. The initial arbitrary40MiB cap rejected an available
  52,903,255-byte source; the failed attempt is preserved and successor binds
  actual declared/observed byte sizes plus MD5 under the same120s/8GiB bound.
- Coverage is not a28-object current API census. All28objects have declaration,
  wave, source/config and cache metadata inventory. Initial shared/retained
  candidates had no current expected-hash match; none is promoted to current
  acceptance. Eighteen other objects retain explicit source-only, prior
  acceptance, derived or non-household-crosswalk status. Six-API checkpoint is
  immutable; the final packet is additive.
- No production code/test/fixture change is warranted. Nonfood/housing cohorts
  were not replayed; genuine location gaps and1290/1297/689 limits remain.

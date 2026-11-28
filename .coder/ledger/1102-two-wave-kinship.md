# Prior-Art Ledger — #1102 two-wave roster kinship

Search tier is git, ripgrep and Python AST. Inherits [STANDING.md](STANDING.md).

## §1 Task, restated

Keep the released bare `Relationship` in the Malawi IHS2 2004–05 and Uganda
UNHS 2005–06 Country rosters. The 2026-11-27 maintainer ruling approves a
known match with no determined `Generation`, `Distance` or `Affinity` for
just those two responses. Feature inherits the served roster result.

## §2 Existing machinery

| Anchor | Reuse |
| --- | --- |
| `country.py`, `Country._finalize_result` and `_read_l2_country_cache` | Pass roster context at the existing post-read call. Exercise the manifest-filtered cache hit with invented operands. |
| `country.py`, `_expand_kinship` and `_load_kinship_map` | Extend lookup narrowly. Keep the tuple/missing distinction and dtype handling covered by `tests/test_kinship_{other_relative,affinity_conflation,yml_hygiene}.py`. |
| `feature.py`, `Feature.__call__` | Keep the normal forwarding and assembly. Verify one exact country/wave per call. |
| `_build_registry.py`, `_EXCLUDED_CALLABLES`, and `country.py`, `_ORG_HASH_SKIP` | Use the existing post-read and documentation boundaries without adding hash exclusions. |

## §3 Definitions & conventions in force

Follow STANDING.md §3, `CLAUDE.md` §Canonical Schema, and
`lsms_library/data_info.yml` (`household_roster`). The country `CONTENTS.org`
entries for #1102 document the exact spellings and source variables. Uganda's
#698 history remains in force for affinity conflations and stepchild.

## §4 Invariants & assumptions

Per STANDING.md §4, no kinship components are written to a parquet by this
change. Match the exact label and actual row `t` within the roster method,
never infer row provenance from the requested waves. Keep the shared `Other`
mapping, true unknown warning, native decode, and keys untouched.

## §5 Reuse decision

Extend the existing kinship lookup with the scoped match. Reuse the shared
fallback, warning, dtype coercion, sample join, and Feature assembly unchanged.

## §6 Open questions

None for these two labels. No disposition is implied for other #1102 cases.

### Phase 3 — verification

- `Country._finalize_result` — OK (§§2, 4). Country and Feature tests run
  the real hit/finalizer paths. The non-roster finalizer remains unchanged.
- `_expand_kinship` — OK (§§3–5). Exact keyed frames and dtype checks cover
  both baselines, mixed and adjacent waves, named controls, and the genuine
  unknown warning. Both baseline assertions fail on the pinned base.
- Cache boundary — OK (§4). No registry or hash-rule change. The registered
  framework closure and bounded source-free country and wave hashes match.

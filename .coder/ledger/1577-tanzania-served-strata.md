# Prior-Art Ledger — #1577 Tanzania served strata

**Search tier:** git, ripgrep, release data dictionaries, and exact
projected Stata metadata and household-round sample comparisons.

## §1 Task, restated

Record what Tanzania's six logical waves serve under `sample().strata`
without changing the sample, proposing new design strata, or equating a
household's stratum with its cluster.

## §2 Existing machinery

| Machinery | Source | Reuse |
|-----------|--------|-------|
| `served_strata` | `lsms_library/sampling.py`, `STRATA_HOLDS` and `_check_served_strata` | Existing `native-code` and `native-label` tags and attrs carrier. |
| UPD source and round map | Tanzania `2008-15/_/sample.py` | One script reads `strataid`, renders its native code and validates invariance before household-round deduplication. |
| SDD and Wave 5 producers | Tanzania `2019-20/_/data_info.yml` and `2020-21/_/data_info.yml` | Reuse their own labelled `strataid`. The SDD `sample` hook derives the cluster, not the stratum. |
| Panel continuity | Tanzania `_/sampling.yml`, `_/CONTENTS.org` and `.claude/skills/tanzania-panel-design.md` | Preserve the existing warning boundaries and their measurements. |
| Native-path checks | `tests/test_sampling.py` | Retain the generic YAML guard. Exercise the shared UPD script through an invented projection as its own native-source check. |

## §3 Definitions & conventions in force

Use the #1577 ruling on the registry and sampling tag, `STANDING.md`
§§3–4 and `sampling.py`'s served-value definitions. Tanzania
`_/CONTENTS.org` distinguishes the 2014-15 sub-panels and the SDD
household's own strata from inherited `v`. The served-value tag does
not establish design-strata membership or a cross-wave crosswalk.

## §4 Invariants & assumptions

- Rounds 1–4 share the file and producer, not a household key space.
  Check the read and output at household-round grain. Do not use
  `strataid2` or fill any blank.
- The two modern fields have separate Stata value-label bindings,
  regardless of their variable titles. For 2020-21 a missing stratum
  stays missing, even though a cross-sectional weight can be served.
- Add only six tags and their provenance. The first wave gets a
  metadata record, not a panel predecessor. No keys, values, warning
  behavior or cache fingerprints should change.

## §5 Reuse decision

Use the existing four-value vocabulary and normal sampling attrs. No
new derivation, design label, or runtime code.

## §6 Open questions for the human

None for the served-value classification. Design inference, pooled
variance estimation and cross-wave mapping are separate decisions.

---
### Phase 3 — verification

- **OK (§§2–4).** Three admitted held covers (19,691,997 bytes) matched
  their DVC digests and lengths in separate private roots. Independent
  column-projected reads established the name bindings and matched
  every pre-finalize row's served value or blank. Round 4 has native
  codes on all 989 other households and no code on the 3,352
  loader-defined Refresh households. The Wave 5 null mask coincides
  with all 545 missing panel weights.
- **OK (§§2–4).** Ten strict cold/warm comparisons preserve all 22,433
  Country and Feature rows, index, dtypes, warnings and unrelated typed
  attrs. Wave 1 gets a strata-only record instead of the unrecorded
  placeholder; all five existing panel blocks are exact. Fresh warm
  processes neither open a native supplier nor rewrite the sample
  cache. All 175 Tanzania cache fingerprints are identical.
- **OK (§§2–5).** The existing YAML native guard covers the modern
  waves, and the source-free UPD check runs the actual producer with
  invented operands. It checks the source path, numeric rendering,
  missingness, no alternate-field fill and household-round grain.
  Verify with `pytest tests/test_sampling.py -m 'not requires_s3'`.

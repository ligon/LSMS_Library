# Prior-art ledger — #1275, Malawi 2024-25 housing

Inherits [STANDING.md](STANDING.md) §0. Search tier was git and ripgrep,
live issue history, and a four-object, hard-offline native audit on 47df62d49.

## §1 Task

Wire exactly the existing Malawi housing schema for IHS6 and the distinct
IHPS interviews in 2024-25. Serve each shared refresh interview once,
using IHS6, without changing sample, weights, aliases, or previous waves.

## §2 Existing machinery

- `2024-25/_/mapping.py:cs_i`, `_/malawi.py:CS_24` and
  `local_tools.format_id`: keep the established cross-section namespace.
- `2016-17/_/data_info.yml` and `2019-20/_/data_info.yml`: established
  Module F two-component Roof/Floor schema.
- `Wave.grab_data`, `_merge_subframes`, `_collect_file_paths_from_block`:
  existing YAML reader, merge, and nested-source hashing.
- `_finalize_result`: existing housing label maps, ID walk, and sample-v
  join from standard data.

## §3 Definitions in force

- One row per interview (GH #1275 decision, issue comment 6643393923);
  household housing at written `(t,i)`, two string columns only.
- Malawi `_/CONTENTS.org`, housing, 2024-25 registration, and #1281
  sections. IHS2 uses Module G, later waves use F. The 815 refresh
  interviews are the same IHS6 interviews; 1,948 retained interviews are
  separate. Do not infer fielding from an unwired table.
- The current `Roof` and `Floor` tables in `_/categorical_mapping.org`
  remain authoritative. No walls or Other-specify text in this feature.

## §4 Invariants and evidence

- Exact source bindings: IHS6 F `46660ca64ad473e96fe9cc50c6edf5b8`
  (18,189,876 B); IHPS F `c0ac8c142c7eaaefb9c08ab5dda078d1` (3,829,566 B);
  IHS6 cover `77291c794d64fb001df3266e9d7b417a` (2,751,262 B); IHPS cover
  `c4c79be4babe53ffc52d4df01723e9f7` (892,953 B).
- Phase A: 12,888 IHS6 and 2,763 IHPS F keys each match their cover
  exactly. No missing/duplicate key or orphan. All 815 counterpart pairs
  agree on both native answers, with no missing materials in the overlap.
  The two exact codebooks agree and are fully covered by the existing
  aliases. One plain Stata-system-missing IHS6 roof outside the overlap;
  zero missing floors; no both-blank case.
- Add the IHPS cover as an explicit YAML subframe so its sidecar affects
  the housing hash. The full public Country/Feature proof needs all covers
  because of the existing sample-v join. F1 is a different land roster.
- Retain missingness and component grain. Never choose by weight, row
  order, raw ID prefix erasure, or duplicate-grain collapse.

## §5 Reuse decision

Use the existing 2024 namespace and native two-file schema. Build a narrow
wave-local guard over the existing YAML merge. The outer merge makes an
unmatched panel-cover row visible to the guard, rather than disappearing
from a left join. The IHS6 cover is proof-only, not a new runtime dependency.
Both codebooks agree exactly, so no build-time housing label normalization
or new alias is needed. The normal finalizer still handles labels.

## §6 Open questions

None about the component scope or material interpretation. Full Phase B/C
is separately gated and not represented as complete here.

### Phase 3 — verification

- OK (§§2-5): offline Phase A projected only the four exact sidecars'
  keys and material fields. No further native data or public all-wave
  production build was run. A local aggregate receipt and reproducer are
  in runtime/mw-1275-housing-a.of85GG9p.
- OK (§§2-5): 572 source-free tests passed across the new real-YAML housing
  controls and the existing failure-not-absence suite. The controls cover
  both form reads, identity collision with unprefixed IHS6 IDs, exact
  preservation of ordinary missingness, matched blanks, one-sided blanks,
  conflicting material, false/missing/duplicate counterparts, unmatched
  panel records in both directions, and cache invalidation by the Panel
  cover. No old test was modified.
- OK (§4): metadata-only comparison moved 22 of 22 country fingerprints
  and 22 of 22 modern wave fingerprints, and none of 110 older wave
  fingerprints. Only 2024-25 housing became newly declared.
- Phase B/C Country/Feature data parity remains pending separate execution.

### Integration placement

The housing YAML and hook are between unchanged sections, so the separately
reviewed roster, sample-component and date work can compose without clashing
at the end of the wave files. Code ASTs and parsed YAML were checked equal
before and after the move. This is placement only, not a prerequisite merge
or a change to another study module.

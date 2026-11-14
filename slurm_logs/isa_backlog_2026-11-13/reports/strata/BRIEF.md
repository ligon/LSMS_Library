# Stream: strata -- #1577 strata meaning tag (+ #1134 vocabulary)

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/strata`, branch `feature/1577-strata-tag-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/strata/`.
- Issues: #1577, #1134, #792, #1574 (read with `gh issue view`).
- Rulings in `isa-decisions.org`:
  - "Strata meaning record, GH1577": "Registry + sampling.yml tag".
    Constructed strata stay in the derivations registry.  Add a small per-wave
    tag in `_/sampling.yml` for the native-code, native-label and absent cases,
    decided with #1134's vocabulary extension.  The #793 vocabulary is frozen;
    widen it one tag at a time, each with its first evidenced record.
  - The 11-10 #1575 and #1573 strata entries.
- Read CLAUDE.md "Site C" and `lsms_library/sampling.py`: the vocabulary, the
  refusals, and how `sampling.yml` is validated.  Read `tests/test_sampling.py`
  too.

## Work
1. Design the tag: its field name, its values, and where it sits in a wave's
   `sampling.yml` record.  The values cover native-code, native-label, absent,
   and a pointer to the registry for constructed strata.
2. Widen the vocabulary in `sampling.py` by exactly that tag.  It needs a first
   evidenced record: `source_type`, `confidence`, `locator`.  Test the refusal
   of unknown values.
3. Write evidenced records for every (country, wave) whose `sample` serves
   `strata`, where the evidence is already in the repo: CONTENTS, the
   derivations registry, data_info comments.  Draft the rest as a coverage
   table in the report; do not guess any of them.
   - Mali 2017-18 (#1574: EAC-I design codes) is a known case.
   - Leave Benin's and Guinea-Bissau's `sampling.yml` untouched: other streams
     edit those files.  List their proposed records in the report, and Sue will
     add them at integration.
4. Measure that the change moves 0 hashes.  `sampling.yml` is not a build input
   and `sampling.py` is read-path, but measure it rather than assume it.

## Verify
- `tests/test_sampling.py`, `tests/test_sample.py` (the non-S3 parts),
  `tests/test_schema_consistency.py`, `tests/test_build_transform_hash.py`.
- A hash census, expected 0 moved.
- `Country(C).sampling` loads for every country you edit.

## Deliver
Return `REPORT.md` and `PR_BODY.md` in your final message.  Commit as you go.

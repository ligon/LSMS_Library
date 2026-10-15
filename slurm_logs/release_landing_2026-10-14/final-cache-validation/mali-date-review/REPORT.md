SCOPE DEVIATIONS

None. This diagnosis made no table API calls, cache writes, source edits or
baseline changes. It read one private raw parquet and ran pure normalization
and dtype helpers on in-memory copies. Its SHA-256/size/mtime/inode remained
unchanged. The final runner was stopped with its complete descendant tree
verified quiescent, then resumed on the same source at the coordinator's
instruction after diagnostic evidence was preserved.

The exact Mali warm-only loss in existing issue #798 is reproduced and its
mechanism is now established. This is not a release-introduced source change.

| Wave | Raw cache / cold API | Current warm API | Default parse creates NaT |
|---|---:|---:|---:|
| 2014-15 EAC-I | 3,804 | 3,804 | 0 |
| 2017-18 EAC-I | 8,390 | 8,390 | 0 |
| 2018-19 EHCVM | 6,602 | 0 | 6,602 |
| 2021-22 EHCVM | 6,143 | 0 | 6,143 |
| Total | 24,939 | 12,194 | 12,745 |

The raw country parquet is complete, fresh, and carries all four waves in its
footer. Its pre-finalize fingerprint is
`7c4a369598abe0ae917d68d3b688ca018af1b8c30d9538cc1929a62391ee440a`.
The copied file remains byte-identical to the completed cold corpus. No source
or coverage-manifest change explains the warm difference.

The raw strings use `YYYY-MM-DD HH:MM:SS` for every EAC-I row, and `YYYY-MM-DD`
for every EHCVM row. `_normalize_dataframe_index` retains all 24,939 rows and
the identical raw fingerprint. `_enforce_declared_dtypes` calls whole-column
`pd.to_datetime(..., errors='coerce')`; pandas 3.0.6 infers the first format and
turns all date-only EHCVM values into NaT. Canonical dtype enforcement leaves
that result unchanged, and `_finalize_result`'s later `dropna(how='all')`
removes exactly those 12,745 rows. Reordering the index and running those pure
operations reproduces the recorded warm fingerprint exactly:
`9285bfbf9190053270d4f4d20b4f8ec4b92e52610c4e5637559003c63196aa94`.

As a diagnostic counterfactual only, per-wave parsing and whole-column
`format='mixed'` are exactly frame-equal. Both preserve all 24,939 rows with
zero NaT and reproduce the cold served fingerprint exactly:
`2d4fefbce6238f216a09e3042abaec82c8ed3aaeab330a630564f471ffeaf299`.
No production repair is included in this release change.

The September 9 comment on #798 records these exact four raw counts and this
warm loss under `eecd6305` and `d2729e81`. Its statement that coercion cannot
be the cause used separate per-wave probes; each homogeneous wave does parse
successfully, so that probe did not test the failing mixed whole column.
The suggested fabricated-visit mechanism is unnecessary to produce the loss:
normalization is lossless here and dtype coercion plus empty-row filtering
reproduces the exact observed digest. The issue's October 14 claim that "both
halves no longer reproduce on development" is contradicted by this warm call;
the cited successful cold call did not establish warm behavior.

Source attribution is unusually strong: AST-extracted function text hashes
for `_enforce_declared_dtypes`, `_enforce_canonical_dtypes`, `_finalize_result`,
and `to_parquet` are identical at v0.14.0, pre-train
452302adcfdbf3af538ef06375df1ac6c5aaa8a0, cold-corpus
7fa05179c8519f44ea329ba6387f5d6d6441a8ea, and final
3914d8804d7b2936d75041cea98bbc9ddb6d4c9f. This proves source equivalence for
the demonstrated route, not a historical rebuild under every dependency set.
The older issue independently records the behavior before this release train.

Named country idiosyncrasy: Mali combines EAC-I (2014/2017) and EHCVM
(2018/2021), rather than one uniform survey family. CONTENTS itself corrects
the Overview: "Only 2018-19 and 2021-22 are EHCVM; 2014-15 and 2017-18 are
EAC-I". Its sampling note says each cluster is visited in "exactly one vague";
the data_scheme comment instead says "EHCVM visits each household across
multiple vagues/passages". That existing design/grain documentation tension
is retained, not treated as permission to change visit keys or the reducer.
CONTENTS has no TODO/WAITING entry headings or LOGBOOK blocks for this issue;
its TODO keyword declaration was checked separately from task entries.

Coordinator disposition: keep #798 as an explicit existing limitation, as
with the previously attributed Feature food loss. Its complete, byte-verified
raw caches may be considered for promotion after the remaining gates and
independent review. Do not claim universal cold/warm API equality, mark the
issue resolved, change source, or hide the 12,745 lost warm-served rows.

Stable evidence: replay.py, replay.json, replay.log; saved issue snapshot
`control/ci-completion/issue798-before.json`; original and final Mali per-table
records and raw snapshots under `control/runtime/{corpus,final-promotion/run}`.

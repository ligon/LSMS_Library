# Prior-Art Ledger — GH #1287 Nigeria plot_features concatenator

Search tier: git/ripgrep + AST; inherits `STANDING.md`. No GitNexus exposed.
Nigeria CONTENTS including WAITING/LOGBOOK was read before source work.

## §1 Task

Prevent a country concatenator from silently writing a partial result when a
DECLARED wave parquet cannot be read. Scope one independent case:
`Nigeria/_/plot_features.py`. Preserve all healthy rows and PP quarter labels.
No framework/schema, Makefile, native source, sample or scientific pin changes.

## §2 Existing machinery

- `Nigeria/_/nigeria.py::Waves` names five physical folders; each has an
  existing `_ /plot_features.py` (without the space). Each wave script assigns
  its own PP quarter and writes `(t,i,plot_id)`; CONTENTS plot_features section.
- `Nigeria/_/plot_features.py` loops `Waves.keys()` but catches **every** reader
  exception and continues; `assert pieces` accepts any one successful wave.
- `local_tools.get_dataframe` and `to_parquet` are the existing sanctioned
  reader/writer. Reuse both; do not replace the framework aggregation path.
- Other concatenators: roster/housing enumerate all five folders; food,
  interview_date and nonfood enumerate four folders with four existing wave
  scripts (no demonstrated W5 omission). community_prices has the same broad
  skip hazard; food_coping has deliberate W4/W5 nondeclarations. Defer both.

## §3 Definitions

Cite STANDING §§3–4, CLAUDE canonical schema and Nigeria CONTENTS plot_features:
lasting attributes are PP-only (2010Q3/2012Q3/2015Q3/2018Q3/2023Q3).
Folder names are not public quarter labels. Missing declared inputs are
failures, not permission to omit a wave from a written country result.

## §4 Invariants

Keep five folder order, original frame content, nulls, dtypes and source
ownership. A reader failure must propagate before the sole country write;
never synthesize a missing wave or add an undeclared source. This country
script is hashed as a build input: an edit can invalidate other Nigeria
caches too. No actual caches are inspected/cleared/rewarmed in this task.

## §5 Reuse decision

Reuse existing reader/concat/writer and Waves. Remove the exception suppression
only. Test the actual script with invented per-folder parquet reader frames and
a capturing writer; include healthy five-quarter conservation and failures
for missing/unreadable/invalid declared middle-wave inputs. No real parquet IO.

## §6 Remaining acceptance

Source-only synthetic conservation cannot certify native sources or public
Country/Feature acceptance. Independent review is required before integration.
The root-owned successful-empty framework fix is an unrelated path.

## Verification against §§2–5

Actual script/invented leaves: baseline3 failures /1 healthy pass; repaired4
passes, no skips, zero IO guard refusals. Missing-file, runtime and key errors
on declared W3 each propagate by identity before writing; earlier W1/W2 reads
are preserved, later waves are not attempted. Healthy five-folder concat keeps
PP t, keys, nulls, Float64 and reader inputs exactly. No new aggregator,
transform or source reader was introduced. api_surface_audit.py passes using
PROJECT_PYTHON (Poetry is absent), and git diff --check passes. Own author
checks only; independent review and native/API acceptance remain pending.

## Declaration continuation (2026-10-28)

Actual `Country.waves`, `Country.__getitem__`, `Wave.data_scheme`, and `wave_folder_map` are the existing metadata machinery. Native scripts and YAML keys both declare a table; this change adds no YAML override policy. PP/PH logical quarters share physical folders, read once in original registry order. Stable hhid/plot_id and PP-quarter output follow CONTENTS.org; no ID walk, table builder, source mapping or science policy changes.

Source extracted from private0416008120af723b592bb6cba3b8cec41b00c6a1 onto public14df451d8837e14f0bf6fb3089ab195295d6e799, excluding inherited unrelated work. Author cohort27PASS versus13retirementFAIL/14healthy; independent extracted-candidate verification passed the same27cases with zero errors/skips/guard denials, unchanged source/config and empty data/L1. This is the same27-case population, not54unique cases. Prior four controls remain separate.

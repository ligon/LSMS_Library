# Prior-Art Ledger — #1743 sampling configuration duplicate keys

**Search tier used:** ripgrep + git, sampling validation/tests, installed
PyYAML constructor, #1578 duplicate registry guard and tests, and current
Ethiopia #1763 configuration diff.

## §1 Task, restated

Refuse duplicate keys in the sampling metadata carrier before YAML or the
wave-key normalization silently drops documented content. This is a
sampling-config validation change, not a new survey metadata claim.

## §2 Existing machinery

| symbol | path | what it does | choice |
|---|---|---|---|
| `_RegistryLoader` (#1578) | `lsms_library/derivations.py` | Detects explicit duplicates with two source marks before PyYAML flattens merges | extract once to shared opt-in loader |
| `_SamplingLoader` | `lsms_library/sampling.py` | Adds curated-config validation to SchemeLoader | extend here only |
| `_NonCanonicalIntError` | same | A config error raised with the annotated file path, not a warning and empty annotation | reuse the failure boundary |
| `SamplingRecord.from_config` | same | Validates a surviving public sampling record | retain |
| `SchemeLoader` | `lsms_library/yaml_utils.py` | Shared config loader including !make | leave unchanged |

## §3 Definitions & conventions

- Follow `STANDING.md §3` for cluster metadata. `sampling.yml` documents
  design and disclosure on the read side, not sampling weights or strata
  construction. The existing tests pin the metadata vocabulary.
- The public loader names waves with `str(wave)`, including Albania's
  calendar-year strings, so YAML keys that differ in type can still collide.

## §4 Invariants & assumptions

- Follow `STANDING.md §4` for build fingerprints. No changes to shipped
  records, default values, survey data or other configuration loaders.
- Check repeated explicit mapping keys before SafeLoader expands merges:
  a legal inherited default overridden by an explicit key is not itself
  a repeated explicit declaration. Refuse duplicates within anchor sources.
- Preserve the existing warning/empty-return behavior for unreadable or
  syntactically invalid YAML and the existing noncanonical integer refusal.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| YAML keys | reuse the #1578 node walk and PyYAML scalar/merge logic | avoid regex-based or second text parser and do not disable valid overrides |
| Wave keys | add a check at existing string normalization | PyYAML treats integer and quoted string keys as distinct, the public carrier does not |

## §6 Open questions for the human

- None for this fault guard. No change to the sampling design vocabulary or
  the interpretation of any survey.

### Phase 3 — verification

- Shared node walk — OK (anchored on §2, §4): the existing #1578 derivation
  tests cover source marks, merges, recursive aliases, tag dispatch and
  generic loader behavior. Derivations keep their exact error type and
  context. Sampling uses a separate duplicate-specific config error.
- Normalized waves — OK (anchored on §3–5): both YAML typing orders and a
  merge producing the same normalized wave refuse before a record is lost.
  The baseline silently accepted the duplicate controls, while ordinary
  inherited overrides and invalid-YAML warning behavior are preserved.
- Adoption scope — OK (anchored on §4): all 53 shipped records across 14
  files match values, types and order. Four exact in-review country
  proposals also match baseline records. Zero changes in 556 country,
  2,293 nonempty wave and 41 build-transform fingerprints.

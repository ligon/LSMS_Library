# Prior-Art Ledger — #1710 preferred asset label collisions

**Search tier:** Git and ripgrep. Inherits [STANDING.md](STANDING.md).

## §1 Task

Keep served assets frames exactly as before. Emit a separate, count-only
warning when the preferred asset relabel makes distinct pre-relabel item keys
coincide in the retained result. Scope is Burkina Faso, Mali, Niger and Uganda.

## §2 Existing machinery

| Symbol | What it does | Decision |
|---|---|---|
| `Country._finalize_result`, `_categorical_key_column` | Resolve the effective preferred relabel and apply it after input grain normalization | Observe this path in place |
| `_normalize_dataframe_index`, `_record_grain_report` | Audit and sometimes reduce pre-existing repeated keys, with strict mode and L2 replay | Keep unchanged |
| `Feature._harmonize_country_frame` | Preserve canonical index rows, with separate rules for shape promotion/projection | Keep unchanged |

## §3 Definitions

Use `lsms_library/data_info.yml` (Index Info for assets), the assets and
cross-country Feature skills, and `CLAUDE.md` (§ Categorical mappings and
§ Grain Collapse). The approved decision is “Keep current rows and keys, and
warn with counts only.”

## §4 Invariants

Use the actual retained final index and actual rename, not a second
interpretation of a mapping or an inference about raw records. Missing key
identity cannot establish a collision. Respect extra index levels that survived
finalization. No frame, warning filter, cache ledger, or build-input change.

## §5 Reuse decision

Extend only the excluded post-read finalizer with an independent signal. The
existing grain policy audits row loss, including its own payload-based notion
of losslessness. Neither is an oracle for distinct item keys after relabeling.

## §6 Open questions

Togo, Benin and Côte d’Ivoire remain outside this ruling. No claims about
native records or an untested survey shape follow from the synthetic tests.

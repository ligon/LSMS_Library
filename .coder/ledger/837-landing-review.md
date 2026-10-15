# Prior-Art Ledger — #837 historical landing review

**Search tier used:** ripgrep + git floor; no index mutation.

## §1 Task, restated
Review recovered `c8bf36d81` country hooks for Ethiopia and Tanzania
`cluster_features` against current development and dispose of the work under
the approved release plan without changing the signed aggregation contract.

## §2 Existing machinery

| machinery | source | decision |
|---|---|---|
| household projection and loss audit | `Wave.cluster_features`, `country._collapse_to_cluster_grain` | retain |
| most complete observed-row selection | `country.most_complete_row`, GH #871 | retain signed behavior |
| completion / conflict blanking | `build_transforms.reduce_to_agreed` | available, not authorized for these cases |
| Tanzania residency and geocode helpers | `Tanzania/_/tanzania.py` | retain; existing country tests cover |
| household-native Ethiopia merge | `.coder/ledger/323-ethiopia-config.md` | reuse; independently validate raw one-to-one join |

## §3 Definitions and conventions in force
- `STANDING.md` §3/§4 and `CLAUDE.md`, GH #871 signed 2026-09-12.
- `.coder/ledger/323-tanzania-key.md` §6.1: "The config does NOT collapse to
  `(t, v)` itself"; household grain stays visible to the core loss audit.
- Ethiopia CONTENTS: native household ids differ by wave and GPS column names
  have four spellings. Tanzania CONTENTS: "The residue, honestly"; tracking
  movers keep original clusters, and geocode interpretation is wave-specific.

## §4 Invariants and assumptions
- Preserve source identities, all DVC sidecars, fixtures and country API values.
- Distinguish missing-versus-value from multiple distinct nonmissing values.
- Zero administrative disagreement does not validate every GPS observation.
- Read live TODO/WAITING/LOGBOOK caveats: Tanzania #114 back-casting remains
  live despite closed issue; it is unrelated to this disposition.

## §5 Reuse decision
Reuse existing household-grain regression as the negative control. No new
transform or redundant test is needed. Base/candidate cold frames and a
separate raw-source one-to-one join provide independent aggregate evidence.

## §6 Open scientific residual
#837 still needs country-specific decisions about shipped coordinate
disagreement and district harmonization. Neither a composite key nor blanket
conflict-to-NA is justified merely by the loss warning. The release review
does not sign a new completion/blanking policy.

### Phase 3 — verification
- Recovered early projections — **CONTRADICTION** (§3): remove household grain
  and override signed observed-row selection; rejected, original ref retained.
- Ethiopia prior CONTENTS inference — **CONTRADICTION** (raw-source replay):
  W1 conflicts are 34 mixed-missing GPS groups, not administrative disagreement.
  Corrected with the old claim quoted rather than silently discarded.
- Final source state — **OK** (§4): the three WIP source files match
  `452302adc` exactly. Only documentation and evidence differ from development.

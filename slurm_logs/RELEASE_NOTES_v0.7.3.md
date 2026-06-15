# LSMS Library v0.7.3

Released from `development` (119 commits since v0.7.2).

## Highlights

### New country: Ethiopian Rural Household Survey (EthiopiaRHS)
- 5 waves wired (#272, closes #271); 1989 income + transitive `panel_ids` (#273).
- Bellemare-etal13 custom food-label scheme as a worked example of a bespoke
  `j` aggregation (note: the *out-of-tree* mechanism remains tracked in #279).
- `Wave.data_scheme` now tolerates an unwired stub wave with no `_/` dir
  instead of raising `FileNotFoundError` (#276, closes #274).

### `plot_features` rolled out across LSMS-ISA (closes #167)
Uganda (8 waves) plus Nigeria, Tanzania, Ethiopia, Mali, Senegal,
Guinea-Bissau, Benin, Niger, Togo, Burkina Faso, and Malawi. Cross-country
`TenureSystem` spellings harmonized. EthiopiaRHS plot_features design landed
(#328); implementation tracked in #278.

### Feature-coverage sweeps
- **assets**: Guyana, Albania, Iraq, Kosovo, Liberia, Serbia (#319) — with
  per-`(t,i,j)` instance aggregation so duplicate durable rows aren't dropped.
- **interview_date**: Tajikistan, Cambodia, Guyana, Pakistan, India, Kosovo,
  Iraq (#318).
- **individual_education**: Tajikistan, GhanaLSS, SouthAfrica, Albania,
  Kazakhstan, Ethiopia, Kosovo, India, Liberia (#320).

## Fixes
- `roster_to_characteristics` applies a mover sentinel for NaN `v` instead of
  dropping mover households (#270, closes #268).
- `_add_market_index` dedups the cluster-fallback `v_lookup` (#267, closes #266).
- `_join_v_from_sample` warns when it would silently drop a wave (#265, closes #256).
- Replace removed-in-pandas-2.1 `groupby(axis=1)` with `T.groupby().T` (#261).
- Drop obsolete Uganda `household_characteristics` build rule (#259).

## Notes
- Version is derived from this git tag via poetry-dynamic-versioning.
- Known open backlog after this release: recent bug findings (#321–#327, #329),
  data-gap issues (#107–#118), and the enhancement backlog (#168, #170, #171,
  #218, #223, #226, #231, #251, #260, #262, #279).

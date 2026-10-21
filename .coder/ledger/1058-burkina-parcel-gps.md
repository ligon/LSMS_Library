# Prior-Art Ledger - #1058 Burkina parcel coordinates

**Search tier:** git/ripgrep/AST; read-only profiling of existing hash inputs.

## §1 Task, restated

Expose the recorded Burkina_Faso 2021-22 parcel latitude/longitude through
the existing plot_features helper and retained source rows. Root approved
the bounded Tier B proposal after the complete read-only source diagnosis.
No coordinate eligibility, household identity, panel, area, unit, source,
universe, canonical-schema or shared-reducer change is authorized.

## §2 Existing machinery

| Symbol/path | Existing behavior | Decision |
|---|---|---|
| local_tools.get_dataframe / data_access.get_data_file | sanctioned typed source and held metadata access | reuse |
| Burkina_Faso/_/burkina_faso.py::plot_features_for_wave | native parcel mask, positional values, six old payloads | extend with optional typed coordinates |
| 2018-19/_/plot_features.py | passes ehcvm_i | preserve |
| 2021-22/_/plot_features.py::_old_i | wraps country i; matches that wave's sample namespace | preserve; add two colmap entries |
| local_tools.id_walk / Country load_from_waves | panel map before country write; id_converted attrs | reuse tested mapper |
| Country._join_v_from_sample | API-time cluster join | reuse |
| canonical data_info.yml tables.plot_features | float Latitude / Longitude already permitted | reuse |
| Country._table_cache_hash / Wave._input_hash | actual hashes and wave-coverage gates | reuse, no stamp or bypass |
| Feature / bench/feature_audit | canonical assembly and row conservation | reuse with Senegal comparator |

## §3 Definitions and conventions in force

Inherit STANDING.md sections 3/4, CLAUDE.md data-access/index/attrs/cache
conventions, canonical lsms_library/data_info.yml and the full country
CONTENTS.org, including its live tasks. The plot axis is plot_id.
Burkina 2021 is a half-panel restricted to households in their original EA;
2018 vague is a sample split, not a new measurement axis.

## §4 Invariants and source grounding

The source has 8,467 rows: 7,350 native parcels and 1,117 placeholders.
All 4,180 coordinate pairs are on retained parcels; 3,170 retained parcels
lack both coordinates and none is a partial pair. Ten GPS-measured parcels
have area but no position. q46 is position; q47 is area.

The helper/scheme claim "EHCVM s16a has no decimal-degree parcel GPS"
conflicts with the dated CONTENTS correction and held 2021 variable labels.
The concatenator's pre-transformation cache description differs from the
default framework route: load_from_waves applies id_walk before writing
the raw country cache, which carries id_converted=True. Native and walked
keys are distinct proof layers. Preserve both existing formatters and attrs.

The current public table has 19,784 rows. Seven all-six-payload-missing 2018
rows are excluded; 2021 retains all 7,350 parcels. Private invariance must
preserve those outcomes without permanent tests pinning unrelated defects.
Native and walked parcel keys have no collision and public sample orphans
are zero. Held Mali/Niger/Senegal 2021 s16a headers/labels have no positions;
their full CONTENTS were read before the diagnosis source checks.

## §5 Reuse decision

Add optional latitude/longitude keys to the existing helper and bind them
only in 2021. Use pd.array on retained src positions, independently for
each axis. A declared absent source column raises. Missing optional keys
leave a wave unchanged. Declare existing canonical fields as optional float
in the country. No new helper, merge, axis, filter, proxy, interpolation or
coordinate plausibility rule is needed.

## §6 Open questions

None blocks this approved source mapping. Other country files and survey
design questions remain outside this implementation. The prior diagnosis
is immutable; all writer evidence belongs to the separate 1058-fix root.

### Phase 3 - verification

Pending actual paired private cold/warm/source/Feature/test/hash evidence.
The diagnosis observed 24 Burkina country hashes depending on the helper
and scheme, with 525 other cells and 39 framework tags unaffected; actual
candidate hashes will be measured after implementation.

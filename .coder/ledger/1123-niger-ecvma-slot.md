# Prior-Art Ledger: Niger ECVMA native slots (#1123)

## §1 Task
Preserve applied ECVMA roster lines through Country and Feature on d96a8152b.
Tentative representation for coordinator review, not public integration.

## §2 Existing machinery (git/rg search; GitNexus unavailable)
- Niger `_finish_plot_inputs`, four native producers; tested by
  test_ehcvm_plot_inputs_identity and test_niger_u_sentinel.
- Malawi `_seed_source_reference` / `_seed_block`, merged #1323: source-release
  qualified input_obs, tested by test_gh1298_malawi_bounded_observations.
- canonical data_info.yml already registers input_obs and unresolved_observation.
- Country normalization / most_complete_row implements #871; leave unchanged.
- Feature canonical harmonization already transports input_obs; reuse unchanged.
- local_tools.cached_file_hash supplies sidecar SHA256; Malawi's helper
  composes path@dvc:filename:sidecarhash (no generic token helper exists).

## §3 Definitions
Cite STANDING.md §§3–4, CLAUDE.md grain doctrine, canonical data_info.yml.
Niger CONTENTS: slots 11–17 all semences, distinct source occurrences even when
crop/unit/payload agree. 2014 household includes EXTENSION; no crosswalk edits.
Live #1123 comment 6513812395 proposes shared input_obs release-qualified slots.
Older ledger says no guard because collisions remain: new axis removes that
premise; neither #871 nor an arbitrary reducer is a solution to missing identity.

## §4 Invariants
Keep eligibility ==1, zeros/NA/numeric precision, units/crops, household mapping,
all non-seed inputs. No provider/network in source actors; private runtime/L1.
Source token + household + recorded slot must be injective on retained lines.
EHCVM remains unresolved_observation and keeps existing guards and payloads.

## §5 Reuse decision
Extend Niger schema/helper with existing input_obs axis. New ECVMA token builder
reuses cached_file_hash and Malawi's reference format (sidecar-only). Native
slot instead of Malawi file position: ECVMA has a recorded unique household/slot
key. No global schema or core reducer changes. Check native key before writing.

## §6 Review gates
Root arranges independent review. Consumers addressing levels positionally need
the appended axis. Explicit household seed aggregations may legitimately change
because recovered lines now contribute. #1288 freshness remains separate.

## Verification against §§2–5
OK: actual ECVMA native projections exactly equal the pinned-parent native
frames; source payload/mask/EA comparisons and normal Country/Feature routes
preserve every retained line (8,538 / 8,179). Fresh old-key collision counts
67/39, excess73/45; no native household/slot duplicates. Tiny-at-zero and
nextafter controls reject approximate comparisons. Nitrogen transform exact;
seed sum intentionally gains recovered inputs. 89 targeted tests pass,
including the unchanged #871 selector and Malawi observation tests.
No generic source token helper exists: the initial inventory's tentative name
was corrected after inspecting Malawi; implementation reuses cached_file_hash
and the existing sidecar reference format locally, avoiding cross-country imports.
Root owns independent scientific review; no public issue or PR was written.

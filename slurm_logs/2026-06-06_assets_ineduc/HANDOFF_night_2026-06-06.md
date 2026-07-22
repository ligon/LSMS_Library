# Handoff — matrix-fill night 2026-06-06 → 07

## Merged (user, on gh)
- #351 housing (12 fills + 3 absent #350), #353 shocks (3 fills + 13 absent #352).

## Open PRs (awaiting merge)
- **#358** plot_features — 8 fills + 9 absent (#357). off development.
- **#359** subjective_well_being LADDER — 10 fills (#331 feature 1) + GhanaLSS/Ethiopia absent. off development.
- **#360** food_security FIES — 11 fills (#332 FIES-canonical). **STACKED ON #359** (merge #359 first; diff then reduces to food only).

Branch chain: development → swb-ladder-fill (#359) → food-security-fies (#360).
Overlap note: #358 & #359 both touch Albania data_scheme (trivial). #360 stacked on #359 to avoid 8 EHCVM conflicts.

## Held for Ligon's schema decision (NOT done)
1. **life_satisfaction** (#331 feature 2). Recommended: long-form (t,i,Domain)+Satisfaction,
   household-level, finances-as-domain. Surveys: Iraq §23, Tajikistan M8AQ9, South Africa §9,
   Tanzania §G, Timor §13, Serbia&Montenegro, Armenia §H.
2. **Non-FIES food_security families** (#332, "per-family features"): HFIAS (Nigeria, Tajikistan),
   rCSI/coping (Tanzania, Nepal, Ethiopia W1-3), months-of-shortage (India, Liberia, Uganda,
   Timor), bespoke (EthiopiaRHS, Iraq). Each needs its own feature + schema sign-off.

## Remaining matrix cleanup
- Structural-N/A docs (Armenia/Nepal no-microdata; EthiopiaRHS/Serbia-and-Montenegro legacy).
- food_acquired sweep (deferred, the heavy one).
- Pre-existing bugs surfaced: GhanaLSS 1991-92 roster doesn't materialize (Makefile gap);
  Malawi 2016-17 case_id↔sample v-mismatch (44% null v, pre-existing); Iraq 2012 roster hh
  mis-key (GH #256); Albania 2002/05 roster within-cluster-id (#340).

## Method notes (what worked)
- Main-checkout-scoped agents, get_dataframe only (lock-free), verify-or-document, no agent commits.
- Triage was systematically over-optimistic (shocks/plot esp.); implementation agents reading
  ACTUAL variable labels caught many wrong-file/wrong-column claims — nothing fabricated.
- Throttle ceiling ~13 concurrent agents (1 died at 13 in housing); kept waves ≤8 after.

# Design — #331 subjective_well_being, "two features" decision (2026-06-06)

Decision (Ligon): split the construct into TWO features rather than one broad table.

## Feature 1: `subjective_well_being` = the welfare/Cantril LADDER (extend Malawi anchor)
Schema (Malawi anchor): index (t, i); `Own Step` (int) [+ optional `Neighbors Step`,
`Friends Step` where the survey asks them]. Add a per-country note of the ladder's
step-count (Albania 6, Malawi 10, Cantril 10) — comparability caveat; do NOT rescale.

Wire (ladder self-placement):
- Malawi — already wired (Module T, hh_t05/06/07).
- **EHCVM §20 `s20q05`** (self-placement on a poor→rich welfare ladder) — ONE shared
  extractor: Burkina_Faso (2018-19, 2021-22), CotedIvoire (2018-19), Mali (2018-19, 2021-22),
  Senegal (2018-19, 2021-22), Togo (2018). + Benin, Guinea-Bissau IF §20 confirmed (unknowns).
- Kazakhstan 1996 — Cantril `d_095` (KZ96OCC_PUF; individual file → may need HH reduction).
- Albania — Module 7 "Subjective Poverty" ladder (6-step; verify per wave).
- Niger 2014-15 §15 Aspirations — ladder-ish; verify it's a self-placement step.

## Feature 2: `life_satisfaction` (NEW) = life / domain satisfaction RATINGS
Distinct construct (ordinal satisfaction, often multi-domain). OPEN schema sub-decisions:
- **Shape**: LONG `(t, i, Domain)` + `Satisfaction` (ordinal str) — recommended, mirrors
  shocks `(t,i,Shock)`; handles the multi-domain surveys (Tanzania, Iraq) naturally. vs
  WIDE per-domain columns.
- **Unit**: household vs individual — Timor §13 S13A is INDIVIDUAL (t,i,pid); SA §9, Tanzania
  §G, Iraq §23, Tajikistan are household. Pick (t,i) household-level and reduce/representative-
  member the individual ones, OR allow (t,i,pid). (Recommend household (t,i[,Domain]).)
Wire (satisfaction):
- Iraq 2012 §23 `q2302_*` (satisfaction with food, …, multi-domain).
- Tajikistan (all 4 waves) — `M8AQ9` "overall satisfied with life" (+ subjective poverty/food).
- South Africa 1993 §9 "Perceived Quality of Life" (S5_PQL).
- Tanzania (6 waves) §G "Subjective Welfare" (HH_SEC_G; multi-domain: health/finance/housing/job…).
- Timor-Leste (2001, 2007-08) §13 (2001 individual S13A + household S13B).

## Feature 3 (open): subjective financial position
Serbia-and-Montenegro (5-pt financial standing), Armenia §H (satisfaction w/ econ situation),
Senegal `s20q02` (living-standard rating). OPTIONS: (a) a "Finances" Domain inside
`life_satisfaction`; (b) its own `subjective_finances` feature. Lean (a) to avoid feature sprawl.

## Unknowns to resolve before wiring (per #331)
Benin, Ethiopia, GhanaLSS, Guinea-Bissau — deeper questionnaire check (don't read as absent).
EHCVM §20 strongly implies Benin/Guinea-Bissau have the ladder.

## Implementation plan (after plot_features PR)
1. Resolve the 4 unknowns (quick recon).
2. **subjective_well_being ladder** batch — EHCVM §20 shared extractor (5-7 ctry) + standalone
   ladders (Kazakhstan, Albania, Niger). High-leverage, schema already exists.
3. **life_satisfaction** batch — once the shape/unit sub-decisions are fixed (firm up first).
4. Document any confirmed-absent (Cambodia, China, India, Pakistan, Uganda, Nigeria, Guyana,
   Guatemala, Liberia, Kosovo, Azerbaijan, EthiopiaRHS per #331's "absent" rows).
Reference: #331 + slurm_logs/research_swb_foodsec_2026-06-05.json (machine-readable findings).

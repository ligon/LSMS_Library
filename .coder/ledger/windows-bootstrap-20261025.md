# Prior-Art Ledger — Windows Conda bootstrap, 2026-10-25

Search tier: rg/git and primary GitHub Actions/source receipts. Inherits
STANDING.md. This is a CI bootstrap repair; no country transform changes.

## §1 Task

Restore the existing Windows script-build verification, which fails during
Conda initialization before library installation, credentials or data commands.
Base: development763bd90a163c011cd5511ed9eb41b1b60c022a40.

## §2 Existing machinery

Reuse .github/workflows/ci.yml windows-script-build, setup-miniconda@v3,
environment-windows.yml, the installed-wheel check and private_ci_check.py.
The existing bootstrap input supports an explicit miniforge-version.
Independent diagnosis: private ci-postmerge/REPORT.org and primary receipts.

## §3 Definitions in force

Preserve STANDING.md, existing project/environment dependency declarations,
credential boundaries and the private output wrapper. A green Windows smoke
test is not a successful credentialed script build.

## §4 Evidence and invariants

Current763bd90a run40200435444 and exact pre-batch57d255585 run40026955948
both fail conda init --all with Miniforge26.9.4-0. Prior run39898120087 at
9a0ed5f3c succeeds with26.7.2-1. The action SHA, Windows image and relevant
workflow bootstrap section match across these observations. This establishes
a pre-existing failure and an observed working installer, not its exact OS
cause. Conda's needs-sudo label can represent an OSError.

## §5 Reuse decision

Pin only miniforge-version to26.7.2-1 temporarily. Preserve all environment
packages, library pins, commands and gates. Do not suppress initialization
errors, weaken checks or retry the known failing latest installer.

## §6 Verification plan

Review the exact one-line workflow delta and parse YAML. The prior successful
native run is precedent, not fresh acceptance. Require a fresh Windows job on
the corrected ref to verify initialization, tool/environment identity and the
unchanged installed-wheel/private-wrapper/cold-build route. No local runtime
or new project dependencies are needed for the source change.

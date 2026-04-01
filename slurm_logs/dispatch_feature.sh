#!/bin/bash
# Dispatch a Claude agent on a compute node to add a feature to a country.
#
# Usage:
#   ./dispatch_feature.sh <country> <feature> [wave]
#
# Example:
#   ./dispatch_feature.sh Niger assets 2021-22
#   ./dispatch_feature.sh Mali food_acquired 2017-18
#
# The agent runs in a git worktree so it doesn't conflict with other
# agents or the main working tree.  Results are committed on a branch
# named claude/<country>-<feature> and can be reviewed/merged.

set -euo pipefail

COUNTRY="${1:?Usage: dispatch_feature.sh <country> <feature> [wave]}"
FEATURE="${2:?Usage: dispatch_feature.sh <country> <feature> [wave]}"
WAVE="${3:-}"

REPO=/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
SKILLS=/global/scratch/fsa/fc_jevons/ligon/mirrors/sucoder-skills
LOGDIR="${REPO}/slurm_logs"
BRANCH="claude/${COUNTRY,,}-${FEATURE}"
PROMPT_FILE="${LOGDIR}/prompt_${COUNTRY,,}_${FEATURE}.txt"

WAVE_NOTE=""
WAVE_FLAG=""
[ -n "$WAVE" ] && WAVE_NOTE="Focus on wave ${WAVE}." && WAVE_FLAG="--wave ${WAVE}"

# Write prompt to file first (avoids shell quoting issues in --wrap)
mkdir -p "${REPO}/worktrees" "${LOGDIR}"

cat > "${PROMPT_FILE}" << PROMPTEOF
Add the '${FEATURE}' feature to ${COUNTRY} in the LSMS Library.

You are running in a git worktree. Your current directory IS the repo root.
All file paths should be relative to the current directory (e.g.,
lsms_library/countries/${COUNTRY}/_/data_scheme.yml). Do NOT use absolute
paths to ${REPO} — that is the main tree and changes there will conflict
with other agents.

Read these skills first:
- ${SKILLS}/add-feature/SKILL.md
- ${SKILLS}/add-feature/${FEATURE}/SKILL.md (if it exists)

${WAVE_NOTE}

Steps:
1. Read existing data_scheme.yml and data_info.yml for ${COUNTRY}
2. Read a reference implementation (Mali 2018-19 for EHCVM, Uganda for others)
3. Inspect .dta column names with pyreadstat metadataonly=True
4. Write data_scheme.yml and data_info.yml entries
5. Run: LSMS_SKIP_AUTH=1 ${REPO}/.venv/bin/python slurm_logs/run_validate.py ${COUNTRY} ${FEATURE} ${WAVE_FLAG}
   (The .venv is in the main repo; use the full path for the Python binary only)
6. If validation fails (errors), fix and retry
7. If validation warns about:
   - Extra index levels: remove them from idxvars (e.g., i should not be in cluster_features)
   - String inconsistency: create or update categorical_mapping.org with a normalization table
   - Unmapped labels: add mappings to data_info.yml or categorical_mapping.org
   - Cross-country column mismatch: this may be expected (different schemas), note it
8. Fix all fixable warnings before committing. File gh issues for warnings you cannot fix.
9. Commit when report.ok is True AND warnings are addressed

Rules:
- Use get_dataframe() from local_tools for data access, NOT dvc.api.open
- Use pd.NA not np.nan
- Do NOT touch other countries files
- Clear DVC locks if needed: rm -f lsms_library/countries/.dvc/tmp/*.lock
- Do NOT pip install anything. The venv has everything you need.
- You have a dedicated compute node with many cores and plenty of RAM.
  Use parallel agents to inspect multiple waves simultaneously.
  Run validation in the background while writing configs for the next wave.
PROMPTEOF

sbatch \
  --job-name="feat_${COUNTRY,,}_${FEATURE}" \
  --partition=savio2_htc \
  --account=fc_jevons \
  --time=02:00:00 \
  --nodes=1 --ntasks=1 --cpus-per-task=4 \
  --output="${LOGDIR}/feat_${COUNTRY,,}_${FEATURE}_%j.out" \
  --error="${LOGDIR}/feat_${COUNTRY,,}_${FEATURE}_%j.err" \
  --export=ALL \
  --wrap="cd ${REPO} && WORKTREE=${REPO}/worktrees/${COUNTRY,,}-${FEATURE} && (git worktree add \${WORKTREE} -b ${BRANCH} master 2>/dev/null || git worktree add \${WORKTREE} ${BRANCH} 2>/dev/null || git worktree add \${WORKTREE} -B ${BRANCH} master) && cd \${WORKTREE} && cat ${PROMPT_FILE} | claude --dangerously-skip-permissions --print - && cd ${REPO} && git worktree remove \${WORKTREE} --force 2>/dev/null || true"

echo "Submitted: ${COUNTRY} ${FEATURE} on branch ${BRANCH}"

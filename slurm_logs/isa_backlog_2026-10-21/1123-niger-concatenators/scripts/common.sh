# sourced by every run script: pin the base commit, set OUT.
set -u
: "${LABEL:?set LABEL}"
if [ "$LABEL" = base ]; then
  git -C "$CLONE" checkout -q --detach 57d255585 || { echo "BASE CHECKOUT FAILED"; exit 4; }
fi
echo "LABEL=$LABEL head=$(git -C "$CLONE" log --oneline -1)"
OUT="$JOBDIR/out-$LABEL"; mkdir -p "$OUT"
S="$JOBDIR/scripts"

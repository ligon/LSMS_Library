#!/bin/bash
# Sue, GPT-6. Runnable recipe; use a NEW private root or new scenario names.
# Existing evidence is immutable: stage-scenario intentionally refuses reuse.
# All detailed output must remain private. Do not execute against shared data.
set -eu
P=/local/job40255479/control/isa-backlog-20261021/Uganda/1078
PY=/local/job40255479/venv_img/bin/python

# Preparation used for this diagnosis. snapshot.py before is taken only once.
# "$PY" "$P/snapshot.py" before > "$P/snapshot-before.log" 2>&1
# bash "$P/run.sh" baseline preparation "$P/prepare.py" > "$P/preparation-attempt2.log" 2>&1

# Normal cold and independent fresh-process warm; these use naturally built L2.
# for REVISION in baseline with1062; do
#   bash "$P/run.sh" "$REVISION" cold "$P/probe.py" cold > "$P/cold-$REVISION-private.log" 2>&1
#   bash "$P/run.sh" "$REVISION" cold "$P/probe.py" warm > "$P/warm-$REVISION-private.log" 2>&1
# done

# Actual historical-log nodes, two ordinary serial orders per frozen head.
# MARKET=tests/test_sample.py::TestUganda2009MarketFallback::test_food_expenditures_retains_hybrid_v_HH
# CURRENCY=tests/test_currency.py::test_country_currency_index_roundtrip
# for REVISION in baseline with1062; do
#   "$PY" "$P/stage-scenario.py" "$REVISION" market-then-currency
#   TRACE1078=1 bash "$P/run.sh" "$REVISION" market-then-currency "$P/exercise.py" "$MARKET" "$CURRENCY" > "$P/$REVISION-market-then-currency-private.log" 2>&1
#   "$PY" "$P/stage-scenario.py" "$REVISION" currency-then-market
#   TRACE1078=1 bash "$P/run.sh" "$REVISION" currency-then-market "$P/exercise.py" "$CURRENCY" "$MARKET" > "$P/$REVISION-currency-then-market-private.log" 2>&1
# done

# Explicitly bounded concurrency: two actual tests, two consumers/root.
# Only scheduling pauses change. Real Path.exists, frames and write calls do not.
# for REVISION in baseline with1062; do
#   "$PY" "$P/stage-scenario.py" "$REVISION" timed-pair
#   "$PY" "$P/coordinate.py" "$REVISION" > "$P/coordinate-$REVISION-private.log" 2>&1
#   bash "$P/run.sh" "$REVISION" timed-pair "$P/corrupt-warm.py" > "$P/corrupt-warm-$REVISION-attempt2-private.log" 2>&1
# done
# "$PY" "$P/trace-proof.py" > "$P/trace-proof-private.log" 2>&1

# Existing held/tracked CSVs only. FDC fallback entry and all IP IO forbidden.
# for REVISION in baseline with1062; do
#   "$PY" "$P/stage-scenario.py" "$REVISION" nutrient-orders
#   TRACE1078=1 FORBID_FDC1078=1 bash "$P/run.sh" "$REVISION" nutrient-orders "$P/nutrient-probe.py" > "$P/nutrient-$REVISION-private.log" 2>&1
#   bash "$P/run.sh" "$REVISION" registry "$P/registry-inspect.py" > "$P/registry-$REVISION-private.log" 2>&1
# done
# bash "$P/run.sh" baseline source-proof "$P/source-proof.py" > "$P/source-proof-attempt4-private.log" 2>&1

# Final receipt only after every consumer has exited.
# "$PY" "$P/final-proof.py" > "$P/final-proof-private.log" 2>&1
# "$PY" "$P/snapshot.py" after > "$P/snapshot-after.log" 2>&1

printf '%s\n' 'Recipe is intentionally inert: use a new evidence root before replaying.'

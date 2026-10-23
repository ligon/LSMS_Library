#!/bin/bash
# queue.sh <label> <checkout> <parallel> countries...
label=$1; checkout=$2; par=$3; shift 3
printf "%s\n" "$@" | xargs -P $par -I@ /local/job40390255/data-private/1245/run_country.sh $label $checkout @

#!/bin/sh
# A/B helper: min internal time over N single-thread runs of IMAGE on dataset DS.
# Usage: IMAGE=tag bench/ab.sh DS N
DS="$1"; N="${2:-5}"
for i in $(seq "$N"); do NCPU=${NCPU:-1} bench/vrun.sh "$DS" 2>&1 | grep -E "\] done" | sed 's/^\[ *\([0-9.]*\)s.*/\1/'; done | sort -n | head -1

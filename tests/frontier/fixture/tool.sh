#!/bin/sh
# Writes its mode argument and the concatenation of its inputs to its output.
out=$1; mode=$2; shift 2
{ printf '%s\n' "$mode"; cat "$@"; } > "$out"

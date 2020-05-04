#!/bin/bash

set -e

if [ "$#" -ne 3 ]; then
    echo "$0 PBRT_BIN PBRT_DIR OUT_DIR"
    exit 1
fi

PBRT_BIN="`realpath \"$1\"`"
PBRT_DIR="`realpath \"$2\"`"
OUT_DIR="`realpath \"$3\"`"

rm -r "$OUT_DIR"
cd "$PBRT_DIR"

for pbrt in `find . -name '*.pbrt'`; do
    name=`basename $pbrt .pbrt`
    mkdir -p "$OUT_DIR/$name"
    "$PBRT_BIN" --nomaterial --dumpscene "$OUT_DIR/$name" "$pbrt"
done

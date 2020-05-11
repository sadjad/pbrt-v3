#!/bin/bash

set -e
set -x

if [ "$#" -ne 3 ]; then
    echo "$0 PBRT_BIN PBRT_DIR OUT_DIR"
    exit 1
fi

PBRT_BIN="`realpath \"$1\"`"
PBRT_DIR="`realpath \"$2\"`"
OUT_DIR="`realpath \"$3\"`"

cd "$PBRT_DIR"

for pbrt in `find . -name '*.pbrt'`; do
    name=`basename $pbrt .pbrt`
    rm -rf "$OUT_DIR/$name"
    mkdir -p "$OUT_DIR/$name"
    "$PBRT_BIN" --nthreads=1 --proxydir "$OUT_DIR" --nomaterial --dumpscene "$OUT_DIR/$name" "$pbrt"
done

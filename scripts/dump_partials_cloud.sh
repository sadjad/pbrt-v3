#!/bin/bash

set -e
trap "exit" INT TERM ERR
trap "kill 0" EXIT

if [ "$#" -ne 4 ]; then
    echo "$0 PBRT_BIN INSTANCE_DIR CHUNK_DIR OUT_DIR"
    exit 1
fi

PBRT_BIN=`readlink -f $1`
INSTANCE_DIR=`readlink -f $2`
CHUNK_DIR=`readlink -f $3`
OUT_DIR=`readlink -f $4`

# Preparation

if [ -d "${OUT_DIR}" ] && [ -n "$(ls -A "${OUT_DIR}")" ]
then
  echo "Error: ${OUT_DIR} is not empty."
  exit 1
fi

echo -n "> Creating ${OUT_DIR}... "
mkdir -p "${OUT_DIR}"
echo "done."

cd "${OUT_DIR}"
gg init

# Stage I (dumping instances)

## I.1 Generating thunks

gg_pipe=$(mktemp -u)
mkfifo "${gg_pipe}" # safe; this would fail if a file with this name exists
gg repl <"${gg_pipe}" &

# add the basic entities
cat >>"${gg_pipe}" <<EOT
push executable ${PBRT_BIN}
push arg pbrt
push arg --nthreads=1
push arg --nomaterial
push arg --dumpscene
push arg .
push arg input.pbrt
push output T0
push output HEADER
push output MANIFEST
push output MAT0
push output MAT1
EOT

target_thunks="${OUT_DIR}/targets.gg"

for pbrt_file in $(ls "${INSTANCE_DIR}")
do
  name="`basename ${pbrt_file} .pbrt`"
  output_dir="${OUT_DIR}/${name}"
  mkdir -p "${output_dir}"

  {
    echo "push data ${INSTANCE_DIR}/${pbrt_file}"
    echo "push link input.pbrt ${INSTANCE_DIR}/${pbrt_file}"
    echo "create thunk"
    echo "create placeholder T0 ${output_dir}/T0"
    echo "create placeholder HEADER ${output_dir}/HEADER"
    echo "create placeholder MANIFEST ${output_dir}/MANIFEST"
    echo "create placeholder MAT0 ${output_dir}/MAT0"
    echo "create placeholder MAT1 ${output_dir}/MAT1"
    echo "pop data"
    echo "pop link"
  } >>"${gg_pipe}"

  {
    echo "${output_dir}/T0"
    echo "${output_dir}/HEADER"
    echo "${output_dir}/MANIFEST"
    echo "${output_dir}/MAT0"
    echo "${output_dir}/MAT1"
  } >>"${target_thunks}"
done

wait # wait for gg repl to die

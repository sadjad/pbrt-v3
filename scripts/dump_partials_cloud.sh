#!/bin/bash

set -e
trap "exit" INT TERM ERR
trap "kill 0" EXIT

if [ "$#" -ne 5 ]; then
    echo "Usage: $0 ACTION PBRT_BIN INSTANCE_DIR CHUNK_DIR OUT_DIR"
    echo "  Valid actions: prepare-instances"
    echo "                 prepare-chunks"
    exit 1
fi

ACTION="$1"
PBRT_BIN=`readlink -f $2`
INSTANCE_DIR=`readlink -f $3`
CHUNK_DIR=`readlink -f $4`
OUT_DIR=`readlink -f $5`

INSTANCE_TARGETS="${OUT_DIR}/instance-targets.gg"
CHUNK_TARGETS="${OUT_DIR}/chunk-targets.gg"

echo -n "> Creating ${OUT_DIR}... "
mkdir -p "${OUT_DIR}"
echo "done."

cd "${OUT_DIR}"
gg init

function prepare_instances {
  gg_pipe=$(mktemp -u)
  mkfifo "${gg_pipe}"
  gg repl <"${gg_pipe}" &

  exec 3>"${gg_pipe}"

  cat >&3 <<EOT
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

  : >"${INSTANCE_TARGETS}"

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
    } >&3

    {
      echo "${output_dir}/T0"
      echo "${output_dir}/HEADER"
      echo "${output_dir}/MANIFEST"
      echo "${output_dir}/MAT0"
      echo "${output_dir}/MAT1"
    } >>"${INSTANCE_TARGETS}"
  done

  echo "exit" >>"${gg_pipe}"
  exec 3>&-
  wait # wait for gg repl to die
}

function prepare_chunks {
  instance_archive="instance-headers.tar"
  find . -type f -name HEADER | tar cf ${instance_archive} -T -

  entry_program=$(mktemp)
  cat >>"${entry_program}" <<EOT
#!/bin/sh

mkdir -p proxies
/bin/tar -C proxies -xf ${instance_archive}
./pbrt --nthreads=1 --nomaterial --proxydir proxies --dumpscene . input.pbrt
rm -rf proxies
EOT

  gg_pipe=$(mktemp -u)
  mkfifo "${gg_pipe}"
  gg repl <"${gg_pipe}" &

  exec 3>"${gg_pipe}"

  # add the basic entities
  cat >&3 <<EOT
push executable ${entry_program}
push executable ${PBRT_BIN}
push arg entry-program
push output T0
push output HEADER
push output MANIFEST
push output LIGHTS
push output MAT0
push output MAT1
push data ${instance_archive}
push link ${instance_archive} ${instance_archive}
push link pbrt ${PBRT_BIN}
EOT

  : >"${CHUNK_TARGETS}"

  for pbrt_file in $(ls "${CHUNK_DIR}")
  do
    name="`basename ${pbrt_file} .pbrt`"
    output_dir="${OUT_DIR}/${name}"
    mkdir -p "${output_dir}"

    {
      echo "push data ${CHUNK_DIR}/${pbrt_file}"
      echo "push link input.pbrt ${CHUNK_DIR}/${pbrt_file}"
      echo "create thunk"
      echo "create placeholder T0 ${output_dir}/T0"
      echo "create placeholder HEADER ${output_dir}/HEADER"
      echo "create placeholder MANIFEST ${output_dir}/MANIFEST"
      echo "create placeholder LIGHTS ${output_dir}/LIGHTS"
      echo "create placeholder MAT0 ${output_dir}/MAT0"
      echo "create placeholder MAT1 ${output_dir}/MAT1"
      echo "pop data"
      echo "pop link"
    } >&3

    {
      echo "${output_dir}/T0"
      echo "${output_dir}/HEADER"
      echo "${output_dir}/MANIFEST"
      echo "${output_dir}/LIGHTS"
      echo "${output_dir}/MAT0"
      echo "${output_dir}/MAT1"
    } >>${CHUNK_TARGETS}
  done

  echo "exit" >>"${gg_pipe}"
  exec 3>&-
  wait # wait for gg repl to die
}

case $ACTION in

  prepare-instances)
    prepare_instances
    ;;

  prepare-chunks)
    prepare_chunks
    ;;

  *)
    echo "Invalid action: $ACTION"
    exit 1
    ;;

esac

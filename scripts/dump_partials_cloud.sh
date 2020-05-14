#!/bin/bash

set -e
trap "exit" INT TERM ERR
trap "kill 0" EXIT

if [ "$#" -lt 5 ]; then
    echo "Usage: $0 ACTION PBRT_BIN INSTANCE_DIR CHUNK_DIR OUT_DIR [EXTRA-FILE]..."
    echo "  Valid actions: prepare-instances"
    echo "                 prepare-chunks"
    exit 1
fi

ACTION="$1"; shift
PBRT_BIN=`readlink -f $1`; shift
INSTANCE_DIR=`readlink -f $1`; shift
CHUNK_DIR=`readlink -f $1`; shift
OUT_DIR=`readlink -f $1`; shift

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

  while (( "$#" ))
  do
    cat >&3 <<EOT
push data $1
push link $(basename $1) $1
EOT
    shift
  done

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

    cat >&3 <<EOT
push data ${INSTANCE_DIR}/${pbrt_file}
push link input.pbrt ${INSTANCE_DIR}/${pbrt_file}
create thunk
create placeholder T0 ${output_dir}/T0
create placeholder HEADER ${output_dir}/HEADER
create placeholder MANIFEST ${output_dir}/MANIFEST
create placeholder MAT0 ${output_dir}/MAT0
create placeholder MAT1 ${output_dir}/MAT1
pop data
pop link
EOT

    cat >>"${INSTANCE_TARGETS}" <<EOT
${output_dir}/T0
${output_dir}/HEADER
${output_dir}/MANIFEST
${output_dir}/MAT0
${output_dir}/MAT1
EOT
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

  while (( "$#" ))
  do
    cat >&3 <<EOT
push data $1
push link $(basename $1) $1
EOT
    shift
  done

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

    cat >&3 <<EOT
push data ${CHUNK_DIR}/${pbrt_file}
push link input.pbrt ${CHUNK_DIR}/${pbrt_file}
create thunk
create placeholder T0 ${output_dir}/T0
create placeholder HEADER ${output_dir}/HEADER
create placeholder MANIFEST ${output_dir}/MANIFEST
create placeholder LIGHTS ${output_dir}/LIGHTS
create placeholder MAT0 ${output_dir}/MAT0
create placeholder MAT1 ${output_dir}/MAT1
pop data
pop link
EOT

    cat >>${CHUNK_TARGETS} <<EOT
${output_dir}/T0
${output_dir}/HEADER
${output_dir}/MANIFEST
${output_dir}/LIGHTS
${output_dir}/MAT0
${output_dir}/MAT1
EOT
  done

  echo "exit" >>"${gg_pipe}"
  exec 3>&-
  wait # wait for gg repl to die
}

case $ACTION in

  prepare-instances)
    prepare_instances "$@"
    ;;

  prepare-chunks)
    prepare_chunks "$@"
    ;;

  *)
    echo "Invalid action: $ACTION"
    exit 1
    ;;

esac

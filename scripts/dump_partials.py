#!/usr/bin/env python3

import os
import sys
import typing
import tempfile
import dataclasses
import subprocess as sub
import multiprocessing as mp

from textwrap import dedent

CORE_COUNT = mp.cpu_count()
INSTANCES_ARCHIVE = "instance-headers.tar"
ENTRY_PROGRAM = "entry-program.sh"


def chunks(lst, n):
    # Yield successive n-sized chunks from lst.
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


@dataclasses.dataclass
class Config:
    prepare_chunks: bool = False
    pbrt_bin: str = ""
    instance_dir: str = ""
    chunk_dir: str = ""
    out_dir: str = ""
    extra_files: typing.List[str] = dataclasses.field(default_factory=list)


def prepare_worker(config, file_list):
    print("[generator start]")
    primary_targets = []
    secondary_targets = []

    with sub.Popen(["gg", "repl"], stdin=sub.PIPE, stdout=sub.DEVNULL, text=True) as proc:
        for f in config.extra_files:
            proc.stdin.write(
                f"push data {f}\n"
                f"push link {os.path.basename(f)} {f}\n")

        if config.prepare_chunks:
            proc.stdin.write(dedent(
                f"""\
                push executable {ENTRY_PROGRAM}
                push data {INSTANCES_ARCHIVE}
                push link {INSTANCES_ARCHIVE} {INSTANCES_ARCHIVE}
                push link pbrt {config.pbrt_bin}
                push arg entry-program
                """))
        else:
            proc.stdin.write(dedent(
                f"""\
                push arg pbrt
                push arg --nthreads=1
                push arg --nomaterial
                push arg --dumpscene
                push arg .
                push arg input.pbrt
                """))

        proc.stdin.write(dedent(
            f"""\
            push executable {config.pbrt_bin}
            push output T0
            push output HEADER
            push output MANIFEST
            push output MAT0
            push output MAT1
            """))

        if config.prepare_chunks:
            proc.stdin.write("push output LIGHTS\n")

        for f in file_list:
            name = os.path.basename(os.path.splitext(f)[0])
            f_dir = os.path.join(config.out_dir, name)
            os.makedirs(f_dir, exist_ok=True)

            proc.stdin.write(dedent(
                f"""\
                push data {f}
                push link input.pbrt {f}
                create thunk
                create placeholder T0 {f_dir}/T0
                create placeholder HEADER {f_dir}/HEADER
                create placeholder MANIFEST {f_dir}/MANIFEST
                create placeholder MAT0 {f_dir}/MAT0
                create placeholder MAT1 {f_dir}/MAT1
                pop data
                pop link
                """))

            primary_targets += [os.path.join(f_dir, "T0")]
            secondary_targets += [os.path.join(f_dir, x)
                                  for x in ["HEADER", "MANIFEST", "MAT0", "MAT1"]]

            if config.prepare_chunks:
                proc.stdin.write(f"create placeholder LIGHTS {f_dir}/LIGHTS\n")
                secondary_targets += [os.path.join(f_dir, "LIGHTS")]

        proc.communicate("exit\n")

    print("[generator end]")
    return primary_targets, secondary_targets


def prepare(config: Config):
    if config.prepare_chunks:
        sub.check_call(
            f"find . -type f -name HEADER | tar cf {INSTANCES_ARCHIVE} -T -", shell=True)

        with open(ENTRY_PROGRAM, "w") as fout:
            fout.write(dedent(
                f"""\
                #!/bin/sh

                mkdir -p proxies
                /bin/tar -C proxies -xf {INSTANCES_ARCHIVE}
                ./pbrt --nthreads=1 --nomaterial --proxydir proxies --dumpscene . input.pbrt
                rm -rf proxies
                """
            ))

    source_dir = config.chunk_dir if config.prepare_chunks else config.instance_dir
    all_files = [os.path.join(source_dir, x) for x in os.listdir(source_dir)]

    with mp.Pool(processes=CORE_COUNT * 2) as pool:
        targets = pool.starmap(prepare_worker,
                               [(config, x) for x in chunks(all_files, CORE_COUNT * 2)])

    with open("chunk-targets.gg" if config.prepare_chunks else "instance-targets.gg", "w") as out_secondary, \
            open("chunks.gg" if config.prepare_chunks else "instances.gg", "w") as out_primary:
        for target in targets:
            for t in target[0]:
                print(t, file=out_primary)

            for t in target[1]:
                print(t, file=out_secondary)


if __name__ == "__main__":
    if len(sys.argv) < 6 or sys.argv[1] not in ["prepare-chunks", "prepare-instances"]:
        print(
            f"Usage: {sys.argv[0]} ACTION PBRT_BIN INSTANCE_DIR CHUNK_DIR OUT_DIR [EXTRA_FILES]...")
        sys.exit(1)

    config = Config()
    config.prepare_chunks = (sys.argv[1] == "prepare-chunks")
    config.pbrt_bin = os.path.abspath(sys.argv[2])
    config.instance_dir = os.path.abspath(sys.argv[3])
    config.chunk_dir = os.path.abspath(sys.argv[4])
    config.out_dir = os.path.abspath(sys.argv[5])
    config.extra_files = [os.path.abspath(x) for x in sys.argv[6:]]

    print(f"Creating {config.out_dir}... ", end="")
    os.makedirs(config.out_dir, exist_ok=True)
    print(f"done.")

    os.chdir(config.out_dir)
    sub.check_call(["gg", "init"])

    prepare(config)

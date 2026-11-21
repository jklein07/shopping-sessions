# Hand-over: continuing Shopping Sessions on another machine

The goal is the fastest correct run on the Databricks Shopping Sessions track. The practice board times about
20M events; the final runs about 60M events, with a 3-minute cap per run. Submissions close **Sun Nov 22, 11:00 AM ET**.
You get 2 submissions per hour on the track page
(https://www.joinbuilders.org/buildathon/tracks/shopping-sessions; team "fortnite pirate token").
Keep every submitted commit in this public repo.

## Setup (Windows 11 + WSL2, or any Linux x86 box)

The setup needs Docker (Docker Desktop with the WSL2 backend), Git and Python 3. Work inside WSL2 (Ubuntu), on the Linux
filesystem (`~/`), not `/mnt/c`: reading CSVs through `/mnt/c` is very slow.

```sh
sudo apt-get update && sudo apt-get install -y build-essential curl git python3-venv unzip
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && . ~/.cargo/env
git clone https://github.com/jklein07/shopping-sessions && cd shopping-sessions
python3 -m venv ~/venv && ~/venv/bin/pip install polars==1.35.2 pyarrow duckdb==1.5.6
# track files (public URLs): rules, starter (has the 36 fixtures), dev answer, generator
mkdir -p ~/dl && cd ~/dl
for f in rules.md starter-duckdb.zip dev-answer.zip generator.zip; do
  curl -sSLO https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/$f; done
for z in *.zip; do mkdir -p ${z%.zip} && unzip -q -o $z -d ${z%.zip}; done
cd ~/dl/generator/generator && cargo build --release && cp target/release/generate ~/generate
```

## Data

```sh
~/generate --seed 1 --events 1000000  --clicks 250000   --output ~/data/dev   # public dev set (dev-answer.zip)
~/generate --seed 7 --events 20000000 --clicks 5000000  --output ~/data/d20   # ~1.8 GB, practice size
~/generate --seed 9 --events 60000000 --clicks 15000000 --output ~/data/d60   # ~5.4 GB, final size
```

## Build, check, time

```sh
cargo build --release && cargo test --release
FX=~/dl/starter-duckdb/starter-duckdb/fixtures
PY=~/venv/bin/python tools/fixtures.sh $FX target/release/ss            # expect 36 passed
DATA_DIR=~/data/dev OUTPUT_DIR=/tmp/o target/release/ss && ~/venv/bin/python tools/compare.py /tmp/o ~/dl/dev-answer/dev-answer
rm -rf /tmp/o20 && NCPU=8 DATA_DIR=~/data/d20 OUTPUT_DIR=/tmp/o20 target/release/ss   # prints wall ms and cpu-ms per phase
```

`reference/reference.py` is an independent, slow Python implementation of rules.md. Run it on small generated
sets (any seed, at most 2M events) and compare it with `tools/compare.py` before any change that touches the logic.

The scorer-like run: `docker build -t ss . && CPUS=8 MEM=24g tools/docker_run.sh ss ~/data/d20 /tmp/o20d linux/amd64`

### Approximating the scorer (8 vCPUs = 4 Xeon cores with hyperthreading, AVX-512)

On an i9-13900HX the logical CPUs 0–15 are usually the 8 P-cores × 2 threads. For runs closest to the scorer, pin to
4 P-cores / 8 threads: `taskset -c 0-7 env NCPU=8 ...`, or `docker run --cpuset-cpus 0-7 ...`. Under WSL2 these are
VM vCPUs, and Windows decides which physical cores back them, so keep the laptop on AC power with the
best-performance power mode. Compare versions A/B, alternating several runs; absolute times will not match the scorer.

## Where things stand (2026-11-20, 9:10 PM ET)

| Commit | Change | Board (≈20M events) |
| --- | --- | --- |
| b2ff277 | first correct version | 1.86 s |
| ea40960 | SIMD CSV scan, SWAR timestamps, 7-bit ID keys | 1.66 s |
| b0904ad … e45477e | prefetched link lookups, 128-byte splitter, table dates, early unmap | not submitted |
| a592edd | + pages released in parallel before exit, no memcmp, SWAR product IDs, mask-based quoted-line split | 1.44 s |
| b0fb546, d4ce5ee | faster Parquet writing; links read ahead first | not submitted |
| b0d2093 | + SIMD date-time parse (NEON / SSSE3) | submitted 9:08 PM |

The leader was at 1.17 s at 8:10 PM.

**What the board numbers say.** M4 cycle counts for the whole 20M run (`/usr/bin/time -l`, same conditions):
b2ff277 ≈ 21 G, ea40960 ≈ 17 G, a592edd ≈ 10.9 G. Board times fall much less than the cycles do, so a large part
of each board time does not depend on our work: a straight-line fit gives about 0.9–1.05 s fixed (container start,
reading the data, exit) plus about 0.04 s per G cycles. Compute is now roughly 0.45 s of the 1.44 s. Machines vary by up
to 20%, so treat this as a rough guide. At 60M events in the final the compute share is about three times larger, so
CPU work still matters most there.

Measuring on the shared Mac: wall time is useless while other sessions run (load 50–140). Compare builds with
`/usr/bin/time -l` (instructions retired and cycles elapsed) and with `ss bench FILE` (single thread, per line). Per
event line on one M4 core (b0d2093): split 10 ns, both timestamps 10.5 ns, handler total about 32 ns. In the full run,
link-table lookups add about 10 ns per line (DRAM).

Linux profiling: the `ssperf` image (rust:1.91-slim-bookworm + linux-perf) runs `perf record` in the Colima VM with
`--cap-add PERFMON --cap-add SYS_ADMIN --security-opt seccomp=unconfined`. The 20M data set is in the docker volume
`ssd20` (VM-local disk; host bind mounts are slow). Scratch builds go in the `ssprof` volume.

Correctness gates (run all before every submission): `cargo test --release` (2M random timestamps against a slow
parser, 3M SIMD vs SWAR prefixes, 3.9M random CSV lines fast vs scalar splitter, 1M ID round trips, 2M dense-ID
lookups), the 36 fixtures, dev-answer, and 14 cross-check datasets (seeds 2–13, 15, 21, 272, from 1k to 3M events, two
of them spanning Feb 29 2028) against `reference/reference.py`. Also run with `SS_INTERVAL_CLICKS=0`, the x86 build
(`x86_64-apple-darwin`, x86-64-v3, under Rosetta), and the Linux arm64 build in a container
(`shopping-sessions-work/linux_check.sh`). `cargo check --target x86_64-unknown-linux-gnu` in a container catches
compile errors on the scorer's target.

Tried and dropped: MAP_POPULATE on the inputs (more time in open than saved in parse), glibc
`glibc.malloc.hugetlb=1` (thread arenas get no huge pages), a branch-free scalar timestamp parser (no faster on M4), a
fingerprinted dedup table (no faster).

## Next ideas, roughly by expected gain

1. Parse is about 55% of CPU. Per line: split 10 ns, timestamps 10.5 ns (F3 epoch values still SWAR), link lookup about
   10 ns of DRAM latency in the full run. Ideas: SIMD F3; a bitmap filter so unlinked users (about 38% of events) skip the
   link table; a compacted link table after naming.
2. Sessions (0.55 s CPU): per-person sorting is about a third. Idea: one counting pass by coarse time before the
   person scatter, so the per-person insertion sorts see nearly sorted input.
3. Huge pages: our own per-thread arenas with MADV_HUGEPAGE for the partition buffers (fewer faults, fewer TLB misses in
   the 1024-way scatter). Only helps if the scorer has THP enabled.
4. Dedup (0.36 s CPU): prefetch the hash probes.
